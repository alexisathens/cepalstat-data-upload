# ---- Project libraries ----

library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
library(here)
library(assertthat)
library(quarto)
library(memoise)
library(beepr)
library(pdftools)
library(CepalStatR)
library(FAOSTAT)
library(fishstat)
library(usethis)

# Settings updated by hand (max years by source, yearbook file, AI model)
source(here("Scripts/config.R"))

# ---- Global variables ----

iso <- read_xlsx(here("Data/Tables/iso_codes.xlsx"))  # built by build_tables.R

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

# Last full calendar year (current year minus 1). Used as the cutoff for sources whose current-year
# data is incomplete, such as EM-DAT disasters.
last_complete_year <- as.integer(format(Sys.Date(), "%Y")) - 1L

# FAOSTAT bulk files: downloaded to Data/Raw/fao/ by download_fao.R and read by process_fao.R.
# Each name is the object the data is loaded into in process_fao.R.
fao_bulk_files <- c(
  use   = "Inputs_LandUse_E_All_Data_(Normalized).zip",                 # land use (RL)
  clim  = "Environment_Temperature_change_E_All_Data_(Normalized).zip", # temperature change (ET)
  cover = "Environment_LandCover_E_All_Data_(Normalized).zip",          # land cover (LC)
  crop  = "Production_Crops_Livestock_E_All_Data_(Normalized).zip",     # crops and livestock products (QCL)
  fert  = "Inputs_FertilizersNutrient_E_All_Data_(Normalized).zip",     # fertilizers by nutrient (RFN)
  pest  = "Inputs_Pesticides_Use_E_All_Data_(Normalized).zip"           # pesticide use (RP)
)


# ---- Small helpers ----

# Helper: take sum, but retain NA if every value in the group is NA (rather than summing to 0)
# Sample usage: summarize(value = sum_or_na(as.numeric(value)))
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

# Helper: create a local folder (and any parent folders) if it doesn't exist yet, then return its path.
# Data/Raw and other data folders are not tracked by git, so they are missing on a fresh copy of the repository.
# Sample usage: input_path <- ensure_dir(here("Data/Raw/olade"))
ensure_dir <- function(path) {
  if (!dir.exists(path)) {
    dir.create(path, recursive = TRUE)
    message(glue("📁 Created missing folder: {path}"))
  }
  invisible(path)
}

# Helper: stop with clear instructions if any raw data file that must be obtained separately is missing.
# `how` explains how to get the file(s); by default it points to the source's section in instructions_other.qmd.
# Returns the path(s) unchanged, so it can wrap a read call directly.
# Sample usage: read_xlsx(check_raw_file(here("Data/Raw/other/irena_raw.xlsx"), source = "IRENA"))
check_raw_file <- function(paths, source,
                           how = glue("Download it manually following the steps under '{source}' in Scripts/instructions_other.qmd.")) {
  missing_files <- paths[!file.exists(paths)]
  if (length(missing_files) > 0) {
    stop(glue(
      "❌ Missing raw data for {source}. Expected file(s):\n",
      "{paste0('   - ', missing_files, collapse = '\n')}\n",
      "   {how}\n",
      "   Save each file with exactly the name and location shown above, then re-run the script."
    ), call. = FALSE)
  }
  invisible(paths)
}

# Helper: fetch and parse a JSON response from a CEPALSTAT API endpoint
# Sample usage: fetch_cepalstat_json(glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/metadata?lang=en&format=json"))
fetch_cepalstat_json <- function(url) {
  request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
}


# ---- Assertions ----

# Stop code if any column (besides value, by default) has NA values that didn't match; else return data unchanged
# Sample usage: assert_no_na_cols(i2486f, cols = ends_with("_id"))
assert_no_na_cols <- function(data, cols = everything(), data_name = deparse(substitute(data))) {
  na_summary <- data %>%
    select({{ cols }}) %>%
    summarise(across(everything(), ~ sum(is.na(.)))) %>%
    pivot_longer(everything(), names_to = "column", values_to = "na_count") %>%
    filter(na_count > 0)

  if (nrow(na_summary) > 0) {
    msg <- paste0(
      "❌ NA values found in ", data_name, ":\n",
      paste0(" - ", na_summary$column, ": ", na_summary$na_count, " NA(s)", collapse = "\n")
    )
    stop(msg)
  }

  data #invisible(TRUE)
}

# Stop code if there are duplicate rows across all key columns (all columns except 'value'); else return data unchanged
# Sample usage: assert_no_duplicates(df)
assert_no_duplicates <- function(data, data_name = deparse(substitute(data))) {

  # Check for duplicates
  n_keys <- data %>%
    select(all_of(setdiff(names(data), "value"))) %>%
    distinct() %>%
    nrow()

  n_rows <- nrow(data)

  if (n_keys != n_rows) {
    msg <- paste0(
      "❌ Duplicate keys found in ", data_name, ":\n",
      " - Expected ", n_keys, " unique rows\n",
      " - Found ", n_rows, " rows\n",
      " - Duplicates: ", n_rows - n_keys, "\n",
      "Common causes: transform_data() didn't summarise across a category that isn't in dim_config, ",
      "or two source names were standardized to the same country name."
    )
    stop(msg)
  }

  data #invisible(TRUE)
}

# Validate cleaned data before regional aggregation: no NA values in 'value', no unexpected columns beyond
# dim_config + value/num/denom, and drop years beyond max_year. Stops on failure, else returns data.
# Sample usage: df %>% assert_data_reqs(dim_config_4046, indicator_id = 4046, max_year = last_complete_year)
assert_data_reqs <- function(data, dim_config, indicator_id, max_year = NULL) {

  # Years: stored as whole numbers (integers) during processing; get_cepalstat_ids() converts them to
  # text later, to match CEPALSTAT's labels. Skipped for indicators without a Years column (e.g. 2031,
  # where the value itself is a year).
  if ("Years" %in% names(data)) {
    years <- suppressWarnings(as.integer(as.character(data$Years)))
    invalid <- unique(data$Years[is.na(years) | years < 1900 | years > 2100])
    if (length(invalid) > 0) {
      stop(glue(
        "❌ The Years column has values that aren't valid years: {paste(head(invalid, 5), collapse = ', ')}. ",
        "Check how Years is created in filter_data() or transform_data()."
      ))
    }
    data$Years <- years

    # Optional cutoff, for sources whose latest year is incomplete (set as max_year in the spec)
    if (!is.null(max_year)) data <- data %>% filter(Years <= max_year)

    data <- data %>% arrange(Country, Years)
  }
  
  # check for NA values
  na_vals <- data %>%
    filter(is.na(value))

  if (nrow(na_vals) > 0) {
    na_examples <- na_vals %>%
      select(-value) %>%
      head(5) %>%
      pmap_chr(function(...) paste(c(...), collapse = " | "))
    msg <- glue(
      "❌ {nrow(na_vals)} NA values found in 'value'. First rows affected:\n",
      "{paste0('   - ', na_examples, collapse = '\n')}\n",
      "Drop them in filter_data() or transform_data() (e.g. filter(!is.na(value))), ",
      "or check that a join or calculation in transform_data() isn't creating them."
    )
    stop(msg)
  }

  # check for extra columns
  acceptable_cols <- c(dim_config$data_col, "value")
  extra_cols <- setdiff(names(data), acceptable_cols)

  if(!is_empty(extra_cols)) {
    msg <- paste0(
      "❌ extra columns found: ",
      paste0(extra_cols, collapse = ", "), "\n",
      "Only the columns listed in dim_config$data_col (", paste0(dim_config$data_col, collapse = ", "),
      ") plus 'value' are allowed. Drop or rename the extra columns in transform_data(), ",
      "or add them to dim_config if they are a real dimension."
    )
    stop(msg)
  }

  return(data)
}


# ---- CEPALSTAT API lookups ----

# Get an indicator's dimensions (English + Spanish names) — used when building a new indicator's dim_config
# Sample usage: get_indicator_dimensions(4046)
get_indicator_dimensions <- function(indicator_id) {
  # English
  url_en <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=en&format=json&in=1&path=0")
  result_en <- fetch_cepalstat_json(url_en)
  dims_en <- result_en %>%
    pluck("body", "dimensions") %>%
    as_tibble() %>%
    select(name, id)

  # Spanish
  url_es <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=es&format=json&in=1&path=0")
  result_es <- fetch_cepalstat_json(url_es)
  dims_es <- result_es %>%
    pluck("body", "dimensions") %>%
    as_tibble() %>%
    select(name, id)

  # Join Spanish names as 'name_es'
  dims <- dims_en %>%
    left_join(dims_es %>% select(id, name_es = name), by = "id")

  return(dims)
}

# Get an indicator's currently published metadata fields (definition, methodology, etc.)
# Sample usage: get_indicator_metadata(3881, lang = "en")
get_indicator_metadata <- function(indicator_id, lang = "en") {
  ## Get footnotes_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/metadata?lang={lang}&format=json")

  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)

  metadata_tbl <- result$body$metadata %>%
    enframe(name = "variable", value = "value") %>%
    mutate(value = map_chr(value, as.character))

  return(metadata_tbl)
}

# Get an indicator's currently assigned source(s) from CEPALSTAT
# Sample usage: get_indicator_source(4046) %>% slice(1) %>% pull(id)
get_indicator_source <- function(indicator_id, lang = "en") {
  # Get sources from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/sources?lang={lang}&format=json")

  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)

  sources_tbl <- result$body$sources %>%
    as_tibble()

  return(sources_tbl)
}

# Get an indicator's currently published footnotes from CEPALSTAT. This returns the full set of
# footnotes used anywhere across the indicator's published data, not tied to specific rows.
# Sample usage: get_indicator_footnotes(2019)$id
get_indicator_footnotes <- function(indicator_id, lang = "en") {
  # Get footnotes from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/footnotes?lang={lang}&format=json")

  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)

  footnotes_tbl <- result$body$footnotes %>%
    as_tibble()

  return(footnotes_tbl)
}

# Get the full member table (English + Spanish names) for a single CEPALSTAT dimension
# Sample usage: get_dimension_table(208)  # 208 = Country dimension
get_dimension_table <- function(dimension_id) {
  # English
  url_en <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/dimensions/{dimension_id}?lang=en")
  result_en <- fetch_cepalstat_json(url_en)
  dim_info_en <- result_en %>%
    pluck("body", "dimensions") %>%
    as_tibble()
  members_en <- dim_info_en$members[[1]] %>% as_tibble()
  members_en <- members_en %>%
    mutate(dim_id = dim_info_en$id[1],
           dim_name = dim_info_en$name[1])

  # Spanish
  url_es <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/dimensions/{dimension_id}?lang=es")
  result_es <- fetch_cepalstat_json(url_es)
  dim_info_es <- result_es %>%
    pluck("body", "dimensions") %>%
    as_tibble()
  members_es <- dim_info_es$members[[1]] %>% as_tibble()
  members_es <- members_es %>%
    mutate(dim_id = dim_info_es$id[1],
           dim_name = dim_info_es$name[1])

  # Join Spanish names as '*_es'
  members <- members_en %>%
    left_join(members_es %>% select(id, name_es = name, dim_name_es = dim_name), by = "id")

  return(members)
}
# Country/Years dimension tables get re-fetched identically across nearly every indicator in a bulk
# run; caching avoids redundant API calls. In-memory only (cleared on session restart) — that's fine
# here since CEPALSTAT's dimension/source data won't change mid-session.
get_dimension_table  <- memoise(get_dimension_table)

# Fetch the currently published CEPALSTAT data for an indicator (dim_* columns + value only)
# Sample usage: get_cepalstat_data(4046)
get_cepalstat_data <- function(indicator_id) {
  # Build URL
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/data?lang=en&format=json")

  # Perform the request and parse JSON
  result <- request(url) %>%
    req_perform() %>%
    resp_body_json(simplifyDataFrame = TRUE)

  # Extract and flatten the data portion
  pub <- result$body$data %>%
    as_tibble()
  
  # Stop with instructions if CEPALSTAT has no published data for this indicator,
  # which usually means it's a new indicator whose spec is missing the new-indicator settings
  if (nrow(pub) == 0 || !"value" %in% names(pub)) {
    stop(glue(
      "❌ No published data found in CEPALSTAT for indicator {indicator_id}.\n",
      "If this is a new indicator, add these two fields to spec_{indicator_id}:\n",
      "  define_source = <your source function>, # e.g. define_source_aquastat (see Docs/new_indicator_guide.md)\n",
      "  new_indicator = TRUE"
    ))
  }

  # Force dim_* columns to characters
  pub %<>%
    mutate(across(starts_with("dim_"), as.character))

  # Keep dimensions and value only
  pub %<>%
    select(starts_with("dim_"), value)

  return(pub)
}

# Join CEPALSTAT dimension ids onto cleaned indicator data, using a dim_config lookup table
# Sample usage: get_cepalstat_ids(df, dim_config_4046)
get_cepalstat_ids <- function(df, dim_config) {

  df_l <- df # create new df with labels

  for(i in 1:nrow(dim_config)) {
    this_data_col <- dim_config$data_col[i]
    this_dim_id <- dim_config$dim_id[i]
    this_pub_col <- dim_config$pub_col[i]
    this_dim_col <- paste0("dim_", this_dim_id)

    this_dim_table <- get_dimension_table(this_dim_id)
    this_name_col <- ifelse(str_detect(this_pub_col, "es"), "name_es", "name") # get label in spanish or english

    this_dim_table %<>%
      select(id, all_of(this_name_col)) %>%
      rename(!!this_dim_col := id,
             name = !!sym(this_name_col))

    # CEPALSTAT labels are text (e.g. Years "2020"), so the column is converted to text to match
    df_l[[this_data_col]] <- as.character(df_l[[this_data_col]])

    df_l %<>%
      left_join(this_dim_table, by = setNames("name", this_data_col))

    # Stop with the unmatched labels, rather than a generic NA error later on
    unmatched <- df_l %>%
      filter(is.na(.data[[this_dim_col]])) %>%
      distinct(.data[[this_data_col]]) %>%
      pull()
    if (length(unmatched) > 0) {
      stop(glue(
        "❌ {length(unmatched)} label(s) in column '{this_data_col}' don't match CEPALSTAT dimension {this_dim_id}: ",
        "{paste(unmatched, collapse = ', ')}\n",
        "Recode them in transform_data() to the exact CEPALSTAT spelling. ",
        "See the valid labels with get_dimension_table({this_dim_id})."
      ), call. = FALSE)
    }
  }

  df_l %>%
    mutate(across(starts_with("dim_"), as.character))
}

# Join CEPALSTAT dimension labels onto published (pub) indicator data, using a dim_config lookup table
# Used inside create_comp_file() to make published data comparable to newly cleaned data
# Sample usage: get_cepalstat_labels(pub, dim_config_4046)
get_cepalstat_labels <- function(pub, dim_config) {

  pub_l <- pub # create new df with labels

  for(i in 1:nrow(dim_config)) {
    this_data_col <- dim_config$data_col[i]
    this_dim_id <- dim_config$dim_id[i]
    this_pub_col <- dim_config$pub_col[i]
    this_dim_col <- paste0("dim_", this_dim_id)

    this_dim_table <- get_dimension_table(this_dim_id)
    this_name_col <- ifelse(str_detect(this_pub_col, "es"), "name_es", "name") # get label in spanish or english

    this_dim_table %<>%
      select(id, all_of(this_name_col)) %>%
      rename(!!this_dim_col := id,
             !!sym(this_data_col) := name) %>%
      mutate(across(starts_with("dim_"), as.character))

    pub_l %<>%
      left_join(this_dim_table, by = this_dim_col)
  }

  pub_l %>%
    mutate(across(starts_with("dim_"), as.character)) %>%
    relocate(c("value", starts_with("dim")), .after = last_col())
}


# ---- CEPALSTAT bulk downloads (used by build_tables.R) ----

CEPALSTAT_API_URL <- "https://api-cepalstat.cepal.org/cepalstat/api/v1"

# Walks one node of the CEPALSTAT thematic tree; returns one row per indicator beneath it, with the
# categories above each indicator as a path (e.g. "Environmental > Environmental conditions > ...").
flatten_tree <- function(node, path = character()) {
  if (!is.null(node$indicator_id)) {
    return(tibble(id = node$indicator_id, indicator = node$name,
                  path = paste(path, collapse = " > ")))
  }
  map_dfr(node$children, flatten_tree, path = c(path, node$name))
}

# Pulls the environmental area (area_id = 493) of the CEPALSTAT thematic tree in one language
# and returns one row per indicator, with the categories above it as a path.
# Sample usage: get_env_indicators("es")
get_env_indicators <- function(lang) {
  url  <- glue("{CEPALSTAT_API_URL}/thematic-tree?lang={lang}&format=json&area_id=493")
  tree <- request(url) %>% req_perform() %>% resp_body_json()
  
  out <- tree$body$children %>% map_dfr(flatten_tree)
  assert_that(nrow(out) > 0, msg = glue("No indicators found in the thematic tree (lang = {lang})"))
  out
}

# Fetches many URLs in parallel (up to max_active at a time) and parses each response as JSON.
# Failed requests return NULL, so one failure doesn't stop the whole batch.
# Sample usage: fetch_cepalstat_json_many(c(url1, url2))
fetch_cepalstat_json_many <- function(urls, max_active = 8) {
  assert_that(
    packageVersion("httr2") >= "1.1.0",
    msg = "fetch_cepalstat_json_many() requires httr2 1.1.0 or later. Update it with install.packages(\"httr2\")."
  )
  
  reqs  <- map(urls, function(url) request(url) %>% req_retry(max_tries = 3))
  resps <- req_perform_parallel(reqs, on_error = "continue", max_active = max_active, progress = TRUE)
  
  map(resps, function(resp) {
    if (!inherits(resp, "httr2_response")) return(NULL)
    fromJSON(resp_body_string(resp), flatten = TRUE)
  })
}

# Converts an API table to a tibble of text columns, so tables from different indicators can be
# combined even when a field comes back as a different type (e.g. NA in one, text in another).
# List fields are collapsed into one string, e.g. "a; b".
as_character_tibble <- function(x) {
  if (is.null(x) || length(x) == 0) return(tibble())
  
  # Converts one column to text; list columns are collapsed into one string per row
  to_text <- function(col) {
    if (is.list(col)) {
      map_chr(col, function(item) paste(unlist(item), collapse = "; "))
    } else {
      as.character(col)
    }
  }
  
  as_tibble(x) %>%
    mutate(across(everything(), to_text))
}

# Fetches one indicator endpoint (e.g. "sources", "footnotes") for every indicator in English and
# Spanish, in parallel, and returns one row per indicator, language and item. Columns named in
# `drop` are removed before processing (e.g. large member lists). Stops if any request fails, so
# incomplete tables are never saved.
# Sample usage: fetch_indicator_tables(inds, endpoint = "footnotes")
fetch_indicator_tables <- function(inds, endpoint, field = endpoint, extra_query = "", drop = character()) {
  calls <- expand_grid(indicator_id = inds, lang = c("en", "es")) %>%
    mutate(url = glue("{CEPALSTAT_API_URL}/indicator/{indicator_id}/{endpoint}?lang={lang}&format=json{extra_query}"))
  
  calls$json <- fetch_cepalstat_json_many(calls$url)
  
  failed <- calls %>% filter(map_lgl(json, is.null))
  if (nrow(failed) > 0) {
    stop(glue(
      "{nrow(failed)} {endpoint} request(s) failed: ",
      "{paste(failed$indicator_id, failed$lang, collapse = ', ')}. Please re-run."
    ))
  }
  
  calls %>%
    mutate(items = map(json, function(j) {
      j$body[[field]] %>%
        as_tibble() %>%
        select(-any_of(drop)) %>%
        as_character_tibble()
    })) %>%
    select(indicator_id, lang, items) %>%
    unnest(items)
}

# Turns a long table (one row per item and language) into one row per item, with English columns
# unsuffixed and Spanish columns ending in _es (e.g. name, name_es). If an item appears for several
# indicators, the first version is kept.
# Sample usage: to_wide_by_lang(note_long, "note_id")
to_wide_by_lang <- function(long, id_col) {
  long %>%
    distinct(.data[[id_col]], lang, .keep_all = TRUE) %>%
    mutate(lang = if_else(lang == "en", "", "_es")) %>%
    pivot_wider(names_from = lang, values_from = -all_of(c(id_col, "lang")), names_glue = "{.value}{lang}")
}


# ---- Comparing and reviewing table updates (used by build_tables.R) ----

# Reads one sheet of a previously saved table, or returns NULL if the file doesn't exist yet
# (e.g. on the first run). Sample usage: read_previous(here("Data/Tables/source_table.xlsx"), "sources")
read_previous <- function(path, sheet = 1) {
  if (!file.exists(path)) return(NULL)
  read_xlsx(path, sheet = sheet)
}

# Compares two versions of a table with one row per item (e.g. sources) and returns one row per
# change: items added or removed, and each field whose value changed. For added and removed items,
# the label column (e.g. the name) is recorded too, so it's clear what each id was.
# Sample usage: compare_tables(old_sources, sources, key = "source_id", table = "sources", label = "name")
compare_tables <- function(old, new, key, table, label = NULL) {
  if (is.null(old)) return(tibble())

  # Compare everything as text, so 3158 and 3158.0 (as read back from Excel) count as equal.
  # Items listed more than once (e.g. an indicator in two places in the tree) are compared once.
  old <- old %>% mutate(across(everything(), as.character)) %>% distinct(across(all_of(key)), .keep_all = TRUE)
  new <- new %>% mutate(across(everything(), as.character)) %>% distinct(across(all_of(key)), .keep_all = TRUE)

  get_label <- function(df) if (is.null(label) || !label %in% names(df)) NA_character_ else df[[label]]

  added <- anti_join(new, old, by = key) %>%
    transmute(id = .data[[key]], change = "added", field = label, new_value = get_label(.))

  removed <- anti_join(old, new, by = key) %>%
    transmute(id = .data[[key]], change = "removed", field = label, old_value = get_label(.))

  # Fields present in both versions, compared item by item (a blank and a missing value count as equal)
  shared_fields <- setdiff(intersect(names(old), names(new)), key)
  changed <- inner_join(
      old %>% pivot_longer(all_of(shared_fields), names_to = "field", values_to = "old_value"),
      new %>% pivot_longer(all_of(shared_fields), names_to = "field", values_to = "new_value"),
      by = c(key, "field")
    ) %>%
    filter(coalesce(old_value, "") != coalesce(new_value, "")) %>%
    transmute(id = .data[[key]], change = "changed", field, old_value, new_value)

  bind_rows(added, removed, changed) %>%
    mutate(table = table, .before = 1)
}

# Compares two versions of a link table (e.g. indicator_id x source_id) and returns one row per
# link added or removed.
# Sample usage: compare_links(old_links, indicator_source, item_col = "source_id", table = "sources by indicator")
compare_links <- function(old, new, item_col, table) {
  if (is.null(old)) return(tibble())

  old <- old %>% select(indicator_id, all_of(item_col)) %>% mutate(across(everything(), as.character))
  new <- new %>% select(indicator_id, all_of(item_col)) %>% mutate(across(everything(), as.character))
  keys <- c("indicator_id", item_col)

  bind_rows(
    anti_join(new, old, by = keys) %>% transmute(id = indicator_id, change = "added",   field = item_col, new_value = .data[[item_col]]),
    anti_join(old, new, by = keys) %>% transmute(id = indicator_id, change = "removed", field = item_col, old_value = .data[[item_col]])
  ) %>%
    mutate(table = table, .before = 1)
}

# Summarizes compare_tables()/compare_links() output as one line per table,
# e.g. "sources: 1 added, 2 changed". Changed items are counted once, however many fields changed.
summarize_changes <- function(changes) {
  if (nrow(changes) == 0) return(character())
  changes %>%
    distinct(table, id, change) %>%
    count(table, change) %>%
    summarise(text = paste(n, change, collapse = ", "), .by = table) %>%
    mutate(line = paste0(table, ": ", text)) %>%
    pull(line)
}

# Prints the end-of-run summary. `review` and `info` are named lists: each name is a heading and
# each element a character vector of lines (empty elements are skipped). Every line is printed.
print_review_summary <- function(review, info) {
  print_section <- function(items, icon) {
    for (title in names(items)) {
      lines <- items[[title]]
      if (length(lines) == 0) next
      message(glue("{icon} {title} ({length(lines)}):"))
      message(paste0("   ", lines, collapse = "\n"))
    }
  }

  rule <- strrep("═", 70)
  message("\n", rule)
  message("  SUMMARY OF CHANGES FROM CEPALSTAT")
  message(rule)
  if (all(lengths(review) == 0) && all(lengths(info) == 0)) message("✅ No changes since the last download")
  print_section(review, "⚠️ REVIEW:")
  print_section(info, "ℹ️")
  message(rule, "\n")
}

# Asks a yes/no question in the console and returns TRUE for "y" or "yes". Returns FALSE when the
# script isn't run interactively (e.g. with Rscript), since there's no one to answer.
ask_yes_no <- function(question) {
  if (!interactive()) return(FALSE)
  answer <- readline(glue("{question} (y/n): "))
  tolower(trimws(answer)) %in% c("y", "yes")
}

# ---- Country / regional handling ----

# Standardize country names to CEPALSTAT's iso table, restrict to LAC countries, and drop subregion groupings.
# Always run for every indicator, right after transform_data().
# Sample usage: df %>% standardize_countries()
standardize_countries <- function(df, indicator_id) {
  df %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name)
  
  # allow World entry to persist if it's a climate change indicator
  ind_source <- meta %>% filter(id == indicator_id) %>% pull(source)
  if(ind_source == "CAIT - WRI") { df %<>% filter(Country %in% iso$name | Country == "World") } 
  else { df %<>% filter(Country %in% iso$name) }

  # remove subregions
  if (!identical(indicator_id, 3381)) { # exception for mean temperature change
    df %<>% filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America"))
  }

  df
}

# Default regional strategy: keep the source's own LAC total as-is (no recalculation).
# Used for all OLADE indicators, which publish their own regional aggregate (see 02_olade.R).
# Sample usage: df %>% maintain_regional()
maintain_regional <- function(df) {
  df
}

# Regional strategy: drop the source's own LAC total and recalculate it as a simple sum across countries.
# Sample usage: df %>% calculate_regional_sum() # with "value" as col name
calculate_regional_sum <- function(df) {
  # remove all LAC sub/regional totals
  df %<>%
    filter(!Country %in% c("South America", "Central America", "Caribbean",
                           "Latin America and the Caribbean", "Latin America"))

  # calculate LAC region sum
  lac_total <- df %>%
    filter(Country != "World") %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value"))))) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")

  df %>%
    bind_rows(lac_total)
}

# Regional strategy for ratio indicators (num / denom): percentages, per-capita and per-GDP rates.
# Recomputes the value for each country and for the LAC total from the summed num and denom
# (i.e., a weighted average), multiplied by `multiplier` and optionally rounded to `digits`.
#
# This is a "function factory": calling it with settings returns the function that
# process_indicator() runs later on the data. In a spec, always include the parentheses:
#   calculate_regional = calculate_regional_ratio(multiplier = 1000)  # e.g. per billion $ (denom in millions of $)
#   calculate_regional = calculate_regional_ratio()                   # plain ratio (multiplier = 1)
calculate_regional_ratio <- function(multiplier = 1, digits = NULL) {
  # Catch a spec that left off the parentheses (the data frame would arrive here as `multiplier`)
  if (!is.numeric(multiplier)) {
    stop("calculate_regional_ratio() needs parentheses in the spec, e.g. calculate_regional_ratio(multiplier = 100)")
  }
  
  # Lock in the settings now, rather than when the indicator runs (R evaluates arguments lazily)
  force(multiplier)
  force(digits)
  
  function(df) {
    # Catch data that lacks the expected columns. Without this check, R finds an unrelated
    # function named `num()` and fails with the unclear error "invalid 'type' (closure)"
    missing_cols <- setdiff(c("num", "denom"), names(df))
    if (length(missing_cols) > 0) {
      stop(glue(
        "calculate_regional_ratio() expects columns `num` and `denom`. ",
        "Missing: {paste(missing_cols, collapse = ', ')}. ",
        "Columns received: {paste(names(df), collapse = ', ')}. ",
        "Rename the numerator and denominator at the end of transform_data, e.g. rename(num = value, denom = area)."
      ))
    }

    # remove all LAC sub/regional totals
    df <- df %>%
      filter(!Country %in% c("South America", "Central America", "Caribbean",
                             "Latin America and the Caribbean", "Latin America"))

    # calculate LAC totals of num and denom
    lac_total <- df %>%
      filter(Country != "World") %>%
      group_by(across(all_of(setdiff(names(df), c("Country", "num", "denom"))))) %>%
      summarise(num = sum_or_na(num), denom = sum_or_na(denom), .groups = "drop") %>%
      mutate(Country = "Latin America and the Caribbean")
    
    df <- df %>%
      bind_rows(lac_total) %>%
      mutate(value = num / denom * multiplier) %>%
      select(-num, -denom)
    
    if (!is.null(digits)) df <- df %>% mutate(value = round(value, digits))
    df
  }
}

# Shortcut for percentage indicators: num / denom * 100, rounded to 1 decimal.
# Already a finished function, so use it in a spec WITHOUT parentheses:
#   calculate_regional = calculate_regional_ratio_pct
calculate_regional_ratio_pct <- calculate_regional_ratio(multiplier = 100, digits = 1)


# ---- Footnotes & source ----

# Reusable footnote rule: flags the LAC total row with footnote 6970 (calculated from available country data).
# Applied automatically by add_footnotes() whenever calculate_regional actually recalculates LAC --
# no need to list this in a spec's own footnotes.
lac_footnote <- list("6970" = function(df) df$Country == "Latin America and the Caribbean")

# Apply a named list of footnote rules (id -> predicate function taking df, returning a logical vector)
# to a labeled indicator dataframe. Initializes footnotes_id and appends every matching rule's id.
# Whenever calculate_regional isn't maintain_regional (i.e. it actually recalculates a LAC total from
# country data, rather than passing through a published one), lac_footnote (6970) is appended
# automatically.
# Sample usage: df_l %>% add_footnotes(footnotes = list("7177" = function(df) df$Years == "2002"), calculate_regional)
add_footnotes <- function(df, footnotes, calculate_regional) {
  append_id <- function(existing, new) {
    case_when(
      is.na(existing) | existing == "" ~ new,
      !grepl(paste0("\\b", new, "\\b"), existing) ~ paste(existing, new, sep = ","),
      TRUE ~ existing
    )
  }

  if (!identical(calculate_regional, maintain_regional)) {
    footnotes <- c(footnotes, lac_footnote)
  }

  df$footnotes_id <- ""
  for (id in names(footnotes)) {
    df$footnotes_id <- if_else(footnotes[[id]](df), append_id(df$footnotes_id, id), df$footnotes_id)
  }
  df
}

# Compare a cleaned indicator's assigned source and footnotes against what's currently published on
# CEPALSTAT, and print both with their names (before and after when they changed). Never stops
# execution or modifies df. Only runs when diagnostics = TRUE. For new indicators (nothing published
# yet), prints the assigned source and footnotes without comparing.
# Published sources and footnotes come from the CEPALSTAT API; names come from the tables built by
# build_tables.R (Data/Tables/source_table.xlsx and note_table.xlsx). Footnotes are compared as a set
# across the whole indicator, since CEPALSTAT's /footnotes endpoint isn't row-level.
# Sample usage: df_l %>% compare_metadata(indicator_id = 4046, new_indicator = FALSE, diagnostics = TRUE)
compare_metadata <- function(df, indicator_id, new_indicator, diagnostics, note_width = 100) {
  if (!diagnostics) return(df)

  ## names for labeling, from the local tables (first sheet of each) and, for published items, the API
  read_table <- function(file) {
    path <- here("Data/Tables", file)
    if (file.exists(path)) read_xlsx(path, sheet = 1) else NULL
  }
  src <- read_table("source_table.xlsx")
  nts <- read_table("note_table.xlsx")

  source_names <- tibble(id = as.integer(src$source_id), label = as.character(glue("{src$organization_acronym}: {src$description}")))
  note_names   <- tibble(id = as.integer(nts$note_id), label = as.character(nts$description))

  # Formats ids as "id - name" lines (marked when added or removed); unknown ids get a hint
  label_lines <- function(ids, names, width = Inf, mark = integer(), mark_text = "") {
    if (length(ids) == 0) return("   (none)")
    tibble(id = as.integer(ids)) %>%
      left_join(distinct(names, id, .keep_all = TRUE), by = "id") %>%
      mutate(
        label = coalesce(label, "(not in Data/Tables; re-run build_tables.R to get its name)"),
        label = if (is.finite(width)) str_trunc(label, width) else label,
        line  = glue("   {id} - {label}{if_else(id %in% mark, mark_text, '')}")
      ) %>%
      pull(line)
  }

  ## sources and footnotes assigned in this run
  new_source <- unique(as.integer(df$source_id))
  new_notes <- df$footnotes_id %>%
    str_split(",") %>%
    unlist() %>%
    trimws() %>%
    discard(~ .x == "") %>%
    as.integer() %>%
    unique() %>%
    sort()

  if (new_indicator) {
    message(" - Source (new indicator):\n", paste(label_lines(new_source, source_names), collapse = "\n"))
    message(glue(" - Footnotes (new indicator) ({length(new_notes)}):"), "\n",
            paste(label_lines(new_notes, note_names, width = note_width), collapse = "\n"))
    return(df)
  }

  ## compare source
  pub_source_df <- get_indicator_source(indicator_id) %>% slice(1)
  pub_source <- as.integer(pub_source_df$id)
  source_names <- bind_rows(
    source_names,
    tibble(id = pub_source, label = as.character(glue("{pub_source_df$organization_acronym}: {pub_source_df$description}")))
  )

  if (setequal(new_source, pub_source)) {
    message(" - Source unchanged:\n", paste(label_lines(pub_source, source_names), collapse = "\n"))
  } else {
    message("⚠️ Source changed:\n",
            "   Before:\n", paste0("   ", label_lines(pub_source, source_names), collapse = "\n"), "\n",
            "   After:\n",  paste0("   ", label_lines(new_source, source_names), collapse = "\n"))
  }

  ## compare footnotes (set-level, across the whole indicator)
  pub_footnotes <- get_indicator_footnotes(indicator_id)
  pub_notes <- if ("id" %in% names(pub_footnotes)) sort(as.integer(pub_footnotes$id)) else integer(0)
  if (all(c("id", "description") %in% names(pub_footnotes))) {
    note_names <- bind_rows(note_names, tibble(id = as.integer(pub_footnotes$id), label = pub_footnotes$description))
  }

  added   <- setdiff(new_notes, pub_notes)
  removed <- setdiff(pub_notes, new_notes)

  if (length(added) == 0 && length(removed) == 0) {
    message(glue(" - Footnotes unchanged ({length(new_notes)}):"), "\n",
            paste(label_lines(new_notes, note_names, width = note_width), collapse = "\n"))
  } else {
    message("⚠️ Footnotes changed:\n",
            glue("   Before ({length(pub_notes)}):"), "\n",
            paste0("   ", label_lines(pub_notes, note_names, note_width, mark = removed, mark_text = "  [removed]"), collapse = "\n"), "\n",
            glue("   After ({length(new_notes)}):"), "\n",
            paste0("   ", label_lines(new_notes, note_names, note_width, mark = added, mark_text = "  [added]"), collapse = "\n"))
  }

  df
}

# Returns the latest year with a value in the new data and in the published data, from a comparison
# file (create_comp_file()). Both are NA for indicators without a Years column (e.g. 2031).
# Sample usage: latest_years(result_4046$comp)   # list(new = 2025, published = 2024)
latest_years <- function(comp) {
  latest <- function(values) {
    years <- suppressWarnings(as.integer(comp$Years[!is.na(values)]))
    if (all(is.na(years))) NA_integer_ else max(years, na.rm = TRUE)
  }
  if (!"Years" %in% names(comp)) return(list(new = NA_integer_, published = NA_integer_))
  list(new = latest(comp$value), published = latest(comp$value.pub))
}

# Describes how the new data's latest year compares with the published one, e.g. "+1 year",
# "same as published", or a warning if the new data ends earlier (uploading it would remove years).
describe_year_change <- function(new_year, pub_year) {
  case_when(
    is.na(new_year)       ~ "",
    is.na(pub_year)       ~ "not published yet",
    new_year == pub_year  ~ "same as published",
    new_year > pub_year   ~ glue("+{new_year - pub_year} year(s)"),
    TRUE                  ~ "⚠️ ends earlier than published"
  )
}

# Prints the latest year in the new data, next to the latest published year (and the cutoff, if the
# spec sets max_year). Flags new data that ends earlier than what's published.
# Sample usage: report_latest_year(comp, max_year = NULL)
report_latest_year <- function(comp, max_year = NULL) {
  years <- latest_years(comp)
  if (is.na(years$new)) return(invisible(NULL))

  pub_text     <- if (is.na(years$published)) "none" else years$published
  cutoff_text  <- if (is.null(max_year)) "" else glue("; data after {max_year} excluded")
  ends_earlier <- !is.na(years$published) && years$new < years$published
  icon         <- if (ends_earlier) "⚠️" else " -"

  message(glue("{icon} Latest year in the data: {years$new} (published: {pub_text}{cutoff_text})"))
  invisible(NULL)
}


# ---- Source definitions (define_source) ----

# Each indicator spec's define_source assigns the CEPALSTAT source ID to every row. The default,
# existing_source(), keeps the source already published for the indicator. The functions below set a
# specific source instead, e.g. for new indicators (nothing published yet) or ECLAC calculations.
# To add one, copy a function below and change its name and source_id (IDs are in
# Data/Tables/source_table.xlsx).
# Sample usage in a spec: define_source = define_source_aquastat

# Default: look up the indicator's existing CEPALSTAT source and assign it as source_id
existing_source <- function(df, indicator_id) {
  df %>% mutate(source_id = get_indicator_source(indicator_id) %>% slice(1) %>% pull(id))
}

## FAO

define_source_fao <- function(df, indicator_id) {
  df %>% mutate(source_id = 651)  # Online Statistical Database (FAOSTAT)
}

define_source_fao_w_cepal_calcs <- function(df, indicator_id) {
  df %>% mutate(source_id = 5174) # Calculations made by ECLAC based on data from the FAOSTAT database of FAO
}

define_source_aquastat <- function(df, indicator_id) {
  df %>% mutate(source_id = 5173) # Global Information System on Water and Agriculture (AQUASTAT)
}

## OLADE

define_source_olade_w_cepal_calcs <- function(df, indicator_id) {
  df %>% mutate(source_id = 885)  # Calculations made by ECLAC based on data from the Energy Information System of OLACDE
}

## Climate Watch

define_source_cw <- function(df, indicator_id) {
  df %>% mutate(source_id = 1742) # Climate Watch Data Explorer (WRI)
}


# ---- Comparison & diagnostics ----

# Build the comparison dataframe between newly cleaned data (df_l) and currently published CEPALSTAT data.
# When new_indicator = TRUE (no published data yet), value.pub is filled with NA instead of fetched.
# Sample usage: df_l %>% create_comp_file(indicator_id = 4046, dim_config_4046, new_indicator = FALSE)
create_comp_file <- function(df_l, indicator_id, dim_config, new_indicator) {
  # Get public data and create comparison file
  if(!new_indicator) { # if public data exists, use it in comparison check
    pub <- get_cepalstat_data(indicator_id) %>%
      mutate(value = as.numeric(value)) %>%
      get_cepalstat_labels(dim_config)

    join_keys <- intersect(names(df_l), names(pub)) %>% setdiff("value")
    comp <- full_join(df_l, pub, by = join_keys, suffix = c("", ".pub"))

  } else { # else fill comparison checks with NAs for public data
    comp <- df_l %>% mutate(value.pub = NA_integer_)
  }

  return(comp)
}

# Summarize dimension-level overlap between new and published data (Present/New/Old Only), for diagnostics.
# Sample usage: get_comp_summary(comp, dim_config_4046)
get_comp_summary <- function(comp, dim_config) {
  dim_comp_table <- NULL
  for(i in dim_config$data_col) {

    this_dim_comp <- comp %>%
      rename(dim = !!sym(i)) %>%
      mutate(dim_name = i) %>%
      group_by(dim, dim_name) %>%
      summarize(
        value = sum_or_na(as.numeric(value)),
        value.pub = sum_or_na(as.numeric(value.pub)),
        .groups = "drop"
      ) %>%
      mutate(status = case_when(
        !is.na(value) & !is.na(value.pub) ~ "Present in Both",
        !is.na(value) & is.na(value.pub) ~ "New Only",
        is.na(value) & !is.na(value.pub) ~ "Old Only",
        TRUE ~ "Missing in Both"
      )) %>%
      arrange(desc(status))

    dim_comp_table %<>% bind_rows(this_dim_comp)
  }
  return(dim_comp_table)
}

# Build the formatted comparison table (abs/pct diffs, flags) used in the QC report and Data/Checks export.
# Sample usage: run_comparison_checks(comp, dim_config_4046)
run_comparison_checks <- function(comp, dim_config) {
  # Relabel columns to match old comp formatting
  rename_labels <- setNames(dim_config$data_col, paste0("dim_",dim_config$dim_id, "_label"))

  comp %<>%
    rename(!!!rename_labels) %>%
    rename_with(
      ~ str_replace(., "^d(\\d+)_id$", "dim_\\1"),
      .cols = matches("^d\\d+_id$")
    ) %>%
    rename(value_data = value, value_pub = value.pub) %>%
    select(starts_with("dim"), everything())

  # Calculate absolute and relative differences. abs_diff/abs_perc_diff are magnitudes (always
  # >= 0 - abs_perc_diff needed the denominator absolute-valued too, not just the numerator, or a
  # negative value_pub silently flipped its sign); diff/perc_diff are their signed counterparts,
  # for anything that cares about direction (e.g. Value Differences' charts).
  comp %<>%
    mutate(value_data = as.numeric(value_data),
           value_pub = as.numeric(value_pub)) %>%
    mutate(
      abs_diff = abs(value_data - value_pub),
      diff = value_data - value_pub,
      abs_perc_diff = case_when(
        is.na(value_pub) | is.na(value_data) ~ NA_real_,
        value_pub == 0 ~ NA_real_,
        TRUE ~ round((abs_diff / abs(value_pub)) * 100, 2)
      ),
      perc_diff = case_when(
        is.na(value_pub) | is.na(value_data) ~ NA_real_,
        value_pub == 0 ~ NA_real_,
        TRUE ~ round((diff / value_pub) * 100, 2)
      )
    )

  # Flag issues - handle NA values properly
  comp %<>%
    mutate(
      flag_20_diff = !is.na(abs_perc_diff) & abs_perc_diff > 20,  # Only flag if abs_perc_diff exists and > 20
      flag_missing_entry = is.na(value_data) & !is.na(value_pub), # exists in pub but not in data
      flag_new_entry = is.na(value_pub) & !is.na(value_data),     # entry exists in data but not in pub
      flag_some_na = is.na(value_data) | is.na(value_pub) # either new or missing entry
    )

  # Label data availability status
  comp %<>%
    mutate(status = case_when(
      is.na(value_data) & is.na(value_pub) ~ "Missing in Both",
      is.na(value_data) ~ "Old Only",
      is.na(value_pub) ~ "New Only",
      TRUE ~ "Present in Both"
    ))

  return(comp)
}

# Print a console summary of dimension mismatches between new and published data (missing/new entries).
# Sample usage: comp %>% get_comp_summary(dim_config_4046) %>% run_diagnostics()
run_diagnostics <- function(comp_sum) {
  # Inspect differences between public and new file
  # 1️⃣ Missing from new data (likely label changes or dropped series)
  missing_old <- comp_sum %>%
    filter(status == "Old Only")
  if (nrow(missing_old) > 0) {
    message("⚠️  Dimensions present in old data only:")
    print(missing_old %>% count(dim_name, sort = TRUE))
    print(missing_old)
  } else {
    message(glue(" - No missing dimensions"))
  }

  # 2️⃣ Newly added in updated data (new years or countries)
  missing_new <- comp_sum %>%
    filter(status == "New Only")
  if (nrow(missing_new) > 0) {
    message("🆕  Dimensions present in new data only:")
    print(missing_new %>% count(dim_name, sort = TRUE))
    print(missing_new)
  } else {
    message(glue(" - No new dimensions"))
  }
}


# ---- Export / formatting ----

# Format cleaned + labeled indicator data into the final CEPALSTAT Wasabi upload shape.
# Expects data already has source_id and footnotes_id columns (see existing_source(), add_footnotes()).
# Sample usage: format_for_wasabi(df_f, indicator_id = 4046)
format_for_wasabi <- function(data, indicator_id){

  ## Wasabi format:
  # record_id: identificador único de cada fila (String)
  # indicator_id: id del indicador (Integer)
  # source_id: id de la fuente (Integer)
  # footnotes_id: lista de ids separados por coma (String)
  # members_id: lista de ids de dimensiones separadas por coma (String)
  # value: valor numérico (Float)

  # Merge members_id field
  data %<>%
    mutate(
      members_id = data %>%
        select(starts_with("dim_")) %>%
        pmap_chr(~ paste(c(...), collapse = ","))
    ) %>%
    select(-starts_with("dim_"), members_id, footnotes_id)

  # Create record_id field
  data %<>%
    mutate(
      record_id = sprintf(
        "r%0*d",                 # Format string with dynamic width
        nchar(nrow(.)),           # Width = number of digits in row count
        row_number()              # Values to format
      ))

  # Create indicator_id field
  data %<>%
    mutate(indicator_id = indicator_id) # Inherit from manually defined vector

  # Select final columns
  data %<>%
    select(record_id, indicator_id, source_id, footnotes_id, members_id, value)

  return(data)
}

# ---- Bulk processing ----

# Run one indicator: look up its spec, process it, and store the result as result_<id>.
# Sample usage: run_one_indicator(4046)
run_one_indicator <- function(id, global = global_spec) {
  # Stop with instructions if the spec hasn't been loaded into the session yet
  if (!exists(paste0("spec_", id))) {
    stop(glue(
      "❌ No spec found for indicator {id} (object `spec_{id}`). ",
      "Source the script that defines it first (e.g. source(here('Scripts/process_fao.R'))), ",
      "or run the source() lines at the top of run_indicators.R."
    ), call. = FALSE)
  }
  spec <- get(paste0("spec_", id))
  result <- process_indicator(spec, global)
  assign(paste0("result_", id), result, envir = .GlobalEnv)
  invisible(result)
}

# Run many indicators and cache error messages
# Sample usage: run_many_indicators(run_list) # where run_list is list of ids
run_many_indicators <- function(run_list, global = global_spec) {
  run_log <- tibble(id = numeric(), status = character(), latest_year = integer(),
                    published_year = integer(), message = character())

  for (i in seq_along(run_list)) {
    id <- run_list[i]

    outcome <- tryCatch({
      result <- run_one_indicator(id, global)
      years <- latest_years(result$comp)
      list(status = "ok", latest_year = years$new, published_year = years$published, message = NA_character_)
    }, error = function(e) {
      message(glue("❌ Indicator {id} failed: {conditionMessage(e)}"))
      list(status = "error", latest_year = NA_integer_, published_year = NA_integer_, message = conditionMessage(e))
    })

    run_log <- add_row(run_log, id = id, status = outcome$status, latest_year = outcome$latest_year,
                       published_year = outcome$published_year, message = outcome$message)
  }

  # Summary table: one row per indicator, with its latest year next to the published one
  run_log <- run_log %>%
    left_join(meta %>% select(id, indicator), by = "id") %>%
    mutate(year_change = describe_year_change(latest_year, published_year)) %>%
    relocate(indicator, .after = id)

  message(glue("\n✅ {sum(run_log$status == 'ok')}/{nrow(run_log)} indicators processed successfully"))
  if (any(run_log$status == "error")) {
    message("❌ Failed: ", paste(run_log$id[run_log$status == "error"], collapse = ", "))
  }

  message("\nLatest year of data by indicator:")
  run_log %>%
    transmute(id, indicator = str_trunc(indicator, 50), status, latest_year, published_year, year_change) %>%
    as.data.frame() %>%
    print(row.names = FALSE)

  # Save the summary alongside the comparison files, when exporting
  if (isTRUE(global$export)) {
    write_xlsx(run_log, ensure_dir(here("Data/Checks")) %>% file.path("run_summary.xlsx"))
    message("✅ Summary saved to Data/Checks/run_summary.xlsx")
  }

  invisible(run_log)
}


# ---- Currently unused (deletable) ----

# Dev helper: pull an indicator's available footnote text/ids from CEPALSTAT for manual review.
# Zero code references anywhere — but this looks like an intentional interactive tool (see its own comment,
# "for manual assignment") for exploring footnote options when building a new indicator, not dead code from
# an abandoned approach. Worth confirming before deleting for good.
# Sample usage: get_indicator_footnotes(2022)
# get_indicator_footnotes <- function(indicator_id) {
#   ## Get footnotes_id from CEPALSTAT
#   url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/footnotes?lang=en&format=json")
#
#   # Send request and parse JSON
#   result <- fetch_cepalstat_json(url)
#
#   footnotes_tbl <- result %>%
#     pluck("body", "footnotes") %>%
#     as_tibble()
#
#   # Remove specific footnotes that are no longer applicable
#   if(!is_empty(footnotes_tbl)){
#     # f 6996 - note about what Caribbean country category includes. this category is no longer included.
#     footnotes_tbl %<>%
#       filter(!id %in% c(6996))
#   }
#
#   return(footnotes_tbl)
# }

# Wraps get_cepalstat_ids() with a defensive Years-to-character coercion. Not called anywhere —
# process_indicator() calls get_cepalstat_ids() directly instead. Wire this back in (or delete) once decided.
# Sample usage: join_labels(df, dim_config_4046)
# join_labels <- function(df, dim_config) {
#   get_cepalstat_ids(df %>% mutate(Years = as.character(Years)), # ** define std types later
#                             dim_config)
# }

# Helper: append a footnote id to an existing comma-separated list, without duplicating.
# Still used by legacy footnotes_XXXX() functions in 0102_fao.R and 02_other.R that haven't been migrated
# to add_footnotes() yet — safe to remove once those are converted to the footnotes-list pattern.
# Sample usage: append_footnote(footnotes_id, "6970")
# append_footnote <- function(existing, new) {
#   case_when(
#     is.na(existing) | existing == "" ~ new,
#     !grepl(paste0("\\b", new, "\\b"), existing) ~ paste(existing, new, sep = ","),
#     TRUE ~ existing  # footnote already exists, don't duplicate
#   )
# }