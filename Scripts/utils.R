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
library(CepalStatR)
library(FAOSTAT)
library(fishstat)

# ---- Global variables ----

iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

# ---- Small helpers ----

# Helper: take sum, but retain NA if every value in the group is NA (rather than summing to 0)
# Sample usage: summarize(value = sum_or_na(as.numeric(value)))
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
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
      " - Duplicates: ", n_rows - n_keys
    )
    stop(msg)
  }

  data #invisible(TRUE)
}

# Validate cleaned data before regional aggregation: no NA values in 'value', no unexpected columns beyond
# dim_config + value/num/denom, and drop years beyond max_year. Stops on failure, else returns data.
# Sample usage: df %>% assert_data_reqs(dim_config_4046, indicator_id = 4046, max_year = max_year_emdat)
assert_data_reqs <- function(data, dim_config, indicator_id, max_year) {
  # check for NA values
  na_vals <- data %>%
    filter(is.na(value))

  if (nrow(na_vals) > 0) {
    msg <- glue(
      "❌ {nrow(na_vals)} NA values found in 'value'"
    )
    stop(msg)
  }

  # check for extra columns
  acceptable_cols <- c(dim_config$data_col, "value", "num", "denom")
  extra_cols <- setdiff(names(data), acceptable_cols)

  if(!is_empty(extra_cols)) {
    msg <- paste0(
      "❌ extra columns found: ",
      paste0(extra_cols, collapse = ", ")
    )
    stop(msg)
  }

  # filter out extra years and coerce type
  data %<>%
    filter(Years <= max_year) %>%
    mutate(Years = as.character(Years)) %>%
    arrange(Country, Years)

  if (indicator_id == 2031) { # exception for MEA indicator where Years is the value
    data %<>% mutate(Years = as.integer(Years))
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
get_indicator_source <- function(indicator_id) {
  ## Get footnotes_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/sources?lang=en&format=json")

  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)

  sources_tbl <- result$body$sources %>%
    as_tibble()

  return(sources_tbl)
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

    df_l %<>%
      left_join(this_dim_table, by = setNames("name", this_data_col))
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


# ---- Country / regional handling ----

# Standardize country names to CEPALSTAT's iso table, restrict to LAC countries, and drop subregion groupings.
# Always run for every indicator, right after transform_data().
# Sample usage: df %>% standardize_countries()
standardize_countries <- function(df) {
  df %>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name) %>%
    filter(Country %in% iso$name) %>%
    filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America")) # always remove subregions
}

# Regional strategy: drop the source's own LAC total and recalculate it as a simple sum across countries.
# Default calculate_regional option — returns the full replacement df (not just the new total rows).
# Sample usage: df %>% calculate_regional_sum()
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

# Regional strategy: recalculate the LAC total as a weighted average (e.g. sum(num)/sum(denom) across countries).
# ** not yet implemented — placeholder for indicators like fertilizer intensity that need num/denom columns
# carried through filter_data()/transform_data() (see assert_data_reqs()'s acceptable_cols).
# Sample usage: df %>% calculate_regional_wgt_avg()
calculate_regional_wgt_avg <- function(df) {
  # ** write this function
}

# Regional strategy: keep the source's own LAC total as-is (no recalculation).
# Used for all OLADE indicators, which publish their own regional aggregate (see 02_olade.R).
# Sample usage: df %>% maintain_regional()
maintain_regional <- function(df) {
  df
}


# ---- Footnotes & source ----

# Apply a named list of footnote rules (id -> predicate function taking df, returning a logical vector)
# to a labeled indicator dataframe. Initializes footnotes_id and appends every matching rule's id.
# Sample usage: df_l %>% add_footnotes(footnotes = c(lac_footnote, list("7177" = function(df) df$Years == "2002")))
add_footnotes <- function(df, footnotes) {
  append_id <- function(existing, new) {
    case_when(
      is.na(existing) | existing == "" ~ new,
      !grepl(paste0("\\b", new, "\\b"), existing) ~ paste(existing, new, sep = ","),
      TRUE ~ existing
    )
  }

  df$footnotes_id <- ""
  for (id in names(footnotes)) {
    df$footnotes_id <- if_else(footnotes[[id]](df), append_id(df$footnotes_id, id), df$footnotes_id)
  }
  df
}

# Reusable footnote rule: flags the LAC total row with footnote 6970 (calculated from available country data).
# Sample usage: indicator_spec(..., footnotes = lac_footnote)
lac_footnote <- list("6970" = function(df) df$Country == "Latin America and the Caribbean")

# Default define_source: look up an indicator's existing CEPALSTAT source and assign it as source_id.
# Sample usage: df_l %>% existing_source(indicator_id = 4046)
existing_source <- function(df, indicator_id) { # default: get existing source
  df %>% mutate(source_id = get_indicator_source(indicator_id) %>% slice(1) %>% pull(id))
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

  # Calculate absolute and relative differences
  comp %<>%
    mutate(value_data = as.numeric(value_data),
           value_pub = as.numeric(value_pub)) %>%
    mutate(abs_diff = abs(value_data - value_pub),
           perc_diff = case_when(
             is.na(value_pub) | is.na(value_data) ~ NA_real_,
             value_pub == 0 ~ NA_real_,
             TRUE ~ round((abs_diff / value_pub) * 100, 2)
           ))

  # Flag issues - handle NA values properly
  comp %<>%
    mutate(
      flag_large_diff = !is.na(perc_diff) & perc_diff > 20,  # Only flag if perc_diff exists and > 20
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

# Render 03_qc_report.qmd for an indicator and open it in the browser.
# Sample usage: render_qc_checks(4046, new_indicator = FALSE, open_qmd = TRUE)
render_qc_checks <- function(indicator_id, new_indicator = FALSE, open_qmd = TRUE) {

  # Construct qmd file name
  output_file   <- paste0("qc_report_", indicator_id, ".html")

  # Render the report with injected parameters
  quarto_render(
    input          = paste0(here(), "/Scripts/03_qc_report.qmd"),
    output_file    = output_file,
    output_format  = "html",
    execute_params = list(
      indicator_id = indicator_id,
      new_indicator = new_indicator
    )
  )

  # Manually move output from Scripts/ to QC Reports/
  file.rename(
    from = here::here("Scripts", output_file),
    to   = here::here("QC Reports", output_file)
  )

  # Open file in browser
  if(open_qmd == TRUE){
    browseURL(here::here("QC Reports", output_file))
  }
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