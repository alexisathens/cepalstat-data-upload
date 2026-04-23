library(tidyverse)
library(magrittr)
library(httr2)
library(jsonlite)
library(glue)
library(quarto)

# Stop code if there are any NA fields that didn't match
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
  
  invisible(TRUE)
}

# Stop code if there are any NA fields that didn't match
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
  
  invisible(TRUE)
}

# Helper: take sum, but retain NAs if all values are NA
sum_or_na <- function(x) {
  if (all(is.na(x))) NA_real_ else sum(x, na.rm = TRUE)
}

# Helper: Fetch and parse JSON from CEPALSTAT API
fetch_cepalstat_json <- function(url) {
  request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
}

# Helper function to append a footnote if it doesn't already exist
append_footnote <- function(existing, new) {
  case_when(
    is.na(existing) | existing == "" ~ new,
    !grepl(paste0("\\b", new, "\\b"), existing) ~ paste(existing, new, sep = ","),
    TRUE ~ existing  # footnote already exists, don't duplicate
  )
}
# Sample usage in indicator-specific footnotes_XXXX function:
# data %>% 
#   mutate(
#     footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id),
#     footnotes_id = if_else(Years == "2002", append_footnote(footnotes_id, "7177"), footnotes_id))

# Get table of indicator footnotes for manual assignment
get_indicator_footnotes <- function(indicator_id) {
  ## Get footnotes_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/footnotes?lang=en&format=json")
  
  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)
  
  footnotes_tbl <- result %>%
    pluck("body", "footnotes") %>%
    as_tibble()
  
  # Remove specific footnotes that are no longer applicable
  if(!is_empty(footnotes_tbl)){
    # f 6996 - note about what Caribbean country category includes. this category is no longer included.
    footnotes_tbl %<>% 
      filter(!id %in% c(6996))
  }
  
  return(footnotes_tbl)
}

# Get indicator dimensions with English and Spanish names
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

# Get table of indicator metadata
get_indicator_metadata <- function(indicator_id) {
  ## Get footnotes_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/metadata?lang=en&format=json")
  
  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)
  
  metadata_tbl <- result$body$metadata %>%
    enframe(name = "variable", value = "value") %>%
    mutate(value = map_chr(value, as.character))
  
  return(metadata_tbl)
}

# Get table of indicator sources
get_indicator_sources <- function(indicator_id) {
  ## Get footnotes_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/sources?lang=en&format=json")
  
  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)
  
  sources_tbl <- result$body$sources %>%
    as_tibble()
  
  return(sources_tbl)
}

# Get full dimension table (members) with English and Spanish names
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

# Function to join indicator labels to flat indicator data
get_indicator_labels <- function(df, dim_config) {
  
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

# Get indicator-specific dimension members for an indicator, with English and Spanish names
# get_ind_dimension_table <- function(indicator_id, dimension_id) {
#   # English
#   url_en <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=en&format=json&in=1&path=0")
#   result_en <- fetch_cepalstat_json(url_en)
#   indicator_info_en <- result_en %>%
#     pluck("body", "dimensions") %>%
#     as_tibble() %>%
#     filter(id == dimension_id)
#   members_en <- indicator_info_en$members[[1]] %>% as_tibble()
#   members_en <- members_en %>%
#     mutate(dim_id = indicator_info_en$id[1],
#            dim_name = indicator_info_en$name[1])
# 
#   # Spanish
#   url_es <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=es&format=json&in=1&path=0")
#   result_es <- fetch_cepalstat_json(url_es)
#   indicator_info_es <- result_es %>%
#     pluck("body", "dimensions") %>%
#     as_tibble() %>%
#     filter(id == dimension_id)
#   members_es <- indicator_info_es$members[[1]] %>% as_tibble()
#   members_es <- members_es %>%
#     mutate(dim_id = indicator_info_es$id[1],
#            dim_name = indicator_info_es$name[1])
# 
#   # Join Spanish names as '*_es'
#   members <- members_en %>%
#     left_join(members_es %>% select(id, name_es = name, dim_name_es = dim_name), by = "id")
# 
#   return(members)
# }

# Fetch currently published CEPALSTAT data for given indicator
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

# Take bare minimum df with value and dim_* fields only and format for CEPALSTAT Wasabi upload
format_for_wasabi <- function(data, indicator_id, source_fn = NULL){
  
  ## Final format:
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
  
  # Create source_id field
  if(is.function(source_fn)) { # Use provided source_id directly (already assigned)

    # Create source_id field
    data %<>% 
      mutate(source_id = source_fn())
    
  } else { # Fall back to CEPALSTAT API lookup
    
    # Get source_id from CEPALSTAT
    url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/sources?lang=en&format=json")
    
    # Send request and parse JSON
    result <- fetch_cepalstat_json(url)
    
    sources_tbl <- result %>%
      pluck("body", "sources") %>%
      as_tibble()
    
    ## Transition these manual source assignments to indicator-specific code -- just need to fix once
    # if(nrow(sources_tbl) > 1) {
    #   if(indicator_id == 2036) {sources_tbl %<>% filter(id == 652)} # keep only FRA, drop CEPAL calcs since direct from source
    #   if(indicator_id == 2530) {sources_tbl %<>% filter(id == 1338)} # keep only CEPAL based on FRA source, since we're doing intermediate calcs
    #   if(indicator_id == 2531) {sources_tbl %<>% filter(id == 1338)} # keep only CEPAL based on FRA source, since we're doing intermediate calcs
    #   if(indicator_id == 2021) {sources_tbl %<>% filter(id == 1338)} # keep only CEPAL based on FRA source, since we're doing intermediate calcs
    # }
    
    # Create source_id field
    data %<>% 
      mutate(source_id = sources_tbl %>% pull(id))
    
  }
  
  # Select final columns
  data %<>% 
    select(record_id, indicator_id, source_id, footnotes_id, members_id, value)
  
  return(data)
}

# Take dimension map to long df with value and dimensions only and join them
# join_data_dim_members <- function(data, dim_config) {
#   
#   for(i in 1:nrow(dim_config)) {
#     data_col <- dim_config$data_col[i]
#     dim_id <- dim_config$dim_id[i]
#     dim_field <- str_remove(dim_config$pub_col[i], "^\\d+_")
#     
#     if(dim_field == "iso"){
#       matching_dim_id <- dim_config$dim_id[i]
#       matching_dim_col <- read_xlsx(here("Data/iso_codes.xlsx")) %>% select(id = cepalstat, match = name) %>% distinct(id, match)
#       matching_dim_col %<>% rename(!!paste0("d", dim_id, "_id") := id)
#     } else {
#       matching_dim_id <- dim_config$dim_id[i]
#       matching_dim_table <- get_full_dimension_table(matching_dim_id)
#       matching_dim_col <- matching_dim_table %>% select(id, match = !!sym(dim_field))
#       matching_dim_col %<>% rename(!!paste0("d", dim_id, "_id") := id)
#     }
#     
#     # Join members
#     data %<>% 
#       left_join(matching_dim_col, by = setNames("match", data_col))
#     
#   }
#   
#   return(data)
# }

# Function that takes public CEPALSTAT data [output from get_cepalstat_data] and matches to readable labels
# match_cepalstat_labels <- function(pub) {
#   
#   # Extract dimension numbers from column names
#   dim_cols <- names(pub) %>%
#     str_extract("\\d+") %>%   # Extract first sequence of digits
#     na.omit() %>%             # Drop anything without a number (e.g. "value")
#     as.vector()
#   
#   for(i in dim_cols) {
#     this_pub_col <- names(pub)[str_detect(names(pub), i)] # Get corresponding name of column in pub
#     this_dim_table <- get_full_dimension_table(i) # Get full dimension member table
#     # Relabel columns for join
#     this_dim_table %<>% 
#       mutate(across(everything(), as.character)) %>% 
#       select(id, name, name_es) %>% 
#       rename(!!this_pub_col := id,
#              !!glue::glue("{i}_name") := name,
#              !!glue::glue("{i}_name_es") := name_es)
#     # Join labels on pub df
#     pub %<>% 
#       left_join(this_dim_table, by = setNames(this_pub_col, this_pub_col))
#   }
#   
#   # Assert no NA values in dimension columns (NA values in 'value' column are legitimate missing data)
#   assert_no_na_cols(pub, !contains("value"))
#   
#   return(pub)
# }

# Function that takes comp data frame and returns table with a flag for what members are present between the new and public data
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

# Function that creates basic comparison checks and formats for QC report
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

# Function that renders 03_qc_report.qmd for given indicator
render_qc_checks <- function(indicator_id, open_qmd = TRUE, new_indicator = FALSE) {
  
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

# Function that updates indicator_metadata.xlsx with code versioning
update_indicator_metadata <- function(indicator_id, ind_notes = NULL) {
  
  # Load metadata
  meta <- read_xlsx(here("Data/indicator_metadata.xlsx"), sheet = "metadata")
  
  # Convert ind_notes to empty string if null
  if (is.null(ind_notes)) ind_notes <- NA_character_
  
  # Throw error if metadata doesn't exist (e.g., for a new indicator)
  if(is_empty(meta %>% filter(id == indicator_id) %>% pull(id))) stop("Indicator metadata doesn't exist. Create metadata row in Data/indicator_metadata.xlsx.")
  
  meta <- meta %>%
    mutate(
      last_run = lubridate::ymd(last_run, quiet = TRUE)) %>% 
    mutate(
      automated = if_else(id == indicator_id, "Y", automated),
      last_run = if_else(id == indicator_id, as.Date(Sys.Date()), as.Date(last_run)),
      notes = if_else(id == indicator_id, ind_notes, notes)
    )
  
  # Write back
  writexl::write_xlsx(list(metadata = meta), here("Data/indicator_metadata.xlsx"))
}
