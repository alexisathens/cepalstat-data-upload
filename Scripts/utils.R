library(tidyverse)
library(magrittr)
library(httr2)
library(jsonlite)
library(glue)

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
  # f 6996 - note about what Caribbean country category includes. this category is no longer included.
  footnotes_tbl %<>% 
    filter(!id %in% c(6996))
  
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

# Get full dimension table (members) with English and Spanish names
get_full_dimension_table <- function(dimension_id) {
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

# Get indicator-specific dimension members for an indicator, with English and Spanish names
get_ind_dimension_table <- function(indicator_id, dimension_id) {
  # English
  url_en <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=en&format=json&in=1&path=0")
  result_en <- fetch_cepalstat_json(url_en)
  indicator_info_en <- result_en %>%
    pluck("body", "dimensions") %>%
    as_tibble() %>%
    filter(id == dimension_id)
  members_en <- indicator_info_en$members[[1]] %>% as_tibble()
  members_en <- members_en %>%
    mutate(dim_id = indicator_info_en$id[1],
           dim_name = indicator_info_en$name[1])

  # Spanish
  url_es <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=es&format=json&in=1&path=0")
  result_es <- fetch_cepalstat_json(url_es)
  indicator_info_es <- result_es %>%
    pluck("body", "dimensions") %>%
    as_tibble() %>%
    filter(id == dimension_id)
  members_es <- indicator_info_es$members[[1]] %>% as_tibble()
  members_es <- members_es %>%
    mutate(dim_id = indicator_info_es$id[1],
           dim_name = indicator_info_es$name[1])

  # Join Spanish names as '*_es'
  members <- members_en %>%
    left_join(members_es %>% select(id, name_es = name, dim_name_es = dim_name), by = "id")

  return(members)
}

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

# Take bare minimum df with value and *_id fields only and format for CEPALSTAT Wasabi upload
format_for_wasabi <- function(data, indicator_id){
  
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
        select(ends_with("_id"), -footnotes_id) %>%
        pmap_chr(~ paste(c(...), collapse = ","))
    ) %>%
    select(-ends_with("_id"), members_id, footnotes_id)
  
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
  
  ## Get source_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/sources?lang=en&format=json")
  
  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)
  
  sources_tbl <- result %>%
    pluck("body", "sources") %>%
    as_tibble()
  
  # Create source_id field
  data %<>% 
    mutate(source_id = sources_tbl %>% pull(id))
  
  ## Get footnotes_id from CEPALSTAT -- do this fully in processing script since manual
  # footnotes_tbl <- get_indicator_footnotes(indicator_id)
  # 
  # # Create footnotes_id field
  # data %<>% 
  #   mutate(footnotes_id = ifelse(!(is_empty(footnotes_tbl) | nrow(footnotes_tbl) == 0), footnotes_tbl %>% pull(id), ''))
  
  # Select final columns
  data %<>% 
    select(record_id, indicator_id, source_id, footnotes_id, members_id, value)
  
  return(data)
}

# Take dimension map to long df with value and dimensions only and join them
join_data_dim_members <- function(data, dim_config) {
  
  for(i in 1:nrow(dim_config)) {
    data_col <- dim_config$data_col[i]
    dim_id <- dim_config$dim_id[i]
    dim_field <- str_remove(dim_config$pub_col[i], "^\\d+_")
    
    if(dim_field == "iso"){
      matching_dim_id <- dim_config$dim_id[i]
      matching_dim_col <- read_xlsx(here("Data/iso_codes.xlsx")) %>% select(id = cepalstat, match = name) %>% distinct(id, match)
      matching_dim_col %<>% rename(!!paste0("d", dim_id, "_id") := id)
    } else {
      matching_dim_id <- dim_config$dim_id[i]
      matching_dim_table <- get_full_dimension_table(matching_dim_id)
      matching_dim_col <- matching_dim_table %>% select(id, match = !!sym(dim_field))
      matching_dim_col %<>% rename(!!paste0("d", dim_id, "_id") := id)
    }
    
    # Join members
    data %<>% 
      left_join(matching_dim_col, by = setNames("match", data_col))
    
  }
  
  return(data)
}

# Function that takes public CEPALSTAT data [output from get_cepalstat_data] and matches to readable labels
match_cepalstat_labels <- function(pub) {
  
  # Extract dimension numbers from column names
  dim_cols <- names(pub) %>%
    str_extract("\\d+") %>%   # Extract first sequence of digits
    na.omit() %>%             # Drop anything without a number (e.g. "value")
    as.vector()
  
  for(i in dim_cols) {
    this_pub_col <- names(pub)[str_detect(names(pub), i)] # Get corresponding name of column in pub
    this_dim_table <- get_full_dimension_table(i) # Get full dimension member table
    # Relabel columns for join
    this_dim_table %<>% 
      mutate(across(everything(), as.character)) %>% 
      select(id, name, name_es) %>% 
      rename(!!this_pub_col := id,
             !!glue::glue("{i}_name") := name,
             !!glue::glue("{i}_name_es") := name_es)
    # Join labels on pub df
    pub %<>% 
      left_join(this_dim_table, by = setNames(this_pub_col, this_pub_col))
  }
  
  assert_no_na_cols(pub)
  
  return(pub)
}

# Function that takes comp data frame and returns table with a flag for what members are present between the new and public data
get_comp_summary_table <- function(comp, dim_config) {
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
create_comparison_checks <- function(comp, dim_config) {
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
           perc_diff = round((abs_diff / value_pub) * 100, 2))
  
  # Flag issues
  comp %<>%
    mutate(
      flag_large_diff = perc_diff > 20,  # adjust threshold as needed
      flag_missing_entry = is.na(value_data), # exists in pub but not in data
      flag_new_entry = is.na(value_pub),     # entry exists in data but not in pub
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
}