library(tidyverse)
library(magrittr)
library(httr2)
library(jsonlite)
library(glue)

# Helper: Fetch and parse JSON from CEPALSTAT API
fetch_cepalstat_json <- function(url) {
  request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
}

# # Helper: Join English and Spanish names, add 'name_es' column
# add_spanish_names <- function(df_en, df_es) {
#   df_en %>%
#     left_join(df_es %>% select(id, name_es = name), by = "id")
# }

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
        select(ends_with("_id")) %>%
        pmap_chr(~ paste(c(...), collapse = ","))
    ) %>% select(-ends_with("_id"), members_id)
  
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
  
  ## Get footnotes_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/footnotes?lang=en&format=json")
  
  # Send request and parse JSON
  result <- fetch_cepalstat_json(url)
  
  footnotes_tbl <- result %>%
    pluck("body", "footnotes") %>%
    as_tibble()
  
  # Create footnotes_id field
  data %<>% 
    mutate(footnotes_id = ifelse(!is_empty(footnotes_tbl), footnotes_tbl %>% pull(id), ''))
  
  # Select final columns
  data %<>% 
    select(record_id, indicator_id, source_id, footnotes_id, members_id, value)
  
  return(data)
}