get_dimension_table <- function(dimension_id, lang = "en") {
  ## Get dimension members and info from CEPALSTAT
  
  # Build dimension URL
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/dimensions/{dimension_id}?lang={lang}")
  
  # Send request and parse JSON
  result <- request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
  
  # Extract and process dimension info
  dim_info <- result %>%
    pluck("body", "dimensions") %>%
    as_tibble()
  
  # Extract members info
  dim_members <- dim_info$members[[1]] %>% 
    as_tibble()
  
  # Add dimension info
  dim_members %<>% 
    mutate(dim_id = dim_info$id[1],
           dim_name = dim_info$name[1])
  
  return(dim_members)
}

get_indicator_dimensions <- function(indicator_id, lang = "en") {
  ## Get indicator dimensions from CEPALSTAT
  
  # Build indicator URL
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang={lang}&format=json&in=1&path=0")
  
  # Send request and parse JSON
  result <- request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
  
  # Extract dimension IDs for the indicator
  indicator_info <- result %>%
    pluck("body", "dimensions") %>%
    as_tibble() %>% 
    select(name, id)
  
  # Change dimension labels to English if in Spanish
  if(lang == "es") {
    # Build indicator URL
    url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=en&format=json&in=1&path=0")
    
    # Send request and parse JSON
    result <- request(url) %>%
      req_perform() %>%
      resp_body_string() %>%
      fromJSON(flatten = TRUE)
    
    # Extract dimension IDs for the indicator
    indicator_info_en <- result %>%
      pluck("body", "dimensions") %>%
      as_tibble() %>% 
      select(name, id)
    
    # Overwrite name to be in English for joining purposes
    indicator_info %<>% 
      full_join(indicator_info_en, by = "id", suffix = c(".es", "")) %>% 
      select(name, id)
  }
  
  return(indicator_info)
}

get_published_dimension_table <- function(indicator_id, dimension_id, lang = "en") {
  ## Get published indicator dimensions from CEPALSTAT for given indicator
  
  # Build indicator URL
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang={lang}&format=json&in=1&path=0")
  
  # Send request and parse JSON
  result <- request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
  
  # Extract dimension IDs for the indicator
  indicator_info <- result %>%
    pluck("body", "dimensions") %>%
    as_tibble() %>% 
    filter(id == dimension_id)
  
  # Extract published members info
  dim_members <- indicator_info$members[[1]] %>% 
    as_tibble()
  
  # Add dimension info
  dim_members %<>% 
    mutate(dim_id = indicator_info$id[1],
           dim_name = indicator_info$name[1])
  
  return(dim_members)
}
