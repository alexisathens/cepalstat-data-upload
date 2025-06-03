get_dimension_table <- function(dimension_id) {
  ## Get dimension members and info from CEPALSTAT
  
  # Build dimension URL
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/dimensions/{dimension_id}")
  
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
