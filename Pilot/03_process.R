
indicator_id <- 2486
data <- i2486

### function:

library(writexl)

# ---- Get CEPALSTAT dimensions ----

# Retrieve dimension list for indicator
dims_list <- get_indicator_dimensions(indicator_id)

# Initialize table storing full dimension info
dims_tbl <- NULL

# Gather all relevant dimension members
for(this_dim_id in dims_list$id) {
  
  this_dims_tbl <- get_full_dimension_table(this_dim_id)
  
  # Bind to full table
  dims_tbl %<>% bind_rows(this_dims_tbl)
}

dims_tbl

# ---- Join CEPALSTAT dimensions ----

# Loop through each dimension and join its ID column to the main data
for(this_dim_name in unique(dims_tbl$dim_name)) {
  
  # Get the table for this dimension
  this_dim_df <- dims_tbl %>% filter(dim_name == this_dim_name)

  # Get data join key from dimension name
  this_join_key <- str_remove(this_dim_name, "__.*")
  
  # Join only if the main data includes this field
  if(this_join_key %in% names(data)) {
    
    data %<>% 
      left_join(
        this_dim_df %>% select(name, id), 
        by = setNames("name", this_join_key)
      ) %>% 
      rename(!!paste0(this_join_key, "_id") := id)
    
  } else { 
    stop(glue("Column '{this_join_key}' not found in main data.")) # throw an error if key isn't found
  }
}

# Check all columns that end in "_id" for missing values
stopifnot(
  all(!is.na(data %>% select(ends_with("_id"))))
)
# data %>% filter(if_any(ends_with("_id"), is.na)) %>% View()

# ---- Format for Wasabi upload ----

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
result <- request(url) %>%
  req_perform() %>%
  resp_body_string() %>%
  fromJSON(flatten = TRUE)

sources_tbl <- result %>%
  pluck("body", "sources") %>%
  as_tibble()

# Create source_id field
data %<>% 
  mutate(source_id = sources_tbl %>% pull(id))

## Get footnotes_id from CEPALSTAT
url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/footnotes?lang=en&format=json")

# Send request and parse JSON
result <- request(url) %>%
  req_perform() %>%
  resp_body_string() %>%
  fromJSON(flatten = TRUE)

footnotes_tbl <- result %>%
  pluck("body", "footnotes") %>%
  as_tibble()

# Create footnotes_id field
data %<>% 
  mutate(footnotes_id = ifelse(!is_empty(footnotes_tbl), footnotes_tbl %>% pull(id), ''))

# Select final columns
data %<>% 
  select(record_id, indicator_id, source_id, footnotes_id, members_id, value)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# write_csv(data, glue("Pilot/Data/Cleaned/id{indicator_id}_{dt_stamp}.csv"))

write_xlsx(data, glue("Pilot/Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx"))
