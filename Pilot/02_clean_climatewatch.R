library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)

indicator_id <- 4461
iso_join_field <- "iso" # ex: name, iso, iso2

# ---- Transform raw data to long format ----

## Download country and regional data
c <- read_csv("pilot/data/raw/ghg-emissions_country.csv")
r <- read_csv("pilot/data/raw/ghg-emissions_region.csv")

c %<>% filter(iso != "Data source")
r %<>% filter(iso != "Data source")

long <- c %>% bind_rows(r)

## Make data long

# Pivot long based on year
long %<>% 
  pivot_longer(cols = matches("^\\d{4}$"), names_to = "year")

# Drop unit and country name
long %<>% 
  select(iso, year, value)

# Rename to match CEPALSTAT field
long %<>% 
  rename(Country = iso, Years = year)


# ---- Join CEPALSTAT dimensions ----

## Retrieve dimensions for specific indicator from CEPALSTAT json
# Build URL
url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/dimensions?lang=en&format=json&in=1&path=0")

# Send request and parse JSON
result <- request(url) %>%
  req_perform() %>%
  resp_body_string() %>%
  fromJSON(flatten = TRUE)

# Extract and process dimension info
dims_tbl <- result %>%
  pluck("body", "dimensions") %>%
  as_tibble()

## Integrate iso codes into country dimension
# Isolate country dimension
dim_country <- dims_tbl %>% 
  filter(name == "Country__ESTANDAR") %>% 
  pluck("members") %>% 
  .[[1]]

# Get iso map
iso_map <- read_csv("Data/iso_codes.csv")

# Join iso info to country dim
dim_country %<>% 
  left_join(iso_map)

# Replace country dimension with df including iso codes
dims_tbl <- dims_tbl %>%
  mutate(
    members = if_else(
      name == "Country__ESTANDAR",
      list(dim_country),  # updated with ISO codes
      members)
    )

rm(iso_map, dim_country)

## Join dimensions to long data
# Get a named list of dimension tables
dim_lookup_list <- dims_tbl %>%
  mutate(name = make.names(name)) %>%  # Make names safe for list indexing
  select(name, members) %>%
  deframe()  # creates a named list

# Initialize the main data frame that you'll add columns to
data <- long

# Loop through each dimension and join its ID column to the main data
for(dim_name in names(dim_lookup_list)) {
  
  # Get the lookup table for this dimension
  dim_df <- dim_lookup_list[[dim_name]]
  
  # Define dim join key
  dim_join_key <- ifelse(dim_name == "Country__ESTANDAR", iso_join_field, "name")
  
  # Define data join key
  data_join_key <- str_remove(dim_name, "__.*")
  
  # Join only if the main data includes this field
  if(data_join_key %in% names(data)) {
    
    data %<>%
      left_join(
        dim_df %>% select(!!sym(dim_join_key), id), # get key and id from dimension df
        by = setNames(dim_join_key, data_join_key)   # joins like by = c("Years" = "name")
      ) %>%
      rename(!!paste0(data_join_key, "_id") := id)
  } else { 
    stop(glue("Column '{data_join_key}' not found in main data.")) # throw an error if key isn't found
  }
}

# Check there's no missing fields



### Final format:
# record_id: identificador único de cada fila (String)
# indicator_id: id del indicador (Integer)
# source_id: id de la fuente (Integer)
# footnotes_id: lista de ids separados por coma (String)
# members_id: lista de ids de dimensiones separadas por coma (String)
# value: valor numérico (Float)





# ---- Get sources
url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/sources?lang=en&format=json")

# Send request and parse JSON
result <- request(url) %>%
  req_perform() %>%
  resp_body_string() %>%
  fromJSON(flatten = TRUE)

sources_tbl <- result %>%
  pluck("body", "sources") %>%
  as_tibble()

sources_id <- sources_tbl %>% pull(id)


# ---- Get footnotes
url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/footnotes?lang=en&format=json")

# Send request and parse JSON
result <- request(url) %>%
  req_perform() %>%
  resp_body_string() %>%
  fromJSON(flatten = TRUE)

footnotes_tbl <- result %>%
  pluck("body", "footnotes") %>%
  as_tibble()

if(!is_empty(footnotes_tbl)){
  footnotes_id <- footnotes_tbl %>% pull(id)
} else {
  footnotes_id <- ''
}


