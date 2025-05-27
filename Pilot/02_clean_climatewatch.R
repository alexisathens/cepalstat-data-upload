library(tidyverse)
library(magrittr)
library(readxl)

library(httr)
library(httr2)
library(jsonlite)
library(dplyr)
library(glue)
library(purrr)
library(tibble)
library(reqres)

indicator_id <- 4461

# Transform into long format

## Download country and regional data
c <- read_csv("pilot/data/raw/ghg-emissions_country.csv")
r <- read_csv("pilot/data/raw/ghg-emissions_region.csv")

c %<>% filter(iso != "Data source")
r %<>% filter(iso != "Data source")

long <- c %>% bind_rows(r)

## Make data long

# pivot long based on year
long %<>% 
  pivot_longer(cols = matches("^\\d{4}$"), names_to = "year")

# drop unit and country name
long %<>% 
  select(iso, year, value)


### Final format:
# record_id: identificador único de cada fila (String)
# indicator_id: id del indicador (Integer)
# source_id: id de la fuente (Integer)
# footnotes_id: lista de ids separados por coma (String)
# members_id: lista de ids de dimensiones separadas por coma (String)
# value: valor numérico (Float)

# ---- Get dimensions
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


### PICK UP HERE TOMORROW: HOW/WHEN TO JOIN ISO CODES (inside loop to long or outside loop to dims_tbl)

dims_tbl %>% 
  filter(name == "Country__ESTANDAR") %>% 
  pluck("members")

dims_tbl %>%
  mutate(name = make.names(name)) %>%
  select(name, members) %>%
  deframe()



# Step 1: Get a named list of dimension tables
dim_lookup_list <- dims_tbl %>%
  mutate(name = make.names(name)) %>%  # Make names safe for list indexing
  select(name, members) %>%
  deframe()  # creates a named list

# Step 2: Initialize the main data frame that you'll add columns to
joined <- long

# Step 3: Loop through each dimension and join its ID column to the main data
for (dim_name in names(dim_lookup_list)) {
  
  # Get the lookup table for this dimension
  dim_df <- dim_lookup_list[[dim_name]]
  
  # Derive the join key by removing "__" suffix
  join_key <- str_remove(dim_name, "__.*")
  
  # Join only if the main data includes this field
  if (join_key %in% names(joined)) {
    
    joined <- joined %>%
      left_join(
        dim_df %>% select(label, id),
        by = setNames("label", join_key)   # joins like by = c("Country" = "label")
      ) %>%
      rename(!!paste0(join_key, "_id") := id)
  }
}








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


