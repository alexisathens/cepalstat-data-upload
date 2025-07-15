library(tidyverse)
library(magrittr)
library(readxl)
library(here)
library(writexl)
library(CepalStatR)

source(here("Scripts/utils.R"))

# Create basic table with one row per indicator, including id, area, indicator, source, dimensions, anuario Y/N

# ---- access CEPALSTAT indicator list via API ----

# explore available indicators in Viewer
# viewer.indicators()

# save indicators to df
ind <- call.indicators()

ind %<>% as_tibble()

# filter just on environmental indicators of interest
env <- ind %>% 
  filter(Area == "Environmental")

env

# filter out two indicators that don't show up on the front end - presumably moved to social
env %<>% filter(!is.na(`Indicator ID`))

# get vector of environmental IDs
env_ids <- env %>% pull(`Indicator ID`)

# clean
env %<>%
  rename(id = `Indicator ID`) %>% 
  relocate(id, everything()) %>% 
  select(-Area)

env %<>% 
  mutate(name = ifelse(Indicador.2 == "", Indicador.1, Indicador.2)) %>%
  select(!starts_with("Indicador")) %>% 
  relocate(id, name, everything())

env %<>% 
  rename(dimension = Dimension, subdimension = Subdimension)


# ---- get sources ---- 

## UPDATE code from here ->

all_sources <- NULL 

for(i in 1:length(env_ids)){
  
  this_id <- env_ids[i]
  
  ## Get source_id from CEPALSTAT
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{this_id}/sources?lang=en&format=json")
  
  # Send request and parse JSON
  result <- request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
  
  sources_tbl <- result %>%
    pluck("body", "sources") %>%
    as_tibble()
  
  sources_tbl %<>% mutate(env_id = this_id)
  
  all_sources %<>% bind_rows(sources_tbl)
}

all_sources

all_sources %<>% 
  filter(!str_detect(description, "Calculations made|calculated"))

basic_sources <- all_sources %>% 
  distinct(env_id, organization_acronym)

## join to main df
env %<>% 
  left_join(basic_sources, by = c("id" = "env_id"))

env %<>% 
  rename(source = organization_acronym) %>% 
  mutate(source = ifelse(dimension == "Energy resources" & is.na(source), "OLADE", source))



# ---- get dimensions ----

all_dims <- NULL

for(i in 1:length(env_ids)){
  this_id <- env_ids[i]
  
  these_dims <- get_indicator_dimensions(this_id) %>% pull(name)
  
  these_dims <- setdiff(these_dims, c("Country__ESTANDAR", "Years__ESTANDAR", "Reporting Type"))
  
  dims_tbl <- tibble(id = this_id, dimensions = these_dims)
  
  all_dims %<>% bind_rows(dims_tbl)
}

all_dims

# Collapse groups with more than one dimension
all_dims %<>%
  group_by(id) %>%
  summarise(dimensions = paste(unique(dimensions), collapse = "; "), .groups = "drop")

## join to main df
env %<>% 
  left_join(all_dims, by = "id")


# ---- create flags ----


# ---- export ----

# Export spreadsheet
write_xlsx(env, here("Data", "indicator_metadata.xlsx"))
