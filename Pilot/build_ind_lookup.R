library(tidyverse)
library(readxl)
library(magrittr)
library(CepalStatR)


# ---- access CEPALSTAT indicator list via API ----

# explore available indicators in Viewer
viewer.indicators()

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


# ---- get full environmental indicator dimension list ----

all_dims <- NULL

for(i in 1:length(env_ids)){
  this_id <- env_ids[i]
  
  these_dims <- get_indicator_dimensions(this_id)
  
  all_dims <- all_dims %>% bind_rows(these_dims) %>% distinct(name, id, name_es)
}

all_dims


# ---- get sources for each indicator ----

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
  distinct(env_id, organization_acronym, organization_name)

## join to main df
env %<>% 
  left_join(basic_sources, by = c(`Indicator ID` = "env_id"))


## create compact df for anuario planning
anuario <- env

anuario %<>% 
  select(`Indicator ID`, Dimension, Indicador.1, organization_acronym, organization_name)

anuario

anuario %>% group_by(organization_acronym) %>% count() %>% 
  arrange(desc(n))
