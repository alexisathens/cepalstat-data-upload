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







# ---- get full environmental indicator dimension list ----

all_dims <- NULL

for(i in 1:length(env_ids)){
  this_id <- env_ids[i]
  
  these_dims <- get_indicator_dimensions(this_id)
  
  all_dims <- all_dims %>% bind_rows(these_dims) %>% distinct(name, id, name_es)
}

all_dims
