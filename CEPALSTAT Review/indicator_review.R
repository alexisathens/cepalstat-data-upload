library(tidyverse)
library(magrittr)
library(readxl)
library(CepalStatR)
library(here)

source(here("Scripts/utils.R"))

# ---- base df ----

# explore available indicators in CEPALSTAT package
# viewer.indicators()

# save indicators to df
ind <- call.indicators()
ind %<>% as_tibble()

# filter just on environmental indicators
env <- ind %>% 
  filter(Area == "Environmental")

# filter out two indicators that don't show up on the front end - presumably moved to social
env %<>% filter(!is.na(`Indicator ID`))

# organize df
env %<>% 
  mutate(Subdimension = ifelse(Indicador.1 == "Emissions of greenhouse gases (GHGs)", 
                               paste(Subdimension, Indicador.1, sep = " / "), Subdimension)) %>% 
  mutate(Indicador.1 = ifelse(Indicador.1 == "Emissions of greenhouse gases (GHGs)", Indicador.2, Indicador.1))

env %<>% 
  select(-c(Area, Indicador.2, Indicador.3)) %>% 
  rename(cat1 = Dimension, cat2 = Subdimension, indicator = Indicador.1, id = `Indicator ID`) %>% 
  select(id, everything())


# ---- source info ----

env %<>% mutate(source = NA_character_)

for(i in 1:nrow(env)) {
  source <- get_indicator_sources(env$id[i])
  # if(nrow(source) > 1) { stop() } # taking the first entry seems to work best
  source %<>% slice(1)
  acronym <- source %>% pull(organization_acronym)
  env$source[i] <- acronym
}

rm(source, acronym)


# ---- last update info ----

env %<>% mutate(last_update = as.Date(NA)) # initialize

for(i in 1:nrow(env)) {
  meta <- get_indicator_metadata(env$id[i])
  last_update <- meta %>% filter(variable == "last_update") %>% pull(value)
  last_update %<>% str_squish() %>% mdy_hm() %>% as_date()
  env$last_update[i] <- last_update
}

rm(meta, last_update)


# ---- last year of data ----

env %<>% mutate(last_year = as.numeric(NA)) # initialize

for(i in 1:nrow(env)) {
  if(i == 69){ # this indicator data has some NAs and doesn't pass quality checks -- manually update year
    year = 2021
  } else if(i == 84) { # keep NA, there is no year field (yet) for this indicator
    
  } else {
    data <- get_cepalstat_data(env$id[i])
    year <- match_cepalstat_labels(data)
    year %<>% summarize(max = max(`29117_name`)) %>% pull(as.numeric(max))
  }
  env$last_year[i] <- year
}

rm(data, year)

# ---- anuario info ----

# manually add ids of indicators in the yearbook
anuario_plan <- read_xlsx(here("CEPALSTAT Review/anuario_plan_2025.xlsx"), sheet = "Indicator Plan", skip = 0)

anuario_plan %<>% 
  rename(id = `ID INDICADOR CEPALSTAT`, entrega = matches("^FECHA DE ENTREGA")) %>% 
  mutate(id = as.numeric(str_replace_all(id, "[\r\n]", "")))

anuario_plan %<>% 
  filter(!is.na(entrega)) %>% 
  filter(!is.na(id)) %>% 
  distinct(id, entrega)

anuario_plan %<>% 
  mutate(anuario = "Y") %>% 
  mutate(anuario_entrega = ifelse(str_detect(entrega, "sept"), "1_Sept", "2_Nov"))

anuario_plan %<>% select(id, anuario_entrega)

env %<>% 
  left_join(anuario_plan, by = "id")


# ---- indicator review ----

