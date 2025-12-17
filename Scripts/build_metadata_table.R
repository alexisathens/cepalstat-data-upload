library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(lubridate)
library(CepalStatR)

source(here("Scripts/utils.R"))

# Create basic metadata table with one row per indicator, including id, area, indicator, source, dimensions, anuario, code version

## 1. Retrieve full list of environmental indicators ----

ind <- call.indicators() %>% as_tibble()

env <- ind %>%
  filter(Area == "Environmental", !is.na(`Indicator ID`)) %>%
  mutate(Subdimension = ifelse(
    Indicador.1 == "Emissions of greenhouse gases (GHGs)",
    paste(Subdimension, Indicador.1, sep = " / "),
    Subdimension
  )) %>%
  mutate(Indicador.1 = ifelse(
    Indicador.1 == "Emissions of greenhouse gases (GHGs)",
    Indicador.2, Indicador.1
  )) %>%
  select(-c(Area, Indicador.2, Indicador.3)) %>%
  rename(cat1 = Dimension, cat2 = Subdimension, indicator = Indicador.1, id = `Indicator ID`) %>%
  select(id, indicator, cat1, cat2)

env_ids <- env$id


## 2. Get main sources ----

env <- env %>% mutate(source = NA_character_)

for(i in 1:nrow(env)) {
  source <- get_indicator_sources(env$id[i])
  # if(nrow(source) > 1) { stop() } # taking the first entry seems to work best
  source %<>% slice(1)
  acronym <- source %>% pull(organization_acronym)
  env$source[i] <- acronym
}


## 3. Get indicator dimensions ----

all_dims <- map_dfr(env_ids, function(id) {
  dims <- get_indicator_dimensions(id) %>%
    pull(name) %>%
    setdiff(c("Country__ESTANDAR", "Years__ESTANDAR", "Reporting Type"))
  tibble(id = id, dimensions = paste(unique(dims), collapse = "; "))
})

env <- env %>%
  left_join(all_dims, by = "id")


## 4. Get CEPALSTAT metadata info ----

env <- env %>%
  mutate(last_update = as.Date(NA),
         last_year = as.numeric(NA))

for (i in seq_len(nrow(env))) {
  # last update
  meta <- tryCatch(get_indicator_metadata(env$id[i]), error = function(e) NULL)
  if (!is.null(meta)) {
    last_update <- meta %>%
      filter(variable == "last_update") %>%
      pull(value) %>%
      str_squish()
    env$last_update[i] <- suppressWarnings(mdy_hm(last_update) %>% as_date())
  }
  
  # last year
  if (!is.na(env$id[i])) {
    data <- tryCatch(get_cepalstat_data(env$id[i]), error = function(e) NULL)
    if (!is.null(data) && "29117_name" %in% names(data)) {
      data <- match_cepalstat_labels(data)
      env$last_year[i] <- suppressWarnings(max(as.numeric(data$`29117_name`), na.rm = TRUE))
    }
  }
}

# ---- last update info ----

env %<>% mutate(last_update = as.Date(NA)) # initialize

for(i in 1:nrow(env)) {
  meta <- get_indicator_metadata(env$id[i])
  last_update <- meta %>% filter(variable == "last_update") %>% pull(value)
  last_update %<>% str_squish() %>% mdy_hm() %>% as_date()
  env$last_update[i] <- last_update
}


# ---- last year of data ----

env %<>% mutate(last_year = as.numeric(NA)) # initialize

for(i in 1:nrow(env)) {
  if(i == 69){ # this indicator data has some NAs and doesn't pass quality checks -- manually update year
    year = 2021
  } else if(i == 87) { # keep NA, there is no year field (yet) for this indicator
    
  } else {
    data <- get_cepalstat_data(env$id[i])
    year <- match_cepalstat_labels(data)
    year %<>% summarize(max = max(`29117_name`)) %>% pull(as.numeric(max))
  }
  env$last_year[i] <- year
}



## 5. Add Anuario (Yearbook) info ----

anuario_plan <- read_xlsx(here("CEPALSTAT Review/anuario_plan_2025.xlsx"),
                          sheet = "Indicator Plan", skip = 0) %>%
  rename(id = `ID INDICADOR CEPALSTAT`, entrega = matches("^FECHA DE ENTREGA")) %>%
  mutate(id = as.numeric(str_replace_all(id, "[\r\n]", ""))) %>%
  filter(!is.na(entrega), !is.na(id)) %>%
  mutate(anuario_entrega = ifelse(str_detect(entrega, "sept"), "1_Sept", "2_Nov")) %>%
  distinct(id, anuario_entrega)

env <- env %>%
  left_join(anuario_plan, by = "id")


## 6. Add local code tracking fields ----

# Start empty (these get auto-filled when indicators are processed)
env <- env %>%
  mutate(
    script_version = NA_character_,
    last_run = as.Date(NA),
    notes = NA_character_
  )


## 7. Add regional/country profile fields

prof <- read_xlsx(here("Data/profile_data.xlsx"))

# check if there are any ids in prof that don't match to env -- change this with Alberto/Andrés
# prof %>% 
#   left_join(env, by = c("Indicator" = "id")) %>%
#   filter(is.na(indicator)) %>% 
#   filter(!str_detect(Notes, "Econ") | is.na(Notes)) %>% View()

# get list of indicators that are present in profiles
prof_list <- prof %>% distinct(Indicator) %>% mutate(profile = "Y")

# join prof list to env
env %<>% left_join(prof_list, by = c("id" = "Indicator"))

# update anuario tag
env %<>% mutate(anuario = ifelse(!is.na(anuario_entrega), "Y", anuario_entrega))

# organize indicators
env %<>% 
  relocate(anuario, .before = deprecated) %>% 
  relocate(profile, .before = deprecated) %>%
  select(-anuario_entrega) # remove this field bc was only applicable for 2025


## 8. Export master metadata table ----


# write_xlsx(env, here("Data/indicator_metadata.xlsx"))
# env <- read_xlsx(here("Data/indicator_metadata.xlsx"))


#### UPDATE CEPALSTAT METADATA ----------

## write code for this later to update the fields that come from the CEPALSTAT API

