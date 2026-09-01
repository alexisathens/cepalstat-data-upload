library(tidyverse)
library(magrittr)
library(readxl)
library(writexl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(lubridate)
library(CepalStatR)

source(here("Scripts/utils.R"))

# Create basic metadata table with one row per indicator and the following values:
# id, indicator, cat1, cat2, cat3, source_id, source, dim_id, dimension, yearbook, profile, system, notes

## 1. Retrieve full list of environmental indicators ----

ind <- call.indicators() %>% as_tibble()

env <- ind %>%
  filter(Area == "Environmental" & !is.na(`Indicator ID`))

env %<>% 
  rename(cat1 = Dimension, cat2 = Subdimension, cat3 = Group,
         indicator = `Indicator Name`, id = `Indicator ID`) %>% 
  select(id, indicator, cat1, cat2, cat3)

env_ids <- env$id


## 2. Get indicator source ----

env <- env %>% mutate(source_id = NA_integer_, source = NA_character_)

for(i in 1:nrow(env)) {
  message(glue("Getting source ", i, " of ", nrow(env)))
  source <- get_indicator_sources(env$id[i])
  # if(nrow(source) > 1) { stop() } # taking the first entry seems to work best
  # source %<>% slice(1)
  id <- source %>% pull(id)
  name <- source %>% pull(organization_acronym)
  env$source_id[i] <- id
  env$source[i] <- name
  Sys.sleep(2) # give API a break
}

# make manual adjustments

env %<>% 
  mutate(source = ifelse(source == "CEPAL", "ECLAC", source))


## 3. Get indicator dimensions ----

all_dims <- map_dfr(env_ids, function(id) {
  message(glue("Getting dimensions of ", id))
  Sys.sleep(2) # give API a break
  dims <- get_indicator_dimensions(id) |> 
    filter(!name %in% c("Country__ESTANDAR", "Years__ESTANDAR", "Reporting Type"))
  
  tibble(
    id = id,
    dim_id = paste(unique(dims$id), collapse = "; "),
    dimension = paste(unique(dims$name), collapse = "; ")
  )
})

env <- env %>%
  left_join(all_dims, by = "id")


## 4. Get yearbook flag

# read most recent (2026) yearbook index
anuario <- read_xlsx(
  here("Data/AE2026-Indice-WEB-PDF.xlsx"),
  sheet = "AE2026-HTML&PDF_ID",
  .name_repair = "unique_quiet"   # repairs names without messaging
)

# get list of indicators that are present in anuario
anuario_list <- anuario %>% rename(indicator = `ID INDICADOR CEPALSTAT`) %>% 
  distinct(indicator) %>% mutate(yearbook = "Y") %>% mutate(indicator = as.numeric(indicator)) %>% 
  filter(!is.na(indicator))

# join prof list to env
env %<>% left_join(anuario_list, by = c("id" = "indicator")) %>% 
  mutate(yearbook = ifelse(is.na(yearbook), "", yearbook))


## 5. Get profiles flag

prof <- read_xlsx(here("Data/profile_data.xlsx"))

# check if there are any ids in prof that don't match to env -- change this with Alberto/Andrés
prof %>%
  left_join(env, by = c("Indicator" = "id")) %>%
  filter(is.na(indicator)) %>%
  filter(!str_detect(Notes, "Econ") | is.na(Notes)) #%>% View()

# get list of indicators that are present in profiles
prof_list <- prof %>% distinct(Indicator) %>% mutate(profile = "Y")

# join prof list to env
env %<>% left_join(prof_list, by = c("id" = "Indicator")) %>% 
  mutate(profile = ifelse(is.na(profile), "", profile))


## 6. Define management system variable

env %<>% 
  mutate(system = case_when(
    source == "SDG" ~ "sdg",
    id %in% c(5417, # manually define variables that originate from subnational/geospatial data
              5418,
              5422,
              5423,
              5424,
              5425) ~ "geo",
    TRUE ~ "env" # else part of my R-based system
  ))

## 8. Add notes

env %<>% 
  mutate(notes = case_when(
    id == 1755 ~ "historical series that ends in 2015 and does not need to be maintained",
    id == 5730 ~ "this used to be indicator 2041",
    id == 5672 ~ "this used to be indicator 2040",
    TRUE ~ ""
  ))

## 8. Export master metadata table ----

env %<>% 
  select(id, indicator, cat1:cat3, source_id, source, dim_id, dimension, yearbook, profile, system, notes)


# writexl::write_xlsx(list(metadata = env), here("Data/indicator_metadata.xlsx"))
# env <- read_xlsx(here("Data/indicator_metadata.xlsx"))

