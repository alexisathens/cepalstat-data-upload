library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(assertthat)

source(here("utils.R"))

# This script transforms the raw files at Pilot/Data/Raw/fao into flat data files for each indicator.

input_path <- here("Pilot", "Data", "Raw", "fao")

iso <- read_csv(here("Data", "iso_codes.csv"))
lac_iso <- iso %>% filter(ECLAC == "Y") %>% select(name, iso, cepalstat)

#### GRUPO 01: land use ####

grupo1_raw <- readRDS(here(input_path, "fao_land_use.rds"))
grupo1 <- grupo1_raw

grupo1

## Basic cleaning to long format

grupo1 %<>% 
  full_join(lac_iso, by = c("area" = "name"))

# check countries in cepalstat but not in fao
grupo1 %>% filter(!is.na(cepalstat) & is.na(area_code))

# filter just on available LAC countries
grupo1 %<>% filter(!is.na(cepalstat) & !is.na(area_code))

# join years ids too
years_id <- get_full_dimension_table(29117)
years_id %<>% mutate(name = as.numeric(name))
years_id %<>% select(name, years_id = id)

grupo1 %<>% left_join(years_id, by = c("year" = "name"))
assert_that(!anyNA(grupo1$years_id), msg = "'years_id' contains NAs.") # throw error if not all years match

# select relevant columns only
grupo1 %<>% 
  select(area, year, item, value, cepalstat, years_id) %>% 
  rename(Country = area, Years = year, country_id = cepalstat)

grupo1


# ---- IND-2035: country area ----

id <- 2035
i2035 <- grupo1

get_indicator_dimensions(id)



