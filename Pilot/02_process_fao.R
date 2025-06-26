library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
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
# could create a lookup table for this rather than calling it each time

get_full_dimension_table(21899)

# i2035 %>% distinct(item) %>% View()

# manually match over and force labels to match cepalstat
i2035 %<>% 
  filter(item %in% c("Country area", "Land area", "Inland waters")) %>% 
  mutate(item = case_when(
    item == "Country area" ~ "Total area",
    item == "Inland waters" ~ "Area of inland waters",
    TRUE ~ item
  ))

i2035_check <- i2035 %>% 
  pivot_wider(names_from = item, values_from = value) %>% 
  mutate(diff = round(`Total area` - `Land area` - `Area of inland waters`))

# check for leftover values... should in theory net out to 0...
i2035_check %>% filter(diff != 0) # %>% View()
# these are problematic... country land area shouldn't change by the year
# for now, continue processing data to look at before/after comparison with published CEPALSTAT data to see whether this is a new issue or not

# join extra dim ids
i2035 %<>% 
  rename(`Type of area` = item) %>% 
  left_join(get_full_dimension_table(21899) %>% select(name, id), by = c("Type of area" = "name")) %>% 
  rename(area_id = id)

# pare down to minimum df for cepalstat formatting
i2035 %<>% select(ends_with("_id"), "value")

# format for wasabi
i2035f <- format_for_wasabi(i2035, 2035)

i2035f

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Write to cleaned folder
write_xlsx(data, here("Pilot", "Data", "Cleaned", glue("id{indicator_id}_{dt_stamp}.xlsx")))
