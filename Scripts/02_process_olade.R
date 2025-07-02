library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
library(here)

# This script does the full cleaning and standardizing of OLADE indicators

# See data cleaning notes at: cepalstat-data-upload\Data\Raw\olade\energy_indicators_overview.xlsx
# This spreadsheet outlines Alberto's step-by-step cleaning instructions and organizes the olade indicators into sub-groups

# ---- setup ----

source(here("Scripts/utils.R"))

input_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids

iso <- read_csv(here("Data/iso_codes.csv"))

iso %<>% 
  filter(!is.na(spanish_short)) %>% 
  select(name, spanish_short, cepalstat)


# ---- cleaning helpers ----

remove_headers <- function(df, header_row, unit_row) {
  df %>% 
    anti_join(header_row) %>% 
    anti_join(unit_row)
}

standardize_headers <- function(header_row) {
  header <- unlist(header_row, use.names = FALSE) %>% str_trim()
  header[1] <- "Country"
  header
}

fix_country_names <- function(df) {
  df %>%
    mutate(Country = case_when(
      Country == "Grenada" ~ "Granada",
      Country == "Trinidad & Tobago" ~ "Trinidad y Tabago",
      TRUE ~ Country
    ))
}

# ---- DATA CLEANING ----

# ---- GRUPO 1 ----

grupo1 <- read_excel(paste0(input_path, "/olade_grupo1.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo1[3,]
unit_row <- grupo1[4,]

# Remove rows that match these patterns
grupo1 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo1) <- standardize_headers(header_row)

# Create year field
grupo1 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo1 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Check which countries in olade don't match to iso
# grupo1 %>% 
#   filter(!Country %in% iso$spanish_short) %>% 
#   distinct(Country)

# Force olade to match cepalstat names
grupo1 %<>% 
  fix_country_names() %>%
  filter(Country %in% iso$spanish_short)

# Finally make long
grupo1 %<>% 
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))


### ---- IND-2486 ----

# ---- match dimensions and labels ----

# ---- harmonize labels and filter to final set ----

# ---- add summary groups ----

# ---- translate names ----

# ---- join CEPALSTAT dimension IDs ----

# ---- add metadata fields and export ----



### ---- IND-3154 ----

# ---- match dimensions and labels ----

# ---- harmonize labels and filter to final set ----

# ---- add summary groups ----

# ---- translate names ----

# ---- join CEPALSTAT dimension IDs ----

# ---- add metadata fields and export ----

