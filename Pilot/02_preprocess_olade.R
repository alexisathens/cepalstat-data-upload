library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)

# This script transforms the raw files at Pilot/Data/Raw/olade into flat data files for each indicator.

input_path <- "Data/Raw/olade/"

#### GRUPO 01: unidad - energetico - 103 bep - oferta total ####

## Clean into standard flat data format

grupo1 <- read_excel(paste0(input_path, "olade_grupo1.xlsx"), col_names = FALSE)

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo1[3,]
unit_row <- grupo1[4,]

# Remove rows that match these patterns
grupo1 %<>% 
  anti_join(header_row) %>% 
  anti_join(unit_row)

# Format header row
header_row %<>% unlist(use.names = FALSE) %>% str_trim()
header_row[1] <- "Country"
colnames(grupo1) <- header_row

# Create year field
grupo1 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo1 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Filter countries
iso <- read_csv("../Data/iso_codes.csv")

iso %<>% 
  filter(!is.na(spanish_short)) %>% 
  select(name, spanish_short)

# Check what countries don't match over that should
grupo1 %>% 
  filter(!Country %in% iso$spanish_short) %>% 
  distinct(Country)

# Force OLADE names to match CEPALSTAT names
grupo1 %<>% 
  mutate(Country = case_when(
    Country == "Grenada" ~ "Granada",
    Country == "Trinidad & Tobago" ~ "Trinidad y Tabago",
    TRUE ~ Country
  ))

# Filter on CEPALSTAT countries only
grupo1 %<>% 
  filter(Country %in% iso$spanish_short)

# Make data long
grupo1 %<>% 
  pivot_longer(cols = -c(Country, Years),
               names_to = "Type")


# ---- 2487: primary & secondary energy supply ----

