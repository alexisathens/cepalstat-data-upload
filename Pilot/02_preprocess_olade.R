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


### grupo-specific cleaning:

# Make data long
grupo1 %<>% 
  pivot_longer(cols = -c(Country, Years),
               names_to = "Type")

# Store types for reference
energy_types <- unique(grupo1$Type)


# ---- 2487: primary & secondary energy supply ----

i2487 <- grupo1

get_indicator_dimensions(2487)
#d2487 <- get_dimension_table(44966)
d2487 <- get_published_dimension_table(2487, 44966, lang = "es")


### CONTINUE TOMORROW: 
# - Alberto thinks wind and solar may have been added into "other primary", double check if these totals to verify
# - Decisions on whether to keep more types and integrate into CEPALSTAT
# - Check whether totals were calculated by retained types or using the OLADE fields

i2487 %>% 
  filter(!Type %in% d2487$name) %>% 
  distinct(Type)





#### GRUPO 04: unidad - energetico - unidad - consumo energetico - electricidad ####

## Clean into standard flat data format

grupo4 <- read_excel(paste0(input_path, "olade_grupo4.xlsx"), col_names = FALSE)

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo4[3,]
unit_row <- grupo4[4,]

# Remove rows that match these patterns
grupo4 %<>% 
  anti_join(header_row) %>% 
  anti_join(unit_row)

# Format header row
header_row %<>% unlist(use.names = FALSE) %>% str_trim()
header_row[1] <- "Country"
colnames(grupo4) <- header_row

# Create year field
grupo4 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo4 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Filter countries
iso <- read_csv("../Data/iso_codes.csv")

iso %<>% 
  filter(!is.na(spanish_short)) %>% 
  select(name, spanish_short)

# Check what countries don't match over that should
grupo4 %>% 
  filter(!Country %in% iso$spanish_short) %>% 
  distinct(Country)

# Force OLADE names to match CEPALSTAT names
grupo4 %<>% 
  mutate(Country = case_when(
    Country == "Grenada" ~ "Granada",
    Country == "Trinidad & Tobago" ~ "Trinidad y Tabago",
    TRUE ~ Country
  ))

# Filter on CEPALSTAT countries only
grupo4 %<>% 
  filter(Country %in% iso$spanish_short)


### grupo-specific cleaning:

grupo4 %<>% rename(value = Electricidad)



# ---- 1754: consumption of electric power ----

i1754 <- grupo4

i1754
