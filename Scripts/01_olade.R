library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
library(here)
library(assertthat)
library(CepalStatR)

# This script processes OLADE energy indicators using the automated process_indicator() function

### transition data download instructions from: ***
# See data cleaning notes at: cepalstat-data-upload\Data\Raw\olade\energy_indicators_overview.xlsx

input_path <- here("Data/Raw/olade")
output_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

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


# ---- GRUPO 4 ----

# ---- clean to long format ----

grupo4 <- read_excel(paste0(input_path, "/olade_grupo4.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo4[3,]
unit_row <- grupo4[4,]

# Remove rows that match these patterns
grupo4 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo4) <- standardize_headers(header_row)

# Create year field
grupo4 %<>%
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>%
  fill(Years, .direction = "down") %>%
  select(Country, Years, everything())

# Remove year header and first row
grupo4 %<>%
  filter(!str_detect(Country, "\\b\\d{4}$")) %>%
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo4 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo4 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo4 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo4 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo4 %<>%
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo4 %<>%
  filter(!is.na(value))

grupo4

write.csv(grupo4,
          file = file.path(output_path, "grupo4_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)


# ---- GRUPO 1 ----

# ---- clean to long format ----

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

# Overwrite country names with std_name in iso file
grupo1 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo1 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo1 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
grupo1 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo1 %<>%
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo1 %<>%
  filter(!is.na(value))

grupo1

write.csv(grupo1,
          file = file.path(output_path, "grupo1_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)


# ---- GRUPO 5 ----

# ---- clean to long format ----

grupo5 <- read_excel(paste0(input_path, "/olade_grupo5.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo5[3,]
unit_row <- grupo5[4,]

# Remove rows that match these patterns
grupo5 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo5) <- standardize_headers(header_row)

# Create year field
grupo5 %<>%
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>%
  fill(Years, .direction = "down") %>%
  select(Country, Years, everything())

# Remove year header and first row
grupo5 %<>%
  filter(!str_detect(Country, "\\b\\d{4}$")) %>%
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo5 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo5 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo5 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
grupo5 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo5 %<>%
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo5 %<>%
  filter(!is.na(value))

grupo5

write.csv(grupo5,
          file = file.path(output_path, "grupo5_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)


# ---- GRUPO 6 ----

# ---- clean to long format ----

grupo6 <- read_excel(paste0(input_path, "/olade_grupo6.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo6[3,]
unit_row <- grupo6[4,]

# Remove rows that match these patterns
grupo6 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo6) <- standardize_headers(header_row)

# Create year field
grupo6 %<>%
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>%
  fill(Years, .direction = "down") %>%
  select(Country, Years, everything())

# Remove year header and first row
grupo6 %<>%
  filter(!str_detect(Country, "\\b\\d{4}$")) %>%
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo6 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo6 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo6 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
grupo6 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo6 %<>%
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo6 %<>%
  filter(!is.na(value))

grupo6

write.csv(grupo6,
          file = file.path(output_path, "grupo6_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)


# ---- GRUPO 7 ----

# **note this group cleaning code is different from prior

# ---- clean to long format ----

grupo7 <- read_excel(paste0(input_path, "/olade_grupo7.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo7[3,]
unit_row <- grupo7[2,]

# Remove rows that match these patterns
grupo7 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo7) <- standardize_headers(header_row)

# Remove year header and first row
grupo7 %<>%
  filter(!str_detect(Country, "\\b\\d{4}$")) %>%
  filter(!grepl("-", Country)) %>% # **
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo7 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo7 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo7 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
grupo7 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo7 %<>%
  pivot_longer(cols = -Country, names_to = "Years", values_to = "value")

# Remove NAs
grupo7 %<>%
  filter(!is.na(value))

grupo7

write.csv(grupo7,
          file = file.path(output_path, "grupo7_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)


# ---- GRUPO 8 ----

# **note this group cleaning code is different from prior

# ---- clean to long format ----

grupo8 <- read_excel(paste0(input_path, "/olade_grupo8.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo8[3,]
unit_row <- grupo8[2,]

# Remove rows that match these patterns
grupo8 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo8) <- standardize_headers(header_row)

# Remove year header and first row
grupo8 %<>%
  filter(!str_detect(Country, "\\b\\d{4}$")) %>%
  filter(!grepl("[-:]", Country)) %>%  # **
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo8 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo8 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo8 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
grupo8 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo8 %<>%
  pivot_longer(cols = -Country, names_to = "Years", values_to = "value")

# Remove NAs
grupo8 %<>%
  filter(!is.na(value))

grupo8

write.csv(grupo8,
          file = file.path(output_path, "grupo8_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)


# ---- GRUPO 9 ----

# **note this group cleaning code is significantly different from prior
# data for each year is on a separate tab and country labels aren't always in the same order

# ---- clean to long format ----

# Define file path
file_path <- paste0(input_path, "/olade_grupo9.xlsx")

# List sheet names
all_sheets <- excel_sheets(file_path)

# Filter sheets starting from "10.2000" onward
# This extracts the numeric prefix and keeps only sheets where the number is >= 10
sheets_to_read <- all_sheets[str_extract(all_sheets, "^\\d+") %>% as.numeric() >= 10]

# Read and combine all filtered sheets
combined_df <- map_dfr(sheets_to_read, function(sheet) {

  df <- read_excel(file_path, sheet = sheet, col_names = FALSE)

  # Extract header and unit rows for each table in spreadsheet
  header_row <- df[3,]
  unit_row <- df[2,]

  # Remove rows that match these patterns
  df %<>%
    remove_headers(header_row, unit_row)

  # Format header row
  colnames(df) <- standardize_headers(header_row)

  # Add year row
  df$Years <- str_extract(sheet, "\\d{4}")

  # Filter only on rows with sources
  fuentes <- c("Nuclear", "Térmica no renovable (combustión)", "Térmica renovable (combustión)", "Hidro", "Geotermia", "Eólica", "Solar")

  df %<>%
    filter(Country %in% fuentes) %>%
    select(-Unidad)

  # Pivot long (to deal with different country availability)
  df %<>%
    pivot_longer(!c(Country, Years), names_to = "Country_name") %>%
    rename(Type = Country, Country = Country_name) %>%
    select(Country, Years, Type, value)

  return(df)
})

grupo9 <- combined_df

rm(combined_df, file_path, all_sheets, sheets_to_read)


## Clean into standard flat data format

# Overwrite country names with std_name in iso file
grupo9 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo9 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo9 %<>%
  filter(Country %in% iso$name)

# Filter out sub-regions
grupo9 %<>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Convert value to numeric
grupo9 %<>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo9 %<>%
  filter(!is.na(value))

grupo9

write.csv(grupo9,
          file = file.path(output_path, "grupo9_raw.csv"),
          row.names = FALSE)

rm(header_row, unit_row)
