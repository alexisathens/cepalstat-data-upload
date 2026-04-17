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

input_path <- here("Data/Raw/olade")
output_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

# ---- cleaning functions ----

# Generic cleaning function for files with countries as rows
clean_olade_standard <- function(input_file, output_file) {

  # Read data
  data <- read_excel(paste0(input_path, "/", input_file), col_names = FALSE)
  
  # Extract and apply column headers from row 3; rename first column to "Country"
  header_row <- unlist(data[3, ], use.names = FALSE) %>% str_trim()
  header_row[1] <- "Country"
  
    # Remove header and unit rows, then apply column names
  data <- data[-c(3,4), ]
  colnames(data) <- header_row
  
  # Extract year from rows where Country field contains a 4-digit year,
  # fill downward to tag each country block, then remove those year header rows
  data %<>%
    mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>%
    fill(Years, .direction = "down") %>%
    select(Country, Years, everything()) %>%
    filter(!str_detect(Country, "\\b\\d{4}$"), Country != "Supply and demand series")
  
  # Standardize country names against ISO reference table
  data %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name)
  
  # Check which countries don't match to ISO (uncomment to diagnose)
  # data %>%
  #   filter(!Country %in% iso$name) %>%
  #   distinct(Country) %>%
  #   print()
  
  # Filter to LAC countries, excluding regional aggregates
  data %<>%
    filter(Country %in% iso$name, !Country %in% c("Central America", "South America", "Caribbean"))
  
  # Pivot to long format and drop rows with no reported value
  data %<>%
    pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
    mutate(value = as.numeric(value), Years = as.numeric(Years)) %>%
    filter(!is.na(value))
  
  # Export
  write.csv(data, file = file.path(output_path, output_file), row.names = FALSE)
  message(glue("✓ Exported {output_file}"))
  return(data)
}

# Generic cleaning function for grupos with Years as columns (grupos 7, 8)
clean_grupo_years_as_cols <- function(input_file, output_file, filter_pattern = NULL) {

  # Read data
  data <- read_excel(paste0(input_path, "/", input_file), col_names = FALSE)

  # Extract header and unit rows (different row for these grupos)
  header_row <- data[3,]
  unit_row <- data[2,]

  # Remove header rows
  data %<>% remove_headers(header_row, unit_row)

  # Format header row
  colnames(data) <- standardize_headers(header_row)

  # Remove year header and first row
  data %<>%
    filter(!str_detect(Country, "\\b\\d{4}$")) %>%
    filter(Country != "Series de oferta y demanda")

  # Apply additional filter if provided
  if (!is.null(filter_pattern)) {
    data %<>% filter(!grepl(filter_pattern, Country))
  }

  # Standardize country names
  data %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name)

  # Check which countries don't match to iso
  # data %>%
  #   filter(!Country %in% iso$name) %>%
  #   distinct(Country) %>%
  #   print()

  # Filter to LAC countries only
  data %<>%
    filter(Country %in% iso$name) %>%
    filter(!Country %in% c("Central America", "South America", "Caribbean"))

  # Pivot to long format (Years as columns, no Type dimension)
  data %<>%
    pivot_longer(cols = -Country, names_to = "Years", values_to = "value") %>%
    filter(!is.na(value))

  # Export
  write.csv(data, file = file.path(output_path, output_file), row.names = FALSE)

  message(glue("✓ Exported {output_file}"))

  return(data)
}

####


# ---- total energy production ----

production <- clean_olade_standard("energy_production_raw.xlsx", "energy_production_clean.csv")

# ---- total energy supply ----

supply <- clean_olade_standard("energy_supply_raw.xlsx", "energy_supply_clean.csv")

# ---- total final consumption ----

consumption <- clean_olade_standard("energy_consumption_raw.xlsx", "energy_consumption_clean.csv")

# ---- final consumption by sector ----

consumption_sector <- clean_olade_standard("energy_consumption_sector_raw.xlsx", "energy_consumption_sector_clean.csv")



#### OLD CLEANING ------------------------------------------

# ---- GRUPO 1 ----

grupo1 <- clean_grupo_standard("olade_grupo1.xlsx", "grupo1_raw.csv")

# ---- GRUPO 2 ----

# Custom structure: Activities as rows, countries as columns
file_path_g2 <- paste0(input_path, "/olade_grupo2.xlsx")
data_g2 <- read_excel(file_path_g2, col_names = FALSE)

# Extract header and unit rows
header_row_g2 <- data_g2[3,]
unit_row_g2 <- data_g2[4,]

# Remove header rows
data_g2 %<>% remove_headers(header_row_g2, unit_row_g2)

# Format header row
colnames(data_g2) <- standardize_headers(header_row_g2)

# Create year field
data_g2 %<>%
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>%
  fill(Years, .direction = "down") %>%
  select(Country, Years, everything())

# Remove year header rows
data_g2 %<>%
  filter(!str_detect(Country, "\\b\\d{4}$")) %>%
  filter(Country != "Series de oferta y demanda")

# The "Country" column actually contains activities - rename it
data_g2 %<>%
  rename(Activity = Country)

# Pivot to long format: Activities to rows, countries to columns → need to pivot countries
data_g2 %<>%
  pivot_longer(cols = -c(Activity, Years), names_to = "Country", values_to = "value") %>%
  mutate(value = as.numeric(value)) %>%
  filter(!is.na(value))

# Standardize country names
data_g2 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Filter to LAC countries only
data_g2 %<>%
  filter(Country %in% iso$name) %>%
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Reorder columns
data_g2 %<>%
  select(Country, Years, Activity, value)

# Export
write.csv(data_g2, file = file.path(output_path, "grupo2_raw.csv"), row.names = FALSE)

message(glue("✓ Exported grupo2_raw.csv"))


# ---- GRUPO 3 ----

grupo3 <- clean_grupo_standard("olade_grupo3.xlsx", "grupo3_raw.csv")


# ---- GRUPO 4 ----

grupo4 <- clean_grupo_standard("olade_grupo4.xlsx", "grupo4_raw.csv")


# ---- GRUPO 5 ----

## *** this is the excel file with the incorrect Brazil data - manually fix for now
grupo5 <- clean_grupo_standard("olade_grupo5.xlsx", "grupo5_raw.csv")


# ---- GRUPO 6 ----

grupo6 <- clean_grupo_standard("olade_grupo6.xlsx", "grupo6_raw.csv")


# ---- GRUPO 7 ----

# Years as columns, filter out rows with "-"
grupo7 <- clean_grupo_years_as_cols("olade_grupo7.xlsx", "grupo7_raw.csv", filter_pattern = "-")


# ---- GRUPO 8 ----

# Years as columns, filter out rows with "-" or ":"
grupo8 <- clean_grupo_years_as_cols("olade_grupo8.xlsx", "grupo8_raw.csv", filter_pattern = "[-:]")


# ---- GRUPO 9 ----

# Multi-sheet structure - each year is a separate tab
# Data format is unique: energy sources as rows, countries as columns

file_path <- paste0(input_path, "/olade_grupo9.xlsx")
all_sheets <- excel_sheets(file_path)

# Filter sheets from "10.2000" onward (prefix >= 10)
sheets_to_read <- all_sheets[str_extract(all_sheets, "^\\d+") %>% as.numeric() >= 10]

# Energy source categories
fuentes <- c("Nuclear", "Térmica no renovable (combustión)", "Térmica renovable (combustión)",
             "Hidro", "Geotermia", "Eólica", "Solar")

# Read and combine all sheets
grupo9 <- map_dfr(sheets_to_read, function(sheet) {
  df <- read_excel(file_path, sheet = sheet, col_names = FALSE)

  # Extract headers
  header_row <- df[3,]
  unit_row <- df[2,]

  df %<>%
    remove_headers(header_row, unit_row) %>%
    setNames(standardize_headers(header_row)) %>%
    mutate(Years = str_extract(sheet, "\\d{4}")) %>%
    filter(Country %in% fuentes) %>%
    select(-Unidad) %>%
    pivot_longer(!c(Country, Years), names_to = "Country_name") %>%
    rename(Type = Country, Country = Country_name) %>%
    select(Country, Years, Type, value)

  return(df)
})

# Standardize country names and filter
grupo9 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name) %>%
  filter(Country %in% iso$name) %>%
  filter(!Country %in% c("Central America", "South America", "Caribbean")) %>%
  mutate(value = as.numeric(value)) %>%
  filter(!is.na(value))

# Check unmatched countries
# grupo9 %>% filter(!Country %in% iso$name) %>% distinct(Country) %>% print()

# Export
write.csv(grupo9, file = file.path(output_path, "grupo9_raw.csv"), row.names = FALSE)

message(glue("✓ Exported grupo9_raw.csv"))
