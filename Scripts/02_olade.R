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
# See data cleaning notes at: cepalstat-data-upload\Data\Raw\olade\energy_indicators_overview.xlsx

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

input_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

# read in indicator metadata
meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))


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

rm(header_row, unit_row)


# ---- indicator 1754 — Consumption of electric power ----

indicator_id <- 1754

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1754 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_1754 <- function(data) {
  data %>%
    select(-Type) # only type is "Electricidad"
}

transform_1754 <- function(data) {
  data # no transformations needed
}

footnotes_1754 <- function(data) {
  data # keep footnotes_id as empty
}

source_1754 <- function() {
  NULL # use default source
}

result_1754 <- process_indicator(
  indicator_id = indicator_id,
  data = grupo4,
  dim_config = dim_config_1754,
  filter_fn = filter_1754,
  transform_fn = transform_1754,
  footnotes_fn = footnotes_1754,
  source_fn = source_1754,
  diagnostics = TRUE,
  export = FALSE  # set to TRUE when ready to export
)
