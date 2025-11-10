library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
library(here)
library(assertthat)
library(quarto)
library(CepalStatR)
library(FAOSTAT)

# This script cleans and standardizes the CAIT-WRI / Climate Watch indicators

# ---- download ----

## Instructions for DOWNLOADING the Climate Watch data (Step 01):
# 01_climatewatch_instructions.qmd
# This was translated and formatted from Alberto's step-by-step guide

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# Read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

# ---- read downloaded files ----
cw_path <- here("Data/Raw/climate watch")

country_3159 <- read_csv(paste0(cw_path, "/3159-cait-elucf-co2-total-lac.csv")) %>% filter(iso != "Data source")
region_3159 <- read_csv(paste0(cw_path, "/3159-cait-elucf-co2-total-regional.csv")) %>% filter(iso != "Data source")
data_3159 <- bind_rows(country_3159, region_3159)

country_3351 <- read_csv(paste0(cw_path, "/3351-cait-sectors-ghg-total-lac.csv")) %>% filter(iso != "Data source")
region_3351 <- read_csv(paste0(cw_path, "/3351-cait-sectors-ghg-total-regional.csv")) %>% filter(iso != "Data source")
data_3351 <- bind_rows(country_3351, region_3351)


## ---- indicator 3159 - share of carbon dioxide (CO₂) emissions relative to the global total ----

indicator_id <- 3159 # share of co2

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_3159 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3159 <- function(data) {
  data %>% 
    rename(Country = `Country/Region`) %>% 
    select(-iso, -unit) %>% 
    pivot_longer(!Country, names_to = "Years")
}

transform_3159 <- function(data) {
  world <- data %>% 
    filter(Country == "World")
  
  data %<>% 
    filter(Country != "World") %>% 
    left_join(world, by = "Years", suffix = c("", ".wld")) %>% 
    mutate(prop = value/value.wld*100) %>% 
    select(Country, Years, value = prop)
}

# regional_3159 <- function(data) {
#   eclac_totals <- data %>%
#     group_by(across(all_of(setdiff(names(df), c("Country", "value", "area"))))) %>%
#     summarise(value = sum(value, na.rm = TRUE),
#               area = sum(area, na.rm = TRUE), .groups = "drop") %>%
#     mutate(Country = "Latin America and the Caribbean")
# 
#   data <- bind_rows(data, eclac_totals) %>%
#     mutate(value = value/area) %>%
#     arrange(Country, Years) %>%
#     select(Country, Years, value)
# 
#   return(data)
# }

footnotes_3159 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

# overwrite this once because I messed it up
# source_3159 <- function(indicator_id) {
#   1742 # CAIT Explorador de Datos Climáticos
# }

result_3159 <- process_indicator(
  indicator_id = 3159,
  data = data_3159,
  dim_config = dim_config_3159,
  filter_fn = filter_3159,
  transform_fn = transform_3159,
  # regional_fn = regional_3159, # default to sum of lac
  footnotes_fn = footnotes_3159,
  # source_fn = source_3159,
  diagnostics = TRUE,
  export = TRUE
)
