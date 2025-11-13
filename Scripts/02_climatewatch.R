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

# Visit: https://www.climatewatchdata.org/data-explorer/historical-emissions? 
# For information on the API and bulk data downloads


## Instructions for DOWNLOADING the Climate Watch data (Step 01): ---- convert this into code now that the API can be used
# 01_climatewatch_instructions.qmd
# This was translated and formatted from Alberto's step-by-step guide

# Define API endpoint
url <- "https://www.climatewatchdata.org/api/v1/data/historical_emissions"

# Get the API json and convert to R object
result <- request(url) %>% 
  req_perform() %>% 
  resp_body_json(simplifyVector = TRUE)

# Grab main data table and expand the nested list column
cw <- result$data %>% 
  as_tibble() %>% 
  unnest(emissions)

# Basic data cleaning
cw %<>% 
  filter(data_source == "Climate Watch") %>% 
  select(-data_source)


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

data_3351 <- bind_rows(
  read_csv(paste0(cw_path, "/3351-cait-ag-ghg-total-lac.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "ag"),
  read_csv(paste0(cw_path, "/3351-cait-ag-ghg-total-regional.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "ag"),
  read_csv(paste0(cw_path, "/3351-cait-energy-ghg-total-lac.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "energy"),
  read_csv(paste0(cw_path, "/3351-cait-energy-ghg-total-regional.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "energy"),
  read_csv(paste0(cw_path, "/3351-cait-industrial-ghg-total-lac.csv"), col_types = cols(`1990` = col_double())) %>% 
    filter(iso != "Data source") %>% mutate(sector = "industrial"),
  read_csv(paste0(cw_path, "/3351-cait-industrial-ghg-total-regional.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "industrial"),
  read_csv(paste0(cw_path, "/3351-cait-waste-ghg-total-lac.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "waste"),
  read_csv(paste0(cw_path, "/3351-cait-waste-ghg-total-regional.csv")) %>% filter(iso != "Data source") %>% mutate(sector = "waste")
)


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


## ---- indicator 3351 — greenhouse gas (GHG) emissions by sector ----

indicator_id <- 3351 # ghg emissions

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_3351 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "63371"),
  pub_col = c("208_name", "29117_name", "63371_name")
)

filter_3351 <- function(data) {
  data %>% 
    rename(Country = `Country/Region`, Type = sector) %>% 
    select(-iso, -unit) %>% 
    pivot_longer(!c(Country, Type), names_to = "Years") %>% 
    mutate(Type = case_when(
      Type == "ag" ~ "Agriculture",
      Type == "energy" ~ "Energy",
      Type == "industrial" ~ "Industrial processes",
      Type == "waste" ~ "Waste",
      TRUE ~ Type
    )) %>% 
    mutate(value = ifelse(Country == "Bahamas" & Years == 1990 & is.na(value), 0.00, value)) # fill in single missing value
}

transform_3351 <- function(data) {
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    mutate(Type = "Total, excluding land use change and forestry")
  
  data %>% 
    bind_rows(total)
}

footnotes_3351 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_3351 <- process_indicator(
  indicator_id = 3351,
  data = data_3351,
  dim_config = dim_config_3351,
  filter_fn = filter_3351,
  transform_fn = transform_3351,
  # regional_fn = regional_3351, # default to sum of lac
  footnotes_fn = footnotes_3351,
  # source_fn = source_3351,
  diagnostics = TRUE,
  export = TRUE
)
