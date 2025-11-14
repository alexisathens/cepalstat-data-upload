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

# run script 01_climatewatch.R to download raw data from API

# Visit: https://www.climatewatchdata.org/data-explorer/historical-emissions? 
# For information on the API and bulk data downloads

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# Read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y" | name == "World") %>% 
  select(cepalstat, name, std_name)

# ---- read downloaded files ----
cw_path <- here("Data/Raw/climate watch")

data_2027 <- read_csv(paste0(cw_path, "/2027_raw.csv"))
# data_3158 <- read_csv(paste0(cw_path, "/3158_raw.csv"))
data_3159 <- read_csv(paste0(cw_path, "/3159_raw.csv"))
data_3351 <- read_csv(paste0(cw_path, "/3351_raw.csv"))


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
    rename(Country = country, Years = year) %>% 
    select(Country, Years, value)
}

transform_3159 <- function(data) {
  world <- data %>% 
    filter(Country == "World")
  
  data %>% 
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
    rename(Country = country, Type = sector, Years = year) %>% 
    select(Country, Type, Years, value) %>% 
    mutate(Type = case_when(
      Type == "Industrial Processes" ~ "Industrial processes",
      TRUE ~ Type
    ))
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


## ---- indicator 2027 — carbon dioxide (CO₂) emissions (Total, per capita, and per GDP) ----

indicator_id <- 2027 # co2 emissions

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2027 <- tibble(
  data_col = c("Country", "Years", "Calculation"),
  dim_id = c("208", "29117", "26653"),
  pub_col = c("208_name", "29117_name", "26653_name")
)

filter_2027 <- function(data) {
  data %>%
    rename(
      Country = country,
      Years = year
    ) %>%
    mutate(
      total_emissions = as.numeric(emissions),
      population = as.numeric(population),
      gdp_constant_2015_usd = as.numeric(gdp_constant_2015_usd)
    ) %>%
    select(
      Country, Years, unit,
      Total = total_emissions, Pop = population, GDP = gdp_constant_2015_usd
    )
}

transform_2027 <- function(data) {
  data
}

regional_2027 <- function(data) {
  # create eclac total
  eclac_totals <- data %>%
    filter(Country != "World") %>% 
    group_by(Years) %>%
    summarise(Total = sum(Total, na.rm = TRUE),
              Pop = sum(Pop, na.rm = TRUE),
              GDP = sum(GDP, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  # manually subtract out venezuela with missing GDP data
  missing_vz_gdp <- (nrow(data %>% filter(Country == "Venezuela" & is.na(GDP))) > 0)
  
  if(missing_vz_gdp) {
    eclac_totals_gdp_adj <- data %>%
      filter(Country != "World",
             Country != "Venezuela") %>%          # exclude only here
      group_by(Years) %>%
      summarise(
        Total_noVEN = sum(Total, na.rm = TRUE),      # emissions excluding VEN
        GDP_noVEN = sum(GDP, na.rm = TRUE),       # GDP excluding VEN
        .groups = "drop"
      ) %>%
      mutate(
        Country = "Latin America and the Caribbean"
      )
    
    eclac_totals %<>%
      left_join(eclac_totals_gdp_adj, by = c("Years", "Country"))
  }
    
  # join together
  data <- bind_rows(data, eclac_totals) 
  
  # create calculation columns
  data %<>% 
    mutate(`Total, excluding land change use and forestry` = Total,
           `Per capita` = Total / Pop * 1e6,
           `By gross domestic product` = Total / GDP * 1e12) %>% 
    select(-unit)
  
  if(missing_vz_gdp) {
    data %<>% 
      mutate(`By gross domestic product` = ifelse(Country == "Latin America and the Caribbean",
                                                         Total_noVEN / GDP_noVEN * 1e12,
                                                         `By gross domestic product`)) %>% 
      select(-Total_noVEN, -GDP_noVEN)
  }
  
  # clean df
  data %<>% 
    select(-Total, -Pop, -GDP) %>% 
    pivot_longer(cols = c("Total, excluding land change use and forestry", "Per capita", "By gross domestic product"),
                 names_to = "Calculation") %>% 
    filter(!is.na(value)) # remove venezuela total per GDP data
  
  return(data)
}

footnotes_2027 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
}

## Monday: export and review data

result_2027 <- process_indicator(
  indicator_id = indicator_id,
  data = data_2027,
  dim_config = dim_config_2027,
  filter_fn = filter_2027,
  transform_fn = transform_2027,
  footnotes_fn = footnotes_2027,
  regional_fn = regional_2027,
  diagnostics = TRUE,
  export = FALSE
)
