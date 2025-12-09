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

# This script downloads, cleans, and standardizes the second delivery of FAO indicators in a more automated way

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# Read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

# Read in indicator metadata
meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

# Load information about all datasets into a data frame
fao_metadata <- FAOSTAT::search_dataset() %>% as_tibble()
# This shows the status of the data too, of the latest year and whether it's final

# Alternatively go here to see data areas: https://www.fao.org/faostat/en/#data


# ---- download FAO bulk data ----

# download land use (RL) data
rl <- get_faostat_bulk(code = "RL")
rl %<>% as_tibble()

# download climate change (ET) data
et <- get_faostat_bulk(code = "ET")
et %<>% as_tibble()

# download land cover (LC) data
lc <- get_faostat_bulk(code = "LC")
lc %<>% as_tibble()

# download crops and livestock products (QCL) data
qcl <- get_faostat_bulk(code = "QCL")
qcl %<>% as_tibble()

# download fertilizers by Nutrient (RFN) data
rfn <- get_faostat_bulk(code = "RFN")
rfn %<>% as_tibble()

# download pesticide use (RP) data
rp <- get_faostat_bulk(code = "RP")
rp %<>% as_tibble()

# MANUALLY download bulk data from fishstat / fish capture production
# at https://www.fao.org/fishery/statistics-query/en/capture/capture_quantity
fish_folder <- here("Data/Raw/fao fish and aqua//global capture production quantity 20-11-2025")
fish <- read_csv(paste0(fish_folder, "/Capture_Quantity.csv"))
fish_country_map <- read_csv(paste0(fish_folder, "/CL_FI_COUNTRY_GROUPS.csv"))
fish_species_map <- read_csv(paste0(fish_folder, "/CL_FI_SPECIES_GROUPS.csv"))
fish_water_map <- read_csv(paste0(fish_folder, "/CL_FI_WATERAREA_GROUPS.csv"))

# MANUALLY download bulk data from fishstat / aquaculture production
# at https://www.fao.org/fishery/statistics-query/en/aquaculture/aquaculture_quantity
aqua_folder <- here("Data/Raw/fao fish and aqua/global aquaculture production quantity 20-11-2025")
aqua <- read_csv(paste0(aqua_folder, "/Aquaculture_Quantity.csv"))
aqua_country_map <- read_csv(paste0(aqua_folder, "/CL_FI_COUNTRY_GROUPS.csv"))
aqua_species_map <- read_csv(paste0(aqua_folder, "/CL_FI_SPECIES_GROUPS.csv"))
aqua_water_map <- read_csv(paste0(aqua_folder, "/CL_FI_WATERAREA_GROUPS.csv"))
aqua_environment_map <- read_csv(paste0(aqua_folder, "/CL_FI_PRODENVIRONMENT.csv"))

# MANUALLY download data from aquastat
# at https://data.apps.fao.org/aquastat/?lang=en
# making these selections:
# Variable group: water use
# Variable subgroup: water withdrawal by sector
# Variable:
# - agricultural water withdrawal as % of total water withdrawal (for 4185)
# - industrial water withdrawal as % of total water withdrawal (for 4185)
# - municipal water withdrawal as % of total water withdrawal (for 4185)
# - agricultural water withdrawal (for 4186)
# Regions: Select Americas
# Years: Select 1990 through the most recent available year
aquastat_folder <- here("Data/Raw/fao fish and aqua/aquastat")
aquastat <- read_xlsx(paste0(aquastat_folder, "/AQUASTAT Dissemination System.xlsx"))


# FAO LAND USE (RL) INDICATORS -----

## ---- intermediate indicator - cropland area ----

# this variable is used as the denominator for the pesticide and fertilizer use intensity variables

result_cropland <- rl %>% 
  filter(item == "Cropland") %>% # Cropland = Arable Land + Permanent Crops
  filter(element == "area") %>% 
  filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla")) %>% 
  rename(Country = area, Years = year) %>% 
  mutate(Years = as.character(Years)) %>% 
  select(Country, Years, area = value)


## ---- indicator 2035 - country area ----
indicator_id <- 2035

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2035 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21899"),
  pub_col = c("208_name", "29117_name", "21899_name")
)

filter_2035 <- function(data) {
  data %>% 
    filter(item %in% c("Country area", "Land area", "Inland waters")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_2035 <- function(data) {
  data %>% 
    mutate(item = ifelse(item == "Inland waters", "Area of inland waters", item),
           item = ifelse(item == "Country area", "Total area", item)) %>% 
    rename(Country = area, Type = item, Years = year) %>% 
    select(Country, Years, Type, value)
}

footnotes_2035 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_2035 <- process_indicator(
  indicator_id = 2035,
  data = rl,
  dim_config = dim_config_2035,
  filter_fn = filter_2035,
  transform_fn = transform_2035,
  footnotes_fn = footnotes_2035,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 2054 - inland waters area ----
indicator_id <- 2054

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2054 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2054 <- function(data) {
  data %>% 
    filter(item %in% c("Inland waters")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_2054 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

footnotes_2054 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_2054 <- process_indicator(
  indicator_id = 2054,
  data = rl,
  dim_config = dim_config_2054,
  filter_fn = filter_2054,
  transform_fn = transform_2054,
  footnotes_fn = footnotes_2054,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 2036 - forest area ----
indicator_id <- 2036

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2036 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20722"),
  pub_col = c("208_name", "29117_name", "20722_name")
)

filter_2036 <- function(data) {
  data %>% 
    filter(item %in% c("Forest land", "Naturally regenerating forest", "Planted Forest")) %>% 
    filter(element == "area") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "CuraÃ§ao"))
}

transform_2036 <- function(data) {
  data %>% 
    mutate(item = case_when(
      item == "Forest land" ~ "Total forest",
      item == "Naturally regenerating forest" ~ "Natural forest",
      item == "Planted Forest" ~ "Forest plantations",
      TRUE ~ item
    )) %>% 
    rename(Country = area, Years = year, Type = item) %>% 
    select(Country, Years, Type, value)
}

footnotes_2036 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_2036 <- process_indicator(
  indicator_id = 2036,
  data = rl,
  dim_config = dim_config_2036,
  filter_fn = filter_2036,
  transform_fn = transform_2036,
  footnotes_fn = footnotes_2036,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 2530 - natural forest proportion of total forest ----
indicator_id <- 2530

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2530 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2530 <- function(data) {
  data %>% 
    filter(item %in% c("Forest land", "Naturally regenerating forest")) %>% #"Planted Forest"
    filter(element == "area") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "CuraÃ§ao", "Anguilla"))
}

transform_2530 <- function(data) {
  data %>% 
    select(area, item, year, value) %>% 
    mutate(item = ifelse(item == "Forest land", "total", "natural")) %>% 
    pivot_wider(names_from = item, values_from = value) %>% 
    rename(Country = area, Years = year)
}

regional_2530 <- function(data) {
  eclac_totals <- data %>%
    group_by(Years) %>%
    summarise(natural = sum(natural, na.rm = TRUE),
              total = sum(total, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")

  data <- bind_rows(data, eclac_totals) %>%
    mutate(value = round(natural/total * 100, 1)) %>%
    arrange(Country, Years) %>%
    select(Country, Years, value)

  return(data)
}

footnotes_2530 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_2530 <- process_indicator(
  indicator_id = 2530,
  data = rl,
  dim_config = dim_config_2530,
  filter_fn = filter_2530,
  transform_fn = transform_2530,
  regional_fn = regional_2530,
  footnotes_fn = footnotes_2530,
  diagnostics = TRUE,
  export = TRUE
)

## ---- indicator 2531 - forest plantations proportion of total forest ----
indicator_id <- 2531

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2531 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2531 <- function(data) {
  data %>% 
    filter(item %in% c("Forest land", "Planted Forest")) %>%
    filter(element == "area") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "CuraÃ§ao", "Anguilla"))
}

transform_2531 <- function(data) {
  data %>% 
    select(area, item, year, value) %>% 
    mutate(item = ifelse(item == "Forest land", "total", "planted")) %>% 
    pivot_wider(names_from = item, values_from = value) %>% 
    rename(Country = area, Years = year)
}

regional_2531 <- function(data) {
  eclac_totals <- data %>%
    group_by(Years) %>%
    summarise(planted = sum(planted, na.rm = TRUE),
              total = sum(total, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals) %>%
    mutate(value = round(planted/total * 100, 1)) %>%
    arrange(Country, Years) %>%
    select(Country, Years, value)  %>% 
    mutate(value = replace_na(value, 0)) # assume NAs to mean no planted forests (generally true)
  
  return(data)
}

footnotes_2531 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_2531 <- process_indicator(
  indicator_id = 2531,
  data = rl,
  dim_config = dim_config_2531,
  filter_fn = filter_2531,
  transform_fn = transform_2531,
  regional_fn = regional_2531,
  footnotes_fn = footnotes_2531,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 2021 - proportion of forest area ----
indicator_id <- 2021

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2021 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20722"),
  pub_col = c("208_name", "29117_name", "20722_name")
)

filter_2021 <- function(data) {
  data %<>% 
    filter(item %in% c("Forest land", "Naturally regenerating forest", "Planted Forest", "Land area")) %>% 
    filter(element == "area") %>% 
    filter(as.numeric(year) >= 1990) %>% # Filter on data 1990 and beyond - this is when more detailed forest data began
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "CuraÃ§ao"))
}

transform_2021 <- function(data) {
  data %<>% 
    mutate(item = case_when(
      item == "Forest land" ~ "Total forest",
      item == "Naturally regenerating forest" ~ "Natural forest",
      item == "Planted Forest" ~ "Forest plantations",
      TRUE ~ item
    )) %>% 
    rename(Country = area, Years = year, Type = item) %>% 
    select(Country, Years, Type, value)
  
  # filter on country-year combinations that have denominator land area
  data %<>% 
    group_by(Country, Years) %>% 
    filter(any(Type == "Land area")) %>%  # Keep only groups that have "Land area"
    ungroup()
  
  return(data)
}

regional_2021 <- function(data) {
  eclac_totals <- data %>%
    group_by(Years, Type) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals) %>%
    group_by(Country, Years) %>%
    mutate(
      land_area = value[Type == "Land area"],   # pull the denominator for that Country-Year
      prop = ifelse(Type != "Land area", value / land_area, NA_real_), .groups = "drop") %>% 
    ungroup() %>% 
    filter(Type != "Land area") %>% 
    mutate(prop = round(prop * 100, 1)) %>% 
    select(Country, Years, Type, value = prop)
  
  return(data)
}

footnotes_2021 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_2021 <- process_indicator(
  indicator_id = 2021,
  data = rl,
  dim_config = dim_config_2021,
  filter_fn = filter_2021,
  transform_fn = transform_2021,
  regional_fn = regional_2021,
  footnotes_fn = footnotes_2021,
  diagnostics = TRUE,
  export = TRUE
)



## ---- indicator 1869 - ag area by land type use ----
indicator_id <- 1869

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1869 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "26646"),
  pub_col = c("208_name", "29117_name", "26646_name")
)

filter_1869 <- function(data) {
  data %>% 
    filter(item %in% c("Arable land", "Permanent crops", "Permanent meadows and pastures")) %>% 
    filter(element == "area") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_1869 <- function(data) {
  data %<>% 
    mutate(item = case_when(
      item == "Arable land" ~ "Area of arable land",
      item == "Permanent crops" ~ "Area of permanent crops",
      item == "Permanent meadows and pastures" ~ "Area of permanent meadows and pastures",
      TRUE ~ item
    )) %>% 
    rename(Country = area, Type = item, Years = year) %>% 
    select(Country, Years, Type, value)
    
    # create the summed "Agricultural area"
    # compute sum and append
    agri_sum <- data %>%
      group_by(Country, Years) %>%
      summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
      mutate(Type = "Agricultural area")
    
    bind_rows(data, agri_sum)
}

footnotes_1869 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_1869 <- process_indicator(
  indicator_id = 1869,
  data = rl,
  dim_config = dim_config_1869,
  filter_fn = filter_1869,
  transform_fn = transform_1869,
  footnotes_fn = footnotes_1869,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 1739 - irrigated area ----
indicator_id <- 1739

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1739 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_1739 <- function(data) {
  data %>% 
    filter(item %in% c("Land area equipped for irrigation")) %>% 
    filter(element == "area") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_1739 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

footnotes_1739 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_1739 <- process_indicator(
  indicator_id = 1739,
  data = rl,
  dim_config = dim_config_1739,
  filter_fn = filter_1739,
  transform_fn = transform_1739,
  footnotes_fn = footnotes_1739,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 4049 - prop of ag area with organic agriculture ----
indicator_id <- 4049

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4049 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4049 <- function(data) {
  data %>% 
    filter(item %in% c("Agriculture area under organic agric.")) %>% 
    filter(element == "share_in_agricultural_land") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_4049 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

footnotes_4049 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_4049 <- process_indicator(
  indicator_id = 4049,
  data = rl,
  dim_config = dim_config_4049,
  filter_fn = filter_4049,
  regional_fn = FALSE,
  transform_fn = transform_4049,
  footnotes_fn = footnotes_4049,
  diagnostics = TRUE,
  export = TRUE
)


# FAO CLIMATE CHANGE (ET) INDICATORS -----


## ---- indicator 3381 - mean annual temperature change ----
indicator_id <- 3381

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_3381 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3381 <- function(data) {
  data %>% 
    filter(element == "temperature_change" & months == "Meteorological year") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao")) %>% 
    filter(!is.na(value))
  
  ## IMPORTANT:
  # comment out lines from:  remove regional totals, construct ECLAC total from sum of countries to creating ECLAC totals when running it
}

transform_3381 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

footnotes_3381 <- function(data) {
  data
}

result_3381 <- process_fao_indicator(
  indicator_id = 3381,
  data = et,
  dim_config = dim_config_3381,
  filter_fn = filter_3381,
  transform_fn = transform_3381,
  footnotes_fn = footnotes_3381,
  diagnostics = TRUE,
  export = TRUE
)


# FAO LAND COVER (LC) INDICATORS -----


## ---- indicator 3355 - area covered by permanent snow and glaciers ----
indicator_id <- 3355

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_3355 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3355 <- function(data) {
  data %>% 
    filter(element == "area_from_cci_lc" & item == "Permanent snow and glaciers") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao")) %>% 
    filter(!is.na(value))
  
  ## IMPORTANT:
  # comment out lines from:  remove regional totals, construct ECLAC total from sum of countries to creating ECLAC totals when running it
}

transform_3355 <- function(data) {
  data %>% 
    mutate(value = value * 1000) %>% # transform from 1,000 hectares into hectares
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

footnotes_3355 <- function(data) {
  data
}

result_3355 <- process_fao_indicator(
  indicator_id = 3355,
  data = lc,
  dim_config = dim_config_3355,
  filter_fn = filter_3355,
  transform_fn = transform_3355,
  footnotes_fn = footnotes_3355,
  diagnostics = TRUE,
  export = TRUE
)

## ---- indicator 4176 - area covered by mangroves ----
indicator_id <- 4176

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4176 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4176 <- function(data) {
  data %>%
    filter(element == "area_from_cci_lc" & item == "Mangroves") %>%
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao")) %>%
    filter(!is.na(value))
}

transform_4176 <- function(data) {
  data %>%
    rename(Country = area, Years = year) %>%
    select(Country, Years, value)
}

footnotes_4176 <- function(data) {
  data
}

source_4176 <- function() {
  651 # general FAOSTAT source
}

result_4176 <- process_indicator(
  indicator_id = 4176,
  data = lc,
  dim_config = dim_config_4176,
  filter_fn = filter_4176,
  transform_fn = transform_4176,
  footnotes_fn = footnotes_4176,
  source_fn = source_4176,
  diagnostics = TRUE,
  export = TRUE,
  ind_notes = "changed data source from fra to cci_lc"
)


# FAO CROP (QCL) INDICATORS -----


## ---- indicator 1740 - harvested area of main crops ----
indicator_id <- 1740

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1740 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20721"),
  pub_col = c("208_name", "29117_name", "20721_name")
)

filter_1740 <- function(data) {
  data %>% 
    filter(element == "area_harvested") %>% 
    filter(item %in% c("Cereals, primary", "Sugar Crops Primary", "Fibre Crops, Fibre Equivalent",
                       "Oilcrops, Oil Equivalent", "Fruit Primary", "Vegetables Primary",
                       "Pulses, Total", "Treenuts, Total", "Roots and Tubers, Total")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_1740 <- function(data) {
  data %>% 
    mutate(item = case_when(
      item == "Cereals, primary" ~ "Cereals",
      item == "Sugar Crops Primary" ~ "Sugar crops",
      item == "Fibre Crops, Fibre Equivalent" ~ "Fibre crops",
      item == "Oilcrops, Oil Equivalent" ~ "Oilcrops",
      item == "Fruit Primary" ~ "Fruit",
      item == "Vegetables Primary" ~ "Vegetables",
      item == "Pulses, Total" ~ "Pulses",
      item == "Treenuts, Total" ~ "Treenuts",
      item == "Roots and Tubers, Total" ~ "Roots and tubers",
      TRUE ~ item
    )) %>% 
    mutate(value = value / 1000) %>% # transform into 1000s of hectares
    rename(Country = area, Type = item, Years = year) %>% 
    select(Country, Years, Type, value)
}

footnotes_1740 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_1740 <- process_fao_indicator(
  indicator_id = 1740,
  data = qcl,
  dim_config = dim_config_1740,
  filter_fn = filter_1740,
  transform_fn = transform_1740,
  footnotes_fn = footnotes_1740,
  diagnostics = TRUE,
  export = TRUE
)


# FAO FERTILIZERS (RFN) INDICATORS -----

## ---- indicator 2022 - fertilizer use intensity ----
indicator_id <- 2022

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2022 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2022 <- function(data) {
  data %<>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Nutrient nitrogen N (total)", "Nutrient phosphate P2O5 (total)", "Nutrient potash K2O (total)")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_2022 <- function(data) {
  data %<>% 
    group_by(area, year) %>% # sum across fertilizer types (items)
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value) %>% 
    mutate(Years = as.character(Years)) %>% 
    left_join(result_cropland, by = c("Country", "Years")) %>% 
    arrange(Country, Years)
}

regional_2022 <- function(data) {
  eclac_totals <- data %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value", "area"))))) %>%
    summarise(value = sum(value, na.rm = TRUE),
              area = sum(area, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals) %>%
    mutate(value = value/area) %>% 
    arrange(Country, Years) %>% 
    select(Country, Years, value)
  
  return(data)
}

footnotes_2022 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

source_2022 <- function() {
  913 # Calculations made based on fertilizer consumption data and agriculture area data from online statistical database (FAOSTAT) to Food and Agriculture Organization of the United Nations (FAO). 
}

result_2022 <- process_indicator(
  indicator_id = 2022,
  data = rfn,
  dim_config = dim_config_2022,
  filter_fn = filter_2022,
  transform_fn = transform_2022,
  regional_fn = regional_2022,
  footnotes_fn = footnotes_2022,
  source_fn = source_2022,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 2038 - fertilizer consumption ----
indicator_id <- 2038

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2038 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2038 <- function(data) {
  data %>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Nutrient nitrogen N (total)", "Nutrient phosphate P2O5 (total)", "Nutrient potash K2O (total)")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_2038 <- function(data) {
  data %>% 
    group_by(area, year) %>% # sum across fertilizer types (items)
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

# regional_2038 <- function(data) {
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

footnotes_2038 <- function(data) {
  data %>% 
    mutate(footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id),
           # 6970/ Calculado a partir de la información disponible de los países de la región.
           footnotes_id = if_else(Years == "2002", append_footnote(footnotes_id, "7177"), footnotes_id)
           # 7177/ La serie de datos de 1961 a 2001 y la serie de 2002 a la fecha deberán analizarse por separado y no en combinación a fin de crear series cronológicas más largas. Ello se debe a los cambios ocurridos desde 2002: modificaciónes en la metodología relativa a los datos sobre fertilizantes; el paso de una combinación de año civil y año de fertilizantes a la utilización del año civil; la clasificación revisada de los elementos fertilizantes; la adición de un parámetro relativo al uso no fertilizante en el balance de fertilizantes y la utilización de nuevas fuentes para algunos datos por parte de FAO.
            )
}

result_2038 <- process_indicator(
  indicator_id = 2038,
  data = rfn,
  dim_config = dim_config_2038,
  filter_fn = filter_2038,
  transform_fn = transform_2038,
  footnotes_fn = footnotes_2038,
  diagnostics = TRUE,
  export = TRUE
)




# FAO PESTICIDES (RP) INDICATORS -----


## ---- indicator 3382 - pesticide use intensity ----
indicator_id <- 3382

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_3382 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3382 <- function(data) {
  data %<>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Insecticides", "Herbicides", "Fungicides and Bactericides")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_3382 <- function(data) {
  data %<>% 
    group_by(area, year) %>% # sum across pesticide types (items)
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value) %>% 
    mutate(Years = as.character(Years)) %>% 
    left_join(result_cropland, by = c("Country", "Years"))
}

regional_3382 <- function(data) {
  eclac_totals <- data %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value", "area"))))) %>%
    summarise(value = sum(value, na.rm = TRUE),
              area = sum(area, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals) %>%
    mutate(value = value/area) %>% 
    arrange(Country, Years) %>% 
    select(Country, Years, value)
  
  return(data)
}

footnotes_3382 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

source_3382 <- function() {
  1827 # Calculations made based on pesticide consumption data and agriculture area data from online statistical database (FAOSTAT) to Food and Agriculture Organization of the United Nations (FAO). 
}

result_3382 <- process_indicator(
  indicator_id = 3382,
  data = rp,
  dim_config = dim_config_3382,
  filter_fn = filter_3382,
  transform_fn = transform_3382,
  regional_fn = regional_3382,
  footnotes_fn = footnotes_3382,
  source_fn = source_3382,
  diagnostics = TRUE,
  export = TRUE
)


# FAO FISH INDICATORS -----

## ---- indicator 2019 - fish capture production ----
indicator_id <- 2019

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2019 <- tibble(
  data_col = c("Country", "Years", "Species"),
  dim_id = c("208", "29117", "20720"),
  pub_col = c("208_name", "29117_name", "20720_name")
)

## ** indicator specific data cleaning **

fish_country_map %<>% select(UN_Code, Country = Name_En)
fish_species_map %<>% select(`3A_Code`, Species = Name_En, Species_Group = ISSCAAP_Group_En)
fish_water_map %<>% select(Code, Water = Name_En)


fish %<>% 
  left_join(fish_country_map, by = c("COUNTRY.UN_CODE" = "UN_Code")) %>% 
  left_join(fish_species_map, by = c("SPECIES.ALPHA_3_CODE" = "3A_Code")) %>% 
  left_join(fish_water_map, by = c("AREA.CODE" = "Code")) %>% 
  select(-COUNTRY.UN_CODE, -SPECIES.ALPHA_3_CODE, - AREA.CODE)

# ****************************************


filter_2019 <- function(data) {
  whales <- c("Blue-whales, fin-whales", "Sperm-whales, pilot-whales", "Eared seals, hair seals, walruses", "Miscellaneous aquatic mammals")
  
  data %>% 
    filter(!Species_Group %in% whales) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    # filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla")) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country, Years = PERIOD, Species, Species_Group, value = VALUE)
}

transform_2019 <- function(data) {
  data %<>% 
    mutate(
      Species_Division = case_when(
        # 1 Freshwater fishes
        Species_Group %in% c(
          "Carps, barbels and other cyprinids",
          "Tilapias and other cichlids",
          "Miscellaneous freshwater fishes"
        ) ~ "Freshwater fishes",
        
        # 2 Diadromous fishes
        Species_Group %in% c(
          "Sturgeons, paddlefishes",
          "River eels",
          "Salmons, trouts, smelts",
          "Shads",
          "Miscellaneous diadromous fishes"
        ) ~ "Diadromous fishes",
        
        # 3 Marine fishes
        Species_Group %in% c(
          "Flounders, halibuts, soles",
          "Cods, hakes, haddocks",
          "Miscellaneous coastal fishes",
          "Miscellaneous demersal fishes",
          "Herrings, sardines, anchovies",
          "Tunas, bonitos, billfishes",
          "Miscellaneous pelagic fishes",
          "Sharks, rays, chimaeras",
          "Marine fishes not identified"
        ) ~ "Marine fishes",
        
        # 4 Crustaceans
        Species_Group %in% c(
          "Freshwater crustaceans",
          "Crabs, sea-spiders",
          "Lobsters, spiny-rock lobsters",
          "King crabs, squat-lobsters",
          "Shrimps, prawns",
          "Krill, planktonic crustaceans",
          "Miscellaneous marine crustaceans"
        ) ~ "Crustaceans",
        
        # 5 Molluscs
        Species_Group %in% c(
          "Freshwater molluscs",
          "Abalones, winkles, conchs",
          "Oysters",
          "Mussels",
          "Scallops, pectens",
          "Clams, cockles, arkshells",
          "Squids, cuttlefishes, octopuses",
          "Miscellaneous marine molluscs"
        ) ~ "Molluscs",
        
        # 6 Whales, seals and other aquatic mammals
        Species_Group %in% c(
          "Blue-whales, fin-whales",
          "Sperm-whales, pilot-whales",
          "Eared seals, hair seals, walruses",
          "Miscellaneous aquatic mammals"
        ) ~ "Whales, seals and other aquatic mammals",
        
        # 7 Miscellaneous aquatic animals
        Species_Group %in% c(
          "Frogs and other amphibians",
          "Turtles",
          "Crocodiles and alligators",
          "Sea-squirts and other tunicates",
          "Horseshoe crabs and other arachnoids",
          "Sea-urchins and other echinoderms",
          "Miscellaneous aquatic invertebrates"
        ) ~ "Miscellaneous aquatic animals",
        
        # 8 Miscellaneous aquatic animal products
        Species_Group %in% c(
          "Pearls, mother-of-pearl, shells",
          "Corals",
          "Sponges"
        ) ~ "Miscellaneous aquatic animal products",
        
        # 9 Aquatic plants
        Species_Group %in% c(
          "Brown seaweeds",
          "Red seaweeds",
          "Green seaweeds",
          "Miscellaneous aquatic plants"
        ) ~ "Aquatic plants",
        
        TRUE ~ NA_character_
      )
    ) %>% 
    select(-Species, -Species_Group) %>% 
    # map to cepalstat labels
    mutate(Species = case_when(
      Species_Division %in% c("Freshwater fishes") ~ "Freshwater fish",
      Species_Division %in% c("Marine fishes") ~ "Marine fish",
      # keep Molluscs and Crustaceans and Aquatic plans as is
      Species_Division %in% c("Diadromous fishes", "Miscellaneous aquatic animals", "Miscellaneous aquatic animal products") ~ "Other",
      TRUE ~ Species_Division
    )) %>% select(-Species_Division)
  
  data %<>% 
    group_by(Country, Years, Species) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") 
  
  return(data)
}

regional_2019 <- function(data) {
  # first create TOTAL category for Species
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(Species = "TOTAL")
  
  data %<>% bind_rows(total)
  
  eclac_totals <- data %>%
    group_by(Years, Species) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals) %>%
    arrange(Country, Years)
  
  return(data)
}

footnotes_2019 <- function(data) {
    data %>%
      mutate(
        footnotes_id = "6545", # applies to everyone
        # 6545/ Incluye la captura en áreas marinas y en aguas continentales.
        footnotes_id = if_else(
          Country == "Latin America and the Caribbean",
          paste(footnotes_id, "6970", sep = ","),
          footnotes_id
        ),
        # 6970/ Calculado a partir de la información disponible de los países de la región.
        footnotes_id = if_else(
          Species == "TOTAL",
          paste(footnotes_id, "7777", sep = ","),
          footnotes_id
        ),
        # 7777/ El total no incluye ballenas, focas y otros mamíferos acuáticos
        footnotes_id = if_else(
          Species == "Other",
          paste(footnotes_id, "5518", sep = ","),
          footnotes_id
        )
        # 5518/ Incluye peces diádromos, varios animales acuáticos y varios productos de animales acuáticos.
      )
  }
  

result_2019 <- process_indicator(
  indicator_id = 2019,
  data = fish,
  dim_config = dim_config_2019,
  filter_fn = filter_2019,
  transform_fn = transform_2019,
  regional_fn = regional_2019,
  footnotes_fn = footnotes_2019,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 2020 - aquaculture production ----
indicator_id <- 2020

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2020 <- tibble(
  data_col = c("Country", "Years", "Area"),
  dim_id = c("208", "29117", "26819"),
  pub_col = c("208_name", "29117_name", "26819_name")
)

## ** indicator specific data cleaning **

aqua_country_map %<>% select(UN_Code, Country = Name_En)
aqua_water_map %<>% select(Code, Area = InlandMarine_Group_En) # accept all areas
aqua_environment_map %<>% select(Code, Environment = Name_En)

aqua %<>% 
  left_join(aqua_country_map, by = c("COUNTRY.UN_CODE" = "UN_Code")) %>% 
  left_join(aqua_water_map, by = c("AREA.CODE" = "Code")) %>% 
  left_join(aqua_environment_map, by = c("ENVIRONMENT.ALPHA_2_CODE" = "Code")) %>% 
  select(Country, Area, Environment, Years = PERIOD, value = VALUE)

# ****************************************


filter_2020 <- function(data) {
  data %>% 
    filter(Environment %in% c("Freshwater", "Marine")) %>% # remove "Brackishwater"
    filter(Area %in% c("Inland waters", "Marine areas")) %>% # select all
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    # filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla")) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country, Years, Area, value)
}

transform_2020 <- function(data) {
  data %>% 
    group_by(Country, Years, Area) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
}

regional_2020 <- function(data) {
  # first create TOTAL category for Species
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(Area = "Total")
  
  data %<>% bind_rows(total)
  
  eclac_totals <- data %>%
    group_by(Years, Area) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals) %>%
    arrange(Country, Years)
  
  return(data)
}

footnotes_2020 <- function(data) {
  data %>%
    mutate(
      footnotes_id = "5899", # applies to everyone
      # 5899/ Incluye la producción en áreas marinas y en aguas continentales.
      footnotes_id = if_else(
        Country == "Latin America and the Caribbean",
        paste(footnotes_id, "6970", sep = ","),
        footnotes_id
      )
    )
}

result_2020 <- process_indicator(
  indicator_id = 2020,
  data = aqua,
  dim_config = dim_config_2020,
  filter_fn = filter_2020,
  transform_fn = transform_2020,
  regional_fn = regional_2020,
  footnotes_fn = footnotes_2020,
  diagnostics = TRUE,
  export = TRUE
)


# FAO AQUA INDICATORS -----

## ---- indicator 4185 - sectoral distribution of water extraction ----
indicator_id <- 4185

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4185 <- tibble(
  data_col = c("Country", "Years", "Sector"),
  dim_id = c("208", "29117", "59252"),
  pub_col = c("208_name", "29117_name", "59252_name")
)

filter_4185 <- function(data) {
  data %>%
    filter(str_detect(Variable, "as \\% of total")) %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    # filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla")) %>% 
    filter(!Area %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country = Area, Years = Year, Sector = Variable, value = Value)
}

transform_4185 <- function(data) {
  bol_data <- data %>% filter(str_detect(Country, "Bolivia") & Years %in% c(2020, 2021))
  
  if(any(bol_data$value > 100)) { # fix data issue for years 2020/2021 where entries were seemingly reported in units rather than %
    bol_data %<>% 
      group_by(Years) %>% 
      mutate(perc = value/sum(value) * 100) %>% 
      ungroup() %>% 
      select(-value, value = perc)
    
    data %<>% # remove old data and attach corrected
      filter(!(str_detect(Country, "Bolivia") & Years %in% c(2020, 2021))) %>% 
      bind_rows(bol_data)
  }
  
  data %>% 
    mutate(Sector = case_when(
      str_detect(Sector, "Agricultural") ~ "Agricultural",
      str_detect(Sector, "Industrial") ~ "Industrial",
      str_detect(Sector, "Municipal") ~ "Municipal",
      TRUE ~ Sector
    ))
}

footnotes_4185 <- function(data) {
  data
}

result_4185 <- process_indicator(
  indicator_id = 4185,
  data = aquastat,
  dim_config = dim_config_4185,
  filter_fn = filter_4185,
  transform_fn = transform_4185,
  regional_fn = FALSE, # no ECLAC average
  footnotes_fn = footnotes_4185,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 4186 - water intensity of agriculture value added ----
indicator_id <- 4186

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4186 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4186 <- function(data) {
  data %>% 
    filter(Variable == "Agricultural water withdrawal") %>% 
    # filter out any countries too with inconsistent entries (to not impact LAC total)
    # filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla")) %>% 
    filter(!Area %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country = Area, Years = Year, value = Value)
}

transform_4186 <- function(data) {
  # get annual GDP by economic activity from CEPALSTAT, in constant 2018 dollars
  gdp <- CepalStatR::call.data(2216) %>% as_tibble()
  
  gdp %<>% 
    rename(Sector = Rubro__Sector_Cuentas_nacionales_anuales) %>% 
    filter(Sector %in% c("Agriculture, hunting, forestry and fishing")) %>% 
    select(Country, Years, gdp = value)
  
  data %>% 
    left_join(gdp, by = c("Country", "Years")) %>% 
    filter(!is.na(gdp)) %>%
    mutate(intensity = value / gdp * 1e3) %>% 
    select(Country, Years, value = intensity)
}

footnotes_4186 <- function(data) {
  data
}

result_4186 <- process_indicator(
  indicator_id = 4186,
  data = aquastat,
  dim_config = dim_config_4186,
  filter_fn = filter_4186,
  transform_fn = transform_4186,
  regional_fn = FALSE, # no ECLAC average
  footnotes_fn = footnotes_4186,
  diagnostics = TRUE,
  export = TRUE
)
