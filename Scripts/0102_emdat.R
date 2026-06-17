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

# This script cleans and standardizes the EM-DAT disaster indicators

# ---- download ----

## Instructions for DOWNLOADING the EM-DAT data (step 01):
# Go to https://public.emdat.be/
# Create a user account
# Go to "Access Data"
# Select in 'classification' natural; in 'countries' Americas > LAC; in 'time period' 1970 to the last available year

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# Read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

# Read in the downloaded emdat data
emdat <- read_xlsx(here("Data/Raw/emdat/public_emdat_custom_request_2025-11-04.xlsx"))

# Define latest year of data
max_year <- 2024


## ---- indicator 4046 - economic cost of disasters ----
indicator_id <- 4046

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4046 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

filter_4046 <- function(data) {
  data %>% 
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
    filter(as.numeric(`Start Year`) >= 1970 & as.numeric(`Start Year`) <= max_year) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

transform_4046 <- function(data) {
  data %<>% 
    mutate(value = ifelse(!is.na(`Total Damage, Adjusted ('000 US$)`), `Total Damage, Adjusted ('000 US$)`, `Total Damage ('000 US$)`)) %>%
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, value) %>% 
    mutate(Type = paste0(`Disaster Type`, "s")) %>% 
    mutate(Type = case_when(
      Type == "Mass movement (wet)s" ~ "Wet mass displacement",
      Type == "Volcanic activitys" ~ "Volcanic eruptions",
      Type == "Mass movement (dry)s" ~ "Dry mass displacement",
      TRUE ~ Type
    )) %>% 
    mutate(Group = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    select(-`Disaster Subgroup`, -`Disaster Type`) %>% 
    #filter(!is.na(value)) %>% 
    group_by(Country, Years, Group, Type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
  
  type_sum <- data %>% 
    group_by(Country, Years, Group) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Type = Group)
  
  data %<>% 
    select(-Group) %>% 
    bind_rows(type_sum)
}

footnotes_4046 <- function(data) {
  data
}

result_4046 <- process_indicator(
  indicator_id = 4046,
  data = emdat,
  dim_config = dim_config_4046,
  filter_fn = filter_4046,
  transform_fn = transform_4046,
  footnotes_fn = footnotes_4046,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 1837 - occurrence of disasters ----
# this indicator is no longer maintained
indicator_id <- 1837

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1837 <- tibble(
  data_col = c("Country", "Years", "Type", "Indicator"),
  dim_id = c("208", "29117", "21714", "21725"),
  pub_col = c("208_name", "29117_name", "21714_name", "21725_name")
)

filter_1837 <- function(data) {
  data %<>%
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
    filter(as.numeric(`Start Year`) >= 1990 & as.numeric(`Start Year`) <= max_year) %>%
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

transform_1837 <- function(data) {
  data %<>%
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`,
           `Total Deaths`, `Total Affected`) %>%
    mutate(Type = paste0(`Disaster Type`, "s")) %>%
    mutate(Type = case_when(
      Type == "Mass movement (wet)s" ~ "Wet mass displacement", # was "desplacement", fixed typo in CEPALSTAT admin
      Type == "Volcanic activitys" ~ "Volcanic eruptions",
      Type == "Mass movement (dry)s" ~ "Dry mass displacement",
      TRUE ~ Type
    )) %>%
    mutate(Group = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    select(-`Disaster Subgroup`, -`Disaster Type`) %>%
    mutate(`Number of events` = 1) %>%
    select(-DisNo.) %>%
    pivot_longer(cols = c(`Total Deaths`, `Total Affected`, `Number of events`), names_to = "Indicator") %>%
    mutate(Indicator = case_when(
      Indicator == "Total Deaths" ~ "Human deaths",
      Indicator == "Total Affected" ~ "Directly affected persons",
      TRUE ~ Indicator)) %>%
    group_by(Country, Years, Group, Type, Indicator) %>%
    summarize(value = sum(value, na.rm = T), .groups = "drop")

  type_sum <- data %>%
    group_by(Country, Years, Group, Indicator) %>%
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>%
    rename(Type = Group)

  data %<>%
    select(-Group) %>%
    bind_rows(type_sum)
}

footnotes_1837 <- function(data) {
  data
}

result_1837 <- process_indicator(
  indicator_id = 1837,
  data = emdat,
  dim_config = dim_config_1837,
  filter_fn = filter_1837,
  transform_fn = transform_1837,
  footnotes_fn = footnotes_1837,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 5647 - number of disasters ----
indicator_id <- 5647

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_5647 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

filter_5647 <- function(data) {
  data %<>% 
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
    filter(as.numeric(`Start Year`) >= 1990 & as.numeric(`Start Year`) <= max_year) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

transform_5647 <- function(data) {
  data %<>% 
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`) %>% 
    mutate(Type = paste0(`Disaster Type`, "s")) %>% 
    mutate(Type = case_when(
      Type == "Mass movement (wet)s" ~ "Wet mass displacement",
      Type == "Volcanic activitys" ~ "Volcanic eruptions",
      Type == "Mass movement (dry)s" ~ "Dry mass displacement",
      TRUE ~ Type
    )) %>% 
    mutate(Group = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    select(-`Disaster Subgroup`, -`Disaster Type`) %>% 
    group_by(Country, Years, Group, Type) %>% 
    count() %>% 
    ungroup() %>% 
    rename(value = n)
  
  type_sum <- data %>% 
    group_by(Country, Years, Group) %>% 
    summarize(value = sum(value, na.rm = T)) %>% 
    ungroup() %>% 
    rename(Type = Group)
  
  data %<>% 
    select(-Group) %>% 
    bind_rows(type_sum)
}

footnotes_5647 <- function(data) {
  data
}

source_5647 <- function(indicator_id) {
  742  # Base de datos internacional de desastres (EM-DAT)
}

result_5647 <- process_indicator(
  indicator_id = 5647,
  data = emdat,
  dim_config = dim_config_5647,
  filter_fn = filter_5647,
  transform_fn = transform_5647,
  footnotes_fn = footnotes_5647,
  source_fn = source_5647,
  diagnostics = TRUE,
  export = TRUE
)

## ---- indicator 5645 - deaths caused by disasters ----
indicator_id <- 5645

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_5645 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

filter_5645 <- function(data) {
  data %<>% 
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
    filter(as.numeric(`Start Year`) >= 1990 & as.numeric(`Start Year`) <= max_year) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

transform_5645 <- function(data) {
  data %<>% 
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, `Total Deaths`) %>% 
    mutate(Type = paste0(`Disaster Type`, "s")) %>% 
    mutate(Type = case_when(
      Type == "Mass movement (wet)s" ~ "Wet mass displacement",
      Type == "Volcanic activitys" ~ "Volcanic eruptions",
      Type == "Mass movement (dry)s" ~ "Dry mass displacement",
      TRUE ~ Type
    )) %>% 
    mutate(Group = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    select(-`Disaster Subgroup`, -`Disaster Type`) %>% 
    rename(value = `Total Deaths`) %>% 
    filter(!is.na(value)) %>% 
    group_by(Country, Years, Group, Type) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop")
  
  type_sum <- data %>% 
    group_by(Country, Years, Group) %>% 
    summarize(value = sum(value, na.rm = T)) %>% 
    ungroup() %>% 
    rename(Type = Group)
  
  data %<>% 
    select(-Group) %>% 
    bind_rows(type_sum)
}

footnotes_5645 <- function(data) {
  data
}

source_5645 <- function(indicator_id) {
  742  # Base de datos internacional de desastres (EM-DAT)
}

result_5645 <- process_indicator(
  indicator_id = 5645,
  data = emdat,
  dim_config = dim_config_5645,
  filter_fn = filter_5645,
  transform_fn = transform_5645,
  footnotes_fn = footnotes_5645,
  source_fn = source_5645,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 5646 - persons affected by disasters ----
indicator_id <- 5646

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_5646 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

filter_5646 <- function(data) {
  data %<>% 
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
    filter(as.numeric(`Start Year`) >= 1990 & as.numeric(`Start Year`) <= max_year) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

transform_5646 <- function(data) {
  data %<>% 
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, `Total Affected`) %>% 
    mutate(Type = paste0(`Disaster Type`, "s")) %>% 
    mutate(Type = case_when(
      Type == "Mass movement (wet)s" ~ "Wet mass displacement",
      Type == "Volcanic activitys" ~ "Volcanic eruptions",
      Type == "Mass movement (dry)s" ~ "Dry mass displacement",
      TRUE ~ Type
    )) %>% 
    mutate(Group = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    select(-`Disaster Subgroup`, -`Disaster Type`) %>% 
    rename(value = `Total Affected`) %>% 
    filter(!is.na(value)) %>% 
    group_by(Country, Years, Group, Type) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop")
  
  type_sum <- data %>% 
    group_by(Country, Years, Group) %>% 
    summarize(value = sum(value, na.rm = T)) %>% 
    ungroup() %>% 
    rename(Type = Group)
  
  data %<>% 
    select(-Group) %>% 
    bind_rows(type_sum)
}

footnotes_5646 <- function(data) {
  data # No footnote
}

source_5646 <- function(indicator_id) {
  742  # Base de datos internacional de desastres (EM-DAT)
}

result_5646 <- process_indicator(
  indicator_id = 5646,
  data = emdat,
  dim_config = dim_config_5646,
  filter_fn = filter_5646,
  transform_fn = transform_5646,
  footnotes_fn = footnotes_5646,
  source_fn = source_5646,
  diagnostics = TRUE,
  export = TRUE
)
