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

# This script processes "other" indicators from small sources using the automated process_indicator() function
# See the data download instructions at: 01_other_instructions.qmd

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

input_path <- here("Data/Raw/other")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

# read in indicator metadata
# meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

# max_year <- 2024 # define most recent year with full data


# ---- read downloaded files ----

data_irena <- read_xlsx(paste0(input_path, "/irena_raw.xlsx"))
data_iso_2024 <- read_xlsx(paste0(input_path, "/iso_2024_raw.xlsx"), sheet = "ISO 14001", skip = 1) # in future years, we'll only need to download one year at a time
data_iso_2023 <- read_xlsx(paste0(input_path, "/iso_2023_raw.xlsx"), sheet = "ISO 14001", skip = 1)
data_iso_2022 <- read_xlsx(paste0(input_path, "/iso_2022_raw.xlsx"), sheet = "ISO 14001", skip = 1)
data_iso_2021 <- read_xlsx(paste0(input_path, "/iso_2021_raw.xlsx"), sheet = "ISO 14001", skip = 1)


# ---- indicator 4244 — Public investment trends in renewable energy ----

indicator_id <- 4244

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4244 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "78139"),
  pub_col = c("208_name", "29117_name", "78139_name")
)

data <- data_irena
names(data) # check if units for the amount change -- it seems like the base year is updated every so often

filter_4244 <- function(data) {
  data %>%
    filter(Category == "Renewables")
}

transform_4244 <- function(data) {
  data %<>% 
    rename(Country = `Country/Area`,
           Years = Year,
           Type = Technology,
           value = `Amount (2022 Constant USD million)`) %>% 
    mutate(Type = case_when(
      Type == "Renewable hydropower" ~ "Hydroelectric",
      Type == "Solar energy" ~ "Solar",
      Type == "Wind energy" ~ "Wind",
      Type == "Geothermal energy" ~ "Geothermal",
      TRUE ~ Type
    )) %>% 
    group_by(Country, Years, Type) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop")
  
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value), .groups = "drop") %>% 
    mutate(Type = "Total")
  
  data %>% bind_rows(total)
}

footnotes_4244 <- function(data) {
  data # keep footnotes_id as empty
}

source_4244 <- function() {
  10686 # Calculations made by ECLAC based on data from the International Renewable Energy Agency (IRENA) 
}

result_4244 <- process_indicator(
  indicator_id = indicator_id,
  data = data_irena,
  dim_config = dim_config_4244,
  filter_fn = filter_4244,
  transform_fn = transform_4244,
  remove_lac = TRUE, # remove and recalculate LAC as simple sum
  footnotes_fn = footnotes_4244,
  source_fn = source_4244,
  diagnostics = TRUE,
  export = TRUE,
  ind_notes = "Updated source to link to IRENA website"
)


# ---- indicator 1763 - ISO 14001-certified enterprises ----

indicator_id <- 1763

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1763 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# --- data compilation for ISO surveys
# note that this data source is a bit different since it's an annual survey
# we want to take the pre-existing data from CEPALSTAT and just append the new year's survey to it
hist_1763 <- tibble(call.data(1763))
hist_1763 %<>% select(Country, Years, value)

# add new year of data
data_iso_2024 %<>% mutate(Years = 2024) %>% select(Country, Years, value = certificates)
data_iso_2023 %<>% mutate(Years = 2023) %>% select(Country, Years, value = certificates)
data_iso_2022 %<>% mutate(Years = 2022) %>% select(Country, Years, value = certificates)
data_iso_2021 %<>% mutate(Years = 2021) %>% select(Country, Years, value = certificates)

# combine together
data_iso <- hist_1763 %>% bind_rows(data_iso_2024, data_iso_2023, data_iso_2022, data_iso_2021)
data_iso %<>% arrange(Country, Years) %>% filter(!str_detect(Country, "Compared")) # order and remove footnote
# ---

filter_1763 <- function(data) {
  data %>% 
    filter(!is.na(value))
}

transform_1763 <- function(data) {
  zero_countries <- data %>% 
    group_by(Country) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    filter(value == 0) %>% 
    pull(Country)
  
  data %>% filter(!Country %in% zero_countries)
}

footnotes_1763 <- function(data) {
  data %>%
    mutate(
      footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id))
}

result_1763 <- process_indicator(
  indicator_id = indicator_id,
  data = data_iso,
  dim_config = dim_config_1763,
  filter_fn = filter_1763,
  transform_fn = transform_1763,
  remove_lac = TRUE, # remove and recalculate LAC as simple sum
  footnotes_fn = footnotes_1763,
  diagnostics = TRUE,
  export = TRUE
)



# ---- indicator 2029 - ISO 14001-certified enterprises per GDP ----

indicator_id <- 2029

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2029 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# --- data compilation for ISO surveys
# NEED TO update 1763 data on CEPALSTAT before processing this indicator
# because it takes the raw values from the published 1763
base_1763 <- tibble(call.data(1763)) %>% select(Country, Years, value)
data_iso <- base_1763
# ---

filter_2029 <- function(data) {
  data
}

transform_2029 <- function(data) {
  data
}

regional_2029 <- function(data) {
  zero_countries <- data %>% 
    group_by(Country) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    filter(value == 0) %>% 
    pull(Country)
  
  data %<>% 
    filter(!Country %in% zero_countries)
  
  gdp <- tibble(call.data(2204)) %>% 
    select(Country, Years, gdp = value) %>% 
    mutate(Years = as.character(Years))
  
  data %<>% 
    left_join(gdp, by = c("Country", "Years")) %>% 
    filter(!is.na(value) & !is.na(gdp))
  
  lac <- data %>% 
    group_by(Years) %>% 
    summarize(value = sum(value),
              gdp = sum(gdp), .groups = "drop") %>% 
    mutate(Country = "Latin America and the Caribbean")
  
  data %<>% 
    bind_rows(lac) %>% 
    mutate(per = round(value / gdp * 1000, 1)) %>% 
    select(Country, Years, value = per) %>%
    arrange(Country, Years)
  
  data
}

footnotes_2029 <- function(data) {
  data %>%
    mutate(
      footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id))
}

result_2029 <- process_indicator(
  indicator_id = indicator_id,
  data = data_iso,
  dim_config = dim_config_2029,
  filter_fn = filter_2029,
  transform_fn = transform_2029,
  remove_lac = TRUE, # remove and recalculate LAC with custom function
  regional_fn = regional_2029,
  footnotes_fn = footnotes_2029,
  diagnostics = TRUE,
  export = TRUE
)
