# This script cleans and standardizes the CAIT-WRI / Climate Watch indicators

# ---- download ----

# run script climatewatch_download.R to download raw data from API

# Visit: https://www.climatewatchdata.org/data-explorer/historical-emissions?
# For information on the API and bulk data downloads

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# ** placeholder -- the old architecture didn't pin a max_year for this file at all, so please
# confirm the actual latest reliable year for Climate Watch / CAIT data (it may lag more than a
# year, unlike EM-DAT/FAO)
max_year_cw <- 2025 # as of July 2026

# ---- read downloaded files ----

cw_path <- here("Data/Raw/climate watch")

#data_2027 <- read_csv(paste0(cw_path, "/2027_raw.csv")) # note this indicator is HIDDEN in CEPALSTAT and will become deprecated once profiles are linked to new indicators
data_3158 <- read_csv(paste0(cw_path, "/3158_raw.csv"))
data_3159 <- read_csv(paste0(cw_path, "/3159_raw.csv"))
data_3351 <- read_csv(paste0(cw_path, "/3351_raw.csv"))
data_5649 <- read_csv(paste0(cw_path, "/5649_raw.csv"))
data_5650 <- read_csv(paste0(cw_path, "/5650_raw.csv"))
data_4463 <- read_csv(paste0(cw_path, "/4463_raw.csv"))
data_4461 <- read_csv(paste0(cw_path, "/4461_raw.csv"))
data_4462 <- read_csv(paste0(cw_path, "/4462_raw.csv"))
data_3387 <- read_csv(paste0(cw_path, "/3387_raw.csv"))

# ---- shared functions ----

# Regional strategy factory for "ratio" indicators (emissions per capita, emissions per GDP):
# recomputes num/denom for both countries and the LAC total, scaled by `multiplier`, with no
# rounding -- same shape as calculate_regional_wgt_avg, but that one is specifically for *100
# percentages, which these aren't.
# Sample usage: calculate_regional = calculate_regional_ratio(1e6) # tonnes CO2eq per capita
calculate_regional_ratio <- function(multiplier) {
  function(df) {
    df <- df %>%
      filter(!Country %in% c("South America", "Central America", "Caribbean",
                              "Latin America and the Caribbean", "Latin America"))

    lac_total <- df %>%
      filter(Country != "World") %>%
      group_by(across(all_of(setdiff(names(df), c("Country", "num", "denom"))))) %>%
      summarise(num = sum_or_na(num), denom = sum_or_na(denom), .groups = "drop") %>%
      mutate(Country = "Latin America and the Caribbean")

    df %>%
      bind_rows(lac_total) %>%
      mutate(value = num / denom * multiplier) %>%
      select(-num, -denom)
  }
}

# Default source for the CAIT/Climate Watch-derived indicators (CEPAL calculations based on the
# CAIT Climate Data Explorer plus World Bank open data)
define_source_cw <- function(df, indicator_id) {
  df %>% mutate(source_id = 10621)
}


## ---- indicator 4461 - greenhouse gas (GHG) emissions (per capita) ----

dim_config_4461 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4461 <- function(data) {
  data %>%
    rename(Country = country, Years = year) %>%
    select(Country, Years, num = emissions, denom = population)
}

transform_4461 <- function(data) {
  data
}

spec_4461 <- indicator_spec(
  indicator_id = 4461,
  data = data_4461,
  max_year = max_year_cw,
  dim_config = dim_config_4461,
  filter_data = filter_4461,
  transform_data = transform_4461,
  calculate_regional = calculate_regional_ratio(1e6), # tonnes of CO2eq per capita
  define_source = define_source_cw
)


## ---- indicator 4463 - greenhouse gas (GHG) emissions (per GDP) ----

dim_config_4463 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4463 <- function(data) {
  data %>%
    rename(Country = country, Years = year, gdp = gdp_constant_2015_usd) %>%
    select(Country, Years, num = emissions, denom = gdp) %>%
    filter(!is.na(num) & !is.na(denom)) # filter out countries that are missing emissions OR gdp data
}

transform_4463 <- function(data) {
  data
}

spec_4463 <- indicator_spec(
  indicator_id = 4463,
  data = data_4463,
  max_year = max_year_cw,
  dim_config = dim_config_4463,
  filter_data = filter_4463,
  transform_data = transform_4463,
  calculate_regional = calculate_regional_ratio(1e12), # tonnes of CO2eq per million USD GDP
  define_source = define_source_cw
)


## ---- indicator 3351 - greenhouse gas (GHG) emissions by sector ----

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
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Total, excluding land use change and forestry")

  data %>%
    bind_rows(total)
}

spec_3351 <- indicator_spec(
  indicator_id = 3351,
  data = data_3351,
  max_year = max_year_cw,
  dim_config = dim_config_3351,
  filter_data = filter_3351,
  transform_data = transform_3351,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 4462 - greenhouse gas (GHG) emissions of the energy sector ----

dim_config_4462 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "84302"),
  pub_col = c("208_name", "29117_name", "84302_name")
)

filter_4462 <- function(data) {
  data %>%
    rename(Country = country, Type = sector, Years = year) %>%
    select(Country, Type, Years, value) %>%
    mutate(Type = case_when(
      Type == "Transportation" ~ "Transport",
      TRUE ~ Type
    )) %>%
    filter(!is.na(value))
}

transform_4462 <- function(data) {
  total <- data %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Total")

  data %>%
    bind_rows(total)
}

spec_4462 <- indicator_spec(
  indicator_id = 4462,
  data = data_4462,
  max_year = max_year_cw,
  dim_config = dim_config_4462,
  filter_data = filter_4462,
  transform_data = transform_4462,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 3387 - share of greenhouse gas (GHG) emissions relative to the global total ----

dim_config_3387 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3387 <- function(data) {
  data %>%
    rename(Country = country, Years = year) %>%
    select(Country, Years, value)
}

transform_3387 <- function(data) {
  world <- data %>%
    filter(Country == "World")

  data %>%
    filter(Country != "World") %>%
    left_join(world, by = "Years", suffix = c("", ".wld")) %>%
    mutate(prop = value / value.wld * 100) %>%
    select(Country, Years, value = prop)
}

spec_3387 <- indicator_spec(
  indicator_id = 3387,
  data = data_3387,
  max_year = max_year_cw,
  dim_config = dim_config_3387,
  filter_data = filter_3387,
  transform_data = transform_3387,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 3158 - carbon dioxide (CO2) emissions (Total) ----

dim_config_3158 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3158 <- function(data) {
  data %>%
    rename(Country = country, Years = year) %>%
    select(Country, Years, value)
}

transform_3158 <- function(data) {
  data
}

spec_3158 <- indicator_spec(
  indicator_id = 3158,
  data = data_3158,
  max_year = max_year_cw,
  dim_config = dim_config_3158,
  filter_data = filter_3158,
  transform_data = transform_3158,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 5649 - carbon dioxide (CO2) emissions (per capita) ----

dim_config_5649 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_5649 <- function(data) {
  data %>%
    rename(Country = country, Years = year) %>%
    select(Country, Years, num = emissions, denom = population)
}

transform_5649 <- function(data) {
  data
}

spec_5649 <- indicator_spec(
  indicator_id = 5649,
  data = data_5649,
  max_year = max_year_cw,
  dim_config = dim_config_5649,
  filter_data = filter_5649,
  transform_data = transform_5649,
  calculate_regional = calculate_regional_ratio(1e6), # tonnes of CO2eq per capita
  define_source = define_source_cw
)


## ---- indicator 5650 - carbon dioxide (CO2) emissions (per GDP) ----

dim_config_5650 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_5650 <- function(data) {
  data %>%
    rename(Country = country, Years = year, gdp = gdp_constant_2015_usd) %>%
    select(Country, Years, num = emissions, denom = gdp) %>%
    filter(!is.na(num) & !is.na(denom)) # filter out countries that are missing emissions OR gdp data
}

transform_5650 <- function(data) {
  data
}

spec_5650 <- indicator_spec(
  indicator_id = 5650,
  data = data_5650,
  max_year = max_year_cw,
  dim_config = dim_config_5650,
  filter_data = filter_5650,
  transform_data = transform_5650,
  calculate_regional = calculate_regional_ratio(1e12), # tonnes of CO2eq per million USD GDP
  define_source = define_source_cw
)


## ---- indicator 3159 - share of carbon dioxide (CO2) emissions relative to the global total ----

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
    mutate(prop = value / value.wld * 100) %>%
    select(Country, Years, value = prop)
}

spec_3159 <- indicator_spec(
  indicator_id = 3159,
  data = data_3159,
  max_year = max_year_cw,
  dim_config = dim_config_3159,
  filter_data = filter_3159,
  transform_data = transform_3159,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 2027 - carbon dioxide (CO2) emissions (Total, per capita, and per GDP) [DEPRECATED] ----
## this composite indicator was deprecated with the creation of separate total, GDP and per capita indicators for co2 emissions
