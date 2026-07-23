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

# ---- data ----

# Read in the downloaded emdat data
emdat <- read_xlsx(here("Data/Raw/emdat/public_emdat_incl_hist_2026-07-06.xlsx"))

# ---- shared functions ----

# Define latest year of data
max_year_emdat <- 2025

# Common data filter
filter_emdat <- function(data, min_year = 1990) {
  data %>% 
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
    filter(as.numeric(`Start Year`) >= min_year) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

# Label standardization
standardize_emdat <- function(data) {
  data %>%
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
      TRUE ~ NA_character_
    )) %>%
    select(-`Disaster Subgroup`, -`Disaster Type`)
}

# Sub/group/total summarization
add_type_rollups <- function(data) {
  type_sum <- data %>%
    group_by(Country, Years, Group) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    rename(Type = Group)
  
  total_sum <- data %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Total")
  
  data %>%
    select(-Group) %>%
    bind_rows(type_sum) %>%
    bind_rows(total_sum)
}

## ---- indicator 4046 - economic cost of disasters ----
indicator_id <- 4046

dim_config_4046 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

filter_4046 <- function(data) filter_emdat(data, min_year = 1970)

transform_4046 <- function(data) {
  data %>% 
    mutate(value = ifelse(!is.na(`Total Damage, Adjusted ('000 US$)`), `Total Damage, Adjusted ('000 US$)`, `Total Damage ('000 US$)`)) %>%
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, value) %>% 
    standardize_emdat() %>% 
    group_by(Country, Years, Group, Type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1000) %>%  # convert '000 USD to millions
    add_type_rollups
}

spec_4046 <- indicator_spec(
  indicator_id = 4046,
  data = emdat,
  max_year = max_year_emdat,
  dim_config = dim_config_4046,
  filter_data = filter_4046,
  transform_data = transform_4046,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 5647 - number of disasters ----
indicator_id <- 5647

dim_config_5647 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

transform_5647 <- function(data) {
  data %>% 
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`) %>% 
    standardize_emdat() %>% 
    group_by(Country, Years, Group, Type) %>% 
    count() %>% 
    ungroup() %>% 
    rename(value = n) %>% 
    add_type_rollups()
}

spec_5647 <- indicator_spec(
  indicator_id = 5647,
  data = emdat,
  max_year = max_year_emdat,
  dim_config = dim_config_5647,
  filter_data = filter_emdat,
  transform_data = transform_5647,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 5645 - deaths caused by disasters ----
indicator_id <- 5645

dim_config_5645 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

transform_5645 <- function(data) {
  data %<>% 
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, `Total Deaths`) %>% 
    standardize_emdat() %>% 
    rename(value = `Total Deaths`) %>% 
    filter(!is.na(value)) %>% 
    group_by(Country, Years, Group, Type) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    add_type_rollups()
}

spec_5645 <- indicator_spec(
  indicator_id = 5645,
  data = emdat,
  max_year = max_year_emdat,
  dim_config = dim_config_5645,
  filter_data = filter_emdat,
  transform_data = transform_5645,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 5646 - persons affected by disasters ----
indicator_id <- 5646

dim_config_5646 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21714"),
  pub_col = c("208_name", "29117_name", "21714_name")
)

transform_5646 <- function(data) {
  data %<>% 
    select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, `Total Affected`) %>% 
    standardize_emdat() %>% 
    rename(value = `Total Affected`) %>% 
    filter(!is.na(value)) %>% 
    group_by(Country, Years, Group, Type) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    add_type_rollups()
}

spec_5646 <- indicator_spec(
  indicator_id = 5646,
  data = emdat,
  max_year = max_year_emdat,
  dim_config = dim_config_5646,
  filter_data = filter_emdat,
  transform_data = transform_5646,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 1837 - occurrence of disasters ----
## this indicator is no longer maintained
# indicator_id <- 1837
# 
# dim_config_1837 <- tibble(
#   data_col = c("Country", "Years", "Type", "Indicator"),
#   dim_id = c("208", "29117", "21714", "21725"),
#   pub_col = c("208_name", "29117_name", "21714_name", "21725_name")
# )
# 
# transform_1837 <- function(data) {
#   data %<>%
#     select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`,
#            `Total Deaths`, `Total Affected`) %>%
#     mutate(Type = paste0(`Disaster Type`, "s")) %>%
#     mutate(Type = case_when(
#       Type == "Mass movement (wet)s" ~ "Wet mass displacement", # was "desplacement", fixed typo in CEPALSTAT admin
#       Type == "Volcanic activitys" ~ "Volcanic eruptions",
#       Type == "Mass movement (dry)s" ~ "Dry mass displacement",
#       TRUE ~ Type
#     )) %>%
#     mutate(Group = case_when(
#       `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
#       `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
#       TRUE ~ NA_character_)) %>%
#     select(-`Disaster Subgroup`, -`Disaster Type`) %>%
#     mutate(`Number of events` = 1) %>%
#     select(-DisNo.) %>%
#     pivot_longer(cols = c(`Total Deaths`, `Total Affected`, `Number of events`), names_to = "Indicator") %>%
#     mutate(Indicator = case_when(
#       Indicator == "Total Deaths" ~ "Human deaths",
#       Indicator == "Total Affected" ~ "Directly affected persons",
#       TRUE ~ Indicator)) %>%
#     group_by(Country, Years, Group, Type, Indicator) %>%
#     summarize(value = sum(value, na.rm = T), .groups = "drop")
# 
#   type_sum <- data %>%
#     group_by(Country, Years, Group, Indicator) %>%
#     summarize(value = sum(value, na.rm = T), .groups = "drop") %>%
#     rename(Type = Group)
# 
#   total_sum <- data %>% 
#     group_by(Country, Years) %>% 
#     summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
#     mutate(Type = "Total")
#   
#   data %>% 
#     select(-Group) %>% 
#     bind_rows(type_sum) %>% 
#     bind_rows(total_sum)
# }