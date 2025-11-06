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

# Read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

# Read in the downloaded emdat data
emdat <- read_xlsx(here("Data/Raw/emdat/public_emdat_custom_request_2025-11-04.xlsx"))

# Define latest year of data
max_year <- 2024


# ---- generic indicator processing function ----

# now use process_indicator()
# moved to process_indicator_fn.R

# ## testing
# indicator_id <- 4046
# data <- emdat
# dim_config <- dim_config_4046
# filter_fn <- filter_4046
# transform_fn <- transform_4046
# footnotes_fn <- footnotes_4046
# 
# ## this function is the exact same as process_fao_indicator()
# process_emdat_indicator <- function(indicator_id, data, dim_config,
#                                   filter_fn, transform_fn, footnotes_fn,
#                                   source_id = NULL,
#                                   diagnostics = TRUE, export = TRUE) {
#   message(glue("▶ Processing indicator {indicator_id}..."))
# 
#   ## 1. Filter and transform EMDAT data
#   df <- data %>% filter_fn() %>% transform_fn()
#   
#   # Overwrite country names with std_name in iso file
#   df %<>%
#     left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
#     mutate(Country = coalesce(std_name, Country)) %>%
#     select(-std_name)
# 
#   # Filter out extra non-LAC groups
#   df %<>%
#     filter(Country %in% iso$name)
#   # remove Bonaire, Sint Eustatius and Saba and Netherlands Antilles (former)
# 
#   # remove regional totals, construct ECLAC total from sum of countries
#   df %<>%
#     filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America and the Caribbean", "Latin America"))
# 
#   # Correct types
#   df %<>%
#     mutate(Years = as.character(Years))
#   
#   assert_no_duplicates(df)
# 
#   ## 2. Create ECLAC regional total
#   eclac_totals <- df %>%
#     group_by(across(all_of(setdiff(names(df), c("Country", "value"))))) %>%
#     summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
#     mutate(Country = "Latin America and the Caribbean")
# 
#   df <- bind_rows(df, eclac_totals) %>%
#     arrange(Country, Years)
#   
#   ## 3. Harmonize labels
#   pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels()
#   
#   join_keys <- setNames(dim_config$pub_col, dim_config$data_col)
#   
#   # Keep only matching columns and join
#   pub <- pub %>% select(all_of(unname(join_keys)), value)
#   comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
#   
#   # Summarize dimension overlap
#   comp_sum <- get_comp_summary_table(comp, dim_config)
#   
#   ## 4. Inspect differences between public and new file
#   if(diagnostics) {
#     message(glue("🧾 Comparison summary for indicator {indicator_id}:"))
#     
#     # 1️⃣ Missing from new data (likely label changes or dropped series)
#     missing_old <- comp_sum %>%
#       filter(status == "Old Only")
#     if (nrow(missing_old) > 0) {
#       message("⚠️  Dimensions present in old data only:")
#       print(missing_old %>% count(dim_name, sort = TRUE))
#       print(missing_old)
#     }
#     
#     # 2️⃣ Newly added in updated data (new years or countries)
#     missing_new <- comp_sum %>%
#       filter(status == "New Only")
#     if (nrow(missing_new) > 0) {
#       message("🆕  Dimensions present in new data only:")
#       print(missing_new %>% count(dim_name, sort = TRUE))
#       print(missing_new)
#     }
#   }
#   
#   ## 5. Join dimensions
#   df_f <- join_data_dim_members(df, dim_config)
#   assert_no_na_cols(df_f)
#   
#   ## 6. Add footnotes and format
#   df_f %<>%
#     mutate(footnotes_id = "") %>% 
#     footnotes_fn()
#   
#   df_f %<>% 
#     select(ends_with("_id"), value) %>%
#     format_for_wasabi(indicator_id, source_id = source_id)
#   
#   assert_no_na_cols(df_f)
#   
#   ## 7. Create comparison file
#   # Rebuild comp from current cleaned data and latest CEPALSTAT version
#   comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
#   comp <- join_data_dim_members(comp, dim_config)
#   assert_no_na_cols(comp, !contains("value"))
#   
#   # Run comparison checks and format
#   comp <- create_comparison_checks(comp, dim_config)
#   
#   # 8. Optional export
#   if (export) {
#     dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")
#     write_xlsx(df_f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))
#     write_xlsx(comp, glue(here("Data/Checks/comp_id{indicator_id}.xlsx")))
#     message(glue("  ✓ Exported cleaned and comparison files for {indicator_id}"))
#   }
#   
#   return(list(clean = df_f, comp = comp, comp_sum = comp_sum))
#   
# }


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
    filter(as.numeric(`Start Year`) >= 1970 & as.numeric(`Start Year`) < 2025) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

transform_4046 <- function(data) {
  data %>% 
    mutate(Type = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    mutate(Years = as.character(`Start Year`)) %>% 
    # Current year data doesn't have CPI adjustment yet, so use that column if Adjusted is empty
    mutate(value = ifelse(!is.na(`Total Damage, Adjusted ('000 US$)`), `Total Damage, Adjusted ('000 US$)`, `Total Damage ('000 US$)`)) %>%
    mutate(value = replace_na(value, 0)) %>% 
    group_by(Country, Years, Type) %>%
    summarize(value = sum(value, na.rm = T)) %>% 
    ungroup() %>% 
    select(Country, Years, Type, value)
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
## this indicator is no longer maintained
# indicator_id <- 1837
# 
# # Fill out dim config table by matching the following info:
# # get_indicator_dimensions(indicator_id)
# # print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())
# 
# dim_config_1837 <- tibble(
#   data_col = c("Country", "Years", "Type", "Indicator"),
#   dim_id = c("208", "29117", "21714", "21725"),
#   pub_col = c("208_name", "29117_name", "21714_name", "21725_name")
# )
# 
# filter_1837 <- function(data) {
#   data %<>% 
#     filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
#     filter(as.numeric(`Start Year`) >= 1990 & as.numeric(`Start Year`) <= max_year) %>% 
#     filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
# }
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
#     summarize(value = sum(value, na.rm = T)) %>% 
#     ungroup()
#     
#   type_sum <- data %>% 
#     group_by(Country, Years, Group, Indicator) %>% 
#     summarize(value = sum(value, na.rm = T)) %>% 
#     ungroup() %>% 
#     rename(Type = Group)
#   
#   data %<>% 
#     select(-Group) %>% 
#     bind_rows(type_sum)
# }
# 
# footnotes_1837 <- function(data) {
#   data
# }
# 
# result_1837 <- process_emdat_indicator(
#   indicator_id = 1837,
#   data = emdat,
#   dim_config = dim_config_1837,
#   filter_fn = filter_1837,
#   transform_fn = transform_1837,
#   footnotes_fn = footnotes_1837,
#   diagnostics = TRUE,
#   export = TRUE
# )


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
