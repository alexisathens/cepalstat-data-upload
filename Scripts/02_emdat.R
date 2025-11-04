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


# ---- generic indicator processing function ----

## testing
# indicator_id <- 1869
# data <- emdat
# dim_config <- dim_config_1869
# filter_fn <- filter_1869
# transform_fn <- transform_1869
# footnotes_fn <- footnotes_1869

## this function is the exact same as process_fao_indicator()
process_emdat_indicator <- function(indicator_id, data, dim_config,
                                  filter_fn, transform_fn, footnotes_fn,
                                  diagnostics = TRUE, export = TRUE) {
  message(glue("▶ Processing indicator {indicator_id}..."))

  ## 1. Filter and transform EMDAT data
  df <- data %>% filter_fn() %>% transform_fn()
  
  # Overwrite country names with std_name in iso file
  df %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name)

  # Filter out extra non-LAC groups
  df %<>%
    filter(Country %in% iso$name)
  # remove Bonaire, Sint Eustatius and Saba and Netherlands Antilles (former)

  # remove regional totals, construct ECLAC total from sum of countries
  df %<>%
    filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America and the Caribbean", "Latin America"))

  # Correct types
  df %<>%
    mutate(Years = as.character(Years))

  ## 2. Create ECLAC regional total
  eclac_totals <- df %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value"))))) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")

  df <- bind_rows(df, eclac_totals) %>%
    arrange(Country, Years)
  
  ## 3. Harmonize labels
  pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels()
  
  join_keys <- setNames(dim_config$pub_col, dim_config$data_col)
  
  # Keep only matching columns and join
  pub <- pub %>% select(all_of(unname(join_keys)), value)
  comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
  
  # Summarize dimension overlap
  comp_sum <- get_comp_summary_table(comp, dim_config)
  
  ## 4. Inspect differences between public and new file
  if(diagnostics) {
    message(glue("🧾 Comparison summary for indicator {indicator_id}:"))
    
    # 1️⃣ Missing from new data (likely label changes or dropped series)
    missing_old <- comp_sum %>%
      filter(status == "Old Only")
    if (nrow(missing_old) > 0) {
      message("⚠️  Dimensions present in old data only:")
      print(missing_old %>% count(dim_name, sort = TRUE))
      print(missing_old)
    }
    
    # 2️⃣ Newly added in updated data (new years or countries)
    missing_new <- comp_sum %>%
      filter(status == "New Only")
    if (nrow(missing_new) > 0) {
      message("🆕  Dimensions present in new data only:")
      print(missing_new %>% count(dim_name, sort = TRUE))
      print(missing_new)
    }
  }
  
  ## 5. Join dimensions
  df_f <- join_data_dim_members(df, dim_config)
  assert_no_na_cols(df_f)
  
  ## 6. Add footnotes and format
  df_f %<>%
    mutate(footnotes_id = "") %>% 
    footnotes_fn()
  
  df_f %<>% 
    select(ends_with("_id"), value) %>%
    format_for_wasabi(indicator_id)
  
  assert_no_na_cols(df_f)
  
  ## 7. Create comparison file
  # Rebuild comp from current cleaned data and latest CEPALSTAT version
  comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
  comp <- join_data_dim_members(comp, dim_config)
  assert_no_na_cols(comp, !contains("value"))
  
  # Run comparison checks and format
  comp <- create_comparison_checks(comp, dim_config)
  
  # 8. Optional export
  if (export) {
    dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")
    write_xlsx(df_f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))
    write_xlsx(comp, glue(here("Data/Checks/comp_id{indicator_id}.xlsx")))
    message(glue("  ✓ Exported cleaned and comparison files for {indicator_id}"))
  }
  
  return(list(clean = df_f, comp = comp, comp_sum = comp_sum))
  
}


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
    filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical"))
}

transform_4046 <- function(data) {
  data %>% 
    mutate(Type = case_when(
      `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
      `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
      TRUE ~ NA_character_)) %>%
    mutate(value = ifelse(!is.na(`Total Damage, Adjusted ('000 US$)`), `Total Damage, Adjusted ('000 US$)`, `Total Damage ('000 US$)`)) %>%
    filter(!is.na(value)) %>% 
    select(Country, Years, Type, value)
}

## BEGIN HERE WED.
footnotes_4046 <- function(data) {
  data %>% 
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_4046 <- process_fao_indicator(
  indicator_id = 4046,
  data = emdat,
  dim_config = dim_config_4046,
  filter_fn = filter_4046,
  transform_fn = transform_4046,
  footnotes_fn = footnotes_4046,
  diagnostics = TRUE,
  export = FALSE
)