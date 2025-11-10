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


# ---- generic indicator processing function ----

# now use process_indicator()
# moved to process_indicator_fn.R

# ## testing
# indicator_id <- 4176
# data <- lc
# dim_config <- dim_config_4176
# filter_fn <- filter_4176
# transform_fn <- transform_4176
# footnotes_fn <- footnotes_4176
# 
# process_fao_indicator <- function(indicator_id, data, dim_config,
#                                   filter_fn, transform_fn, footnotes_fn,
#                                   diagnostics = TRUE, export = TRUE) {
#   message(glue("▶ Processing indicator {indicator_id}..."))
#   
#   ## 1. Filter and transform FAO data
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
#     format_for_wasabi(indicator_id)
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


# FAO LAND USE (RL) INDICATORS -----

## ---- dummy indicator - cropland area ----

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

result_2035 <- process_fao_indicator(
  indicator_id = 2035,
  data = rl,
  dim_config = dim_config_2035,
  filter_fn = filter_2035,
  transform_fn = transform_2035,
  footnotes_fn = footnotes_2035,
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

result_1869 <- process_fao_indicator(
  indicator_id = 1869,
  data = rl,
  dim_config = dim_config_1869,
  filter_fn = filter_1869,
  transform_fn = transform_1869,
  footnotes_fn = footnotes_1869,
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
