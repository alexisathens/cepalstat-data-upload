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


## ---- indicator 4461 — greenhouse gas (GHG) emissions (per capita) ----

indicator_id <- 4461

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4461 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4461 <- function(data) {
  data %>%
    rename(
      Country = country,
      Years = year
    ) %>% 
    select(Country, Years, emissions, population)
}

transform_4461 <- function(data) {
  data
}

regional_4461 <- function(data) {
  # create eclac total
  eclac_totals <- data %>%
    filter(Country != "World") %>% 
    group_by(Years) %>%
    summarise(emissions = sum(emissions, na.rm = TRUE),
              population = sum(population), 
              .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals)
  
  # calculate per capita
  data %<>% 
    mutate(value = emissions/population * 1e6) %>% # transform into tonnes of CO2eq per capita
    arrange(Country, Years) %>% 
    select(Country, Years, value)
  
  return(data)
}


footnotes_4461 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
}

source_4461 <- function() {
  10621 # Cálculos realizados por la CEPAL en base a datos del Explorador de Datos Climáticos de la CAIT y datos de libre acceso del Banco Mundial 
}

result_4461 <- process_indicator(
  indicator_id = indicator_id,
  data = data_4461,
  dim_config = dim_config_4461,
  filter_fn = filter_4461,
  transform_fn = transform_4461,
  footnotes_fn = footnotes_4461,
  regional_fn = regional_4461,
  source_fn = source_4461,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 4463 — greenhouse gas (GHG) emissions (per GDP) ----

# figure out how to delete extra dimension before proceeding ***

# indicator_id <- 4463
# 
# # Fill out dim config table by matching the following info:
# # get_indicator_dimensions(indicator_id)
# # print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())
# 
# dim_config_4463 <- tibble(
#   data_col = c("Country", "Years"),
#   dim_id = c("208", "29117"),
#   pub_col = c("208_name", "29117_name")
# )
# 
# filter_4463 <- function(data) {
#   data %>%
#     rename(
#       Country = country,
#       Years = year,
#       gdp = gdp_constant_2015_usd
#     ) %>% 
#     select(Country, Years, emissions, gdp) %>% 
#     filter(!is.na(emissions) & !is.na(gdp)) # filter out countries that are missing emissions OR gdp data
# }
# 
# transform_4463 <- function(data) {
#   data
# }
# 
# regional_4463 <- function(data) {
#   # create eclac total
#   eclac_totals <- data %>%
#     filter(!Country %in% c("World")) %>%
#     group_by(Years) %>%
#     summarise(emissions = sum(emissions, na.rm = TRUE),
#               gdp = sum(gdp, na.rm = TRUE), 
#               .groups = "drop") %>%
#     mutate(Country = "Latin America and the Caribbean")
#   
#   data <- bind_rows(data, eclac_totals)
#   
#   # calculate per capita
#   data %<>% 
#     mutate(value = emissions/gdp * 1e12) %>% # transform into tonnes of CO2eq per millions of USD
#     arrange(Country, Years) %>% 
#     select(Country, Years, value)
#   
#   return(data)
# }
# 
# 
# footnotes_4463 <- function(data) {
#   data %>%
#     mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
# }
# 
# source_4463 <- function() {
#   10621 # Cálculos realizados por la CEPAL en base a datos del Explorador de Datos Climáticos de la CAIT y datos de libre acceso del Banco Mundial 
# }
# 
# result_4463 <- process_indicator(
#   indicator_id = indicator_id,
#   data = data_4463,
#   dim_config = dim_config_4463,
#   filter_fn = filter_4463,
#   transform_fn = transform_4463,
#   footnotes_fn = footnotes_4463,
#   regional_fn = regional_4463,
#   source_fn = source_4463,
#   diagnostics = TRUE,
#   export = TRUE
# )


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


## ---- indicator 4462 — greenhouse gas (GHG) emissions of the energy sector ----

indicator_id <- 4462 # ghg emissions

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

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
    )) %>% filter(!is.na(value))
}

transform_4462 <- function(data) {
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    mutate(Type = "Total")
  
  data %>% 
    bind_rows(total)
}

footnotes_4462 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_4462 <- process_indicator(
  indicator_id = 4462,
  data = data_4462,
  dim_config = dim_config_4462,
  filter_fn = filter_4462,
  transform_fn = transform_4462,
  # regional_fn = regional_4462, # default to sum of lac
  footnotes_fn = footnotes_4462,
  # source_fn = source_4462,
  diagnostics = TRUE,
  export = FALSE
)


## ---- indicator 3387 - share of greenhouse gas (GHG) emissions relative to the global total ----

indicator_id <- 3387

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

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
    mutate(prop = value/value.wld*100) %>% 
    select(Country, Years, value = prop)
}

footnotes_3387 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

result_3387 <- process_indicator(
  indicator_id = 3387,
  data = data_3387,
  dim_config = dim_config_3387,
  filter_fn = filter_3387,
  transform_fn = transform_3387,
  # regional_fn = regional_3387, # default to sum of lac
  footnotes_fn = footnotes_3387,
  # source_fn = source_3387,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 3158 — carbon dioxide (CO₂) emissions (Total) ----

indicator_id <- 3158 # co2 emissions

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_3158 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3158 <- function(data) {
  data %>%
    rename(
      Country = country,
      Years = year
    ) %>% 
    select(Country, Years, value)
}

transform_3158 <- function(data) {
  data
}

regional_3158 <- function(data) {
  # create eclac total
  eclac_totals <- data %>%
    filter(Country != "World") %>% 
    group_by(Years) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
    
  data <- bind_rows(data, eclac_totals)
  
  return(data)
}
  

footnotes_3158 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
}

result_3158 <- process_indicator(
  indicator_id = indicator_id,
  data = data_3158,
  dim_config = dim_config_3158,
  filter_fn = filter_3158,
  transform_fn = transform_3158,
  footnotes_fn = footnotes_3158,
  regional_fn = regional_3158,
  diagnostics = TRUE,
  export = FALSE
)

# internal_file <- "C:/Users/aathens/OneDrive - United Nations/Documentos/CEPALSTAT Data Process/cepalstat-data-upload/Data/Cleaned/id3158_2025-11-19T163441.xlsx"
# update <- read_excel(internal_file)
# 
# update %<>% 
#   mutate(members_id = str_remove(members_id, ",85390"))
# 
# write_xlsx(update, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


## ---- indicator 5649 — carbon dioxide (CO₂) emissions (per capita) ----

indicator_id <- 5649 # co2 emissions

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_5649 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_5649 <- function(data) {
  data %>%
    rename(
      Country = country,
      Years = year
    ) %>% 
    select(Country, Years, emissions, population)
}

transform_5649 <- function(data) {
  data
}

regional_5649 <- function(data) {
  # create eclac total
  eclac_totals <- data %>%
    filter(Country != "World") %>% 
    group_by(Years) %>%
    summarise(emissions = sum(emissions, na.rm = TRUE),
              population = sum(population), 
              .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals)
  
  # calculate per capita
  data %<>% 
    mutate(value = emissions/population * 1e6) %>% # transform into tonnes of CO2eq per capita
    arrange(Country, Years) %>% 
    select(Country, Years, value)
  
  return(data)
}


footnotes_5649 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
}

source_5649 <- function() {
  10621 # Cálculos realizados por la CEPAL en base a datos del Explorador de Datos Climáticos de la CAIT y datos de libre acceso del Banco Mundial 
}

result_5649 <- process_indicator(
  indicator_id = indicator_id,
  data = data_5649,
  dim_config = dim_config_5649,
  filter_fn = filter_5649,
  transform_fn = transform_5649,
  footnotes_fn = footnotes_5649,
  regional_fn = regional_5649,
  source_fn = source_5649,
  diagnostics = TRUE,
  export = TRUE
)


## ---- indicator 5650 — carbon dioxide (CO₂) emissions (per GDP) ----

indicator_id <- 5650 # co2 emissions

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_5650 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_5650 <- function(data) {
  data %>%
    rename(
      Country = country,
      Years = year,
      gdp = gdp_constant_2015_usd
    ) %>% 
    select(Country, Years, emissions, gdp) %>% 
    filter(!is.na(emissions) & !is.na(gdp)) # filter out countries that are missing emissions OR gdp data
}

transform_5650 <- function(data) {
  data
}

regional_5650 <- function(data) {
  # create eclac total
  eclac_totals <- data %>%
    filter(!Country %in% c("World")) %>%
    group_by(Years) %>%
    summarise(emissions = sum(emissions, na.rm = TRUE),
              gdp = sum(gdp, na.rm = TRUE), 
              .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  data <- bind_rows(data, eclac_totals)
  
  # calculate per capita
  data %<>% 
    mutate(value = emissions/gdp * 1e12) %>% # transform into tonnes of CO2eq per millions of USD
    arrange(Country, Years) %>% 
    select(Country, Years, value)
  
  return(data)
}


footnotes_5650 <- function(data) {
  data %>%
    mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
}

source_5650 <- function() {
  10621 # Cálculos realizados por la CEPAL en base a datos del Explorador de Datos Climáticos de la CAIT y datos de libre acceso del Banco Mundial 
}

result_5650 <- process_indicator(
  indicator_id = indicator_id,
  data = data_5650,
  dim_config = dim_config_5650,
  filter_fn = filter_5650,
  transform_fn = transform_5650,
  footnotes_fn = footnotes_5650,
  regional_fn = regional_5650,
  source_fn = source_5650,
  diagnostics = TRUE,
  export = TRUE
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


## ---- indicator 2027 — carbon dioxide (CO₂) emissions (Total, per capita, and per GDP)[DEPRECATED] ----
# # this composite indicator was deprecated with the creation of separate total, GDP and per capita indicators for co2 emissions
# indicator_id <- 2027 # co2 emissions
# 
# # Fill out dim config table by matching the following info:
# # get_indicator_dimensions(indicator_id)
# # print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())
# 
# dim_config_2027 <- tibble(
#   data_col = c("Country", "Years", "Calculation"),
#   dim_id = c("208", "29117", "26653"),
#   pub_col = c("208_name", "29117_name", "26653_name")
# )
# 
# filter_2027 <- function(data) {
#   data %>%
#     rename(
#       Country = country,
#       Years = year
#     ) %>%
#     mutate(
#       total_emissions = as.numeric(emissions),
#       population = as.numeric(population),
#       gdp_constant_2015_usd = as.numeric(gdp_constant_2015_usd)
#     ) %>%
#     select(
#       Country, Years, unit,
#       Total = total_emissions, Pop = population, GDP = gdp_constant_2015_usd
#     )
# }
# 
# transform_2027 <- function(data) {
#   data
# }
# 
# regional_2027 <- function(data) {
#   # create eclac total
#   eclac_totals <- data %>%
#     filter(Country != "World") %>% 
#     group_by(Years) %>%
#     summarise(Total = sum(Total, na.rm = TRUE),
#               Pop = sum(Pop, na.rm = TRUE),
#               GDP = sum(GDP, na.rm = TRUE), .groups = "drop") %>%
#     mutate(Country = "Latin America and the Caribbean")
#   
#   # manually subtract out venezuela with missing GDP data
#   missing_vz_gdp <- (nrow(data %>% filter(Country == "Venezuela" & is.na(GDP))) > 0)
#   
#   if(missing_vz_gdp) {
#     eclac_totals_gdp_adj <- data %>%
#       filter(Country != "World",
#              Country != "Venezuela") %>%          # exclude only here
#       group_by(Years) %>%
#       summarise(
#         Total_noVEN = sum(Total, na.rm = TRUE),      # emissions excluding VEN
#         GDP_noVEN = sum(GDP, na.rm = TRUE),       # GDP excluding VEN
#         .groups = "drop"
#       ) %>%
#       mutate(
#         Country = "Latin America and the Caribbean"
#       )
#     
#     eclac_totals %<>%
#       left_join(eclac_totals_gdp_adj, by = c("Years", "Country"))
#   }
#   
#   # join together
#   data <- bind_rows(data, eclac_totals) 
#   
#   # create calculation columns
#   data %<>% 
#     mutate(`Total, excluding land change use and forestry` = Total,
#            `Per capita` = Total / Pop * 1e6,
#            `By gross domestic product` = Total / GDP * 1e12) %>% 
#     select(-unit)
#   
#   if(missing_vz_gdp) {
#     data %<>% 
#       mutate(`By gross domestic product` = ifelse(Country == "Latin America and the Caribbean",
#                                                   Total_noVEN / GDP_noVEN * 1e12,
#                                                   `By gross domestic product`)) %>% 
#       select(-Total_noVEN, -GDP_noVEN)
#   }
#   
#   # clean df
#   data %<>% 
#     select(-Total, -Pop, -GDP) %>% 
#     pivot_longer(cols = c("Total, excluding land change use and forestry", "Per capita", "By gross domestic product"),
#                  names_to = "Calculation") %>% 
#     filter(!is.na(value)) # remove venezuela total per GDP data
#   
#   return(data)
# }
# 
# footnotes_2027 <- function(data) {
#   data %>%
#     mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
# }
# 
# result_2027 <- process_indicator(
#   indicator_id = indicator_id,
#   data = data_2027,
#   dim_config = dim_config_2027,
#   filter_fn = filter_2027,
#   transform_fn = transform_2027,
#   footnotes_fn = footnotes_2027,
#   regional_fn = regional_2027,
#   diagnostics = TRUE,
#   export = TRUE
# )