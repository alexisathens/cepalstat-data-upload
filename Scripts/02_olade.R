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

# This script processes OLADE energy indicators using the automated process_indicator() function
# See data cleaning notes at: cepalstat-data-upload\Data\Raw\olade\energy_indicators_overview.xlsx

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

input_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

# read energy type dimension mappings
energy_types <- read_excel(paste0(input_path, "/energy_dimensions_crosswalk.xlsx"))

# get mappings from olade energy sectors to cepalstat energy sectors
energy_econ_sectors <- read_excel(paste0(input_path, "/energy_dimensions_crosswalk.xlsx"), sheet = "dimensions_crosswalk_78134")

max_year <- 2024 # define most recent year with full data


# ---- read downloaded files ----

data_prod <- read_csv(paste0(input_path, "/energy_production_clean.csv"))
data_supply <- read_csv(paste0(input_path, "/energy_supply_clean.csv"))
data_cons <- read_csv(paste0(input_path, "/energy_consumption_clean.csv"))
data_cons_sec <- read_csv(paste0(input_path, "/energy_consumption_sector_clean.csv"))
data_losses <- read_csv(paste0(input_path, "/electricity_losses_clean.csv"))
data_infra <- read_csv(paste0(input_path, "/electricity_infra_clean.csv"))


# ---- core energy indicators ----

# ---- indicator 5672/2040 — energy production ----
# indicator 2040 used a duplicate dimension (dimension 20726 instead of dimension 44966)
# 20726 - Tipo de energía__Primaria_Secundaria (outdated; missing newer energy sources and total)
# 44966 - Tipo de energía__Primaria_y_Secundaria (current dimension)
# indicator 5672 was created as a clone to be updated with the correct information and utilized moving forward

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 5672

dim_config_5672 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name")
)

# Read in data for this indicator
# data <- data_prod

filter_5672 <- function(data) {
  data %>%
    filter(Years <= max_year)
}

transform_5672 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # summarize by energy type
    group_by(Country, Years, type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(Type = type)
}

footnotes_5672 <- function(data) {
  data # keep footnotes_id as empty
}

source_5672 <- function() {
  # get_indicator_sources(2040) # define source for first time since this is a new indicator
  714 # OLADE - Economic Energy Information System
}

result_5672 <- process_indicator(
  new_indicator = TRUE, # NEW FOR 2026 (**remove this for future runs)
  indicator_id = indicator_id,
  data = data_prod,
  dim_config = dim_config_5672,
  filter_fn = filter_5672,
  transform_fn = transform_5672,
  footnotes_fn = footnotes_5672,
  source_fn = source_5672,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2487 — primary and secondary energy supply ----

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 2487

dim_config_2487 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name")
)

# Read in data for this indicator
# data <- data_supply

filter_2487 <- function(data) {
  data %>% 
    filter(Years <= max_year)
}

transform_2487 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # summarize by energy type
    group_by(Country, Years, type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(Type = type)
}

footnotes_2487 <- function(data) {
  data %>%
    mutate(footnotes_id = case_when(
      Type == "Total primaries" ~ "5896", # Includes the following energy resources: petroleum, natural gas, coal, hydroenergy, geothermal, nuclear, firewood, cane bagasse, wind, solar, ethanol, biodiesel, biogas, other biomass and other primary sources.
      Type == "Total secondaries" ~ "5897", # Includes the following energy resources: electricity, liquefied petroleum gas, gasoline/alcohol, kerosene/jet fuel, diesel oil, fuel oil, coke, charcoal, gases, other secondary and non-energy sources. 
      TRUE ~ footnotes_id
    ))
}

result_2487 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_2487,
  filter_fn = filter_2487,
  transform_fn = transform_2487,
  footnotes_fn = footnotes_2487,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 3154 — renewable proportion of primary energy supply ----

indicator_id <- 3154

dim_config_3154 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_supply

filter_3154 <- function(data) {
  data %>% 
    filter(Years <= max_year)
}

transform_3154 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type, renewable) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # filter on renewables or total primaries
    filter(type == "Total primaries" | renewable == "Y") %>% 
    mutate(renewable = ifelse(!is.na(renewable), "Renewable", "Total")) %>%
    # calculate renewable share of total primary energy
    group_by(Country, Years, renewable) %>%
    summarize(value = sum_or_na(value), .groups = "drop") %>%
    pivot_wider(names_from = renewable) %>%
    mutate(value = Renewable / Total * 100) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

footnotes_3154 <- function(data) {
  data # keep footnotes_id as empty
}

result_3154 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_3154,
  filter_fn = filter_3154,
  transform_fn = transform_3154,
  footnotes_fn = footnotes_3154,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2486 — primary energy supply from renewable and non-renewable sources, by type of energy ----

indicator_id <- 2486

dim_config_2486 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name")
)

# Read in data for this indicator
# data <- data_supply

filter_2486 <- function(data) {
  data %>% 
    filter(Years <= max_year)
}

transform_2486 <- function(data) {
  data %<>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # create clean/renewable groupings
    filter(order == "Primary") %>% 
    mutate(cat = ifelse(is.na(renewable), "Non-renewable energy",
                        ifelse(is.na(clean), "Renewable energy not clean", "Clean renewable energy"))) %>%
    # summarize to country-year-type level
    group_by(Country, Years, type, cat) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
  
  # create category sub-totals
  subtotals <- data %>% 
    group_by(Country, Years, cat) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    rename(type = cat)
  
  # join together and format
  data %>% 
    select(-cat) %>% 
    bind_rows(subtotals) %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    mutate(type = ifelse(type == "Other primary", "Other clean", type)) %>% # adjust one label for this dimension
    rename(Type = type) %>% 
    filter(!is.na(value))
}

footnotes_2486 <- function(data) {
  data # keep footnotes_id as empty (overwritten in manual version, keeping default)
}

result_2486 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_2486,
  filter_fn = filter_2486,
  transform_fn = transform_2486,
  footnotes_fn = footnotes_2486,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4236 — proportion of renewable primary energy supply, by type of energy ----

indicator_id <- 4236

dim_config_4236 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name")
)

# Read in data for this indicator
# data <- data_supply

filter_4236 <- function(data) {
  data %>% 
    filter(Years <= max_year)
}

transform_4236 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    filter(renewable == "Y") %>% 
    # Calculate share of renewable primary energy, broken out by type
    group_by(Country, Years) %>%
    mutate(total = sum(value, na.rm = TRUE)) %>%
    ungroup() %>% 
    mutate(share = round(value / total * 100, 1)) %>%
    mutate(type = ifelse(type == "Other primary", "Other clean", type)) %>% # adjust one label for this dimension
    select(Country, Years, type, share) %>%
    rename(Type = type, value = share) %>%
    filter(!is.na(value))
}

footnotes_4236 <- function(data) {
  data # keep footnotes_id as empty
}

result_4236 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_4236,
  filter_fn = filter_4236,
  transform_fn = transform_4236,
  footnotes_fn = footnotes_4236,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 5730/2041 — energy consumption ----
# indicator 2041 used a duplicate dimension (dimension 20726 instead of dimension 44966)
# 20726 - Tipo de energía__Primaria_Secundaria (outdated; missing newer energy sources and total)
# 44966 - Tipo de energía__Primaria_y_Secundaria (current dimension)
# indicator 5730 was created as a clone to be updated with the correct information and utilized moving forward

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 5730

dim_config_5730 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name")
)

# Read in data for this indicator
# data <- data_cons

filter_5730 <- function(data) {
  data %>%
    filter(Years <= max_year)
}

transform_5730 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # summarize by energy type
    group_by(Country, Years, type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(Type = type)
}

footnotes_5730 <- function(data) {
  data # keep footnotes_id as empty
}

source_5730 <- function() {
  # get_indicator_sources(2041) # define source for first time since this is a new indicator
  714 # OLADE - Economic Energy Information System
}

result_5730 <- process_indicator(
  new_indicator = TRUE, # NEW FOR 2026 (**remove this for future runs)
  indicator_id = indicator_id,
  data = data_cons,
  dim_config = dim_config_5730,
  filter_fn = filter_5730,
  transform_fn = transform_5730,
  footnotes_fn = footnotes_5730,
  source_fn = source_5730,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)

# ---- economic-energy indicators ----

# ---- indicator 4174 — GDP energy intensity (primary energy supply / GDP) ----

indicator_id <- 4174

dim_config_4174 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_supply

filter_4174 <- function(data) {
  data %>%
    filter(Type == "Total primaries" & Years <= max_year) %>%
    select(-Type)
}

transform_4174 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dolllars
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join PIB data and calculate energy intensity
  data %>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(supply = value) %>%
    mutate(value = supply / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

footnotes_4174 <- function(data) {
  data # keep footnotes_id as empty
}

result_4174 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_4174,
  filter_fn = filter_4174,
  transform_fn = transform_4174,
  footnotes_fn = footnotes_4174,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2023 — GDP energy intensity (final energy consumption / GDP) ----

indicator_id <- 2023

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2023 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_cons

filter_2023 <- function(data) {
  data %>%
    filter(Type == "Total" & Years <= max_year) %>%
    select(-Type)
}

transform_2023 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dolllars
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join PIB data and calculate energy intensity
  data %>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(cons = value) %>%
    mutate(value = cons / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

footnotes_2023 <- function(data) {
  data # keep footnotes_id as empty
}

result_2023 <- process_indicator(
  indicator_id = indicator_id,
  data = data_cons,
  dim_config = dim_config_2023,
  filter_fn = filter_2023,
  transform_fn = transform_2023,
  footnotes_fn = footnotes_2023,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4243 — GDP energy intensity by economic activity ----

indicator_id <- 4243

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4243 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "78134"),
  pub_col = c("208_name", "29117_name", "78134_name")
)

# Read in data for this indicator
# data <- data_cons_sec

filter_4243 <- function(data) {
  data %<>%
    filter(Years <= max_year & Years >= 1990) %>%  # remove most recent year with only LatAm, and remove data prior to 1990 as that's when the econ series starts
    filter(Type != "Residential") # olade classifies sectors by consumption and so includes residential use; cepalstat calculates gdp by production and so doesn't include
}

transform_4243 <- function(data) {
  data %<>% 
    left_join(energy_econ_sectors %>% distinct(olade_label, dim_label), by = c("Type" = "olade_label")) %>% 
    group_by(Country, Years, dim_label) %>% 
    summarize(cons = sum(value, na.rm = TRUE), .groups = "drop")
  
  # Obtain GDP by economic activity from CEPALSTAT (indicator 2216)
  pib_sector <- call.data(id.indicator = 2216) %>% as_tibble()
  # 2216 - Annual Gross Domestic Product (GDP) by activity at constant prices in dollars (Millions of dollars, 2018$)
  
  pib_sector %<>% 
    distinct(Country, Years, Type = Rubro__Sector_Cuentas_nacionales_anuales, value) %>% # there are currently exact duplicates in cepalstat, take distinct values until this is fixed (issue confirmed by Patricia)
    left_join(energy_econ_sectors %>% distinct(econ_label, dim_label), by = c("Type" = "econ_label")) %>% 
    group_by(Country, Years, dim_label) %>% 
    summarize(pib = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    filter(!is.na(dim_label)) # remove extra econ categories

  data %>% 
    left_join(pib_sector, by = c("Country", "Years", "dim_label")) %>% 
    mutate(cons = cons / 1e12) %>% # convert from joules to terajoules
    mutate(value = cons / pib) %>% 
    rename(Type = dim_label) %>% 
    select(-cons, -pib) %>% 
    filter(!is.na(value))
}

footnotes_4243 <- function(data) {
  data # keep footnotes_id as empty
}

result_4243 <- process_indicator(
  indicator_id = indicator_id,
  data = data_cons_sec,
  dim_config = dim_config_4243,
  filter_fn = filter_4243,
  transform_fn = transform_4243,
  footnotes_fn = footnotes_4243,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4183 — variation rate of GDP energy intensity (primary energy supply / GDP) ----

indicator_id <- 4183

dim_config_4183 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_supply

filter_4183 <- function(data) {
  data %>%
    filter(Type == "Total primaries" & Years <= max_year) %>%
    select(-Type)
}

transform_4183 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dollars (millions)
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join GDP data and calculate energy intensity
  data %<>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(supply = value) %>%
    mutate(value = supply / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
  
  # Calculate variation rate: ((Mt - Mt-1) / Mt-1) * 100
  data %>%
    arrange(Country, Years) %>%
    group_by(Country) %>%
    mutate(
      value_prev = lag(value),
      value = ((value - value_prev) / value_prev) * 100
    ) %>%
    ungroup() %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

footnotes_4183 <- function(data) {
  data # keep footnotes_id as empty
}

source_4183 <- function() {
  10685 # Calculations made by ECLAC based on the economic and energy information system of OLADE 
}

result_4183 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_4183,
  filter_fn = filter_4183,
  transform_fn = transform_4183,
  footnotes_fn = footnotes_4183,
  source_fn = source_4183,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4184 — variation rate of GDP energy intensity (final energy consumption / GDP) ----

indicator_id <- 4184

dim_config_4184 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_cons

filter_4184 <- function(data) {
  data %>%
    filter(Type == "Total" & Years <= max_year) %>%
    select(-Type)
}

transform_4184 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dollars
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join PIB data and calculate energy intensity
  data %<>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(cons = value) %>%
    mutate(value = cons / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
  
  # Calculate variation rate: ((Mt - Mt-1) / Mt-1) * 100
  data %>%
    arrange(Country, Years) %>%
    group_by(Country) %>%
    mutate(
      value_prev = lag(value),
      value = ((value - value_prev) / value_prev) * 100
    ) %>%
    ungroup() %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

footnotes_4184 <- function(data) {
  data # keep footnotes_id as empty
}

source_4184 <- function() {
  10685 # Calculations made by ECLAC based on the economic and energy information system of OLADE 
}

result_4184 <- process_indicator(
  indicator_id = indicator_id,
  data = data_cons,
  dim_config = dim_config_4184,
  filter_fn = filter_4184,
  transform_fn = transform_4184,
  footnotes_fn = footnotes_4184,
  source_fn = source_4184,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- electricity indicators ----

# ---- indicator 1754 — electricity consumption ----

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 1754

dim_config_1754 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_cons

filter_1754 <- function(data) {
  data %>%
    filter(Years <= max_year) %>% 
    filter(Type == "Electricity") %>% 
    select(-Type)
}

transform_1754 <- function(data) {
  data %>% 
    mutate(value = value / 3.6e12) # convert from joules to GWh
}

footnotes_1754 <- function(data) {
  data # keep footnotes_id as empty
}

result_1754 <- process_indicator(
  indicator_id = indicator_id,
  data = data_cons,
  dim_config = dim_config_1754,
  filter_fn = filter_1754,
  transform_fn = transform_1754,
  footnotes_fn = footnotes_1754,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)

# ---- indicator 4234 — electricity losses ----

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 4234

dim_config_4234 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_losses

filter_4234 <- function(data) {
  data %>%
    filter(Years <= max_year) %>% 
    filter(Type == "Electricity") %>% 
    select(-Type)
}

transform_4234 <- function(data) {
  data
  # note this data source is in its original units (GWh), so no need to convert anything
}

footnotes_4234 <- function(data) {
  data # keep footnotes_id as empty
}

result_4234 <- process_indicator(
  indicator_id = indicator_id,
  data = data_losses,
  dim_config = dim_config_4234,
  filter_fn = filter_4234,
  transform_fn = transform_4234,
  footnotes_fn = footnotes_4234,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)

# ---- indicator 4235 - proportion of electricity losses ----

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 4235

dim_config_4235 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# Read in data for this indicator
# data <- data_supply

filter_4235 <- function(data) {
  data %>%
    filter(Years <= max_year) %>% 
    filter(Type == "Electricity") %>% 
    select(-Type)
}

transform_4235 <- function(data) {
  data %<>% 
    mutate(supply = value / 3.6e12) %>%  # convert from joules to GWh
    select(-value)
  
  losses <- data_losses
  
  losses %<>% 
    filter(Years <= max_year) %>% 
    filter(Type == "Electricity") %>% 
    select(-Type) %>% 
    rename(losses = value)
  
  data %>% 
    full_join(losses, by = c("Country", "Years")) %>% 
    mutate(value = losses / supply * 100) %>% 
    filter(!is.na(value)) %>% 
    select(-supply, -losses)
}

footnotes_4235 <- function(data) {
  data # keep footnotes_id as empty
}

result_4235 <- process_indicator(
  indicator_id = indicator_id,
  data = data_supply,
  dim_config = dim_config_4235,
  filter_fn = filter_4235,
  transform_fn = transform_4235,
  footnotes_fn = footnotes_4235,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4150 — installed capacity for producing electricity ----

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

indicator_id <- 4150

dim_config_4150 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "77605"),
  pub_col = c("208_name", "29117_name", "77605_name")
)

# Read in data for this indicator
# data <- data_infra

filter_4150 <- function(data) {
  data %>%
    filter(Years <= max_year & Years >= 2000) %>% # more complete series begins in 2000
    mutate(Type = case_when(
      Type == "Hidro" ~ "Hidroeléctrica",
      Type == "Térmica no renovable (combustión)" ~ "Térmica no renovable",
      Type == "Térmica renovable (combustión)" ~ "Térmica renovable",
      TRUE ~ Type
    ))
}

transform_4150 <- function(data) {
  data %>% 
    mutate(Type = case_when( # format labels
      Type == "Hydro" ~ "Hydroelectric",
      Type == "Non-renewable thermal (combustion)" ~ "Non-renewable Thermal",
      Type == "Renewable thermal (combustion)" ~ "Renewable Thermal",
      TRUE ~ Type
    )) %>% 
    bind_rows(
      group_by(., Country, Years) %>%  # Create summary row
        summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
        mutate(Type = "Total")) %>%
    arrange(Country, Years, Type)
}

footnotes_4150 <- function(data) {
  data # keep footnotes_id as empty
}

result_4150 <- process_indicator(
  indicator_id = indicator_id,
  data = data_infra,
  dim_config = dim_config_4150,
  filter_fn = filter_4150,
  transform_fn = transform_4150,
  footnotes_fn = footnotes_4150,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)

# ---- indicator 1755 — installed capacity for producing electricity (historical series) ----

# This OLADE series runs from 1970 to 2015. The data shows the total electrical capacity for countries.
# This series was replaced by indicator 4150 — Installed capacity for producing electricity, by source, which includes more detailed data.
