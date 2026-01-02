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

# read in indicator metadata
# meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

max_year <- 2024 # define most recent year with full data


# ---- read downloaded files ----
olade_path <- here("Data/Raw/olade")

data_g1 <- read_csv(paste0(olade_path, "/grupo1_raw.csv"))
data_g2 <- read_csv(paste0(olade_path, "/grupo2_raw.csv"))
data_g3 <- read_csv(paste0(olade_path, "/grupo3_raw.csv"))
data_g4 <- read_csv(paste0(olade_path, "/grupo4_raw.csv"))
data_g5 <- read_csv(paste0(olade_path, "/grupo5_raw.csv"))
data_g6 <- read_csv(paste0(olade_path, "/grupo6_raw.csv"))
data_g7 <- read_csv(paste0(olade_path, "/grupo7_raw.csv"))
data_g8 <- read_csv(paste0(olade_path, "/grupo8_raw.csv"))
data_g9 <- read_csv(paste0(olade_path, "/grupo9_raw.csv"))


# ---- indicator 1754 — Consumption of electric power ----

indicator_id <- 1754

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1754 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_1754 <- function(data) {
  data %>%
    select(-Type) # only type is "Electricidad"
}

transform_1754 <- function(data) {
  data # no transformations needed
}

footnotes_1754 <- function(data) {
  data # keep footnotes_id as empty
}

result_1754 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g4,
  dim_config = dim_config_1754,
  filter_fn = filter_1754,
  transform_fn = transform_1754,
  remove_lac = FALSE, # keep source LAC data from OLADE
  footnotes_fn = footnotes_1754,
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2023 — GDP energy intensity (final energy consumption) ----

indicator_id <- 2023

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2023 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2023 <- function(data) {
  data %>%
    filter(Years <= max_year) # remove most recent year with only LatAm
}

transform_2023 <- function(data) {
  data # no transformations needed
}

footnotes_2023 <- function(data) {
  data # keep footnotes_id as empty
}

result_2023 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g7,
  dim_config = dim_config_2023,
  filter_fn = filter_2023,
  transform_fn = transform_2023,
  footnotes_fn = footnotes_2023,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4235 — Proportion of losses in the electricity sector ----

indicator_id <- 4235

dim_config_4235 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4235 <- function(data) {
  data %>%
    filter(Years <= max_year) # remove most recent year with only LatAm
}

transform_4235 <- function(data) {
  data %>%
    mutate(value = as.numeric(value) * 100) # multiply by 100 to obtain percentage
}

footnotes_4235 <- function(data) {
  data # keep footnotes_id as empty
}

result_4235 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g8,
  dim_config = dim_config_4235,
  filter_fn = filter_4235,
  transform_fn = transform_4235,
  footnotes_fn = footnotes_4235,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2041 — Energy consumption ----

indicator_id <- 2041

dim_config_2041 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20726"),
  pub_col = c("208_name", "29117_name", "20726_name_es")
)

filter_2041 <- function(data) {
  data %>%
    filter(Years <= max_year)
}

transform_2041 <- function(data) {
  data %>%
    mutate(Type = case_when(
      Type == "Bagazo de caña" ~ "Productos de caña",
      Type == "Diésel oil con biodiésel" ~ "Diesel oil",
      Type == "Diésel oil sin biodiésel" ~ "Diesel oil",
      Type == "Gas licuado de petróleo" ~ "Gas licuado",
      Type == "Gasolina con etanol" ~ "Gasolinas/alcohol",
      Type == "Gasolina sin etanol" ~ "Gasolinas/alcohol",
      Type == "Kerosene/jet fuel" ~ "Kerosene y turbo",
      Type == "Coque" ~ "Coques",
      Type == "Total primarias" ~ "PRIMARIA",
      Type == "Total secundarias" ~ "SECUNDARIA",
      TRUE ~ Type
    )) %>%
    group_by(Country, Years, Type) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    # Drop new categories to avoid duplication
    filter(!Type %in% c("Biodiésel", "Biogás", "Etanol", "Eólica", "Otra biomasa", "Solar", "Total"))
}

footnotes_2041 <- function(data) {
  data # keep footnotes_id as empty
}

result_2041 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g6,
  dim_config = dim_config_2041,
  filter_fn = filter_2041,
  transform_fn = transform_2041,
  footnotes_fn = footnotes_2041,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 3154 — Renewable proportion of primary energy supply ----

indicator_id <- 3154

dim_config_3154 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3154 <- function(data) {
  data %>%
    # Standardize type labels
    mutate(Type = case_when(
      Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
      TRUE ~ Type
    ))
}

transform_3154 <- function(data) {
  renew <- c("Hidroenergía", "Geotermia", "Otras primarias", "Eólica", "Solar",
             "Leña", "Caña de azúcar y derivados", "Etanol", "Otra biomasa", "Biodiésel", "Biogás")

  data %>%
    # Filter only on renewables and total primary supply
    filter(Type %in% renew | Type == "Total primarias") %>%
    # Calculate renewable share of total primary energy
    mutate(Renewable = ifelse(Type %in% renew, "Renew", "Total")) %>%
    group_by(Country, Years, Renewable) %>%
    summarize(value = sum_or_na(value), .groups = "drop") %>%
    pivot_wider(names_from = Renewable) %>%
    mutate(value = Renew / Total * 100) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

footnotes_3154 <- function(data) {
  data # keep footnotes_id as empty
}

result_3154 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g1,
  dim_config = dim_config_3154,
  filter_fn = filter_3154,
  transform_fn = transform_3154,
  footnotes_fn = footnotes_3154,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2487 — Primary and secondary energy supply ----

indicator_id <- 2487

dim_config_2487 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name_es")
)

filter_2487 <- function(data) {
  data 
}

transform_2487 <- function(data) {
  data %>%
    mutate(Type = case_when(
      Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
      Type == "Diésel oil con biodiésel" ~ "Diesel oil",
      Type == "Diésel oil sin biodiésel" ~ "Diesel oil",
      Type == "Gas licuado de petróleo" ~ "Gas licuado",
      Type == "Gasolina con etanol" ~ "Gasolina/Alcohol",
      Type == "Gasolina sin etanol" ~ "Gasolina/Alcohol",
      Type == "Kerosene/jet fuel" ~ "Kerosene/Jet fuel",
      TRUE ~ Type
    )) %>%
    group_by(Country, Years, Type) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
}

footnotes_2487 <- function(data) {
  data %>%
    mutate(footnotes_id = case_when(
      Type == "Total primarias" ~ "5896",
      Type == "Total secundarias" ~ "5897",
      TRUE ~ footnotes_id
    ))
}

result_2487 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g1,
  dim_config = dim_config_2487,
  filter_fn = filter_2487,
  transform_fn = transform_2487,
  footnotes_fn = footnotes_2487,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4150 — Installed capacity for producing electricity, by source ----

indicator_id <- 4150

dim_config_4150 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "77605"),
  pub_col = c("208_name", "29117_name", "77605_name_es")
)

filter_4150 <- function(data) {
  data %>%
    mutate(Type = case_when(
      Type == "Hidro" ~ "Hidroeléctrica",
      Type == "Térmica no renovable (combustión)" ~ "Térmica no renovable",
      Type == "Térmica renovable (combustión)" ~ "Térmica renovable",
      TRUE ~ Type
    )) %>%
    filter(Years <= max_year) # remove LatAm only entry for 2024
}

transform_4150 <- function(data) {
  # Create summary row
  data %>%
    bind_rows(
      group_by(., Country, Years) %>%
        summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
        mutate(Type = "Total")) %>%
    arrange(Country, Years, Type)
}

footnotes_4150 <- function(data) {
  data # keep footnotes_id as empty
}

result_4150 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g9,
  dim_config = dim_config_4150,
  filter_fn = filter_4150,
  transform_fn = transform_4150,
  footnotes_fn = footnotes_4150,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2040 — Energy production ----

indicator_id <- 2040

dim_config_2040 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20726"),
  pub_col = c("208_name", "29117_name", "20726_name_es")
)

# ** fix these later to match indicator 2478 (dim 44966) - this is the duplicate dimension

filter_2040 <- function(data) {
  data %>%
    filter(Years <= max_year)
}

transform_2040 <- function(data) {
  ## data-specific issue: ** noticed that the total rows aren't always accurate for Brazil and this data source specifically, so recalculate manually
  primaries <- c("Petróleo", "Gas natural", "Carbón mineral", "Nuclear", "Hidroenergía", "Geotermia", "Eólica", "Solar", "Leña", "Bagazo de caña",
                 "Etanol", "Biodiésel", "Biogás", "Otra biomasa", "Otras primarias")

  secondaries <- c("Electricidad", "Gas licuado de petróleo", "Gasolina sin etanol", "Gasolina con etanol", "Kerosene/jet fuel", "Diésel oil sin biodiésel",
                   "Diésel oil con biodiésel", "Fuel oil", "Coque", "Carbón vegetal", "Gases", "Otras secundarias", "No energético")

  data %<>%
    filter(!Type %in% c("Total primarias", "Total secundarias", "Total")) %>%
    bind_rows(
      data %>% filter(Type %in% primaries) %>% group_by(Country, Years) %>% summarise(value = sum(value), .groups = "drop") %>% mutate(Type = "Total primarias"),
      data %>% filter(Type %in% secondaries) %>% group_by(Country, Years) %>% summarise(value = sum(value), .groups = "drop") %>% mutate(Type = "Total secundarias"),
      data %>% filter(!Type %in% c("Total primarias", "Total secundarias", "Total")) %>% group_by(Country, Years) %>% summarise(value = sum(value), .groups = "drop") %>% mutate(Type = "Total")
    ) %>%
    arrange(Country, Years, Type)
  
  data %>%
    mutate(Type = case_when(
      Type == "Bagazo de caña" ~ "Productos de caña",
      Type == "Diésel oil con biodiésel" ~ "Diesel oil",
      Type == "Diésel oil sin biodiésel" ~ "Diesel oil",
      Type == "Gas licuado de petróleo" ~ "Gas licuado",
      Type == "Gasolina con etanol" ~ "Gasolinas/alcohol",
      Type == "Gasolina sin etanol" ~ "Gasolinas/alcohol",
      Type == "Kerosene/jet fuel" ~ "Kerosene y turbo",
      Type == "Coque" ~ "Coques",
      Type == "Total primarias" ~ "PRIMARIA",
      Type == "Total secundarias" ~ "SECUNDARIA",
      TRUE ~ Type
    )) %>%
    group_by(Country, Years, Type) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    # Drop new categories to avoid duplication
    filter(!Type %in% c("Biodiésel", "Biogás", "Etanol", "Eólica", "Otra biomasa", "Solar", "Total"))
}

footnotes_2040 <- function(data) {
  data # keep footnotes_id as empty
}

result_2040 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g5,
  dim_config = dim_config_2040,
  filter_fn = filter_2040,
  transform_fn = transform_2040,
  footnotes_fn = footnotes_2040,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2486 — Primary energy supply from renewable and non-renewable sources ----

indicator_id <- 2486

dim_config_2486 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name_es")
)

filter_2486 <- function(data) {
  # Standardize labels
  data %>%
    mutate(Type = case_when(
      Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
      Type == "Otras primarias" ~ "Otras limpias", # ** changed for 2025 anuario
      TRUE ~ Type
    ))
}

transform_2486 <- function(data) {
  # Define energy type categories
  clean_renew_types <- c("Hidroenergía", "Geotermia", "Otras limpias", "Eólica", "Solar")
  nonclean_renew_types <- c("Leña", "Caña de azúcar y derivados", "Etanol", "Otra biomasa", "Biodiésel", "Biogás")
  nonrenew_types <- c("Petróleo", "Gas natural", "Carbón mineral", "Nuclear")

  # Manually add new labels for 2025
  these_types <- c(clean_renew_types, nonclean_renew_types, nonrenew_types)

  # Filter to only included types
  data %<>% filter(Type %in% these_types)

  # Create summary groups
  clean_renew <- data %>%
    filter(Type %in% clean_renew_types) %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Energía primaria renovable limpia")

  nonclean_renew <- data %>%
    filter(Type %in% nonclean_renew_types) %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Energía primaria renovable no limpia")

  nonrenew <- data %>%
    filter(Type %in% nonrenew_types) %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Energía primaria no renovable")

  # Join summary rows to data table
  data %>%
    bind_rows(clean_renew, nonclean_renew, nonrenew) %>%
    arrange(Country, Years, Type) %>%
    filter(!is.na(value))
}

footnotes_2486 <- function(data) {
  data # keep footnotes_id as empty (overwritten in manual version, keeping default)
}

result_2486 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g1,
  dim_config = dim_config_2486,
  filter_fn = filter_2486,
  transform_fn = transform_2486,
  footnotes_fn = footnotes_2486,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4236 — Share of renewable energy in primary energy supply by type ----

indicator_id <- 4236

dim_config_4236 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name_es")
)

filter_4236 <- function(data) {
  data %>%
    mutate(Type = case_when(
      Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
      Type == "Otras primarias" ~ "Otras limpias", # ** changed for 2025 anuario
      TRUE ~ Type
    ))
}

transform_4236 <- function(data) {
  # Keep only renewable types
  renew <- c("Hidroenergía", "Geotermia", "Otras limpias", "Eólica", "Solar",
             "Leña", "Caña de azúcar y derivados", "Etanol", "Otra biomasa", "Biodiésel", "Biogás")

  data %>%
    filter(Type %in% renew) %>%
    # Calculate share of renewable primary energy, broken out by type
    group_by(Country, Years) %>%
    mutate(total = sum(value, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(share = round(value / total * 100, 1)) %>%
    select(Country, Years, Type, share) %>%
    rename(value = share) %>%
    filter(!is.na(value))
}

footnotes_4236 <- function(data) {
  data # keep footnotes_id as empty
}

result_4236 <- process_indicator(
  indicator_id = indicator_id,
  data = data_g1,
  dim_config = dim_config_4236,
  filter_fn = filter_4236,
  transform_fn = transform_4236,
  footnotes_fn = footnotes_4236,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 4174 — GDP energy intensity (Primary energy supply / GDP) ----

indicator_id <- 4174

dim_config_4174 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4174 <- function(data) {
  data %>%
    filter(Type == "Total primarias") %>%
    select(-Type)
}

transform_4174 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  pib <- call.data(id.indicator = 2204) %>% as_tibble()

  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)

  # Join PIB data and calculate energy intensity
  data %>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>%
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
  data = data_g1,
  dim_config = dim_config_4174,
  filter_fn = filter_4174,
  transform_fn = transform_4174,
  footnotes_fn = footnotes_4174,
  remove_lac = FALSE, # keep source LAC data from OLADE
  diagnostics = TRUE,
  export = TRUE
)
