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


# ---- read downloaded files ----
olade_path <- here("Data/Raw/olade")

data_g1 <- read_csv(paste0(olade_path, "/grupo1_raw.csv"))
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
  # regional_fn = TRUE, # defaults to sum
  footnotes_fn = footnotes_1754,
  #source_fn = source_1754,
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2023 — GDP energy intensity (final energy consumption) ----

indicator_id <- 2023

dim_config_2023 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2023 <- function(data) {
  data %>%
    filter(Years != "2024") # remove most recent year with only LatAm
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
    filter(Years != "2024") # remove most recent year with only LatAm
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
    filter(Type %in% c("PRIMARIA", "SECUNDARIA")) %>%
    group_by(Country, Years, Type) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    filter(Years != 2024) # remove LatAm only entry for 2024
}

transform_2041 <- function(data) {
  data # no transformations needed
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
  diagnostics = TRUE,
  export = TRUE
)
