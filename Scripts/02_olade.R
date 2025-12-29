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

data_g4 <- read_csv(paste0(olade_path, "/grupo4_raw.csv"))


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
