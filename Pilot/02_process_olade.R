library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)

# This script does the full cleaning and standardizing of OLADE indicators
# (This is an attempted streamlining of 02_preprocess_olade and 03_process)

# ---- setup ----

input_path <- "Pilot/Data/Raw/olade/"

# read in ISO with cepalstat ids

# ---- cleaning helpers ----

remove_headers <- function(df, header_row, unit_row) {
  df %>% 
    anti_join(header_row) %>% 
    anti_join(unit_row)
}

standardize_headers <- function(header_row) {
  header <- unlist(header_row, use.names = FALSE) %>% str_trim()
  header[1] <- "Country"
  header
}

fix_country_names <- function(df) {
  df %>%
    mutate(Country = case_when(
      Country == "Grenada" ~ "Granada",
      Country == "Trinidad & Tobago" ~ "Trinidad y Tabago",
      TRUE ~ Country
    ))
}


# ---- GRUPO 1 ----

# basic olade cleaning here

# basic grupo cleaning here to get into long format


# ---- IND-2486 ----

# ---- match dimensions and labels ----

# ---- harmonize labels and filter to final set ----

# ---- add summary groups ----

# ---- translate names ----

# ---- join CEPALSTAT dimension IDs ----

# ---- add metadata fields and export ----
