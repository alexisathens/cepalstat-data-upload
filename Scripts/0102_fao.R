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

# This script does the full download, cleaning, and standardizing of FAO indicators

# Info for FAOSTAT package: https://r-packages.io/packages/FAOSTAT/download_faostat_bulk

# ---- setup ----

source(here("Scripts/utils.R"))

# read in ISO with cepalstat ids

iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

# read in indicator metadata

meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))
