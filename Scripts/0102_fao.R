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


# ---- DATA CLEANING ----

# ---- LAND USE ----

# Download bulk data
land <- get_faostat_bulk(code = "RL")
land %<>% as_tibble()

# Keep only columns of interest
land %<>% 
  rename(Country = area, Type = item, Calculation = element, Years = year) %>% 
  select(Country, Years, Type, Calculation, value)

# Overwrite country names with std_name in iso file
land %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
land %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country) %>% pull(Country)

# Filter out extra groups
land %<>% 
  filter(Country %in% iso$name) %>% 
  filter(!Country %in% c("Bermudas"))
# remove Bonaire, Sint Eustatius and Saba and Netherlands Antilles (former) and Bermudas because data is inconsistent

# Correct types
land %<>% 
  mutate(Years = as.character(Years))


### ---- Forest Area (2036) ----

# Indicator name: Forest area
# General instructions: -

indicator_id <- 2036
i2036 <- land

### filter on applicable Types/Calculations *****

i2036 %<>% 
  filter(Calculation == "area") %>% 
  filter(Type %in% c("Forest land", "Naturally regenerating forest", "Planted Forest")) %>% 
  select(-Calculation)

# remove regional totals, construct ECLAC total from sum of countries
i2036 %<>% 
  filter(!Country %in% c("South America", "Central America", "Caribbean"))

### ********************************


# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config2036 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20722"),
  pub_col = c("208_name", "29117_name", "20722_name")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

# Rename labels
i2036 %<>% 
  mutate(Type = case_when(
    Type == "Forest land" ~ "Total forest",
    Type == "Naturally regenerating forest" ~ "Natural forest",
    Type == "Planted Forest" ~ "Forest plantations",
    TRUE ~ Type
  ))

# Create ECLAC regional total
eclac_totals <- i2036 %>% 
  group_by(Years, Type) %>% 
  summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
  mutate(Country = "Latin America and the Caribbean") %>% 
  select(Country, Years, Type, value)

# Add ECLAC to main data
i2036 %<>% 
  bind_rows(eclac_totals) %>% 
  arrange(Country, Years)

# **********************************************

join_keys <- setNames(dim_config2036$pub_col, dim_config2036$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i2036, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config2036)

### Run checks
# (1) What dimensions were present in the old file but not in the new one?
comp_sum %>% 
  filter(status == "Old Only") #%>% View()
# These are the manual edits to labels that are needed (or data loss that needs to be investigated)
# This could also show summary rows that are in the data

# (2) What dimensions are only present in the new file?
comp_sum %>% 
  filter(status == "New Only") #%>% View()
# Expect to see the new year of data. Also check if any countries are new, and if so, why were they not included before? (Questions to ask Alberto)

# (3) View all - this can help match up labels
# comp_sum %>% filter(dim_name == "Type") %>% View()


### filter only on labels in CEPALSTAT dims ***

# **********************************************

rm(eclac_totals)

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i2036f <- join_data_dim_members(i2036, dim_config2036)

# Assert that there are no NA values
assert_no_na_cols(i2036f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i2036f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**

i2036f %<>% 
  mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))

# used to use footnote 7771 Includes Bonaire, Sint Eustatius and Saba -> for Ex Netherlands Antilles

# ***************************

i2036f %<>% 
  select(ends_with("_id"), value)

i2036f <- format_for_wasabi(i2036f, 2036)

# Assert that there are no NA values
assert_no_na_cols(i2036f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i2036f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i2036 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i2036, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config2036)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config2036)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))
