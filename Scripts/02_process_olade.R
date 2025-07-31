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

# This script does the full cleaning and standardizing of OLADE indicators

# See data cleaning notes at: cepalstat-data-upload\Data\Raw\olade\energy_indicators_overview.xlsx
# This spreadsheet outlines Alberto's step-by-step cleaning instructions and organizes the olade indicators into sub-groups

# ---- setup ----

source(here("Scripts/utils.R"))

input_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids

iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y") %>% 
  select(cepalstat, name, std_name)

# read in indicator metadata

meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))


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


# ---- DATA CLEANING ----

# ---- GRUPO 1 ----

# ---- clean to long format ----

grupo1 <- read_excel(paste0(input_path, "/olade_grupo1.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo1[3,]
unit_row <- grupo1[4,]

# Remove rows that match these patterns
grupo1 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo1) <- standardize_headers(header_row)

# Create year field
grupo1 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo1 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo1 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo1 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo1 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo1 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo1 %<>% 
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo1 %<>% 
  filter(!is.na(value))

grupo1

rm(header_row, unit_row)


### ---- IND-2486 ----

# Indicator name: Primary energy supply from renewable (combustible and non-combustible) and non-renewable sources by energy resource
# General instructions: primary energy supply (in units of 103 bep) aggregated at the energy resource level

indicator_id <- 2486
i2486 <- grupo1

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config2486 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name_es")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

i2486 %<>% 
  mutate(Type = case_when(
    Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
    TRUE ~ Type
  ))

# **********************************************

join_keys <- setNames(dim_config2486$pub_col, dim_config2486$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i2486, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config2486)

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
# (here keep only primary data sources)
these_types <- comp_sum %>% 
  filter(dim_name == "Type") %>% 
  filter(status == "Present in Both") %>% 
  pull(dim)

# manually add new labels for 2025
these_types %<>%
  union(c("Eólica", "Solar", "Etanol", "Otra biomasa", "Biodiésel", "Biogás"))

i2486 %<>% 
  filter(Type %in% these_types)
# **********************************************


# ---- add summary groups ----

clean_renew <- i2486 %>% 
  filter(Type %in% c("Hidroenergía", "Geotermia", "Otras primarias", "Eólica", "Solar"))

nonclean_renew <- i2486 %>% 
  filter(Type %in% c("Leña", "Caña de azúcar y derivados", "Etanol", "Otra biomasa", "Biodiésel", "Biogás"))

nonrenew <- i2486 %>% 
  filter(Type %in% c("Petróleo", "Gas natural", "Carbón mineral", "Nuclear"))

# summarize
clean_renew %<>% group_by(Country, Years) %>% summarize(value = sum(value, na.rm = TRUE))
nonclean_renew %<>% group_by(Country, Years) %>% summarize(value = sum(value, na.rm = TRUE))
nonrenew %<>% group_by(Country, Years) %>% summarize(value = sum(value, na.rm = TRUE))

# label
clean_renew %<>% mutate(Type = "Energía primaria renovable limpia") %>% select(Country, Years, Type, value)
nonclean_renew %<>% mutate(Type = "Energía primaria renovable no limpia") %>% select(Country, Years, Type, value)
nonrenew %<>% mutate(Type = "Energía primaria no renovable") %>% select(Country, Years, Type, value)

### join summary rows to data table

i2486 %<>% 
  bind_rows(clean_renew) %>% 
  bind_rows(nonclean_renew) %>% 
  bind_rows(nonrenew)

i2486 %<>% 
  arrange(Country, Years, Type) %>% 
  filter(!is.na(value))

rm(clean_renew, nonclean_renew, nonrenew, these_types)

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i2486f <- join_data_dim_members(i2486, dim_config2486)

# Assert that there are no NA values
assert_no_na_cols(i2486f)


# ---- add metadata fields and export ----

i2486f %<>% 
  select(ends_with("_id"), value)

i2486f <- format_for_wasabi(i2486f, 2486)

# ***** overwrite footnotes**
# this pulled comments from the El Caribe entry
i2486f %<>% 
  mutate(footnotes_id = "")
# ***************************

# Assert that there are no NA values
assert_no_na_cols(i2486f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i2486f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i2486 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i2486, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config2486)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config2486)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))



### ---- IND-3154 ----

# Indicator name: Renewable proportion of primary energy supply
# General instructions: sum renewable energy resources and divide by total primary energy supply

indicator_id <- 3154
i3154 <- grupo1
ind_name <- meta %>% filter(id == indicator_id) %>% pull(name)

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config3154 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

# Standardize type labels
i3154 %<>% 
  mutate(Type = case_when(
    Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
    TRUE ~ Type
  ))

renew <- c("Hidroenergía", "Geotermia", "Otras primarias", "Eólica", "Solar", 
           "Leña", "Caña de azúcar y derivados", "Etanol", "Otra biomasa", "Biodiésel", "Biogás")

# Filter only on renewables and total primary supply
i3154 %<>%
  filter(Type %in% renew | Type == "Total primarias")

# Calculate renewable share of total primary energy
i3154 %<>% 
  mutate(Renewable = ifelse(Type %in% renew, "Renew", "Total")) %>% 
  group_by(Country, Years, Renewable) %>% 
  summarize(value = sum_or_na(value)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = Renewable) %>% 
  mutate(value = Renew / Total * 100) %>% 
  select(-Renew, -Total)

# Remove all empty rows
i3154 %<>% 
  filter(!is.na(value))

# **********************************************

join_keys <- setNames(dim_config3154$pub_col, dim_config3154$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i3154, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config3154)

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
# (here keep only primary data sources)



# **********************************************


# ---- add summary groups ----
# none with this indicator

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i3154f <- join_data_dim_members(i3154, dim_config3154)

# Assert that there are no NA values
assert_no_na_cols(i3154f)


# ---- add metadata fields and export ----

i3154f %<>% 
  select(ends_with("_id"), value)

i3154f <- format_for_wasabi(i3154f, 3154)

# Assert that there are no NA values
assert_no_na_cols(i3154f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i3154f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i3154 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i3154, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config3154)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config3154)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))


### ---- IND-4174 ----

# Indicator name: GDP energy intensity (Primary energy supply / GDP at constant prices in 2018 dollars)
# General instructions: GDP at constant prices in dollars (2018) must be obtained, selecting all the countries in the region and LAC. Once both variables are available, 
# the indicator can be calculated by dividing the primary energy supply by the gross domestic product (GDP) at constant prices in dollars, which is obtained from 
# CEPALSTAT [2204]

indicator_id <- 4174
i4174 <- grupo1
ind_name <- meta %>% filter(id == indicator_id) %>% pull(name)

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config4174 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

# Filter only on total primaries
i4174 %<>% 
  filter(Type == "Total primarias") %>% 
  select(-Type)

# Obtain PIB data from CEPALSTAT
pib <- call.data(id.indicator = 2204) %>% as_tibble()

pib %<>%
  mutate(Years = as.character(Years)) %>% 
  select(Country, Years, pib = value)

# Join PIB data
i4174 %<>% 
  left_join(pib, by = join_by(Country, Years))

# Filter out early years where PIB isn't available
i4174 %<>% 
  filter(as.numeric(Years) >= 1990)

# Calculate energy intensity
i4174 %<>% 
  rename(supply = value) %>% 
  mutate(value = supply / pib)

# Remove intermediary rows
i4174 %<>% 
  select(Country, Years, value)

# Remove all empty rows (some for Venezuela)
i4174 %<>% 
  filter(!is.na(value))

# **********************************************

join_keys <- setNames(dim_config4174$pub_col, dim_config4174$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i4174, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config4174)

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
# (here keep only primary data sources)



# **********************************************


# ---- add summary groups ----
# none with this indicator

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i4174f <- join_data_dim_members(i4174, dim_config4174)

# Assert that there are no NA values
assert_no_na_cols(i4174f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i4174f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**

# ***************************

i4174f %<>% 
  select(ends_with("_id"), value)

i4174f <- format_for_wasabi(i4174f, 4174)

# Assert that there are no NA values
assert_no_na_cols(i4174f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i4174f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i4174 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i4174, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config4174)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config4174)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))


### ---- IND-4236 ----

# Indicator name: Primary energy supply from renewable (combustible and non-combustible) and non-renewable sources by energy resource
# General instructions: primary energy supply (in units of 103 bep) aggregated at the energy resource level

indicator_id <- 4236
i4236 <- grupo1

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config4236 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name_es")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

i4236 %<>% 
  mutate(Type = case_when(
    Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
    TRUE ~ Type
  ))

# **********************************************

join_keys <- setNames(dim_config4236$pub_col, dim_config4236$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i4236, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config4236)

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

# (here keep only primary renewable data sources)
renew <- c("Hidroenergía", "Geotermia", "Otras primarias", "Eólica", "Solar", 
           "Leña", "Caña de azúcar y derivados", "Etanol", "Otra biomasa", "Biodiésel", "Biogás")
i4236 %<>% 
  filter(Type %in% renew)

# Calculate share of renewable primary energy, broken out by type
i4236 %<>% 
  group_by(Country, Years) %>% 
  mutate(total = sum(value, na.rm = T)) %>% 
  ungroup() %>% 
  mutate(share = round(value / total * 100, 1)) %>% 
  select(Country, Years, Type, share) %>% 
  rename(value = share)

# Remove all empty rows
i4236 %<>% 
  filter(!is.na(value))

# **********************************************

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i4236f <- join_data_dim_members(i4236, dim_config4236)

# Assert that there are no NA values
assert_no_na_cols(i4236f)


# ---- add metadata fields and export ----

i4236f %<>% 
  select(ends_with("_id"), value)

i4236f <- format_for_wasabi(i4236f, 4236)

# Assert that there are no NA values
assert_no_na_cols(i4236f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i4236f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i4236 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i4236, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config4236)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config4236)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))



### ---- IND-2487 ----

# Indicator name: Primary and secondary energy supply
# General instructions: primary energy supply (in units of 103 bep) aggregated at the energy resource level

indicator_id <- 2487
i2487 <- grupo1

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config2487 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name_es")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

i2487 %<>% 
  mutate(Type = case_when(
    Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
    
    Type == "Diésel oil con biodiésel" ~ "Diesel oil",
    Type == "Diésel oil sin biodiésel" ~ "Diesel oil",
    
    Type == "Gas licuado de petróleo" ~ "Gas licuado",
    
    Type == "Gasolina con etanol" ~ "Gasolina/Alcohol",
    Type == "Gasolina sin etanol" ~ "Gasolina/Alcohol",
    
    Type == "Kerosene/jet fuel" ~ "Kerosene/Jet fuel",
    
    TRUE ~ Type
  ))


i2487 %<>% 
  group_by(Country, Years, Type) %>% 
  summarize(value = sum(value, na.rm = T)) %>% 
  ungroup()


# **********************************************

join_keys <- setNames(dim_config2487$pub_col, dim_config2487$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i2487, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config2487)

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
# keep all categories this time since this dimension is primary + secondary sources

# **********************************************

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i2487f <- join_data_dim_members(i2487, dim_config2487)

# Assert that there are no NA values
assert_no_na_cols(i2487f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i2487f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**
i2487f %<>% 
  mutate(footnotes_id = case_when(
    Type == "Total primarias" ~ "5896",
    Type == "Total secundarias" ~ "5897",
    TRUE ~ footnotes_id
  ))
# ***************************

i2487f %<>% 
  select(ends_with("_id"), value)

i2487f <- format_for_wasabi(i2487f, 2487)

# Assert that there are no NA values
assert_no_na_cols(i2487f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i2487f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i2487 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i2487, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config2487)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config2487)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))



# ---- GRUPO 4 ----

# ---- clean to long format ----

grupo4 <- read_excel(paste0(input_path, "/olade_grupo4.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo4[3,]
unit_row <- grupo4[4,]

# Remove rows that match these patterns
grupo4 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo4) <- standardize_headers(header_row)

# Create year field
grupo4 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo4 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo4 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo4 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo4 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo4 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo4 %<>% 
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo4 %<>% 
  filter(!is.na(value))

grupo4

rm(header_row, unit_row)


### ---- IND-1754 ----

# Indicator name: Consumption of electric power
# General instructions: electric power (in units of GwH)

indicator_id <- 1754
i1754 <- grupo4
ind_name <- meta %>% filter(id == indicator_id) %>% pull(name)

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config1754 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# ---- harmonize labels and filter to final set ----

join_keys <- setNames(dim_config1754$pub_col, dim_config1754$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i1754, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config1754)

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
# (here keep only primary data sources)

i1754 %<>% select(-Type) # only type is Electricidad


# **********************************************


# ---- add summary groups ----
# none with this indicator

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i1754f <- join_data_dim_members(i1754, dim_config1754)

# Assert that there are no NA values
assert_no_na_cols(i1754f)


# ---- add metadata fields and export ----

i1754f %<>% 
  select(ends_with("_id"), value)

i1754f <- format_for_wasabi(i1754f, 1754)

# Assert that there are no NA values
assert_no_na_cols(i1754f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i1754f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i1754 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i1754, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config1754)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config1754)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))


# ---- GRUPO 5 ----

# ---- clean to long format ----

grupo5 <- read_excel(paste0(input_path, "/olade_grupo5.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo5[3,]
unit_row <- grupo5[4,]

# Remove rows that match these patterns
grupo5 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo5) <- standardize_headers(header_row)

# Create year field
grupo5 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo5 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo5 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo5 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo5 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo5 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo5 %<>% 
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo5 %<>% 
  filter(!is.na(value))

grupo5

rm(header_row, unit_row)

### recreate summary rows *******************

## data-specific issue: noticed that the total rows aren't always accurate for Brazil and this data source specifically, so recalculate manually

primaries <- c("Petróleo", "Gas natural", "Carbón mineral", "Nuclear", "Hidroenergía", "Geotermia", "Eólica", "Solar", "Leña", "Bagazo de caña", 
               "Etanol", "Biodiésel", "Biogás", "Otra biomasa", "Otras primarias")

secondaries <- c("Electricidad", "Gas licuado de petróleo", "Gasolina sin etanol", "Gasolina con etanol", "Kerosene/jet fuel", "Diésel oil sin biodiésel", 
                 "Diésel oil con biodiésel", "Fuel oil", "Coque", "Carbón vegetal", "Gases", "Otras secundarias", "No energético")

grupo5 %<>%
  filter(!Type %in% c("Total primarias", "Total secundarias", "Total")) %>%
  bind_rows(
    grupo5 %>% filter(Type %in% primaries) %>% group_by(Country, Years) %>% summarise(value = sum(value), .groups = "drop") %>% mutate(Type = "Total primarias"),
    grupo5 %>% filter(Type %in% secondaries) %>% group_by(Country, Years) %>% summarise(value = sum(value), .groups = "drop") %>% mutate(Type = "Total secundarias"),
    grupo5 %>% filter(!Type %in% c("Total primarias", "Total secundarias", "Total")) %>% group_by(Country, Years) %>% summarise(value = sum(value), .groups = "drop") %>% mutate(Type = "Total")
  ) %>%
  arrange(Country, Years, Type)

# ********************************************

### ---- IND-2040 ----

# Indicator name: Energy production
# General instructions: primary energy supply (in units of 103 bep) aggregated at the energy resource level

indicator_id <- 2040
i2040 <- grupo5

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config2040 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20726"),
  pub_col = c("208_name", "29117_name", "20726_name_es")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

# ** fix these later to match indicator 2478 (dim 44966) - this is the duplicate dimension
i2040 %<>% 
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
  ))


i2040 %<>% 
  group_by(Country, Years, Type) %>% 
  summarize(value = sum(value, na.rm = T)) %>% 
  ungroup()


# **********************************************

join_keys <- setNames(dim_config2040$pub_col, dim_config2040$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i2040, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config2040)

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
# keep all categories this time since this dimension is primary + secondary sources

## ** temp fix: drop all new categories in dim 44966 to not duplicate efforts now -- eventually fix this

drop <- c("Biodiésel", "Biogás", "Etanol", "Eólica", "Otra biomasa", "Solar", "Total")

i2040 %<>% 
  filter(!Type %in% drop)

# remove LatAm entry for 2024, keep up to 2023 - last available for all country data
i2040 %<>% 
  filter(Years != 2024)

# **********************************************

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i2040f <- join_data_dim_members(i2040, dim_config2040)

# Assert that there are no NA values
assert_no_na_cols(i2040f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i2040f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**

# ***************************

i2040f %<>% 
  select(ends_with("_id"), value)

i2040f <- format_for_wasabi(i2040f, 2040)

# Assert that there are no NA values
assert_no_na_cols(i2040f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i2040f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i2040 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i2040, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config2040)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config2040)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))



# ---- GRUPO 6 ----

# ---- clean to long format ----

grupo6 <- read_excel(paste0(input_path, "/olade_grupo6.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo6[3,]
unit_row <- grupo6[4,]

# Remove rows that match these patterns
grupo6 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo6) <- standardize_headers(header_row)

# Create year field
grupo6 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo6 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo6 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo6 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo6 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo6 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
grupo6 %<>% 
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

# Remove NAs
grupo6 %<>% 
  filter(!is.na(value))

grupo6

rm(header_row, unit_row)


### ---- IND-2041 ----

# Indicator name: Energy consumption
# General instructions: primary energy supply (in units of 103 bep) aggregated at the energy resource level

indicator_id <- 2041
i2041 <- grupo6

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config2041 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20726"),
  pub_col = c("208_name", "29117_name", "20726_name_es")
)

# ---- harmonize labels and filter to final set ----

### make manual adjustments to data labels *****

# ** fix these later to match indicator 2478 (dim 44966) - this is the duplicate dimension
i2041 %<>% 
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
  ))

i2041 %<>% 
  filter(Type %in% c("PRIMARIA", "SECUNDARIA"))


i2041 %<>% 
  group_by(Country, Years, Type) %>% 
  summarize(value = sum(value, na.rm = T)) %>% 
  ungroup()


# **********************************************

join_keys <- setNames(dim_config2041$pub_col, dim_config2041$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i2041, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config2041)

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
# keep all categories this time since this dimension is primary + secondary sources

# remove LatAm only entry for 2024
i2041 %<>% 
  filter(Years != 2024)

# **********************************************

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i2041f <- join_data_dim_members(i2041, dim_config2041)

# Assert that there are no NA values
assert_no_na_cols(i2041f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i2041f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**

# ***************************

i2041f %<>% 
  select(ends_with("_id"), value)

i2041f <- format_for_wasabi(i2041f, 2041)

# Assert that there are no NA values
assert_no_na_cols(i2041f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i2041f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i2041 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i2041, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config2041)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config2041)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))


# ---- GRUPO 7 ----

# **note this group cleaning code is different from prior

# ---- clean to long format ----

grupo7 <- read_excel(paste0(input_path, "/olade_grupo7.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo7[3,]
unit_row <- grupo7[2,]

# Remove rows that match these patterns
grupo7 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo7) <- standardize_headers(header_row)

# Create year field
# grupo7 %<>% 
#   mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
#   fill(Years, .direction = "down") %>% 
#   select(Country, Years, everything())

# Remove year header and first row
grupo7 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(!grepl("-", Country)) %>% # **
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo7 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo7 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo7 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo7 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
# grupo7 %<>% 
#   pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
#   mutate(value = as.numeric(value))

grupo7 %<>%
  pivot_longer(cols = -Country, names_to = "Years", values_to = "value")

# Remove NAs
grupo7 %<>% 
  filter(!is.na(value))

grupo7

rm(header_row, unit_row)


### ---- IND-2023 ----

# Indicator name: GDP energy intensity (final energy consumption)
# General instructions: electric power (in units of GwH)

indicator_id <- 2023
i2023 <- grupo7
ind_name <- meta %>% filter(id == indicator_id) %>% pull(name)

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config2023 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# ---- harmonize labels and filter to final set ----

join_keys <- setNames(dim_config2023$pub_col, dim_config2023$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i2023, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config2023)

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
# (here keep only primary data sources)

i2023 %<>% 
  filter(Years != "2024") # remove most recent year with only LatAm

# **********************************************


# ---- add summary groups ----
# none with this indicator

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i2023f <- join_data_dim_members(i2023, dim_config2023)

# Assert that there are no NA values
assert_no_na_cols(i2023f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i2023f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**

# ***************************

i2023f %<>% 
  select(ends_with("_id"), value)

i2023f <- format_for_wasabi(i2023f, 2023)

# Assert that there are no NA values
assert_no_na_cols(i2023f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i2023f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i2023 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i2023, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config2023)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config2023)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))


# ---- GRUPO 8 ----

# **note this group cleaning code is different from prior

# ---- clean to long format ----

grupo8 <- read_excel(paste0(input_path, "/olade_grupo8.xlsx"), col_names = FALSE)

## Clean into standard flat data format

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo8[3,]
unit_row <- grupo8[2,]

# Remove rows that match these patterns
grupo8 %<>%
  remove_headers(header_row, unit_row)

# Format header row
colnames(grupo8) <- standardize_headers(header_row)

# Create year field
# grupo8 %<>% 
#   mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
#   fill(Years, .direction = "down") %>% 
#   select(Country, Years, everything())

# Remove year header and first row
grupo8 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(!grepl("[-:]", Country)) %>%  # **
  filter(Country != "Series de oferta y demanda")

# Overwrite country names with std_name in iso file
grupo8 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo8 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo8 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo8 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
# grupo8 %<>% 
#   pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
#   mutate(value = as.numeric(value))

grupo8 %<>%
  pivot_longer(cols = -Country, names_to = "Years", values_to = "value")

# Remove NAs
grupo8 %<>% 
  filter(!is.na(value))

grupo8

rm(header_row, unit_row)


### ---- IND-4235 ----

# Indicator name: Proportion of losses in the electricity sector over the total supply of electrical energy
# General instructions: electric power (in units of GwH), multiply by 100 for percentage

indicator_id <- 4235
i4235 <- grupo8
ind_name <- meta %>% filter(id == indicator_id) %>% pull(name)

# Fill out dim config table using following info:
get_indicator_dimensions(indicator_id)

pub <- get_cepalstat_data(indicator_id)
pub <- match_cepalstat_labels(pub)
pub

dim_config4235 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# ---- harmonize labels and filter to final set ----

join_keys <- setNames(dim_config4235$pub_col, dim_config4235$data_col)

pub %<>% select(all_of(unname(join_keys)), value) # Keep only used labels

comp <- full_join(i4235, pub, by = join_keys, suffix = c("", ".pub"))

comp_sum <- get_comp_summary_table(comp, dim_config4235)

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
# (here keep only primary data sources)

i4235 %<>% 
  filter(Years != "2024") # remove most recent year with only LatAm

# multiply by 100 to obtain percentage
i4235 %<>% 
  mutate(value = as.numeric(value) * 100)

# **********************************************


# ---- add summary groups ----
# none with this indicator

# ---- join CEPALSTAT dimension IDs ----

# Join dimensions
i4235f <- join_data_dim_members(i4235, dim_config4235)

# Assert that there are no NA values
assert_no_na_cols(i4235f)


# ---- add metadata fields and export ----

# manually update footnotes, if necessary
get_indicator_footnotes(indicator_id)

i4235f %<>% 
  mutate(footnotes_id = "")

# ***** overwrite footnotes**

# ***************************

i4235f %<>% 
  select(ends_with("_id"), value)

i4235f <- format_for_wasabi(i4235f, 4235)

# Assert that there are no NA values
assert_no_na_cols(i4235f)

# Create a date/time stamp for export version control
dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")

# Export!
# write_xlsx(i4235f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))


# ---- create comparison file ----

# Begin with i4235 (before the switch to CEPALSTAT IDs) and pub
# Rejoin comp (in case edits were made to data file)
comp <- full_join(i4235, pub, by = join_keys, suffix = c("", ".pub"))

# Join dimensions
comp <- join_data_dim_members(comp, dim_config4235)

# Assert that there are no NA values in non-value rows
assert_no_na_cols(comp, !contains("value"))

# Run comparison checks and format
comp <- create_comparison_checks(comp, dim_config4235)

comp

# Export comp file!
# write_xlsx(comp, here(glue("Data/Checks/comp_id{indicator_id}.xlsx")))


# ---- GRUPO 9 ----

# **note this group cleaning code is significantly different from prior
# data for each year is on a separate tab and country labels aren't always in the same order

# ---- clean to long format ----

# Define file path
file_path <- paste0(input_path, "/olade_grupo9.xlsx")

# List sheet names
all_sheets <- excel_sheets(file_path)

# Filter sheets starting from "10.2000" onward
# This extracts the numeric prefix and keeps only sheets where the number is >= 10
sheets_to_read <- all_sheets[str_extract(all_sheets, "^\\d+") %>% as.numeric() >= 10]

# Read and combine all filtered sheets
combined_df <- map_dfr(sheets_to_read, function(sheet) {

  df <- read_excel(file_path, sheet = sheet, col_names = FALSE)
  
  # Extract header and unit rows for each table in spreadsheet
  header_row <- df[3,]
  unit_row <- df[2,]
  
  # Remove rows that match these patterns
  df %<>%
    remove_headers(header_row, unit_row)
  
  # Format header row
  colnames(df) <- standardize_headers(header_row)
  
  # Add year row
  df$Years <- str_extract(sheet, "\\d{4}")
  
  # Filter only on rows with sources
  fuentes <- c("Nuclear", "Térmica no renovable (combustión)", "Térmica renovable (combustión)", "Hidro", "Geotermia", "Eólica", "Solar")
  
  df %<>% 
    filter(Country %in% fuentes) %>% 
    select(-Unidad)
  
  # Pivot long (to deal with different country availability)
  df %<>% 
    pivot_longer(!c(Country, Years), names_to = "Country_name") %>% 
    rename(Type = Country, Country = Country_name) %>% 
    select(Country, Years, Type, value)
  
  return(df)
})

grupo9 <- combined_df

rm(combined_df, file_path, all_sheets, sheets_to_read)


## Clean into standard flat data format

# Overwrite country names with std_name in iso file
grupo9 %<>%
  left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
  mutate(Country = coalesce(std_name, Country)) %>%
  select(-std_name)

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
grupo9 %>%
  filter(!Country %in% iso$name) %>%
  distinct(Country)

# Filter out extra groups
grupo9 %<>% 
  filter(Country %in% iso$name)

# Filter out sub-regions
# This is because country data doesn't always total to sub-regional level, and sometimes different methodologies and classifications are used
# Just keep country and regional level for clarity purposes
grupo9 %<>% 
  filter(!Country %in% c("Central America", "South America", "Caribbean"))

# Finally make long
# grupo9 %<>% 
#   pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
#   mutate(value = as.numeric(value))
# 
# grupo9 %<>%
#   pivot_longer(cols = -Country, names_to = "Years", values_to = "value")

# Convert value to numeric
grupo9 %<>% 
  mutate(value = as.numeric(value))

# Remove NAs
grupo9 %<>% 
  filter(!is.na(value))

grupo9

rm(header_row, unit_row)
