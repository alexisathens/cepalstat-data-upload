library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
library(here)
library(assertthat)

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

grupo1

rm(header_row, unit_row)

### ---- IND-2486 ----

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

i2486 %<>% 
  filter(Type %in% these_types)
# **********************************************


# ---- add summary groups ----

clean_renew <- i2486 %>% 
  filter(Type %in% c("Hidroenergía", "Geotermia", "Otras primarias"))

nonclean_renew <- i2486 %>% 
  filter(Type %in% c("Leña", "Caña de azúcar y derivados"))

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

rm(list = ls())


### ---- IND-3154 ----

indicator_id <- 3154
i3154 <- grupo1

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

# i3154 %<>% 
#   mutate(Type = case_when(
#     Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
#     TRUE ~ Type
#   ))

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

clean_renew <- i3154 %>% 
  filter(Type %in% c("Hidroenergía", "Geotermia", "Otras primarias"))

nonclean_renew <- i3154 %>% 
  filter(Type %in% c("Leña", "Caña de azúcar y derivados"))

nonrenew <- i3154 %>% 
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

i3154 %<>% 
  bind_rows(clean_renew) %>% 
  bind_rows(nonclean_renew) %>% 
  bind_rows(nonrenew)

i3154 %<>% 
  arrange(Country, Years, Type) %>% 
  filter(!is.na(value))

rm(clean_renew, nonclean_renew, nonrenew, these_types)

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

rm(list = ls()[!sapply(mget(ls(), .GlobalEnv), is.function)])

