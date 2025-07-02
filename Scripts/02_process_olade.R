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

# Check which countries in olade don't match to iso (if any, add manually to build_iso_table.R script)
# grupo1 %>%
#   filter(!Country %in% iso$name) %>%
#   distinct(Country)

# Filter out extra groups
grupo1 %<>% 
  filter(Country %in% iso$name)

# Finally make long
grupo1 %<>% 
  pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
  mutate(value = as.numeric(value))

grupo1


### ---- IND-2486 ----

id <- 2486
i2486 <- grupo1

# ---- harmonize labels and filter to final set ----

get_indicator_dimensions(2486)
# 44959 - renewable and non-renewable primary energy type

## compare dimensions associated with id, versus full dimensions, versus dimensions in data
# summarize data to dimension level
d44959 <- i2486 %>% 
  group_by(Type) %>% 
  mutate(value = as.numeric(value)) %>% 
  summarize(value = sum(value, na.rm = TRUE)) %>% 
  ungroup()

d44959 %<>% 
  mutate(DATA = "Y")

# get full dimension table names
f44959 <- get_full_dimension_table(44959)

f44959 %<>% 
  select(name_es, name, id) %>% 
  mutate(FULL = "Y")

# get current indicator dimension table
c44959 <- get_ind_dimension_table(2486, 44959)

c44959 %<>% 
  select(id) %>% 
  mutate(CURR = "Y")

## join into combined table to cross-check dims
comb44959 <- d44959 %>% 
  full_join(f44959, by = c("Type" = "name_es")) %>% 
  full_join(c44959, by = c("id"))

comb44959


### make manual adjustments to data labels
i2486 %<>% 
  mutate(Type = case_when(
    Type == "Bagazo de caña" ~ "Caña de azúcar y derivados",
    TRUE ~ Type
  ))


### filter only on labels in CEPALSTAT dims
# (here keep only primary data sources)
cs44959 <- comb44959 %>% 
  filter(FULL == "Y")

i2486 %<>% 
  filter(Type %in% cs44959$Type)

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

# ---- join CEPALSTAT dimension IDs ----

# Define dimensions for this group and get members
ind2486_dims <- get_indicator_dimensions(2486) %>% pull(id)

# Get map from data to dimensions by sampling function
ind2486_dim_map <- get_data_dim_map(ind2486_dims, i2486)

# Join dimensions
i2486f <- join_data_dim_members(ind2486_dim_map, i2486)

# Assert that there are no NA member values
assert_that(
  all(
    i2486f %>%
      select(ends_with("_id")) %>%
      summarise(across(everything(), ~ all(!is.na(.)))) %>%
      unlist()
  ),
  msg = "❌ Some _id columns in `i2486f` contain NA values."
)
# i2486f %>% filter(if_any(ends_with("_id"), is.na))


# ---- add metadata fields and export ----



### ---- IND-3154 ----

# ---- match dimensions and labels ----

# ---- harmonize labels and filter to final set ----

# ---- add summary groups ----

# ---- translate names ----

# ---- join CEPALSTAT dimension IDs ----

# ---- add metadata fields and export ----

