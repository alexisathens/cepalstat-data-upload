library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)

# This script transforms the raw files at Pilot/Data/Raw/olade into flat data files for each indicator.

input_path <- "Pilot/Data/Raw/olade/"

#### GRUPO 01: unidad - energetico - 103 bep - oferta total ####

## Clean into standard flat data format

grupo1 <- read_excel(paste0(input_path, "olade_grupo1.xlsx"), col_names = FALSE)

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo1[3,]
unit_row <- grupo1[4,]

# Remove rows that match these patterns
grupo1 %<>% 
  anti_join(header_row) %>% 
  anti_join(unit_row)

# Format header row
header_row %<>% unlist(use.names = FALSE) %>% str_trim()
header_row[1] <- "Country"
colnames(grupo1) <- header_row

# Create year field
grupo1 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo1 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Filter countries
iso <- read_csv("Data/iso_codes.csv")

iso %<>% 
  filter(!is.na(spanish_short)) %>% 
  select(name, spanish_short)

# Check what countries don't match over that should
grupo1 %>% 
  filter(!Country %in% iso$spanish_short) %>% 
  distinct(Country)

# Force OLADE names to match CEPALSTAT names
grupo1 %<>% 
  mutate(Country = case_when(
    Country == "Grenada" ~ "Granada",
    Country == "Trinidad & Tobago" ~ "Trinidad y Tabago",
    TRUE ~ Country
  ))

# Filter on CEPALSTAT countries only
grupo1 %<>% 
  filter(Country %in% iso$spanish_short)


### grupo-specific cleaning:

# Make data long
grupo1 %<>% 
  pivot_longer(cols = -c(Country, Years),
               names_to = "Type")

grupo1 %<>% 
  mutate(value = as.numeric(value))

# Store types for reference
energy_types <- unique(grupo1$Type)


# ---- 2486: primary energy supply from renewable and non-renewable energy sources (by energy resource) ----

id <- 2486
i2486 <- grupo1


## function this: 
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


### create summary rows

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

i2486


# ---- 2487: primary & secondary energy supply ----

id <- 2487
i2487 <- grupo1

get_indicator_dimensions(2487)
#d2487 <- get_dimension_table(44966)
d2487 <- get_published_dimension_table(2487, 44966, lang = "es")


### CONTINUE TOMORROW: 
# - Alberto thinks wind and solar may have been added into "other primary", double check if these totals to verify
# - Decisions on whether to keep more types and integrate into CEPALSTAT
# - Check whether totals were calculated by retained types or using the OLADE fields

i2487 %>% 
  filter(!Type %in% d2487$name) %>% 
  distinct(Type)





#### GRUPO 04: unidad - energetico - unidad - consumo energetico - electricidad ####

## Clean into standard flat data format

grupo4 <- read_excel(paste0(input_path, "olade_grupo4.xlsx"), col_names = FALSE)

# Extract header and unit rows for each table in spreadsheet
header_row <- grupo4[3,]
unit_row <- grupo4[4,]

# Remove rows that match these patterns
grupo4 %<>% 
  anti_join(header_row) %>% 
  anti_join(unit_row)

# Format header row
header_row %<>% unlist(use.names = FALSE) %>% str_trim()
header_row[1] <- "Country"
colnames(grupo4) <- header_row

# Create year field
grupo4 %<>% 
  mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>% 
  fill(Years, .direction = "down") %>% 
  select(Country, Years, everything())

# Remove year header and first row
grupo4 %<>% 
  filter(!str_detect(Country, "\\b\\d{4}$")) %>% 
  filter(Country != "Series de oferta y demanda")

# Filter countries
iso <- read_csv("Data/iso_codes.csv")

iso %<>% 
  filter(!is.na(spanish_short)) %>% 
  select(name, spanish_short)

# Check what countries don't match over that should
grupo4 %>% 
  filter(!Country %in% iso$spanish_short) %>% 
  distinct(Country)

# Force OLADE names to match CEPALSTAT names
grupo4 %<>% 
  mutate(Country = case_when(
    Country == "Grenada" ~ "Granada",
    Country == "Trinidad & Tobago" ~ "Trinidad y Tabago",
    TRUE ~ Country
  ))

# Join English names
grupo4 %<>% 
  left_join(iso, by = c("Country" = "spanish_short"))

# Double check non-matches
grupo4 %>% 
  filter(is.na(name)) %>% 
  distinct(Country)

# Filter on iso matches only
grupo4 %<>% 
  filter(!is.na(name)) %>% 
  select(-Country) %>% 
  select(Country = name, everything())


### grupo-specific cleaning:

# How to identify value field generally?

grupo4 %<>% rename(value = Electricidad)

# Format value as numeric
grupo4 %<>% mutate(value = as.numeric(value))

# Filter NAs of countries missing older values
grupo4 %<>% filter(!is.na(value))


# ---- 1754: consumption of electric power ----

i1754 <- grupo4

i1754
