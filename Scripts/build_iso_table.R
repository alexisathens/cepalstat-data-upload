library(tidyverse)
library(magrittr)
library(readxl)
library(here)

source(here("Scripts/utils.R"))

# This script creates one long iso table that captures all possible country spellings and maps them to the standard CEPALSTAT country name and ID

# ---- Create standard country name/id file ----

# CEPALSTAT country names and IDs
cs_iso <- get_full_dimension_table(208)

# Treat this is as the standard country names / source of truth
cs_std <- cs_iso %>% 
  select(cepalstat = id, std_name = name) # CEPALSTAT ID and English name


# Create another file with standard country names and iso codes
# Maria Paz’s file from World Data has iso codes
mp_iso <- read_csv(here("Data/Misc/code_iso_mp.csv")) %>%
  rename(name = `...1`) %>%
  rename(iso2 = alpha2, iso3 = alpha3) %>%
  select(name, iso2, iso3)

# Force names to match CEPALSTAT
mp_iso %<>% 
  mutate(name = case_when(
    name == "Bermuda" ~ "Bermudas",
    name == "Czechia" ~ "Czech Republic",
    name == "North Korea" ~ "Democratic Peoples Republic of Korea (North Korea)",
    # name == "United Kingdom" ~ "England", # World Data has England/Scotland as UK. CEPALSTAT breaks out so no way to map correctly.
    name == "Falkland Islands" ~ "Falkland Islands (Malvinas)",
    name == "Guinea-Bissau" ~ "Guinea Bissau",
    name == "Netherlands" ~ "Holland",
    name == "South Korea" ~ "Republic of Korea (South Korea)",
    name == "United States of America" ~ "United States",
    name == "Sint Maarten" ~ "Sint Maarten (Dutch part)",
    name == "Virgin Islands" ~ "United States Virgin Islands",
    TRUE ~ name
  ))

cs_std_iso <- cs_std %>% 
  full_join(mp_iso, by = c("std_name" = "name"), relationship = "many-to-many")

# Drop countries without cepalstat id - not needed for these data uploads
cs_std_iso %<>% filter(!is.na(cepalstat))

rm(mp_iso)


# ---- Develop long iso file ----

iso <- cs_iso %>% 
  select(cepalstat = id, name, name_es) %>% 
  pivot_longer(!cepalstat, names_to = "lang", values_to = "name") %>% # drop lang, make long on all name variations
  distinct(cepalstat, name)

# Join with std_names
iso %<>% 
  left_join(cs_std, by = c("cepalstat"))


iso








# My ISO file (ECLAC countries with multiple language spellings)
my_iso <- read_excel(here("Data/Misc/lac_countries.xlsx")) %>%
  select(iso3, english, english_short, spanish, spanish_short)

# Convert to long format
my_iso_long <- my_iso %>%
  pivot_longer(cols = -iso3, names_to = "type", values_to = "name") %>%
  distinct(iso3, name)







# last step - join with iso codes because this will make the df way longer
cs_std_iso




cs_countries %<>% select(name, id) %>% rename(cepalstat = id)

iso %<>% 
  left_join(cs_countries, by = "name")



### REFACTOR!!!

# ---- Begin with World Data country table ----

## Read in Maria Paz's iso code file
# I believe this was downloaded from https://www.worlddata.info/countrycodes.php (a paid source)
mp_iso <- read_csv(here("Data/Misc/code_iso_mp.csv"))
mp_iso %<>% rename(name = `...1`)

# Keep relevant columns
iso <- mp_iso %>% 
  select(name:numeric)

iso %<>% 
  rename(iso2 = alpha2, iso3 = alpha3)

# ---- Join LAC country names ----

## Read in my iso code file that only has ECLAC countries
my_iso <- read_excel(here("Data/Misc/lac_countries.xlsx"))

my_iso %<>% 
  select(iso3, english:spanish_short)

iso %<>% 
  left_join(my_iso, by = "iso3") %>% 
  mutate(ECLAC = ifelse(!is.na(spanish), "Y", ""))

iso %<>% 
  mutate(region = ifelse(is.na(numeric), "Y", ""))

# ---- Create world entry ----

# Create entry for world
world <- tibble(name = "World", iso2 = NA_character_, iso3 = "WLD", numeric = NA_real_, 
                english = "World", english_short = "World", spanish = "Mundo", spanish_short = "Mundo",
                ECLAC = "", region = "Y")

iso %<>% bind_rows(world)

# Create another iso with full WORLD spelled out (also common)
iso %<>% 
  mutate(iso = ifelse(name == "World", "WORLD", iso3))

# ---- Join CEPALSTAT country codes ----

# Get util functions for easy json access
source(here("utils.R"))

cs_countries <- get_full_dimension_table(208)

cs_countries %<>% select(name, id) %>% rename(cepalstat = id)

iso %<>% 
  left_join(cs_countries, by = "name")

# double check all the ECLAC countries match, yes
# iso %>% filter(ECLAC == "Y") %>% View()

# ---- Final formatting + export ----

# Organize rows
iso %<>% 
  select(name, iso2, iso3, iso, numeric, ECLAC, region, everything())

# Export spreadsheet
write_csv(iso, here("Data", "iso_codes.csv"))
