library(tidyverse)
library(magrittr)
library(readxl)
library(here)
library(writexl)

source(here("Scripts/utils.R"))

# This script creates one long iso table that captures all possible country spellings and maps them to the standard CEPALSTAT country name and ID

# ---- Create standard country name/id file ----

# CEPALSTAT country names and IDs
cs_country <- get_full_dimension_table(208)

# Treat this is as the standard country names / source of truth
cs_std <- cs_country %>% 
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

# Make any manual edits
cs_std_iso %<>% 
  mutate(iso3 = case_when(
    std_name == "World" ~ "WLD",
    TRUE ~ iso3
  ))

rm(mp_iso)


# ---- Develop long iso file ----

iso <- cs_country %>% 
  select(cepalstat = id, name, name_es) %>% 
  pivot_longer(!cepalstat, names_to = "lang", values_to = "name") %>% # drop lang, make long on all name variations
  distinct(cepalstat, name)

# Join with std_names
iso %<>%
  left_join(cs_std, by = c("cepalstat"))

# Add my ISO file (ECLAC countries with multiple language spellings)
my_iso <- read_excel(here("Data/Misc/lac_countries.xlsx")) %>%
  select(iso3, english, english_short, spanish, spanish_short)

# Convert to long format
my_iso %<>%
  pivot_longer(cols = -iso3, names_to = "type", values_to = "name") %>%
  distinct(iso3, name)

# Map from iso3 to cepalstat id
my_iso %<>% 
  left_join(cs_std_iso, by = c("iso3")) %>% 
  select(cepalstat, name, std_name)

# Join to main iso and keep distinct entries
iso %<>% 
  bind_rows(my_iso) %>% 
  distinct(cepalstat, name, std_name)

rm(my_iso)


# ---- Add manual entries ----

# Enter manual entries here as they're found in the various data sources
manual_entries <- tribble(
  # name in data              # "correct" name in cs_std
  ~name,                      ~std_name,
  "Trinidad & Tobago",        "Trinidad and Tobago",
  "Sint Maarten",             "Sint Maarten (Dutch part)",
  "Bermuda",                  "Bermudas",
  "CuraÃ§ao",                 "Curaçao"
)

# Add manual entries and fill CEPALSTAT ids
iso %<>% 
  bind_rows(manual_entries) %>% 
  group_by(std_name) %>% 
  fill(cepalstat, .direction = "downup") %>% 
  ungroup() %>% 
  distinct(cepalstat, name, std_name)



# ---- Add any flags ----

# Add flag for ECLAC countries and regions
lac_flags <- read_excel(here("Data/Misc/lac_countries.xlsx"))

lac_flags %<>% 
  select(iso3, english) %>% 
  mutate(ECLAC = ifelse(iso3 != "WLD", "Y", ""),
         region = ifelse(iso3 %in% c("LAA", "CAR", "SAA", "CAA"), "Y", ""),
         world = ifelse(iso3 == "WLD", "Y", ""))

lac_flags %<>% 
  left_join(cs_std_iso, by = "iso3") %>% 
  select(cepalstat, ECLAC, region, world)

# Join to long iso
iso %<>% 
  left_join(lac_flags, by = "cepalstat")
  
# Force NA to "" for flags
iso %<>%
  mutate(across(ECLAC:last_col(), ~ ifelse(is.na(.), "", .)))


# Create another flag for ECLAC associate members
# List found here: # https://www.cepal.org/en/about/member-states-and-associate-members

ECLACa <- c("Anguilla", "Aruba", "Bermudas", "British Virgin Islands", "Cayman Islands", "Curaçao", "Guadeloupe", "French Guiana", "Martinique",
            "Montserrat", "Puerto Rico", "Sint Maarten (Dutch part)", "Turks and Caicos Islands", "United States Virgin Islands")
# all(ECLACa %in% cs_std$std_name)

# Add associate member countries field
iso %<>% 
  mutate(ECLACa = ifelse(std_name %in% ECLACa | ECLAC == "Y", "Y", "")) %>% 
  relocate(ECLACa, .after = "ECLAC")


# ---- Format for export ----

# Arrange in ECLAC-alphabetical order
iso %<>% 
  arrange(desc(ECLAC), std_name)

# Join iso codes
iso %<>% 
  left_join(cs_std_iso %>% select(-std_name), by = c("cepalstat"))

# Export spreadsheet
write_xlsx(iso, here("Data", "iso_codes.xlsx"))
