library(tidyverse)
library(magrittr)
library(readxl)

## Read in Maria Paz's iso code file
# I believe this was downloaded from https://www.worlddata.info/countrycodes.php (a paid source)
mp_iso <- read_csv("C:/Users/aathens/OneDrive - United Nations/Documentos/CEPALSTAT Data Process/cepalstat-data-upload/Data/Misc/code_iso_mp.csv")
mp_iso %<>% rename(name = `...1`)

# Keep relevant columns
iso <- mp_iso %>% 
  select(name:numeric)

iso %<>% 
  rename(iso2 = alpha2, iso3 = alpha3)

## Read in my iso code file that only has ECLAC countries
my_iso <- read_excel("C:/Users/aathens/OneDrive - United Nations/Documentos/LAC_countries.xlsx")

my_iso %<>% 
  select(iso3, english:spanish_short)

iso %<>% 
  left_join(my_iso, by = "iso3") %>% 
  mutate(ECLAC = ifelse(!is.na(spanish), "Y", ""))

iso %<>% 
  mutate(region = ifelse(is.na(numeric), "Y", ""))

# create entry for world
world <- tibble(name = "World", iso2 = NA_character_, iso3 = "WLD", numeric = NA_real_, 
                english = "World", english_short = "World", spanish = "Mundo", spanish_short = "Mundo",
                ECLAC = "", region = "Y")

iso %<>% bind_rows(world)

# Create another iso with full WORLD spelled out (also common)
iso %<>% 
  mutate(iso = ifelse(name == "World", "WORLD", iso3)) 

# Organize rows
iso %<>% 
  select(name, iso2, iso3, iso, numeric, ECLAC, region, everything())


# Export spreadsheet
write_csv(iso, "C:/Users/aathens/OneDrive - United Nations/Documentos/CEPALSTAT Data Process/cepalstat-data-upload/Data/iso_codes.csv")
