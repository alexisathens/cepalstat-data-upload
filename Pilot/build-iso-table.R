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

# create entry for world
world <- tibble(name = "World", iso2 = NA_character_, iso3 = "WLD", numeric = NA_real_)

iso %<>% bind_rows(world)

# Create another iso with full WORLD spelled out (also common)
iso %<>% 
  mutate(iso = ifelse(name == "World", "WORLD", iso3)) %>% 
  select(name, iso2, iso3, iso, numeric)

# Export spreadsheet
write_csv(iso, "C:/Users/aathens/OneDrive - United Nations/Documentos/CEPALSTAT Data Process/cepalstat-data-upload/Data/iso_codes.csv")
