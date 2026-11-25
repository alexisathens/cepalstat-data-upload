# This script downloads the FAO indicators

# ---- setup ----

library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# ---- download data ----

# Load information about all datasets into a data frame
fao_metadata <- FAOmetaTable$domainTable %>% as_tibble()
# Alternatively go here to see data areas: https://www.fao.org/faostat/en/#data

## create custom bulk download function
get_fao_bulk <- function(filename) {
  data_folder <- ensure_dir(here("Data/Raw/fao"))
  download_faostat_bulk(url_bulk = paste0("https://bulks-faostat.fao.org/production/", filename), 
                        data_folder = data_folder)
  read_faostat_bulk(file.path(data_folder, filename)) %>% as_tibble() %>% filter(!is.na(value))
}

# download land use (RL) data
use <- get_fao_bulk("Inputs_LandUse_E_All_Data_(Normalized).zip")

# download climate change (ET) data
clim <- get_fao_bulk("Environment_Temperature_change_E_All_Data_(Normalized).zip")

# download land cover (LC) data
cover <- get_fao_bulk("Environment_LandCover_E_All_Data_(Normalized).zip")

# download crops and livestock products (QCL) data
crop <- get_fao_bulk("Production_Crops_Livestock_E_All_Data_(Normalized).zip")

# download fertilizers by Nutrient (RFN) data
fert <- get_fao_bulk("Inputs_FertilizersNutrient_E_All_Data_(Normalized).zip")

# download pesticide use (RP) data
pest <- get_fao_bulk("Inputs_Pesticides_Use_E_All_Data_(Normalized).zip")

## fishstat downloads
# imports from the fishstat package. See documentation here: https://cran.r-universe.dev/fishstat/doc/manual.html
fish <- capture %>%
  inner_join(country, by = "country") %>%
  inner_join(species, by = "species") %>% 
  as_tibble()

fish <- fish %>% 
  mutate(
    Years = as.integer(year),
    Country = country_name,
    Species = species_name,
    Species_Group = isscaap
  )

aqua <- aquaculture %>%
  inner_join(country, by = "country") %>%
  inner_join(species, by = "species") %>% 
  inner_join(area, by = "area") %>% 
  inner_join(environment, by = "environment") %>% 
  as_tibble()

aqua <- aqua %>% 
  mutate(
    Years = as.integer(year),
    Country = country_name,
    Area = inlandmarine
  )

## aquastat manual download
# follow instructions in instructions_other.qmd for bulk file
aquastat_folder <- ensure_dir(here("Data/Raw/fao/aquastat"))
#aquastat <- read_csv(paste0(aquastat_folder, "/bulk_aquastat_raw.csv"))
# AQUASTAT bulk file is Latin-1 encoded (e.g., "Côte d'Ivoire", "Türkiye")
aquastat <- read_csv(check_raw_file(paste0(aquastat_folder, "/bulk_aquastat_raw.csv"), source = "AQUASTAT"),
                     locale = locale(encoding = "latin1"))

aquastat %<>% 
  rename(Country = AREA, Years = timePointYears...5, Variable = aquastatElement...2, value = Value) %>% 
  select(Country, Years, Variable, value)