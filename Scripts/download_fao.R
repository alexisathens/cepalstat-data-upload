# This script downloads the FAOSTAT bulk files used by process_fao.R and saves them to Data/Raw/fao/.
# Run it when you want the latest FAO data; process_fao.R then reads the saved files without
# downloading again. To add a file, add it to fao_bulk_files below and read it in process_fao.R.
#
# Other FAO data used by process_fao.R doesn't need this script:
#   - Fishery and aquaculture data come with the fishstat R package.
#   - AQUASTAT is downloaded manually; follow the steps in instructions_other.qmd.

# ---- setup ----

library(here)
source(here("Scripts/utils.R"))

# Information about all FAOSTAT datasets, for reference when adding a new file
fao_metadata <- FAOmetaTable$domainTable %>% as_tibble()
# Alternatively go here to see data areas: https://www.fao.org/faostat/en/#data

# ---- download data ----

# FAOSTAT bulk files to download (the same files are read in process_fao.R)
fao_bulk_files <- c(
  "Inputs_LandUse_E_All_Data_(Normalized).zip",                 # land use (RL)
  "Environment_Temperature_change_E_All_Data_(Normalized).zip", # temperature change (ET)
  "Environment_LandCover_E_All_Data_(Normalized).zip",          # land cover (LC)
  "Production_Crops_Livestock_E_All_Data_(Normalized).zip",     # crops and livestock products (QCL)
  "Inputs_FertilizersNutrient_E_All_Data_(Normalized).zip",     # fertilizers by nutrient (RFN)
  "Inputs_Pesticides_Use_E_All_Data_(Normalized).zip"           # pesticide use (RP)
)

fao_folder <- ensure_dir(here("Data/Raw/fao"))

walk(fao_bulk_files, function(filename) {
  message(glue("▶ Downloading {filename}..."))
  download_faostat_bulk(url_bulk = paste0("https://bulks-faostat.fao.org/production/", filename),
                        data_folder = fao_folder)
})

message(glue("✅ FAOSTAT files saved to Data/Raw/fao/"))
