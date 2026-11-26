# Runs the full indicator update from top to bottom:
#   1. Download new data (only for the sources switched on below)
#   2. Load each source's saved data and indicator specs
#   3. Process indicators (run calls at the bottom)
#
# Before running:
#   - Optionally run build_tables.R first, to refresh Data/indicator_metadata.xlsx from CEPALSTAT.
#   - Some sources are downloaded by hand (see each source below). If a file is missing, the script
#     stops and says which file and where to get it.

library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# ---- 1. choose downloads ----

# Set a source to TRUE to download (or prepare) new data for it in this run. With FALSE, the files
# already saved in Data/Raw/ are used, so nothing is downloaded again.
download_new <- c(
  tables       = FALSE,  # build_tables.R: CEPALSTAT table downloads to refresh Data/indicator_metadata.xlsx
  fao          = FALSE,  # download_fao.R: FAOSTAT bulk files
  climatewatch = FALSE,  # download_climatewatch.R: Climate Watch and World Bank APIs
  olade        = FALSE   # format_olade.R: restructures the OLADE files downloaded by hand
)

# ---- 2. download and load each source ----

## tables: download fresh indicator/dimension/source info from CEPALSTAT API
if (download_new[["tables"]]) source(here("Scripts/build_tables.R"))

## OLADE: download by hand first (Scripts/instructions_olade.qmd)
if (download_new[["olade"]]) source(here("Scripts/format_olade.R"))
source(here("Scripts/process_olade.R"))

## EM-DAT: download by hand first (Scripts/instructions_other.qmd)
source(here("Scripts/process_emdat.R"))

## Climate Watch
if (download_new[["climatewatch"]]) source(here("Scripts/download_climatewatch.R"))
source(here("Scripts/process_climatewatch.R"))

## Other sources: InforMEA, UNEP, Ramsar, IRENA, ISO Survey
# InforMEA: run Scripts/download_informea.qmd by hand (it opens a browser and needs the Python setup
# described in the README). The other sources are downloaded by hand (Scripts/instructions_other.qmd).
source(here("Scripts/process_other.R"))

## FAO (AQUASTAT is downloaded by hand: Scripts/instructions_other.qmd)
if (download_new[["fao"]]) source(here("Scripts/download_fao.R"))
source(here("Scripts/process_fao.R"))

# ---- 3. run calls ----

# define global specs
global_spec <- list(diagnostics = TRUE, export = FALSE) # for init testing
global_spec <- list(diagnostics = TRUE, export = TRUE) # for export

# define run_list and run many
run_list <- meta %>% filter(source == "CRED") %>% pull(id)
fao2 <- c(1739,1869,4049,2019,2020,1740,2038,2022,2039,3382)
fix <- c(2019,2022,3382)

run_many_indicators(run_list)
#beepr::beep(1) # notify when done running

# run single indicator
run_one_indicator(4185)
