library(tidyverse)
library(magrittr)
library(readxl)
library(here)
library(FAOSTAT)

# Info for FAOSTAT package: https://r-packages.io/packages/FAOSTAT/download_faostat_bulk


# Load information about all datasets into a data frame
fao_metadata <- search_dataset() %>% as_tibble()
# This shows the status of the data too, of the latest year and whether it's final, which will be helpful

# Alternatively go here to see data areas: https://www.fao.org/faostat/en/#data

# Load sample data
land_use <- get_faostat_bulk(code = "RL")
# Show the structure of the data
str(land_use)

land_use %<>% as_tibble()

# Save these files as rds since they're large - they take up about 1/20 of the space of a xlsx
saveRDS(land_use, here("Pilot", "Data", "Raw", "fao", "fao_land_use.rds"))

# For the download portion, just save this file as-is, and do all the processing in later scripts.



## Test interacting with/processing indicator 2035
i2035 <- land_use %>% filter(item == "Country area")

i2035
# need to verify that Central America (list) and my manual selections are selecting the same countries!
# on the front end, it's possible to just select regions and it will auto-select all of the countries inside of that.

# overall, this is an extremely straightforward way to download the core FAOSTAT data