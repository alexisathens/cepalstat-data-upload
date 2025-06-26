library(tidyverse)
library(magrittr)
library(readxl)
library(FAOSTAT)

# Info for FAOSTAT package: https://r-packages.io/packages/FAOSTAT/download_faostat_bulk


# Load information about all datasets into a data frame
fao_metadata <- search_dataset() %>% as_tibble()

# Alternatively go here to see data areas: https://www.fao.org/faostat/en/#data

# Load sample data
land_use <- get_faostat_bulk(code = "RL")
# Show the structure of the data
str(land_use)

