library(tidyverse)
library(magrittr)
library(readxl)
library(glue)
library(lubridate)

# Manually define
indicator_id <- 4461

# ---- Get latest file of indicator ----
# Folder where cleaned files live
folder_path <- "Pilot/Data/Cleaned"

# List all relevant files for this indicator
file_list <- list.files(folder_path, pattern = glue("id{indicator_id}_\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{6}}\\.csv"), full.names = TRUE)

# Parse timestamps and extract latest file path
latest_file <- tibble(file = file_list) %>%
  mutate(
    file_name = basename(file),
    timestamp_str = str_extract(file_name, "\\d{4}-\\d{2}-\\d{2}T\\d{6}"),
    timestamp = ymd_hms(timestamp_str, tz = "UTC", truncated = 1)
  ) %>%
  arrange(desc(timestamp)) %>% 
  slice(1) %>% 
  pull(file)

# Read in csv
data <- read_csv(latest_file)
# will need to clean this further, but pause on it for now until the final format (compared to CEPALSTAT UI) is understood

# ---- Download current public version of indicator ----

# Build URL
url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/data?lang=en&format=json")

# Perform the request and parse JSON
result <- request(url) %>%
  req_perform() %>%
  resp_body_json(simplifyDataFrame = TRUE)

# Extract and flatten the data portion
pub <- result$body$data %>%
  as_tibble()

pub


# ---- Compare with internal data file ----
# Perhaps create a separate script for this process