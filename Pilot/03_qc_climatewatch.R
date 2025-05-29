library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
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
data <- read_csv(latest_file, col_types = cols(members_id = col_character()))

# Split out members_id into separate columns
data %<>%
  mutate(members_vec = str_split(members_id, ",")) %>%
  unnest_wider(members_vec, names_sep = "_") %>%
  rename_with(~ paste0("dim_", letters[seq_along(.)]), starts_with("members_vec_")) # give generic dimension name for now

# Keep relevant columns for data comparison
data %<>% 
  select(record_id, value, starts_with("dim_"))

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

# Force dim_* columns to characters
pub %<>% 
  mutate(across(starts_with("dim_"), as.character))

# Keep relevant columns for data comparison
pub %<>% 
  select(value, starts_with("dim_"))

# ---- Distinguish how columns match ----

set.seed(123)  # for reproducibility

# Sample 3 values from each column of data_dims
sampled_values <- data %>%
  select(starts_with("dim_")) %>%
  reframe(across(everything(), ~ sample(unique(.x), size = 3))) %>%
  pivot_longer(everything(), names_to = "dim_col", values_to = "sampled_val")

# Get distinct values from each dim_* column in pub
pub_long <- pub %>%
  select(starts_with("dim_")) %>%
  pivot_longer(everything(), names_to = "dim_col", values_to = "pub_val") %>%
  distinct()

# Get mapping from sampled_values to pub cols
dim_map <- pub_long %>% 
  left_join(sampled_values, by = c("pub_val" = "sampled_val"), suffix = c("", ".match")) %>% 
  filter(!is.na(dim_col.match)) %>% 
  distinct(dim_col, dim_col.match)

# Throw an error if more matches than dimensions
if(nrow(dim_map) != ncol(data %>% select(starts_with("dim_")))) {
  stop(glue::glue("Error: dim_map has {nrow(dim_map)} rows, but {ncol(data %>% select(starts_with('dim_')))} dimension columns found in data."))
}

# Rename columns of data with correct dimension IDs
data %<>%
  rename(!!!setNames(dim_map$dim_col.match, dim_map$dim_col))

# ---- Create comparison data frame ----

data %>% 
  full_join(pub, by = dim_map$dim_col)

dim_map$dim_col

data
pub


# ---- Compare with internal data file ----
# Perhaps create a separate script for this process