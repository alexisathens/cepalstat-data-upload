library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(lubridate)
library(here)
library(assertthat)

# Manually define
indicator_id <- 2035

# ---- Get latest file of indicator ----
# Folder where cleaned files live
folder_path <- here("Pilot", "Data", "Cleaned")

# List all relevant files for this indicator
file_list <- list.files(folder_path, pattern = glue("id{indicator_id}_\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{6}}\\.xlsx"), full.names = TRUE)

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
#data <- read_csv(latest_file, col_types = cols(members_id = col_character()))
data <- read_excel(latest_file, col_types = c("text", "numeric", "text", "text", "text", "numeric"))

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
  left_join(sampled_values, by = c("pub_val" = "sampled_val"), suffix = c("", ".match")) %>% View()
filter(!is.na(dim_col.match)) %>% 
  distinct(dim_col, dim_col.match)

# Throw an error if more matches than dimensions
assert_that(
  nrow(dim_map) == ncol(select(data, starts_with("dim_"))),
  msg = glue(
    "Error: dim_map has {nrow(dim_map)} rows, but {ncol(select(data, starts_with('dim_')))} dimension columns found in data."
  )
)

dim_map

# Rename columns of data with correct dimension IDs
data %<>%
  rename(!!!setNames(dim_map$dim_col.match, dim_map$dim_col))


# ---- Create comparison file ----

# Create a comparison df
comp <- data %>% 
  full_join(pub, by = dim_map$dim_col, suffix = c("_data", "_pub"))

# Give any new entries that exist in public but not in internal a record id
comp %<>% 
  mutate(record_id = ifelse(is.na(record_id), paste0("p", str_pad(row_number(), 4, pad = "0")), record_id))

comp %<>% 
  select(record_id, value_data, value_pub, starts_with("dim_")) %>% 
  mutate(across(starts_with("value_"), as.numeric))


# ---- Get dimension member names ----

# Initialize table storing full dimension info
dims_tbl <- NULL

# Gather all relevant dimension members
for(dimension_id in str_remove(dim_map$dim_col, "dim_")) {
  # Build dimension URL
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/dimensions/{dimension_id}")
  
  # Send request and parse JSON
  result <- request(url) %>%
    req_perform() %>%
    resp_body_string() %>%
    fromJSON(flatten = TRUE)
  
  # Extract and process dimension info
  this_dims_tbl <- result %>%
    pluck("body", "dimensions") %>%
    as_tibble()
  
  # Bind to full table
  dims_tbl %<>% bind_rows(this_dims_tbl)
}

# Loop through each dimension and join its labels
for(i in 1:nrow(dims_tbl)) {
  dim_id     <- dims_tbl$id[i]
  dim_name   <- dims_tbl$name[i]
  dim_members <- dims_tbl$members[[i]]  # the data frame of id–label mappings
  dim_members$id <- as.character(dim_members$id)
  
  # The column name in `data` that holds this dim's values
  col_id <- paste0("dim_", dim_id)
  col_label <- paste0(col_id, "_label")
  
  if(col_id %in% names(comp)) {
    comp %<>%
      left_join(
        dim_members %>%
          select(id, name) %>%
          rename(!!col_id := id, !!col_label := name),
        by = col_id
      )
  }
}

rm(dim_id, dim_name, dim_members, col_id, col_label, dims_tbl, this_dims_tbl)

comp %<>% 
  select(record_id, starts_with("dim_"), starts_with("value"))


# ---- Build comparison indicators ----

# Calculate absolute and relative differences
comp %<>% 
  mutate(abs_diff = abs(value_data - value_pub),
         perc_diff = round((abs_diff / value_pub) * 100, 2))

# Flag issues
comp %<>%
  mutate(
    flag_large_diff = perc_diff > 20,  # adjust threshold as needed
    flag_missing_entry = is.na(value_data), # exists in pub but not in data
    flag_new_entry = is.na(value_pub),     # entry exists in data but not in pub
    flag_some_na = is.na(value_data) | is.na(value_pub) # either new or missing entry
  )

# Label data availability status
comp %<>% 
  mutate(status = case_when(
    is.na(value_data) & is.na(value_pub) ~ "Missing in Both",
    is.na(value_data) ~ "Old Only",
    is.na(value_pub) ~ "New Only",
    TRUE ~ "Present in Both"
  ))

# Export comp file for now
write_csv(comp, glue("Pilot/Data/Checks/comp_id{indicator_id}.csv"))


# ---- Compare with internal data file ----
# Perhaps create a separate script for this process