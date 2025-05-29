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
  
  if(col_id %in% names(data)) {
    data %<>%
      left_join(
        dim_members %>%
          select(id, name) %>%
          rename(!!col_id := id, !!col_label := name),
        by = col_id
      )
  }
}

rm(dim_id, dim_name, dim_members, col_id, col_label, dims_tbl, this_dims_tbl)

data %<>% 
  select(record_id, starts_with("dim_"), value)


# ---- Create comparison data frame ----

# Create a comparison df
comp <- data %>% 
  full_join(pub, by = dim_map$dim_col, suffix = c("_data", "_pub"))

comp %<>% 
  select(record_id, starts_with("dim_"), value_data, value_pub) %>% 
  mutate(across(starts_with("value_"), as.numeric))

# Calculate absolute and relative differences
comp %<>% 
  mutate(abs_diff = abs(value_data - value_pub),
         perc_diff = round((abs_diff / value_pub) * 100, 2))

# Flag issues
comp %<>%
  mutate(
    flag_large_diff = abs_diff > 0.1,  # adjust threshold as needed
    flag_missing_data = is.na(value_data) | is.na(value_pub),
    flag_new_entry = is.na(value_pub),     # entry exists in data but not in pub
    flag_missing_entry = is.na(value_data) # exists in pub but not in data
  )

# Plot scatterplot of expected vs observed
ggplot(comp, aes(x = value_pub, y = value_data)) +
  geom_point(alpha = 0.6) +
  geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "red") +
  labs(x = "Public Data", y = "Internal Data", title = "Value Comparison")

# Plot histogram of differences
ggplot(comp, aes(x = abs_diff)) +
  geom_histogram(bins = 30, fill = "steelblue") +
  labs(title = "Absolute Differences", x = "Difference", y = "Count")




# ---- Compare with internal data file ----
# Perhaps create a separate script for this process