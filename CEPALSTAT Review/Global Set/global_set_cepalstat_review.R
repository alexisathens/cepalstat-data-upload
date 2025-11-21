library(tidyverse)
library(readxl)
library(magrittr)
library(here)
library(writexl)

source(here("Scripts/utils.R"))

# ============================================================================
# STEP 1: Read the mapping file
# ============================================================================

message("Reading cisat_cepalstat_mapping.xlsx...")
map <- read_xlsx(here("CEPALSTAT Review", "Global Set", "cisat_cepalstat_mapping.xlsx"))

# ============================================================================
# STEP 2: Read and clean CISAT_Part_2.xlsx
# ============================================================================

message("Reading CISAT_Part_2.xlsx...")
cisat_file <- here("CEPALSTAT Review", "Global Set", "CISAT_Part_2.xlsx")
cisat_raw <- read_excel(cisat_file, sheet = "Self assessment tool", col_names = FALSE)

# Find header row
header_row_idx <- which.max(map_int(seq_len(min(5, nrow(cisat_raw))), 
                                     ~ sum(!is.na(unlist(cisat_raw[., ], use.names = FALSE)))))

header_row <- cisat_raw[header_row_idx, ]
header_names <- unlist(header_row, use.names = FALSE) %>% 
  str_trim() %>%
  str_replace_all("\\s+", " ") %>%
  str_replace("^\\s+|\\s+$", "")

header_names[is.na(header_names) | header_names == ""] <- paste0("V", 
  which(is.na(header_names) | header_names == ""))

cisat_clean <- cisat_raw %>%
  slice(-(1:header_row_idx))

colnames(cisat_clean) <- header_names

# Fill downward for hierarchical columns
fill_cols <- names(cisat_clean)[
  str_detect(names(cisat_clean), regex("area|topic|number|indicator", ignore_case = TRUE))
]
fill_cols <- fill_cols[!str_detect(fill_cols, "\\d")]

if(length(fill_cols) == 0) {
  fill_cols <- names(cisat_clean)[seq_len(min(5, ncol(cisat_clean)))]
  fill_cols <- fill_cols[!str_detect(fill_cols, "\\d")]
}

cisat_clean %<>%
  mutate(across(where(is.character), ~ str_trim(.))) %>%
  mutate(across(where(is.character), ~ if_else(. == "" | . == "NA", NA_character_, .))) %>%
  fill(all_of(fill_cols), .direction = "down") %>%
  filter(if_any(everything(), ~ !is.na(.))) %>%
  filter(if_any(where(is.character), ~ !is.na(.) & str_squish(.) != ""))

if("Statistic" %in% names(cisat_clean)) {
  # Fill Statistic with Indicator when Statistic is missing
  if("Indicator" %in% names(cisat_clean)) {
    cisat_clean %<>% 
      mutate(Statistic = ifelse(
        is.na(Statistic) | Statistic == "",
        Indicator,
        ifelse(Statistic %in% c("Equivalent to the indicator",
                               "Refer to original source in metadata"),
               Indicator, Statistic)
      ))
  }
  # Now filter out rows without Statistic
  cisat_clean %<>% filter(!is.na(Statistic) & Statistic != "")
}

cisat_clean %<>% select(where(~ !all(is.na(.))))

# ============================================================================
# STEP 3: Fill missing Area, Topic, Tier, Theme from CISAT based on Number
# ============================================================================

message("Filling missing information based on Number...")

# Columns to fill
cols_to_fill <- c("Area", "Topic", "Tier", "Theme")
cols_to_fill <- cols_to_fill[cols_to_fill %in% names(map) & cols_to_fill %in% names(cisat_clean)]

if(length(cols_to_fill) == 0) {
  stop("No matching columns found between mapping file and CISAT file.")
}

# Create lookup from CISAT by Number
cisat_lookup <- cisat_clean %>%
  filter(!is.na(Number)) %>%
  select(Number, all_of(cols_to_fill)) %>%
  distinct(Number, .keep_all = TRUE)

# Convert Number to character for matching (handle both numeric and character)
if(is.numeric(map$Number)) {
  map %<>% mutate(Number_char = as.character(Number))
  cisat_lookup %<>% mutate(Number_char = as.character(Number))
} else {
  map %<>% mutate(Number_char = Number)
  cisat_lookup %<>% mutate(Number_char = Number)
}

# Fill missing values
for(col in cols_to_fill) {
  missing_count <- sum(!is.na(map$Number_char) & (is.na(map[[col]]) | map[[col]] == ""))
  message(paste("Filling", col, "-", missing_count, "rows with Number but missing", col))
  
  if(missing_count > 0) {
    map %<>%
      left_join(
        cisat_lookup %>% select(Number_char, !!sym(col)),
        by = "Number_char",
        suffix = c("", "_cisat")
      ) %>%
      mutate(
        !!sym(col) := ifelse(
          !is.na(Number_char) & (is.na(!!sym(col)) | !!sym(col) == ""),
          !!sym(paste0(col, "_cisat")),
          !!sym(col)
        )
      ) %>%
      select(-paste0(col, "_cisat"))
  }
}

# Remove helper column
map %<>% select(-Number_char)

# ============================================================================
# STEP 4: Save cleaned file
# ============================================================================

output_file <- here("CEPALSTAT Review", "Global Set", "cisat_cepalstat_mapping_cleaned.xlsx")
writexl::write_xlsx(map, output_file)
message(paste("\nCleaned mapping file saved to:", output_file))

message("\n✓ Cleaning complete!")


output_file <- here("CEPALSTAT Review", "Global Set", "cisat_cepalstat_mapping_satellite.xlsx")

satellite <- map %>% 
  select(Area:Theme) %>% 
  mutate(`Satellite Data` = NA)

writexl::write_xlsx(satellite, output_file)
message(paste("\nCleaned mapping file saved to:", output_file))

message("\n✓ Cleaning complete!")

# ============================================================================
# STEP 5: Read cleaned file for analysis
# ============================================================================

map <- read_xlsx(here("CEPALSTAT Review", "Global Set", "cisat_cepalstat_mapping_cleaned.xlsx"))

message(paste("Mapping file ready for analysis:", nrow(map), "rows,", ncol(map), "columns"))

satellite <- read_xlsx(here("CEPALSTAT Review", "Global Set", "cisat_cepalstat_mapping_satellite_chatgpt.xlsx"))

satellite %<>% select(Number, Statistic, `Satellite Data`, Notes)

# ============================================================================
# STEP 6: Merge map with satellite and identify opportunities
# ============================================================================

message("\nMerging map with satellite data...")

# Use both Number and Statistic as merge keys
merge_keys <- c()

if("Number" %in% names(map) && "Number" %in% names(satellite)) {
  merge_keys <- c(merge_keys, "Number")
}

if("Statistic" %in% names(map) && "Statistic" %in% names(satellite)) {
  merge_keys <- c(merge_keys, "Statistic")
}

if(length(merge_keys) == 0) {
  stop("Need both Number and Statistic columns in both files for merging")
}

message(paste("Merging on:", paste(merge_keys, collapse = " and ")))

# Convert merge keys to character in both files for consistent matching
for(key in merge_keys) {
  map %<>% mutate(!!sym(key) := as.character(!!sym(key)))
  satellite %<>% mutate(!!sym(key) := as.character(!!sym(key)))
}

# Merge map with satellite
merged <- map %>%
  left_join(satellite, by = merge_keys, suffix = c("", "_satellite"))

message(paste("Merged file has", nrow(merged), "rows"))

# Clean merged data
merged %<>% 
  mutate(CEPALSTAT = ifelse(CEPALSTAT == "-", NA_character_, CEPALSTAT)) %>% 
  select(-Notes) %>% 
  rename(`Satellite` = `Satellite Data`, Notes = Notes_satellite)

# Save as full spreadsheet
full <- merged

output_file <- here("CEPALSTAT Review", "Global Set", "cisat_cepalstat_satellite_final.xlsx")
writexl::write_xlsx(full, output_file)
