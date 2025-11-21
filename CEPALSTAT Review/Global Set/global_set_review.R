library(tidyverse)
library(readxl)
library(magrittr)
library(CepalStatR)
library(here)
library(stringdist)
library(writexl)

source(here("Scripts/utils.R"))

# ============================================================================
# STEP 1: Read and clean CISAT_Part_2.xlsx
# ============================================================================

message("Reading CISAT_Part_2.xlsx...")

# Read the Excel file
cisat_file <- here("CEPALSTAT Review", "Global Set", "CISAT_Part_2.xlsx")

# Read without headers first to inspect structure
cisat_raw <- read_excel(cisat_file, sheet = "Self assessment tool", col_names = FALSE)

message("\nFirst 15 rows of raw data:")
print(head(cisat_raw, 15))

# Identify header rows (typically first few rows with column names)
# Look for rows that might contain headers
message("\nIdentifying header structure...")

# Find the row that contains the actual column headers
# Usually headers are in one of the first few rows
header_candidates <- head(cisat_raw, 5)

# Display potential header rows
for(i in seq_len(min(5, nrow(cisat_raw)))) {
  message(paste("\nRow", i, ":"))
  print(unlist(header_candidates[i, ], use.names = FALSE))
}

# Identify which row has the most non-NA values (likely the header row)
header_row_idx <- which.max(map_int(seq_len(min(5, nrow(cisat_raw))), 
                                     ~ sum(!is.na(unlist(cisat_raw[., ], use.names = FALSE)))))

message(paste("\nUsing row", header_row_idx, "as header row"))

# Extract and clean header row
header_row <- cisat_raw[header_row_idx, ]
header_names <- unlist(header_row, use.names = FALSE) %>% 
  str_trim() %>%
  str_replace_all("\\s+", " ") %>%  # Replace multiple spaces with single space
  str_replace("^\\s+|\\s+$", "")     # Remove leading/trailing whitespace

# Replace empty or NA headers with generic names
header_names[is.na(header_names) | header_names == ""] <- paste0("V", 
  which(is.na(header_names) | header_names == ""))

# Remove header rows and any rows before the header
cisat_clean <- cisat_raw %>%
  slice(-(1:header_row_idx))

# Assign cleaned column names
colnames(cisat_clean) <- header_names

message("\nCleaned column names:")
print(colnames(cisat_clean))

# Identify columns that need to be filled downward (Area, Topics, Numbers)
# Look for columns that match common patterns
fill_cols <- names(cisat_clean)[
  str_detect(names(cisat_clean), regex("area|topic|number|indicator", ignore_case = TRUE))
]

# Filter out columns with numbers in their names
fill_cols <- fill_cols[!str_detect(fill_cols, "\\d")]

# Also check first few columns which often contain hierarchical data
if(length(fill_cols) == 0) {
  # If no obvious fill columns, check first few columns for sparse data
  # (columns with many NAs that should be filled)
  sparse_cols <- names(cisat_clean)[seq_len(min(5, ncol(cisat_clean)))]
  # Filter out columns with numbers
  sparse_cols <- sparse_cols[!str_detect(sparse_cols, "\\d")]
  fill_cols <- sparse_cols
  message("No obvious fill columns found. Checking first few columns for sparse data.")
}

message(paste("Columns to fill downward:", paste(fill_cols, collapse = ", ")))

# Clean and fill downward
cisat_clean %<>%
  # Trim whitespace from all character columns
  mutate(across(where(is.character), ~ str_trim(.))) %>%
  # Replace empty strings with NA for better handling
  mutate(across(where(is.character), ~ if_else(. == "" | . == "NA", NA_character_, .))) %>%
  # Fill downward for hierarchical columns (Area, Topics, Numbers)
  fill(all_of(fill_cols), .direction = "down") %>%
  # Remove completely empty rows
  filter(if_any(everything(), ~ !is.na(.))) %>%
  # Remove rows where all character columns are empty/whitespace
  filter(if_any(where(is.character), ~ !is.na(.) & str_squish(.) != ""))

# Remove rows without statistics (we just want the statistics level of detail)
cisat_clean %<>% 
  filter(!is.na(Statistic))

# Fill in detailed statistic when info is in indicator
cisat_clean %<>% 
  mutate(Statistic = ifelse(Statistic %in% c("Equivalent to the indicator",
                                             "Refer to original source in metadata"), Indicator, Statistic))

# Drop columns with all NA values
cisat_clean %<>% 
  select(where(~ !all(is.na(.))))

message(paste("\nData after cleaning:", nrow(cisat_clean), "rows,", ncol(cisat_clean), "columns"))

# Display sample of cleaned data
message("\nSample of cleaned data (first 10 rows):")
print(head(cisat_clean, 10))

# Extract statistics from CISAT
# Use the Statistic column directly
statistic_col <- "Statistic"

# Verify Statistic column exists
if(!statistic_col %in% names(cisat_clean)) {
  stop("Statistic column not found in the data. Please check column names.")
}

message(paste("Using Statistic column for analysis"))

# Get unique statistics from CISAT
# Use ONLY the Statistic column for matching (no area/topic context)
cisat_indicators <- cisat_clean %>%
  select(all_of(statistic_col)) %>%
  distinct() %>%
  filter(!is.na(!!sym(statistic_col))) %>%
  filter(!!sym(statistic_col) != "") %>%
  rename(indicator_cisat = all_of(statistic_col)) %>%
  mutate(indicator_cisat_clean = str_squish(tolower(indicator_cisat)))

message(paste("\nFound", nrow(cisat_indicators), "unique statistics in CISAT"))

# ============================================================================
# STEP 2: Get environmental indicators from CEPALSTAT
# ============================================================================

message("\nFetching environmental indicators from CEPALSTAT...")

# Get all indicators from CEPALSTAT
ind <- call.indicators()
ind %<>% as_tibble()

# Filter for environmental indicators
env <- ind %>%
  filter(Area == "Environmental") %>%
  filter(!is.na(`Indicator ID`))

# Handle special case for GHG emissions indicator
env %<>%
  mutate(Subdimension = ifelse(Indicador.1 == "Emissions of greenhouse gases (GHGs)",
                               paste(Subdimension, Indicador.1, sep = " / "), Subdimension)) %>%
  mutate(Indicador.1 = ifelse(Indicador.1 == "Emissions of greenhouse gases (GHGs)", 
                              Indicador.2, Indicador.1))

# Organize and clean CEPALSTAT indicators
# Use ONLY the indicator column for matching (no dimension/subdimension context)
cepalstat_indicators <- env %>%
  select(id = `Indicator ID`, 
         dimension = Dimension,
         subdimension = Subdimension,
         indicator = Indicador.1) %>%
  mutate(
    indicator_cepalstat = indicator,
    indicator_cepalstat_clean = str_squish(tolower(indicator)),
    # Create a full indicator name for reference (but not for matching)
    indicator_full = paste(dimension, subdimension, indicator, sep = " | ")
  ) %>%
  select(id, dimension, subdimension, indicator, indicator_cepalstat, 
         indicator_cepalstat_clean, indicator_full)

message(paste("Found", nrow(cepalstat_indicators), "environmental indicators in CEPALSTAT"))

# ============================================================================
# STEP 3: Create mapping using fuzzy matching
# ============================================================================

message("\nCreating statistic mapping...")

# Function to find best match for each CISAT statistic
find_best_match <- function(cisat_name, cepalstat_names, threshold = 0.6) {
  # Calculate string distances using Jaro-Winkler distance
  # stringdist returns distance (0 = identical, higher = more different)
  distances <- stringdist(cisat_name, cepalstat_names, method = "jw")
  
  # Get best match
  best_idx <- which.min(distances)
  best_distance <- distances[best_idx]
  similarity <- 1 - best_distance
  
  # Check if similarity meets threshold
  if(similarity >= threshold) {
    return(list(
      match_idx = best_idx,
      match_name = cepalstat_names[best_idx],
      similarity = similarity,
      distance = best_distance
    ))
  } else {
    return(list(
      match_idx = NA,
      match_name = NA_character_,
      similarity = similarity,
      distance = best_distance
    ))
  }
}

# Create mapping for each CISAT statistic
mapping_results <- map_dfr(seq_len(nrow(cisat_indicators)), function(i) {
  cisat_ind <- cisat_indicators$indicator_cisat_clean[i]
  cisat_orig <- cisat_indicators$indicator_cisat[i]
  
  # Find best match
  match_result <- find_best_match(
    cisat_ind, 
    cepalstat_indicators$indicator_cepalstat_clean,
    threshold = 0.5  # Lower threshold for initial matching
  )
  
  if(!is.na(match_result$match_idx)) {
    matched_ind <- cepalstat_indicators[match_result$match_idx, ]
    
    tibble(
      indicator_cisat = cisat_orig,
      indicator_cisat_clean = cisat_ind,
      cepalstat_id = matched_ind$id,
      indicator_cepalstat = matched_ind$indicator,
      dimension = matched_ind$dimension,
      subdimension = matched_ind$subdimension,
      indicator_full = matched_ind$indicator_full,
      similarity_score = match_result$similarity,
      distance = match_result$distance,
      match_status = ifelse(match_result$similarity >= 0.7, "High Confidence", 
                           ifelse(match_result$similarity >= 0.5, "Medium Confidence", "Low Confidence"))
    )
  } else {
    tibble(
      indicator_cisat = cisat_orig,
      indicator_cisat_clean = cisat_ind,
      cepalstat_id = NA_integer_,
      indicator_cepalstat = NA_character_,
      dimension = NA_character_,
      subdimension = NA_character_,
      indicator_full = NA_character_,
      similarity_score = match_result$similarity,
      distance = match_result$distance,
      match_status = "No Match"
    )
  }
})

# Add unmatched CEPALSTAT indicators (full join)
# Find which CEPALSTAT indicators weren't matched to any CISAT statistic
matched_cepalstat_ids <- mapping_results %>%
  filter(!is.na(cepalstat_id)) %>%
  pull(cepalstat_id) %>%
  unique()

unmatched_cepalstat <- cepalstat_indicators %>%
  filter(!id %in% matched_cepalstat_ids) %>%
  mutate(
    indicator_cisat = NA_character_,
    indicator_cisat_clean = NA_character_,
    similarity_score = NA_real_,
    distance = NA_real_,
    match_status = "CEPALSTAT Only"
  ) %>%
  select(indicator_cisat, indicator_cisat_clean, cepalstat_id = id, 
         indicator_cepalstat, dimension, subdimension, indicator_full,
         similarity_score, distance, match_status)

# Combine matched and unmatched results
mapping_results <- bind_rows(mapping_results, unmatched_cepalstat)

# ============================================================================
# STEP 4: Summary and output
# ============================================================================

message("\nMapping Summary:")
message(paste("Total CISAT statistics:", nrow(cisat_indicators)))
message(paste("Total CEPALSTAT indicators:", nrow(cepalstat_indicators)))
message(paste("Matched statistics:", sum(!is.na(mapping_results$cepalstat_id) & !is.na(mapping_results$indicator_cisat))))
message(paste("High confidence matches:", sum(mapping_results$match_status == "High Confidence")))
message(paste("Medium confidence matches:", sum(mapping_results$match_status == "Medium Confidence")))
message(paste("Low confidence matches:", sum(mapping_results$match_status == "Low Confidence")))
message(paste("CISAT statistics with no match:", sum(mapping_results$match_status == "No Match")))
message(paste("CEPALSTAT indicators with no match:", sum(mapping_results$match_status == "CEPALSTAT Only")))

# Sort by match status and similarity
mapping_results %<>%
  arrange(desc(similarity_score))

# Display results
message("\nMapping Results (first 20 rows):")
print(head(mapping_results, 20))

# Save results
output_file <- here("CEPALSTAT Review", "Global Set", "cisat_statistics_cepalstat_fuzzy_mapping.xlsx")
writexl::write_xlsx(mapping_results, output_file)
message(paste("\nResults saved to:", output_file))

# # Create summary statistics
# summary_stats <- mapping_results %>%
#   group_by(match_status) %>%
#   summarise(
#     count = n(),
#     avg_similarity = mean(similarity_score, na.rm = TRUE),
#     min_similarity = min(similarity_score, na.rm = TRUE),
#     max_similarity = max(similarity_score, na.rm = TRUE),
#     .groups = "drop"
#   )
# 
# message("\nSummary Statistics:")
# print(summary_stats)
# 
# # Save summary
# summary_file <- here("CEPALSTAT Review", "Global Set", "mapping_summary.xlsx")
# writexl::write_xlsx(summary_stats, summary_file)
# message(paste("Summary saved to:", summary_file))
# 
# # Show unmatched items for manual review
# # CISAT statistics with no match or low confidence
# unmatched_cisat <- mapping_results %>%
#   filter(match_status == "No Match" | match_status == "Low Confidence") %>%
#   select(indicator_cisat, similarity_score, match_status) %>%
#   arrange(desc(similarity_score))
# 
# # CEPALSTAT indicators with no match
# unmatched_cepalstat_only <- mapping_results %>%
#   filter(match_status == "CEPALSTAT Only") %>%
#   select(cepalstat_id, indicator_cepalstat, dimension, subdimension, match_status) %>%
#   arrange(indicator_cepalstat)
# 
# if(nrow(unmatched_cisat) > 0) {
#   message(paste("\n", nrow(unmatched_cisat), "CISAT statistics need manual review:"))
#   print(unmatched_cisat)
#   
#   unmatched_cisat_file <- here("CEPALSTAT Review", "Global Set", "unmatched_cisat_statistics.xlsx")
#   writexl::write_xlsx(unmatched_cisat, unmatched_cisat_file)
#   message(paste("Unmatched CISAT statistics saved to:", unmatched_cisat_file))
# }
# 
# if(nrow(unmatched_cepalstat_only) > 0) {
#   message(paste("\n", nrow(unmatched_cepalstat_only), "CEPALSTAT indicators have no CISAT match:"))
#   print(head(unmatched_cepalstat_only, 20))
#   
#   unmatched_cepalstat_file <- here("CEPALSTAT Review", "Global Set", "unmatched_cepalstat_indicators.xlsx")
#   writexl::write_xlsx(unmatched_cepalstat_only, unmatched_cepalstat_file)
#   message(paste("Unmatched CEPALSTAT indicators saved to:", unmatched_cepalstat_file))
# }
# 
# message("\n✓ Analysis complete!")
