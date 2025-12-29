# Generic CEPALSTAT indicator processing function
script_version <- "v2025-12-29"  # bump manually when logic significantly changes
script_notes <- "add remove_lac parameter to preserve source LAC data"

process_indicator <- function(indicator_id, data, dim_config,
                              filter_fn, transform_fn, footnotes_fn,
                              regional_fn = NULL, # NULL = default sum, FALSE = skip, function = custom
                              source_fn = NULL,
                              remove_lac = TRUE, # TRUE = remove and recalculate LAC, FALSE = keep source LAC
                              diagnostics = TRUE, export = TRUE,
                              ind_notes = NULL) {
  message(glue("▶ Processing indicator {indicator_id}..."))

  ## 1. Filter and transform raw data
  df <- data %>% filter_fn() %>% transform_fn()

  ## 2. Standardize country names and filter to LAC
  df %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name) %>%
    filter(Country %in% iso$name)

  # Conditionally remove LAC regional groupings
  if (remove_lac) {
    df %<>%
      filter(!Country %in% c("South America", "Central America", "Caribbean",
                             "Latin America and the Caribbean", "Latin America"))
  } else {
    # If keeping source LAC, only filter out sub-regions
    df %<>%
      filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America"))
  }

  df %<>% mutate(Years = as.character(Years))

  ## 3. Create ECLAC regional total (automatically skip if remove_lac = FALSE)
  if (!remove_lac) {
    # Override regional_fn to FALSE when keeping source LAC data
    regional_fn <- FALSE
  }

  if (!isFALSE(regional_fn)) {
    if (is.function(regional_fn)) {
      # Use custom aggregation function
      df <- regional_fn(df)
    } else {
      # Default: simple sum
      eclac_totals <- df %>%
        filter(Country != "World") %>%
        group_by(across(all_of(setdiff(names(df), c("Country", "value"))))) %>%
        summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
        mutate(Country = "Latin America and the Caribbean")

      df <- bind_rows(df, eclac_totals) %>%
        arrange(Country, Years)
    }
  }
  
  assert_no_duplicates(df)
  
  ## 4. Harmonize labels
  pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels()
  join_keys <- setNames(dim_config$pub_col, dim_config$data_col)
  
  # Keep only matching columns and join
  pub <- pub %>% select(all_of(unname(join_keys)), value)
  comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
  
  # Summarize dimension overlap
  comp_sum <- get_comp_summary_table(comp, dim_config)
  
  ## 5. Inspect differences between public and new file
  if(diagnostics) {
    message(glue("🧾 Comparison summary for indicator {indicator_id}:"))
    
    # 1️⃣ Missing from new data (likely label changes or dropped series)
    missing_old <- comp_sum %>%
      filter(status == "Old Only")
    if (nrow(missing_old) > 0) {
      message("⚠️  Dimensions present in old data only:")
      print(missing_old %>% count(dim_name, sort = TRUE))
      print(missing_old)
    } else {
      message(glue(" - No missing dimensions"))
    }
    
    # 2️⃣ Newly added in updated data (new years or countries)
    missing_new <- comp_sum %>%
      filter(status == "New Only")
    if (nrow(missing_new) > 0) {
      message("🆕  Dimensions present in new data only:")
      print(missing_new %>% count(dim_name, sort = TRUE))
      print(missing_new)
    } else {
      message(glue(" - No new dimensions"))
    }
  }
  
  ## 6. Join dimensions
  df_f <- join_data_dim_members(df, dim_config)
  assert_no_na_cols(df_f)
  
  ## 7. Add footnotes, sources and format
  df_f %<>%
    mutate(footnotes_id = "") %>% 
    footnotes_fn()
  
  df_f %<>% 
    select(ends_with("_id"), value) %>%
    format_for_wasabi(indicator_id, source_fn = source_fn)
  
  assert_no_na_cols(df_f)
  
  ## 8. Create comparison file
  # Rebuild comp from current cleaned data and latest CEPALSTAT version
  comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
  comp <- join_data_dim_members(comp, dim_config)
  assert_no_na_cols(comp, !contains("value"))
  
  # Run comparison checks and format
  comp <- create_comparison_checks(comp, dim_config)
  
  # 9. Export
  if (export) {
    dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")
    write_xlsx(df_f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))
    write_xlsx(comp, glue(here("Data/Checks/comp_id{indicator_id}.xlsx")))
    message(glue("✅ Exported cleaned and comparison files for {indicator_id}"))
    
    # Update metadata for code version and last updated
    update_indicator_metadata(indicator_id, ind_notes)
  }
  
  return(list(clean = df, formatted = df_f, comp = comp, comp_sum = comp_sum))
  
}

## Debugging
# indicator_id = 4463
# data = data_4463
# dim_config = dim_config_4463
# filter_fn = filter_4463
# transform_fn = transform_4463
# regional_fn = regional_4463
# footnotes_fn = footnotes_4463
# source_fn = NULL
# diagnostics = TRUE
# export = FALSE

## Sample indicator processing code

# indicator_id <- 2022 # fertilizer use intensity
# 
# # Fill out dim config table by matching the following info:
# # get_indicator_dimensions(indicator_id)
# # print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())
# 
# dim_config_2022 <- tibble(
#   data_col = c("Country", "Years"),
#   dim_id = c("208", "29117"),
#   pub_col = c("208_name", "29117_name")
# )
# 
# filter_2022 <- function(data) {
#   data %<>% 
#     filter(element == "agricultural_use") %>% 
#     filter(item %in% c("Nutrient nitrogen N (total)", "Nutrient phosphate P2O5 (total)", "Nutrient potash K2O (total)")) %>% 
#     # filter out any countries too with inconsistent entries (to not impact LAC total)
#     filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
# }
# 
# transform_2022 <- function(data) {
#   data %<>% 
#     group_by(area, year) %>% # sum across fertilizer types (items)
#     summarize(value = sum(value, na.rm = T)) %>% 
#     ungroup %>% 
#     rename(Country = area, Years = year) %>% 
#     select(Country, Years, value) %>% 
#     mutate(Years = as.character(Years)) %>% 
#     left_join(result_cropland) %>% 
#     arrange(Country, Years)
# }
# 
# regional_2022 <- function(data) {
#   eclac_totals <- data %>%
#     group_by(across(all_of(setdiff(names(df), c("Country", "value", "area"))))) %>%
#     summarise(value = sum(value, na.rm = TRUE),
#               area = sum(area, na.rm = TRUE), .groups = "drop") %>%
#     mutate(Country = "Latin America and the Caribbean")
#   
#   data <- bind_rows(data, eclac_totals) %>%
#     mutate(value = value/area) %>% 
#     arrange(Country, Years) %>% 
#     select(Country, Years, value)
#   
#   return(data)
# }
# 
# footnotes_2022 <- function(data) {
# data %>% 
#   mutate(
#     footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id),
#     footnotes_id = if_else(Years == "2002", append_footnote(footnotes_id, "7177"), footnotes_id))
# }
# 
# source_2022 <- function() {
#   913 # Calculations made based on fertilizer consumption data and agriculture area data from online statistical database (FAOSTAT) to Food and Agriculture Organization of the United Nations (FAO). 
# }
# 
# result_2022 <- process_indicator(
#   indicator_id = 2022,
#   data = rfn,
#   dim_config = dim_config_2022,
#   filter_fn = filter_2022,
#   transform_fn = transform_2022,
#   regional_fn = regional_2022,
#   footnotes_fn = footnotes_2022,
#   source_id = source_2022,
#   diagnostics = TRUE,
#   export = TRUE
# )