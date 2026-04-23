# Generic CEPALSTAT indicator processing function
process_indicator <- function(indicator_id, data, dim_config,
                              filter_fn, transform_fn, footnotes_fn,
                              regional_fn = NULL, # NULL = default sum, FALSE = skip, function = custom
                              source_fn = NULL,
                              remove_lac = TRUE, # TRUE = remove and recalculate LAC, FALSE = keep source LAC
                              diagnostics = TRUE, export = TRUE,
                              ind_notes = NULL,
                              open_qmd = TRUE,
                              new_indicator = FALSE) {
  message(glue("▶ Processing indicator {indicator_id}..."))

  ## 1. Filter and transform raw data
  df <- data %>% filter_fn() %>% transform_fn()

  ## 2. Standardize country names and filter to LAC
  df %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name) %>%
    filter(Country %in% iso$name)
  # if this is dropping LAC countries, manually add the country names as they're represented in the data source in the build_iso_table.R file

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
  
  # change type, for all indicators but those missing the years field
  if(!indicator_id %in% c(2031)) { # MEA indicator
    df %<>% mutate(Years = as.character(Years))
  }

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
  
  ## 4. Join labels
  df_l <- get_indicator_labels(df, dim_config)
  assert_no_na_cols(df_l)
  
  
  ## 5. Get public data and create comparison file
  if(!new_indicator) { # if public data exists, use it in comparison check
    pub <- get_cepalstat_data(indicator_id) %>% 
      mutate(value = as.numeric(value))
    join_keys <- names(df_l) %>% keep(~ str_detect(.x, "^dim_"))
    comp <- full_join(df_l, pub, by = join_keys, suffix = c("", ".pub"))
  } else { # else fill comparison checks with NAs for public data
    comp <- df_l %>% mutate(value.pub = NA_integer_)
  }
  
  assert_no_na_cols(comp, !contains("value"))
  
  # Summarize dimension overlap
  comp_sum <- get_comp_summary(comp, dim_config)
  comp_table <- run_comparison_checks(comp, dim_config)
  
  
  ## 6. Inspect differences between public and new file
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
  
  ## 7. Add footnotes, sources and format
  df_f <- df_l %>% 
    mutate(footnotes_id = "") %>% 
    footnotes_fn()
  
  df_f %>% 
    select(starts_with("dim_"), value, footnotes_id) %>%
    format_for_wasabi(indicator_id, source_fn = source_fn)
  
  assert_no_na_cols(df_f)
  
  # 8. Export data and QC report
  if (export) {
    dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")
    
    # First, throw error if metadata file is open
    xlsx_con <- tryCatch(file(here::here("Data/indicator_metadata.xlsx"), open = "a"), error = function(e) e)
    if (inherits(xlsx_con, "error")) stop("Close indicator_metadata.xlsx before running this script.")
    close(xlsx_con)
    
    # Write excel files
    write_xlsx(df_f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))
    write_xlsx(comp_table, glue(here("Data/Checks/comp_id{indicator_id}.xlsx")))
    message(glue("✅ Exported cleaned and comparison files for {indicator_id}"))
    
    # Render data quality checks file
    render_qc_checks(indicator_id, open_qmd, new_indicator)
    message(glue("✅ Exported quality check file for {indicator_id}"))
    
    # Update metadata for code version and last updated
    update_indicator_metadata(indicator_id, ind_notes)
    message(glue("✅ Updated internal metadata for {indicator_id}"))
    message(glue("✅ Indicator {indicator_id} processing complete"))
  }
  
  return(list(clean = df_l, formatted = df_f, comp = comp, comp_sum = comp_sum))
  
}

## Debugging
# indicator_id = 5672
# data = data_prod
# dim_config = dim_config_5672
# filter_fn = filter_5672
# transform_fn = transform_5672
# remove_lac = FALSE
# regional_fn = regional_5672
# footnotes_fn = footnotes_5672
# source_fn = source_5672
# diagnostics = TRUE
# export = FALSE
# open_qmd = TRUE
# new_indicator = TRUE

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
#   source_fn = source_2022,
#   diagnostics = TRUE,
#   export = TRUE
# )