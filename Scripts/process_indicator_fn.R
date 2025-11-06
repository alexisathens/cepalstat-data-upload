## testing
# indicator_id <- 4046
# data <- emdat
# dim_config <- dim_config_4046
# filter_fn <- filter_4046
# transform_fn <- transform_4046
# footnotes_fn <- footnotes_4046

# Generic CEPALSTAT indicator processing function
process_indicator <- function(indicator_id, data, dim_config,
                              filter_fn, transform_fn, footnotes_fn,
                              source_id = NULL,
                              diagnostics = TRUE, export = TRUE) {
  message(glue("▶ Processing indicator {indicator_id}..."))
  
  ## 1. Filter and transform EMDAT data
  df <- data %>% filter_fn() %>% transform_fn()
  
  # Overwrite country names with std_name in iso file
  df %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name)
  
  # Filter out extra non-LAC groups
  df %<>%
    filter(Country %in% iso$name)
  # remove Bonaire, Sint Eustatius and Saba and Netherlands Antilles (former)
  
  # remove regional totals, construct ECLAC total from sum of countries
  df %<>%
    filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America and the Caribbean", "Latin America"))
  
  # Correct types
  df %<>%
    mutate(Years = as.character(Years))
  
  assert_no_duplicates(df)
  
  ## 2. Create ECLAC regional total
  eclac_totals <- df %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value"))))) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  df <- bind_rows(df, eclac_totals) %>%
    arrange(Country, Years)
  
  ## 3. Harmonize labels
  pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels()
  
  join_keys <- setNames(dim_config$pub_col, dim_config$data_col)
  
  # Keep only matching columns and join
  pub <- pub %>% select(all_of(unname(join_keys)), value)
  comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
  
  # Summarize dimension overlap
  comp_sum <- get_comp_summary_table(comp, dim_config)
  
  ## 4. Inspect differences between public and new file
  if(diagnostics) {
    message(glue("🧾 Comparison summary for indicator {indicator_id}:"))
    
    # 1️⃣ Missing from new data (likely label changes or dropped series)
    missing_old <- comp_sum %>%
      filter(status == "Old Only")
    if (nrow(missing_old) > 0) {
      message("⚠️  Dimensions present in old data only:")
      print(missing_old %>% count(dim_name, sort = TRUE))
      print(missing_old)
    }
    
    # 2️⃣ Newly added in updated data (new years or countries)
    missing_new <- comp_sum %>%
      filter(status == "New Only")
    if (nrow(missing_new) > 0) {
      message("🆕  Dimensions present in new data only:")
      print(missing_new %>% count(dim_name, sort = TRUE))
      print(missing_new)
    }
  }
  
  ## 5. Join dimensions
  df_f <- join_data_dim_members(df, dim_config)
  assert_no_na_cols(df_f)
  
  ## 6. Add footnotes and format
  df_f %<>%
    mutate(footnotes_id = "") %>% 
    footnotes_fn()
  
  df_f %<>% 
    select(ends_with("_id"), value) %>%
    format_for_wasabi(indicator_id, source_id = source_id)
  
  assert_no_na_cols(df_f)
  
  ## 7. Create comparison file
  # Rebuild comp from current cleaned data and latest CEPALSTAT version
  comp <- full_join(df, pub, by = join_keys, suffix = c("", ".pub"))
  comp <- join_data_dim_members(comp, dim_config)
  assert_no_na_cols(comp, !contains("value"))
  
  # Run comparison checks and format
  comp <- create_comparison_checks(comp, dim_config)
  
  # 8. Optional export
  if (export) {
    dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")
    write_xlsx(df_f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))
    write_xlsx(comp, glue(here("Data/Checks/comp_id{indicator_id}.xlsx")))
    message(glue("  ✓ Exported cleaned and comparison files for {indicator_id}"))
  }
  
  return(list(clean = df_f, comp = comp, comp_sum = comp_sum))
  
}

## Sample indicator processing code

# ## ---- indicator 5646 - persons affected by disasters ----
# indicator_id <- 5646
# 
# # Fill out dim config table by matching the following info:
# # get_indicator_dimensions(indicator_id)
# # print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())
# 
# dim_config_5646 <- tibble(
#   data_col = c("Country", "Years", "Type"),
#   dim_id = c("208", "29117", "21714"),
#   pub_col = c("208_name", "29117_name", "21714_name")
# )
# 
# filter_5646 <- function(data) {
#   data %<>% 
#     filter(`Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological", "Geophysical")) %>%
#     filter(as.numeric(`Start Year`) >= 1990 & as.numeric(`Start Year`) <= max_year) %>% 
#     filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
# }
# 
# transform_5646 <- function(data) {
#   data %<>% 
#     select(`DisNo.`, `Disaster Subgroup`, `Disaster Type`, Country, Years = `Start Year`, `Total Affected`) %>% 
#     mutate(Type = paste0(`Disaster Type`, "s")) %>% 
#     mutate(Type = case_when(
#       Type == "Mass movement (wet)s" ~ "Wet mass displacement",
#       Type == "Volcanic activitys" ~ "Volcanic eruptions",
#       Type == "Mass movement (dry)s" ~ "Dry mass displacement",
#       TRUE ~ Type
#     )) %>% 
#     mutate(Group = case_when(
#       `Disaster Subgroup` %in% c("Climatological", "Hydrological", "Meteorological") ~ "Climate change related",
#       `Disaster Subgroup` %in% c("Geophysical") ~ "Geophysical",
#       TRUE ~ NA_character_)) %>%
#     select(-`Disaster Subgroup`, -`Disaster Type`) %>% 
#     rename(value = `Total Affected`) %>% 
#     filter(!is.na(value)) %>% 
#     group_by(Country, Years, Group, Type) %>% 
#     summarize(value = sum(value, na.rm = T), .groups = "drop")
#   
#   type_sum <- data %>% 
#     group_by(Country, Years, Group) %>% 
#     summarize(value = sum(value, na.rm = T)) %>% 
#     ungroup() %>% 
#     rename(Type = Group)
#   
#   data %<>% 
#     select(-Group) %>% 
#     bind_rows(type_sum)
# }
# 
# footnotes_5646 <- function(data) {
#   data # No footnote
# }
# 
# source_5646 <- function(indicator_id) {
#   742  # Base de datos internacional de desastres (EM-DAT)
# }
# 
# result_5646 <- process_emdat_indicator(
#   indicator_id = 5646,
#   data = emdat,
#   dim_config = dim_config_5646,
#   filter_fn = filter_5646,
#   transform_fn = transform_5646,
#   footnotes_fn = footnotes_5646,
#   source_id = source_5646,
#   diagnostics = TRUE,
#   export = TRUE
# )