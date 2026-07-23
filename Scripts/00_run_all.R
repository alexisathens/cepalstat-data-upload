
# store global variables in utils? or these go to source scripts?
max_year_fao <- 2024
max_year_emdat <- 2025

# ============

standardize_countries <- function(df) {
  df %>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name) %>%
    filter(Country %in% iso$name) %>% 
    filter(!Country %in% c("South America", "Central America", "Caribbean", "Latin America")) %>%  # always remove subregions
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda")) # remove countries with frequent incomplete data
# check whether to centralize Country filter...
}

# idea: could have some functions like filter_fao if there's standard filtering on all of them, or
filter_fao <- function(df) {
  df %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)", "Bermuda"))
}

# recalculate regional totals using a simple sum
calculate_regional_sum <- function(df) {
  # remove all LAC sub/regional totals
  df %<>%
    filter(!Country %in% c("South America", "Central America", "Caribbean",
                           "Latin America and the Caribbean", "Latin America"))
  
  # calculate LAC region sum
  lac_total <- df %>%
    filter(Country != "World") %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value"))))) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  df %>% 
    bind_rows(lac_total)
}

# recalculate regional totals with a weighted average
calculate_regional_wgt_avg <- function(df) {
  # ** write this function
}

# keep regional totals from original data source
maintain_regional <- function(df) {
  df
}

# data requirements: no NA values (throw error), no extra column names (throw error), no data beyond
# max_year (fix silently); return df
assert_data_reqs <- function(data, dim_config, indicator_id, max_year) {
  # check for NA values
  na_vals <- data %>% 
    filter(is.na(value))
  
  if (nrow(na_vals) > 0) {
    msg <- glue(
      "❌ {nrow(na_vals)} NA values found in 'value'"
    )
    stop(msg)
  }
  
  # check for extra columns
  acceptable_cols <- c(dim_config$data_col, "value", "num", "denom")
  extra_cols <- setdiff(names(data), acceptable_cols)
  
  if(!is_empty(extra_cols)) {
    msg <- paste0(
      "❌ extra columns found: ",
      paste0(extra_cols, collapse = ", ")
    )
    stop(msg)
  }
  
  # filter out extra years and coerce type
  data %<>% 
    filter(Years <= max_year) %>% 
    mutate(Years = as.character(Years)) %>% 
    arrange(Country, Years)
  
  if (indicator_id == 2031) { # exception for MEA indicator where Years is the value
    data %<>% mutate(Years = as.integer(Years)) 
  }
  
  return(data)
}

join_labels <- function(df, dim_config) {
  get_cepalstat_ids(df %>% mutate(Years = as.character(Years)), # ** define std types later
                            dim_config)
}

add_footnotes <- function(df, footnotes) {
  append_id <- function(existing, new) {
    case_when(
      is.na(existing) | existing == "" ~ new,
      !grepl(paste0("\\b", new, "\\b"), existing) ~ paste(existing, new, sep = ","),
      TRUE ~ existing
    )
  }
  
  df$footnotes_id <- ""
  for (id in names(footnotes)) {
    df$footnotes_id <- if_else(footnotes[[id]](df), append_id(df$footnotes_id, id), df$footnotes_id)
  }
  df
}

lac_footnote <- list("6970" = function(df) df$Country == "Latin America and the Caribbean")

existing_source <- function(df, indicator_id) { # default: get existing source
  df %>% mutate(source_id = get_indicator_source(indicator_id) %>% slice(1) %>% pull(id)) 
}

create_comp_file <- function(df_l, indicator_id, dim_config, new_indicator) {
  # Get public data and create comparison file
  if(!new_indicator) { # if public data exists, use it in comparison check
    pub <- get_cepalstat_data(indicator_id) %>% 
      mutate(value = as.numeric(value)) %>% 
      get_cepalstat_labels(dim_config)
    
    join_keys <- intersect(names(df_l), names(pub)) %>% setdiff("value")
    comp <- full_join(df_l, pub, by = join_keys, suffix = c("", ".pub"))
    
  } else { # else fill comparison checks with NAs for public data
    comp <- df_l %>% mutate(value.pub = NA_integer_)
  }
  
  return(comp)
}

run_diagnostics <- function(comp_sum) {
  # Inspect differences between public and new file
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




# run

spec_4046 <- indicator_spec(
  indicator_id = 4046,
  data = emdat,
  max_year = max_year_emdat,
  dim_config = dim_config_4046,
  filter_data = filter_4046,
  transform_data = transform_4046,
  calculate_regional = calculate_regional_sum
)

# define global specs
global_spec <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE, metadata = FALSE)
