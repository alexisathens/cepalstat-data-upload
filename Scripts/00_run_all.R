
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
}

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
  
  lac_total
}

#regional options:
  #calculate_regional_sum() %>% 
  #calculate_regional_wgt_avg() %>% 
  #maintain_regional() %>% 
  #calculate_regional_custom() %>% (pass regional_4606 wtv)

# data requirements: no NA values (throw error), no extra column names (throw error), no data beyond
# max_year (fix silently); return df
assert_data_reqs <- function(data, dim_config, indicator_id) {
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

indicator_spec <- function(indicator_id, data, max_year, dim_config, filter_data, transform_data, 
                           calculate_regional, append_footnote = NULL, modify_source = NULL, 
                           new_indicator = FALSE) {
  list(indicator_id = indicator_id, data = data, max_year = max_year, dim_config = dim_config,
       filter_data = filter_data, transform_data = transform_data, calculate_regional = calculate_regional,
       append_footnote = append_footnote, modify_source = modify_source, new_indicator = new_indicator)
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
  #append_footnote = NULL, # create generic LAC footnote, default to null
  #modify_source = NULL
)

global_spec <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE, metadata = FALSE)

# debugging
spec <- spec_4046
global <- global_spec

run_pipeline <- function(spec = indicator_spec, global = global_spec) {
  # get indicator-specific and global variables and functions
  indicator_id   <- spec$indicator_id
  data           <- spec$data
  max_year       <- spec$max_year
  dim_config     <- spec$dim_config
  filter_data    <- spec$filter_data
  transform_data <- spec$transform_data
  calculate_regional <- spec$calculate_regional
  append_footnote    <- spec$append_footnote
  modify_source      <- spec$modify_source
  new_indicator <- spec$new_indicator
  diagnostics <- global$diagnostics
  export <- global$export
  qc_check <- global$qc_check
  open_qmd <- global$open_qmd
  metadata <- global$metadata
  
  # get clean indicator df
  df <- data %>% 
    filter_data() %>% 
    transform_data() %>%
    standardize_countries() %>%
    assert_data_reqs(., dim_config, indicator_id) %>% # check column names and no NA values
    bind_rows(calculate_regional(.)) %>%
    assert_no_duplicates()
  
  
  
  
}



df <- data %>% 
  filter_4046() %>% 
  transform_4046() %>% 
  #check_col_names() %>% # check names are correct for regional fn (dim_config+value+num+denom acceptable)
  standardize_countries() %>% 
  bind_rows(calculate_regional_sum(.)) %>% 
  assert_no_duplicates()  # changed fn to return data
  
df_l <- df %>% 
  join_labels(., dim_config) %>% # store common lookups
  assert_no_na_cols() # changed fn to return data
  
# not sure if this chunk will work properly... might need to rewrite functions
comp <- df_l %>% 
  create_comp_file() %>% 
  run_comparison_checks() %>% 
  assert_no_na_cols() %>%
  run_diagnostics()

df_f <- df_l %>% 
  modify_footnotes() %>% 
  modify_source() %>% 
  format_for_wasabi() %>% 
  assert_no_na_cols()


# only exports df_f and comp_table
if(export) {
  
}

if(qc_check) {
  if(open_qmd) {
    
  }
}

if(metadata) {
  
}



  #calculate_regional_sum() %>% 
  #calculate_regional_wgt_avg() %>% 
  #maintain_regional() %>% 
  #calculate_regional_custom() %>% 
  
# throw error at end of transform col names!

# idea: could have some functions like filter_fao if there's standard filtering on all of them, or 
  # recalculate_regional_proportion that are reutilized at a global/source level (vs just indicator level!)
  
# rename fields in filter/transform num/denom! then can automate
