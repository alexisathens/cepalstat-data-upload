
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

no_footnote <- function(df) { # default: nothing to add
  df %>% mutate(footnotes_id = "")   
} 

lac_footnote <- function(df) {
  df %>% mutate(footnotes_id = ifelse(Country == "Latin America and the Caribbean", "6970", footnotes_id))
  # Says: 6970/ Calculado a partir de la información disponible de los países de la región.
}

existing_source <- function(df, indicator_id) { # default: get existing source
  df %>% mutate(source_id = get_indicator_source(indicator_id) %>% slice(1) %>% pull(id)) 
}

create_comp_file <- function(df, indicator_id, dim_config, new_indicator) {
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

# add lac_footnote as another standard option

indicator_spec <- function(indicator_id, data, max_year, dim_config, filter_data, transform_data, 
                           calculate_regional, append_footnote = no_footnote, 
                           define_source = existing_source, new_indicator = FALSE) {
  list(indicator_id = indicator_id, data = data, max_year = max_year, dim_config = dim_config,
       filter_data = filter_data, transform_data = transform_data, calculate_regional = calculate_regional,
       append_footnote = append_footnote, define_source = define_source, new_indicator = new_indicator)
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
  define_source      <- spec$define_source
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
  
  # get labeled indicator df
  df_l <- df %>% 
    get_cepalstat_ids(., dim_config) %>% # store common lookups - create join_labels as wrapper w storage?
    assert_no_na_cols()
  
  # get base comparison file
  comp <- df_l %>%
    create_comp_file(., indicator_id, dim_config, new_indicator) %>%
    assert_no_na_cols(., !contains("value"))
  
  # run summary diagnostics
  if (diagnostics) {
    comp %>% 
      get_comp_summary(., dim_config) %>% 
      run_diagnostics()
  }
  
  # get comparison table for quality check qmd
  comp_table <- comp %>% 
    run_comparison_checks(., dim_config)
  
  # get wasabi-formatted indicator df
  df_f <- df_l %>% 
    append_footnote() %>% # default = no footnote
    define_source(., indicator_id) %>% # default = existing source
    format_for_wasabi(., indicator_id) %>% 
    assert_no_na_cols()
  
  # export data for wasabi + qc report
  if (export) {
    dt_stamp <- format(Sys.time(), "%Y-%m-%dT%H%M%S")
    
    # Write excel files
    write_xlsx(df_f, glue(here("Data/Cleaned/id{indicator_id}_{dt_stamp}.xlsx")))
    write_xlsx(comp_table, glue(here("Data/Checks/comp_id{indicator_id}.xlsx")))
    message(glue("✅ Exported cleaned and comparison files for {indicator_id}"))
  }
  
  # render quality check report
  if (qc_check) {
    # Render data quality checks file
    render_qc_checks(indicator_id, new_indicator, open_qmd)
    message(glue("✅ Exported quality check file for {indicator_id}"))
  }
  
  # suggest metadata with anthropic api
  if(metadata) {
    suggest_metadata_en(indicator_id)
    message(glue("✅ Exported suggested metadata for {indicator_id} written to Metadata/Outputs/metadata_{indicator_id}_en.txt"))
  }
  
  message(glue("✅ Indicator {indicator_id} processing complete"))
  return(list(data = df, labeled = df_l, formatted = df_f, comp = comp))
}


  #calculate_regional_sum() %>% 
  #calculate_regional_wgt_avg() %>% 
  #maintain_regional() %>% 
  #calculate_regional_custom() %>% 
  
# throw error at end of transform col names!

# idea: could have some functions like filter_fao if there's standard filtering on all of them, or 
  # recalculate_regional_proportion that are reutilized at a global/source level (vs just indicator level!)
  
# rename fields in filter/transform num/denom! then can automate
