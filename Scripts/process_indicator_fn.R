source(here("Scripts/utils.R"))
source(here("Scripts/04_technical_sheet.R"))

# define indicator specs
indicator_spec <- function(
    indicator_id, data, max_year, dim_config, filter_data, transform_data, calculate_regional, 
    footnotes = list(), define_source = existing_source, new_indicator = FALSE) {
  list(indicator_id = indicator_id, data = data, max_year = max_year, dim_config = dim_config,
       filter_data = filter_data, transform_data = transform_data, calculate_regional = calculate_regional,
       footnotes = footnotes, define_source = define_source, new_indicator = new_indicator)
}

# debugging
# spec <- spec_4046
# global <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE, metadata = FALSE)

# generic CEPALSTAT indicator processing function
process_indicator <- function(spec = indicator_spec, global = global_spec) {
  # get indicator-specific and global variables and functions
  indicator_id   <- spec$indicator_id
  data           <- spec$data
  max_year       <- spec$max_year
  dim_config     <- spec$dim_config
  filter_data    <- spec$filter_data
  transform_data <- spec$transform_data
  calculate_regional <- spec$calculate_regional
  add_footnote    <- spec$add_footnote
  define_source      <- spec$define_source
  new_indicator <- spec$new_indicator
  diagnostics <- global$diagnostics
  export <- global$export
  qc_check <- global$qc_check
  open_qmd <- global$open_qmd
  metadata <- global$metadata
  
  message(glue("▶ Processing indicator {indicator_id}..."))
  
  # get clean indicator df
  df <- data %>% 
    filter_data() %>% 
    transform_data() %>%
    standardize_countries() %>%
    assert_data_reqs(., dim_config, indicator_id, max_year) %>%
    calculate_regional() %>% # default = recalculate sum
    assert_no_duplicates()
  
  # get labeled indicator df
  df_l <- df %>% 
    get_cepalstat_ids(., dim_config) %>% # **store common lookups - create join_labels as wrapper w storage?
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
    add_footnotes(., footnotes) %>% # default = no footnote
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
