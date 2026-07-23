source(here("Scripts/process_indicator_fn.R")) # for indicator_spec(), process_indicator(); sources utils.R + 04_technical_sheet.R

# NOTE: also depends on the relevant source script(s) (e.g. 0102_emdat.R) having already been run/sourced
# interactively, for raw data (emdat), max_year_*, and indicator-specific filter/transform/dim_config to
# exist. Not auto-sourced here yet since those scripts still end with their own old-style process_indicator()
# call, which would re-run for real as a side effect of sourcing them.

# store global variables in utils? or these go to source scripts?
max_year_fao <- 2024
max_year_emdat <- 2025

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
