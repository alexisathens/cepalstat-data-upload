source(here("Scripts/process_indicator_fn.R"))
source(here("Scripts/0102_emdat.R"))

# run

# define global specs
global_spec <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE, metadata = FALSE)


process_indicator(spec_4046, global_spec)
# **why is this printing the data table at the end?
