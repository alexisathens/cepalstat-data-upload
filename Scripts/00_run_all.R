source(here("Scripts/process_indicator_fn.R"))
source(here("Scripts/0102_emdat.R"))

# define global specs
global_spec <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE, metadata = FALSE)

# define indicators to bulk run
run_list <- meta %>% 
  filter(source == "CRED") %>% 
  pull(id)

run_list <- c("4046", "5647")

result_4046 <- process_indicator(spec_4046, global_spec)
