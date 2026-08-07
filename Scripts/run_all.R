library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))
source(here("Scripts/fao.R"))
source(here("Scripts/olade.R"))
source(here("Scripts/emdat.R"))
source(here("Scripts/climatewatch.R"))
source(here("Scripts/other.R"))

# ---- run calls ----

# define global specs
global_spec <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE) # for init testing
#global_spec <- list(diagnostics = TRUE, export = TRUE, qc_check = TRUE, open_qmd = TRUE) # for single export
#global_spec <- list(diagnostics = TRUE, export = TRUE, qc_check = TRUE, open_qmd = FALSE) # for many export

# define run_list and run many
run_list <- meta %>% filter(source == "CRED") %>% pull(id)
#run_list <- c("5647")

run_many_indicators(run_list)
#beepr::beep(1) # notify when done running

# run single indicator
run_one_indicator(2487)
