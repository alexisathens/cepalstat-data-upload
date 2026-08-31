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
global_spec <- list(diagnostics = TRUE, export = FALSE) # for init testing
#global_spec <- list(diagnostics = TRUE, export = TRUE) # for export - review results in Scripts/dashboard.qmd

# define run_list and run many
run_list <- meta %>% filter(source == "CRED") %>% pull(id)
#run_list <- c("5647")

run_many_indicators(run_list)
#beepr::beep(1) # notify when done running

# run single indicator
run_one_indicator(2487)
