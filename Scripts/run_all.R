library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))
source(here("Scripts/olade.R"))
source(here("Scripts/emdat.R"))
source(here("Scripts/climatewatch.R"))
source(here("Scripts/other.R"))
source(here("Scripts/fao.R"))

# ---- run calls ----

# define global specs
global_spec <- list(diagnostics = TRUE, export = FALSE) # for init testing
global_spec <- list(diagnostics = TRUE, export = TRUE) # for export

# define run_list and run many
run_list <- meta %>% filter(source == "CRED") %>% pull(id)
fao2 <- c(1739,1869,4049,2019,2020,1740,2038,2022,2039,3382)
fix <- c(2019,2022,3382)

run_many_indicators(run_list)
#beepr::beep(1) # notify when done running

# run single indicator
run_one_indicator(4185)
