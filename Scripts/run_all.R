source(here("Scripts/process_indicator_fn.R"))
source(here("Scripts/technical_sheet.R"))
source(here("Scripts/emdat.R"))


# ---- run functions ----

# Run one indicator: look up its spec, process it, and store the result as result_<id>.
# Sample usage: run_one_indicator(4046)
run_one_indicator <- function(id, global = global_spec) {
  spec <- get(paste0("spec_", id))
  result <- process_indicator(spec, global)
  assign(paste0("result_", id), result, envir = .GlobalEnv)
  invisible(result)
}

# Run many indicators and cache error messages
# Sample usage: run_many_indicators(run_list) # where run_list is list of ids
run_many_indicators <- function(run_list, global = global_spec) {
  run_log <- tibble(id = numeric(), status = character(), message = character())
  
  for (i in seq_along(run_list)) {
    id <- run_list[i]
    
    outcome <- tryCatch({
      run_one_indicator(id)
      list(status = "ok", message = NA_character_)
    }, error = function(e) {
      message(glue("❌ Indicator {id} failed: {conditionMessage(e)}"))
      list(status = "error", message = conditionMessage(e))
    })
    
    run_log <- add_row(run_log, id = id, status = outcome$status, message = outcome$message)
  }
  
  message(glue("\n✅ {sum(run_log$status == 'ok')}/{nrow(run_log)} indicators processed successfully"))
  if (any(run_log$status == "error")) {
    message("❌ Failed: ", paste(run_log$id[run_log$status == "error"], collapse = ", "))
  }
  return(run_log)
}


# ---- run calls ----

# define global specs
global_spec <- list(diagnostics = TRUE, export = TRUE, qc_check = TRUE, open_qmd = FALSE, metadata = FALSE)
#global_spec <- list(diagnostics = TRUE, export = FALSE, qc_check = FALSE, open_qmd = FALSE, metadata = FALSE)

# define run_list and run many
run_list <- meta %>% filter(source == "CRED") %>% pull(id)
# run_list <- c("4046", "5647")

run_many_indicators(run_list)
beepr::beep(1)

# run single indicator
run_one_indicator(5645)


# ---- export metadata -----
# run one "pilot" for indicator group, then update the metadata publicly
pilot <- 5647
suggest_metadata_en(pilot, gold_standard_indicators = 2487)
translate_metadata_es(pilot, gold_standard_indicators = 2487)
export_metadata_admin(pilot)

# next loop over rest in indicator group, using pilot as the gold standard for suggested metadata
run_list <- meta %>% filter(source == "CRED" & id != pilot) %>% pull(id)
gold_meta <- pilot
for(i in run_list){
  suggest_metadata_en(i, gold_standard_indicators = gold_meta)
}

# next is human intervention: manually review the suggested metadata

# once suggested metadata is verified, translate and export admin file, and update the metadata publicly
for(i in run_list){
  translate_metadata_es(i, gold_standard_indicators = gold_meta)
  export_metadata_admin(i)
}
