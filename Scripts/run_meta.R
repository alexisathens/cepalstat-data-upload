source(here("Scripts/technical_sheet.R"))
source(here("Scripts/utils.R"))

# ---- export metadata -----

# run one "pilot" for indicator group, then update the metadata publicly
pilot <- 5647
suggest_metadata_en(pilot, gold_standard_indicators = 2487) # default: 2487
translate_metadata_es(pilot, gold_standard_indicators = 2487)
export_metadata_admin(pilot)

# human intervention: update pilot metadata publicly in CEPALSTAT Admin

# next loop over rest in indicator group, using pilot as the gold standard for suggested metadata
meta_ids <- meta %>% filter(source == "CRED" & id != pilot) %>% pull(id)
gold_meta <- pilot

walk(meta_ids, ~ tryCatch(
  suggest_metadata_en(.x, gold_standard_indicators = gold_meta),
  error = function(e) message(glue("❌ suggest_metadata_en failed for {.x}: {conditionMessage(e)}"))
))

# human intervention: manually review and edit the suggested metadata

walk(meta_ids, ~ tryCatch({
  translate_metadata_es(.x, gold_standard_indicators = gold_meta)
  export_metadata_admin(.x)
}, error = function(e) message(glue("❌ translate/export failed for {.x}: {conditionMessage(e)}"))
))

# human intervention: update all indicator metadata publicly
