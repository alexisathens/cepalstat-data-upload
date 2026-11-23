library(here)
source(here("Scripts/technical_sheet.R"))
source(here("Scripts/utils.R"))

# ---- export metadata -----

# Existing and new indicators use the same calls. New indicators (no published Definition, Methodology
# or Comments in CEPALSTAT) are detected automatically and drafted from their entry in
# Data/indicator_metadata.xlsx by adapting the gold standard, so for new indicators choose a gold
# standard with the same data source and general context.

# run one "pilot" for indicator group, then update the metadata publicly
pilot <- 2022
gold <- 2038
suggest_metadata_en(pilot, gold) # default: 2487
translate_metadata_es(pilot, gold)
export_metadata_admin(pilot)

# human intervention: update pilot metadata publicly in CEPALSTAT Admin

# next loop over rest in indicator group, using pilot as the gold standard for suggested metadata
meta_ids <- meta %>% filter(source == "CAIT - WRI" & id != pilot & !id %in% c(5806:5808)) %>% pull(id)
meta_ids <- 4243
gold <- 2023

walk(meta_ids, ~ tryCatch(
  suggest_metadata_en(.x, gold),
  error = function(e) message(glue("❌ suggest_metadata_en failed for {.x}: {conditionMessage(e)}"))
))

# human intervention: manually review and edit the suggested metadata

walk(meta_ids, ~ tryCatch({
  translate_metadata_es(.x, gold = gold, overwrite = TRUE)
  export_metadata_admin(.x)
}, error = function(e) message(glue("❌ translate/export failed for {.x}: {conditionMessage(e)}"))
))

# human intervention: update all indicator metadata publicly
