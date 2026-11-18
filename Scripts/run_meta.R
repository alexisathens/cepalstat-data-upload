library(here)
source(here("Scripts/technical_sheet_md.R"))
source(here("Scripts/utils.R"))

# ---- export metadata -----

# run one "pilot" for indicator group, then update the metadata publicly
pilot <- 3158
suggest_metadata_en(pilot, gold_standard_indicators = 2487) # default: 2487
translate_metadata_es(pilot, gold_standard_indicators = 2487)
export_metadata_admin(pilot)

# human intervention: update pilot metadata publicly in CEPALSTAT Admin

# next loop over rest in indicator group, using pilot as the gold standard for suggested metadata
meta_ids <- meta %>% filter(source == "CAIT - WRI" & id != pilot & !id %in% c(5806:5808)) %>% pull(id)
meta_ids <- 4463
gold_meta <- 5649

walk(meta_ids, ~ tryCatch(
  suggest_metadata_en(.x, gold_standard_indicators = gold_meta),
  error = function(e) message(glue("❌ suggest_metadata_en failed for {.x}: {conditionMessage(e)}"))
))

# human intervention: manually review and edit the suggested metadata

walk(meta_ids, ~ tryCatch({
  translate_metadata_es(.x, gold_standard_indicators = gold_meta, overwrite = TRUE)
  export_metadata_admin(.x)
}, error = function(e) message(glue("❌ translate/export failed for {.x}: {conditionMessage(e)}"))
))

# human intervention: update all indicator metadata publicly


# ---- new indicators -----

# New indicators have no published metadata to revise. The draft is written from the indicator's entry in
# Data/indicator_metadata.xlsx (name, source, dimensions) by adapting the content of a closely related
# published indicator, so choose a gold standard with the same data source and general context.
new_id <- 5808
gold_new <- 5807

suggest_metadata_en(new_id, gold_standard_indicators = gold_new, new_indicator = TRUE)

# human intervention: review and edit Metadata/{new_id}_en.md

translate_metadata_es(new_id, gold_standard_indicators = gold_new, new_indicator = TRUE)
export_metadata_admin(new_id)

# human intervention: enter the metadata in the CEPALSTAT Admin area
