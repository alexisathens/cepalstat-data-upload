# Revises indicator metadata sheets (definition, methodology, comments) for CEPALSTAT, with or without
# AI assistance. The functions are in update_metadata_fn.R; each one accepts one indicator or several.
#
# Before starting: run build_tables.R if the published text may have changed, so the drafts start from
# the current snapshots (Metadata/{id}.md). Each draft is saved as Metadata/{id}_draft.md.

library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/update_metadata_fn.R"))

# ---- 1. start English drafts ----

# AI-assisted revision (gold standard optional for indicators that already have published text)
start_draft(2022)

# AI edits following your instructions; everything else is kept as published
start_draft(4183, instructions = "Update the GDP base year from 2010 to 2018 dollars")

# Copy the published English as it is, for small edits by hand
start_draft(4185, ai_assist = FALSE)

# New indicator (no published text yet): a closely related gold standard is required
start_draft(5805, gold = 4185)

# Several indicators at once, e.g. a whole source, using a revised pilot as the gold standard
ids <- meta %>% filter(source == "WRI", id != 3158) %>% pull(id)
start_draft(ids, gold = 3158)

# human intervention: edit the English in Metadata/{id}_draft.md


# ---- 2. translate and format for CEPALSTAT Admin ----

# Translates only the English fields you changed (the others keep their published Spanish) and rewrites
# the same draft in Admin format. Use overwrite = TRUE to translate again after editing a finished draft.
finish_draft(c(2022, 4183, 4185, 5805))

# human intervention: copy each field into CEPALSTAT Admin, then run build_tables.R
# (drafts whose text is now published are deleted automatically)


# ---- overview of drafts in progress ----

draft_status()
