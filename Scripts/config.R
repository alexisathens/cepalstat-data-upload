# ---- Settings updated by hand ----
#
# Values that go out of date on a schedule (a new data release, a new statistical yearbook, a new
# AI model version) are kept here, so they are updated in one place. This file is loaded by utils.R,
# so every script that sources utils.R can use these settings.
#
# Review these when a new yearbook or AI model version comes out.


## External reference files (Data/Tables/External/) ----
# Used by build_tables.R for the yearbook and profile flags in Data/indicator_metadata.xlsx.

yearbook_file  <- "AE2026-Indice-WEB-PDF.xlsx"   # statistical yearbook index; replace each year
yearbook_sheet <- "AE2026-HTML&PDF_ID"           # sheet with the CEPALSTAT indicator IDs
profile_file   <- "profile_data.xlsx"            # indicators in the country profiles


## AI model ----
# Used by update_metadata_fn.R to draft and translate indicator metadata.

ANTHROPIC_MODEL <- "claude-sonnet-4-6"
