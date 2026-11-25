# ---- Settings updated by hand ----
#
# Values that go out of date on a schedule (a new data release, a new statistical yearbook, a new
# AI model version) are kept here, so they are updated in one place. This file is loaded by utils.R,
# so every script that sources utils.R can use these settings.
#
# Review these when a source publishes new data or a new yearbook comes out, and update the
# "checked" date next to each value.


## Latest year of reliable data, by source ----
# Data after this year is dropped during cleaning. A single indicator can use an earlier year by
# setting max_year in its spec (e.g. if its data lags the rest of the source).

max_year_fao   <- 2025  # checked July 2026
max_year_olade <- 2024  # checked Dec 2025 (2024 data)
max_year_emdat <- 2025  # checked July 2026
max_year_cw    <- 2023  # checked July 2026 (Climate Watch)
max_year_other <- 2025  # IRENA, ISO Survey, InforMEA, Ramsar, UNEP; also the year in the ISO Survey file name


## External reference files (Data/Tables/External/) ----
# Used by build_tables.R for the yearbook and profile flags in Data/indicator_metadata.xlsx.

yearbook_file  <- "AE2026-Indice-WEB-PDF.xlsx"   # statistical yearbook index; replace each year
yearbook_sheet <- "AE2026-HTML&PDF_ID"           # sheet with the CEPALSTAT indicator IDs
profile_file   <- "profile_data.xlsx"            # indicators in the country profiles


## AI model ----
# Used by technical_sheet.R to draft and translate indicator metadata.

ANTHROPIC_MODEL <- "claude-sonnet-4-6"
