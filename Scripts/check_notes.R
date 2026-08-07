# This script compiles a review spreadsheet of every technical note (footnote) currently attached
# to CEPALSTAT's environmental indicators, in English and Spanish, with the list of indicators using
# each one -- for manual review/annotation before making note updates. See utils.R for
# get_indicator_footnotes(). Mirrors check_sources.R.

library(tidyverse)
library(magrittr)
library(readxl)
library(writexl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(CepalStatR)

source(here("Scripts/utils.R"))

## 1. Retrieve full list of environmental indicators (same approach as build_metadata_table.R) ----

ind <- call.indicators() %>% as_tibble()

env <- ind %>%
  filter(Area == "Environmental" & !is.na(`Indicator ID`)) %>%
  filter(Indicador.2 != "") %>% # this is the indicator level for env area
  rename(id = `Indicator ID`, indicator = Indicador.2) %>%
  select(id, indicator)

env_ids <- env$id


## 2. Collect every technical note attached to each indicator, in English and Spanish ----

get_indicator_notes_all <- function(indicator_id) {
  en <- get_indicator_footnotes(indicator_id, lang = "en")
  es <- get_indicator_footnotes(indicator_id, lang = "es")

  # many indicators legitimately have zero technical notes -- treat that as a normal empty
  # result, not a fetch failure
  if (nrow(en) == 0 && nrow(es) == 0) {
    return(tibble(note_id = numeric(), description_en = character(),
                  description_es = character(), indicator_id = numeric()))
  }

  en %<>% rename(note_id = id, description_en = description)
  es %<>% rename(note_id = id, description_es = description)

  full_join(en, es, by = "note_id") %>%
    mutate(indicator_id = indicator_id)
}

note_links <- map_dfr(env_ids, function(id) {
  message(glue("▶ Fetching notes for indicator {id}..."))
  tryCatch(
    get_indicator_notes_all(id),
    error = function(e) {
      message(glue("⚠️ Failed to fetch notes for indicator {id}: {conditionMessage(e)}"))
      tibble()
    }
  )
})

# flag any indicators that came back with no note at all, for visibility
no_note_ids <- setdiff(env_ids, unique(note_links$indicator_id))
if (length(no_note_ids) > 0) {
  message(glue("ℹ️ {length(no_note_ids)} indicator(s) have no technical notes: {paste(no_note_ids, collapse = ', ')}"))
}


## 3. Compile one row per unique note, with the indicators that use it ----

note_links %<>%
  left_join(env, by = c("indicator_id" = "id")) %>%
  mutate(indicator_label = glue("{indicator_id} - {indicator}"))

notes_compiled <- note_links %>%
  group_by(note_id, description_en, description_es) %>%
  summarize(
    n_indicators = n_distinct(indicator_id),
    indicators = paste(sort(unique(indicator_label)), collapse = "; "),
    .groups = "drop"
  ) %>%
  arrange(note_id) %>%
  mutate(notes = NA_character_) # blank column to append suggested changes


## 4. Export for manual review ----

out_dir <- here("Data/Checks")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

write_xlsx(notes_compiled, file.path(out_dir, "notes_review.xlsx"))
message(glue("✅ Exported {nrow(notes_compiled)} unique notes to Data/Checks/notes_review.xlsx"))
