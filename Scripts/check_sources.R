# This script compiles a review spreadsheet of every source currently attached to CEPALSTAT's
# environmental indicators, in English and Spanish, with the list of indicators using each one --
# for manual review/annotation before making source updates. See utils.R for get_indicator_source().

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


## 2. Collect every source attached to each indicator, in English and Spanish ----

get_indicator_sources_all <- function(indicator_id) {
  en <- get_indicator_source(indicator_id, lang = "en") %>%
    rename(source_id = id) %>%
    rename_with(~ paste0(.x, "_en"), .cols = -source_id)

  es <- get_indicator_source(indicator_id, lang = "es") %>%
    rename(source_id = id) %>%
    rename_with(~ paste0(.x, "_es"), .cols = -source_id)

  full_join(en, es, by = "source_id") %>%
    mutate(indicator_id = indicator_id)
}

source_links <- map_dfr(env_ids, function(id) {
  message(glue("▶ Fetching sources for indicator {id}..."))
  tryCatch(
    get_indicator_sources_all(id),
    error = function(e) {
      message(glue("⚠️ Failed to fetch sources for indicator {id}: {conditionMessage(e)}"))
      tibble()
    }
  )
})

# flag any indicators that came back with no source at all, for visibility
no_source_ids <- setdiff(env_ids, unique(source_links$indicator_id))
if (length(no_source_ids) > 0) {
  message(glue("⚠️ {length(no_source_ids)} indicator(s) returned no source: {paste(no_source_ids, collapse = ', ')}"))
}


## 3. Compile one row per unique source, with the indicators that use it ----

source_links %<>%
  left_join(env, by = c("indicator_id" = "id")) %>%
  mutate(indicator_label = glue("{indicator_id} - {indicator}"))

sources_compiled <- source_links %>%
  group_by(source_id, organization_acronym_en, organization_name_en, description_en,
           publication_url_en, organization_url_en,
           organization_acronym_es, organization_name_es, description_es,
           publication_url_es, organization_url_es) %>%
  summarize(
    n_indicators = n_distinct(indicator_id),
    indicators = paste(sort(unique(indicator_label)), collapse = "; "),
    .groups = "drop"
  ) %>%
  arrange(source_id) %>%
  mutate(notes = NA_character_) # blank column to append suggested changes

sources_compiled %<>% 
  filter(organization_acronym_en != "SDG") # remove SDG sources


## 4. Export for manual review ----

out_dir <- here("Docs/Árbol Reorganización - 2026 Junio")
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

write_xlsx(sources_compiled, file.path(out_dir, "environmental_source_review_20260803.xlsx"))
message(glue("✅ Exported {nrow(sources_compiled)} unique sources to Data/Checks/source_review.xlsx"))
