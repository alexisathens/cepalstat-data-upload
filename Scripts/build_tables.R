# Downloads reference tables for all environmental indicators from the CEPALSTAT API, builds the
# indicator metadata table used throughout the code, and saves them locally:
#   Data/Tables/indicator_table.xlsx  - indicators, with their category path in English and Spanish
#   Data/Tables/source_table.xlsx     - sources    + by_indicator (indicator_id, source_id)
#   Data/Tables/note_table.xlsx       - notes      + by_indicator (indicator_id, note_id)
#   Data/Tables/dimension_table.xlsx  - dimensions + by_indicator (indicator_id, dim_id, order, position)
#   Data/indicator_metadata.xlsx      - one row per indicator; read as `meta` by utils.R
#
# The script runs in four parts:
#   A. Download and build every table in memory (nothing is saved yet)
#   B. Compare with the files currently saved
#   C. Print a summary of the changes
#   D. Save the files. The Data/Tables/ files are always updated. Data/indicator_metadata.xlsx is
#      updated only after you confirm, if any changes need review (e.g. indicators removed from
#      CEPALSTAT or a changed main source), since the cleaning code depends on it.
#
# Requests run in parallel, so a full download takes a few minutes. Helper functions are in utils.R
# (sections "CEPALSTAT bulk downloads" and "Comparing and reviewing table updates").
#
# Note: the private API (api-cepalstat-private.cepal.org/cepalstat/api/v1/theme/22) has bulk endpoints
# for the tree, sources, footnotes and dimensions, but they only cover indicators built in CEPALSTAT
# Admin. Externally imported indicators (e.g. from the SDG database or geospatial system) are missing,
# so this script uses the public per-indicator endpoints instead.

library(here)
source(here("Scripts/utils.R"))

paths <- list(
  indicators = here("Data/Tables/indicator_table.xlsx"),
  sources    = here("Data/Tables/source_table.xlsx"),
  notes      = here("Data/Tables/note_table.xlsx"),
  dimensions = here("Data/Tables/dimension_table.xlsx"),
  metadata   = here("Data/indicator_metadata.xlsx")
)


# ==== A. Download and build tables ====

## 1. Indicators ----

# Category path split into up to three levels; deeper levels are kept together in cat3.
# placement numbers each place an indicator appears in the tree, so that an indicator listed twice
# is matched to the right Spanish categories (both languages list the tree in the same order).
env_en <- get_env_indicators("en") %>%
  separate(path, into = c("area", "cat1", "cat2", "cat3"), sep = " > ", fill = "right", extra = "merge") %>%
  select(-area) %>%
  mutate(placement = row_number(), .by = id)

env_es <- get_env_indicators("es") %>%
  separate(path, into = c("area", "cat1_es", "cat2_es", "cat3_es"), sep = " > ", fill = "right", extra = "merge") %>%
  select(-area) %>%
  mutate(placement = row_number(), .by = id) %>%
  rename(indicator_es = indicator)

env <- env_en %>%
  left_join(env_es, by = c("id", "placement")) %>%
  select(-placement)

inds <- unique(env$id)


## 2. Sources ----

source_long <- fetch_indicator_tables(inds, endpoint = "sources") %>%
  rename(source_id = id) %>%
  mutate(source_id = as.integer(source_id))

indicator_source <- source_long %>%
  filter(lang == "en") %>%
  distinct(indicator_id, source_id)

sources <- source_long %>%
  select(-indicator_id) %>%
  to_wide_by_lang("source_id")


## 3. Footnotes ----

note_long <- fetch_indicator_tables(inds, endpoint = "footnotes") %>%
  rename(note_id = id) %>%
  mutate(note_id = as.integer(note_id))

indicator_note <- note_long %>%
  filter(lang == "en") %>%
  distinct(indicator_id, note_id)

notes <- note_long %>%
  select(-indicator_id) %>%
  to_wide_by_lang("note_id")


## 4. Dimensions ----

# members (every country, year, etc. in each dimension) is dropped, since it's very large
dim_long <- fetch_indicator_tables(inds, endpoint = "dimensions", extra_query = "&in=1&path=0",
                                   drop = "members") %>%
  rename(dim_id = id) %>%
  mutate(dim_id = as.integer(dim_id))

# order and position describe the dimension's place within each indicator, so they are kept in
# the by_indicator sheet rather than the dimension table
indicator_dimension <- dim_long %>%
  filter(lang == "en") %>%
  distinct(indicator_id, dim_id, order, position) %>%
  mutate(across(c(order, position), as.integer))

dimensions <- dim_long %>%
  select(-indicator_id, -any_of(c("order", "position"))) %>%
  to_wide_by_lang("dim_id")


## 5. Indicator metadata table ----

# Manual notes to keep in the metadata table
manual_notes <- tribble(
  ~id,  ~notes,
  1755, "historical series that ends in 2015 and does not need to be maintained",
  # 5730, "this used to be indicator 2041",
  # 5672, "this used to be indicator 2040"
)

# Main source: the first source CEPALSTAT lists for each indicator
main_source <- indicator_source %>%
  slice_head(n = 1, by = indicator_id) %>%
  left_join(sources, by = "source_id") %>%
  select(-ends_with("_es"), -ends_with("_url")) %>%
  mutate(source = case_when(
    description == "Euroclima" ~ "Euroclima",
    TRUE ~ organization_acronym
  )) %>%
  rename(id = indicator_id)

# Indicator-specific dimensions (excluding country, year and reporting type), in display order
std_dims <- c("Country__ESTANDAR", "Years__ESTANDAR", "Reporting Type")

ind_dims <- indicator_dimension %>%
  left_join(dimensions %>% select(dim_id, name), by = "dim_id") %>%
  filter(!name %in% std_dims) %>%
  arrange(indicator_id, position) %>%
  summarise(dim_id = paste(dim_id, collapse = "; "),
            dimensions = paste(name, collapse = "; "),
            .by = indicator_id) %>%
  rename(id = indicator_id)

# Yearbook and profile flags, from local files
yearbook_ids <- read_xlsx(here("Data/AE2026-Indice-WEB-PDF.xlsx"), sheet = "AE2026-HTML&PDF_ID",
                          .name_repair = "unique_quiet") %>%
  pull(`ID INDICADOR CEPALSTAT`) %>%
  as.numeric() %>%   # non-numeric cells (e.g. section headers) become NA and are dropped below
  suppressWarnings() %>%
  na.omit()

profile_ids <- read_xlsx(here("Data/profile_data.xlsx")) %>% pull(Indicator)

# One row per indicator (an indicator listed twice in the tree keeps its first placement)
meta_new <- env %>%
  distinct(id, .keep_all = TRUE) %>%
  select(id, indicator, indicator_es, cat1, cat2, cat3) %>%
  left_join(main_source %>% select(id, source_id, source), by = "id") %>%
  left_join(ind_dims, by = "id") %>%
  mutate(
    yearbook = if_else(id %in% yearbook_ids, "Y", NA_character_),
    profile  = if_else(id %in% profile_ids, "Y", NA_character_),
    system   = case_when(
      source == "SDG" ~ "sdg",
      source == "Euroclima" ~ "geo",
      TRUE ~ "env"           # managed in this repository
    )
  ) %>%
  left_join(manual_notes, by = "id")


# ==== B. Compare with the saved files ====

# `meta` is the current Data/indicator_metadata.xlsx, loaded by utils.R

# Formats indicators as "id: name" lines for the summary (none if the table is empty)
label_ids <- function(df) {
  if (nrow(df) == 0) return(character())
  paste0(df$id, ": ", df$indicator)
}

# Indicators no longer in the CEPALSTAT tree (e.g. archived); they are dropped from the new table
removed <- meta %>% filter(!id %in% meta_new$id)
added   <- meta_new %>% filter(!id %in% meta$id)

renamed <- meta_new %>%
  inner_join(meta %>% select(id, old_name = indicator), by = "id") %>%
  filter(indicator != old_name)

# Main source changes (the cleaning code filters on source, e.g. source == "CRED")
source_changed <- meta_new %>%
  inner_join(meta %>% select(id, old_source = source), by = "id") %>%
  filter(coalesce(source, "") != coalesce(old_source, ""))

no_source  <- meta_new %>% filter(is.na(source_id))
dup_ids    <- env %>% count(id) %>% filter(n > 1) %>% pull(id)
unmatched_profile <- setdiff(profile_ids, env$id)

# Changes to the reference tables in Data/Tables/, summarized as counts
table_changes <- bind_rows(
  compare_tables(read_previous(paths$indicators), env, key = "id", table = "indicators", label = "indicator"),
  compare_tables(read_previous(paths$sources, "sources"), sources, key = "source_id", table = "sources", label = "name"),
  compare_links(read_previous(paths$sources, "by_indicator"), indicator_source, item_col = "source_id", table = "sources by indicator"),
  compare_tables(read_previous(paths$notes, "notes"), notes, key = "note_id", table = "notes", label = "description"),
  compare_links(read_previous(paths$notes, "by_indicator"), indicator_note, item_col = "note_id", table = "notes by indicator"),
  compare_tables(read_previous(paths$dimensions, "dimensions"), dimensions, key = "dim_id", table = "dimensions", label = "name"),
  compare_links(read_previous(paths$dimensions, "by_indicator"), indicator_dimension, item_col = "dim_id", table = "dimensions by indicator")
)


# ==== C. Summary ====

# Changes that could affect the cleaning code; the metadata table is only updated after confirmation
review <- list(
  "Indicators no longer in the CEPALSTAT tree (will be dropped from the metadata table)" = label_ids(removed),
  "Indicators whose main source changed" =
    glue("{source_changed$id}: {source_changed$old_source} -> {source_changed$source}"),
  "Indicators with no source in CEPALSTAT" = label_ids(no_source)
)

# Expected changes, listed for information
info <- list(
  "New indicators" = label_ids(added),
  "Renamed indicators" = glue("{renamed$id}: {renamed$old_name} -> {renamed$indicator}"),
  "Indicators listed in more than one place in the tree" = as.character(dup_ids),
  "Profile indicators not in the environmental tree" = as.character(unmatched_profile),
  "Changes to the tables in Data/Tables/" = summarize_changes(table_changes)
)

print_review_summary(review, info)


# ==== D. Save ====

## Reference tables: always updated ----
ensure_dir(here("Data/Tables"))
write_xlsx(list(indicators = env), paths$indicators)
write_xlsx(list(sources = sources, by_indicator = indicator_source), paths$sources)
write_xlsx(list(notes = notes, by_indicator = indicator_note), paths$notes)
write_xlsx(list(dimensions = dimensions, by_indicator = indicator_dimension), paths$dimensions)
message("✅ Tables saved to Data/Tables/")

## Metadata table: updated without asking only if nothing needs review ----
needs_review <- any(lengths(review) > 0)

if (needs_review) {
  beep()  # sound alert, in case you've switched windows during the download
  update_meta <- ask_yes_no("⚠️ Changes above need review. Update Data/indicator_metadata.xlsx?")
} else {
  update_meta <- TRUE
}

if (update_meta) {
  write_xlsx(list(metadata = meta_new), paths$metadata)
  meta <- meta_new  # refresh the copy loaded by utils.R for the rest of this session
  message("✅ Metadata table saved to Data/indicator_metadata.xlsx")
} else {
  message("⏸️ Data/indicator_metadata.xlsx was NOT updated. Review the changes above, then re-run ",
          "this script in RStudio and answer 'y' to update it.")
}
