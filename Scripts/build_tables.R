# Downloads reference tables for all environmental indicators from the CEPALSTAT API and saves them
# to Data/Tables/. Each file has one sheet listing the items themselves (e.g. sources) and a
# "by_indicator" sheet linking each indicator to its items:
#   indicator_table.xlsx  - indicators, with their category path in English and Spanish
#   source_table.xlsx     - sources       + by_indicator (indicator_id, source_id)
#   note_table.xlsx       - notes         + by_indicator (indicator_id, note_id)
#   dimension_table.xlsx  - dimensions    + by_indicator (indicator_id, dim_id, order, position)
#
# Requests run in parallel, so a full download takes a few minutes. Helper functions are in utils.R
# (section "CEPALSTAT bulk downloads").
#
# Note: the private API (api-cepalstat-private.cepal.org/cepalstat/api/v1/theme/22) has bulk endpoints
# for the tree, sources, footnotes and dimensions, but they only cover indicators built in CEPALSTAT
# Admin. Externally imported indicators (e.g. from the SDG database or geospatial system) are missing,
# so this script uses the public per-indicator endpoints instead.

library(here)
source(here("Scripts/utils.R"))

ensure_dir(here("Data/Tables"))


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

# Flag indicators listed in more than one place in the tree (they appear once per place)
dup_ids <- env %>% count(id) %>% filter(n > 1) %>% pull(id)
if (length(dup_ids) > 0) {
  message(glue("⚠️ Indicators listed in more than one place in the tree: {paste(dup_ids, collapse = ', ')}"))
}

inds <- unique(env$id)

write_xlsx(list(indicators = env), here("Data/Tables/indicator_table.xlsx"))


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

# Indicators with no source in CEPALSTAT, for review
no_source <- setdiff(inds, indicator_source$indicator_id)
if (length(no_source) > 0) {
  message(glue("⚠️ Indicators with no source: {paste(no_source, collapse = ', ')}"))
}

write_xlsx(list(sources = sources, by_indicator = indicator_source),
           here("Data/Tables/source_table.xlsx"))


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

write_xlsx(list(notes = notes, by_indicator = indicator_note),
           here("Data/Tables/note_table.xlsx"))


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

write_xlsx(list(dimensions = dimensions, by_indicator = indicator_dimension),
           here("Data/Tables/dimension_table.xlsx"))

message("✅ Tables saved to Data/Tables/")


## 5. Indicator metadata table ----

# store manual notes to retain
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
  select(-ends_with("_es"), -ends_with("_url"))

main_source %<>% 
  mutate(source_short = case_when(
    description == "Euroclima" ~ "Euroclima",
    TRUE ~ organization_acronym
  )) %>% rename(id = indicator_id)

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

unmatched_profile <- setdiff(profile_ids, env$id)
if (length(unmatched_profile) > 0) {
  message(glue("ℹ️ Profile indicators not in the environmental tree: {paste(unmatched_profile, collapse = ', ')}"))
}

# One row per indicator (an indicator listed twice in the tree keeps its first placement)
meta_new <- env %>%
  distinct(id, .keep_all = TRUE) %>%
  select(id, indicator, indicator_es, cat1, cat2, cat3) %>%
  left_join(main_source %>% select(id, source_id, source_short), by = "id") %>%
  left_join(ind_dims, by = "id") %>%
  mutate(
    yearbook = if_else(id %in% yearbook_ids, "Y", NA_character_),
    profile  = if_else(id %in% profile_ids, "Y", NA_character_),
    system   = case_when(
      source_short == "SDG" ~ "sdg",
      source_short == "Euroclima" ~ "geo",
      TRUE ~ "env"           # managed in this repository
    )
  ) %>% left_join(manual_notes, by = "id")

# Keep indicators from the current table that aren't in the CEPALSTAT tree yet (e.g. new indicators
# not yet published), so the cleaning code can still find them. To drop one for good, delete its row
# from Data/indicator_metadata.xlsx.
not_in_tree <- meta %>% filter(!id %in% meta_new$id)
if (nrow(not_in_tree) > 0) {
  message(glue("ℹ️ Dropped from the current table (not in the CEPALSTAT tree): {paste(not_in_tree$id, '-', not_in_tree$indicator, collapse = ', ')}"))
}

# stopped here

meta_new <- meta_new %>%
  bind_rows(not_in_tree %>% mutate(id = as.integer(id))) %>%
  select(id, indicator, cat1, cat2, cat3, source_id, source, dim_id, dimensions,
         yearbook, profile, system, notes)

# Report renamed indicators and new ones, compared with the current table
renamed <- meta_new %>%
  inner_join(meta %>% select(id, old_name = indicator), by = "id") %>%
  filter(indicator != old_name)
if (nrow(renamed) > 0) {
  message("ℹ️ Renamed indicators:\n", paste0("   ", renamed$id, ": ", renamed$old_name, " -> ", renamed$indicator, collapse = "\n"))
}

added <- setdiff(meta_new$id, meta$id)
if (length(added) > 0) message(glue("ℹ️ New indicators: {paste(added, collapse = ', ')}"))

write_xlsx(list(metadata = meta_new), here("Data/indicator_metadata.xlsx"))
meta <- meta_new  # refresh the copy loaded by utils.R for the rest of this session

message("✅ Metadata table saved to Data/indicator_metadata.xlsx")
