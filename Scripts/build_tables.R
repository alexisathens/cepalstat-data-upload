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


## 3. Notes (footnotes) ----

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