# Downloads reference tables for all environmental indicators from the CEPALSTAT API, builds the
# indicator metadata table used throughout the code, and saves them locally:
#   Data/Tables/indicator_table.xlsx  - indicators, with their category path, unit and last update date
#   Data/Tables/source_table.xlsx     - sources    + by_indicator (indicator_id, source_id)
#   Data/Tables/note_table.xlsx       - notes      + by_indicator (indicator_id, note_id)
#   Data/Tables/dimension_table.xlsx  - dimensions + by_indicator (indicator_id, dim_id, order, position)
#                                       + members (members of the indicator-specific dimensions)
#   Metadata/{id}.md                  - one snapshot per indicator of its published metadata sheet
#                                       (definition, methodology, comments), tracked in git for its history
#   Data/Tables/iso_codes.xlsx        - every known spelling of each country, mapped to its CEPALSTAT
#                                       name and ID, with ISO codes and ECLAC flags; read as `iso` by utils.R
#   Data/indicator_metadata.xlsx      - one row per indicator; read as `meta` by utils.R
#   Data/indicator_changelog.xlsx     - new rows for the changes found in this run (reasons are added by hand)
#
# The script runs in four parts:
#   A. Download and build every table in memory (nothing is saved yet)
#   B. Compare with the files currently saved
#   C. Print a summary of the changes
#   D. Save the files. The Data/Tables/ files are always updated. Data/indicator_metadata.xlsx is
#      updated only after you confirm, if any indicators are no longer in the CEPALSTAT tree (they
#      would be dropped from it), since the cleaning code depends on it.
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
  iso        = here("Data/Tables/iso_codes.xlsx"),
  snapshots  = here("Metadata"),                      # folder: one {id}.md file per indicator, next to the drafts
  metadata   = here("Data/indicator_metadata.xlsx"),
  changelog  = here("Data/indicator_changelog.xlsx")
)

# Reference files from outside CEPALSTAT, used for the metadata flags (step 6) and the country table (step 7)
external_dir <- here("Data/Tables/External")


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

# Standard dimensions shared by most indicators; their members (every country and year) are very
# large and rarely change, so they're left out of the members sheet and the metadata table
std_dims <- c("Country__ESTANDAR", "Years__ESTANDAR", "Reporting Type")

# Members of the indicator-specific dimensions (e.g. "Commercial, services, public"), to catch renamed,
# added or removed members
dim_members <- dimensions %>%
  filter(!name %in% std_dims) %>%
  pull(dim_id) %>%
  fetch_dimension_members()


## 5. Metadata sheets (unit and text) ----

# One request per indicator and language. The text fields (definition, methodology, comments) are
# converted to plain text, and are saved as one snapshot file per indicator in part D.
sheet_long <- fetch_indicator_tables(inds, endpoint = "metadata", drop = "indicator_id") %>%
  mutate(indicator_id = as.integer(indicator_id)) %>%
  select(indicator_id, lang, any_of(c("unit", "last_update", metadata_fields$field))) %>%
  mutate(across(any_of(metadata_fields$field), clean_metadata_text))

# One row per indicator: English columns unsuffixed, Spanish columns ending in _es
sheets <- sheet_long %>%
  to_wide_by_lang("indicator_id") %>%
  select(-any_of("last_update_es"))   # the same date in both languages

# Add the unit and the date CEPALSTAT last updated each indicator to the indicator table
env <- env %>%
  left_join(sheets %>% select(id = indicator_id, unit, unit_es, last_update), by = "id")


## 6. Indicator metadata table ----

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

# Indicator-specific dimensions (excluding country, year and reporting type; std_dims is set in step 4),
# in display order
ind_dims <- indicator_dimension %>%
  left_join(dimensions %>% select(dim_id, name), by = "dim_id") %>%
  filter(!name %in% std_dims) %>%
  arrange(indicator_id, position) %>%
  summarise(dim_id = paste(dim_id, collapse = "; "),
            dimensions = paste(name, collapse = "; "),
            .by = indicator_id) %>%
  rename(id = indicator_id)

# Yearbook and profile flags, from the external reference files
# (file names are set in config.R)
yearbook_ids <- read_xlsx(file.path(external_dir, yearbook_file), sheet = yearbook_sheet,
                          .name_repair = "unique_quiet") %>%
  pull(`ID INDICADOR CEPALSTAT`) %>%
  as.numeric() %>%   # non-numeric cells (e.g. section headers) become NA and are dropped below
  suppressWarnings() %>%
  na.omit()

profile_ids <- read_xlsx(file.path(external_dir, profile_file)) %>% pull(Indicator)

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

# external_ind: other CEPALSTAT indicators each indicator uses (e.g. GDP as a denominator). Found by scanning the
# process_*.R scripts: each indicator's section starts with a header like "## ---- indicator 4186 - ..."
# and ends at the next "# ----" header, and any call.data(<id>) inside it is recorded.
# Commented-out lines and an indicator's use of its own id are ignored.
scan_indicator_uses <- function(script) {
  lines <- readLines(script, warn = FALSE)
  header_rows <- which(str_detect(lines, "^#+ ---- "))   # every section header

  tibble(start = header_rows, end = lead(header_rows, default = length(lines) + 1) - 1) %>%
    mutate(id = as.integer(str_match(lines[start], "^#+ ---- indicator ([0-9]+)")[, 2])) %>%
    filter(!is.na(id)) %>%                               # keep indicator sections only
    mutate(uses = map2(start, end, function(from, to) {
      code <- lines[from:to] %>% discard(~ str_detect(.x, "^\\s*#"))
      str_match_all(code, "call\\.data\\(\\s*(?:id\\.indicator\\s*=\\s*)?([0-9]+)") %>%
        map(~ .x[, 2]) %>%
        unlist() %>%
        as.integer()
    })) %>%
    select(id, uses)
}

indicator_uses <- list.files(here("Scripts"), pattern = "^process_.*\\.R$", full.names = TRUE) %>%
  map(scan_indicator_uses) %>%
  bind_rows(tibble(id = integer(), uses = list()), .) %>%   # keeps the columns even if nothing is found
  unnest(uses) %>%
  filter(uses != id) %>%
  distinct(id, uses) %>%
  arrange(id, uses) %>%
  summarise(external_ind = paste(uses, collapse = "; "), .by = id)

meta_new <- meta_new %>%
  left_join(indicator_uses, by = "id") %>%
  left_join(sheets %>% select(id = indicator_id, unit), by = "id") %>%
  relocate(unit, external_ind, .after = dimensions)


## 7. Countries (ISO table) ----

# Maps every known spelling of each country (CEPALSTAT's English and Spanish names, the names in
# Data/Tables/External/lac_countries.xlsx and the manual entries below) to its standard CEPALSTAT name and ID,
# with ISO codes and ECLAC flags. Read as `iso` by utils.R to standardize country names.

# Spellings found in source data that match none of the names above: name in data -> CEPALSTAT name
manual_names <- tribble(
  ~name,                         ~std_name,
  "Trinidad & Tobago",           "Trinidad and Tobago",
  "Sint Maarten",                "Sint Maarten (Dutch part)",
  "Bermuda",                     "Bermudas",
  "CuraÃ§ao",                    "Curaçao",
  "Dominican Republic (the)",    "Dominican Republic",
  "Bahamas (the)",               "Bahamas",
  "Latin America and Caribbean", "Latin America and the Caribbean"
)

# Country names in the World Data ISO file that differ from CEPALSTAT's: World Data name = CEPALSTAT name.
# (World Data combines England and Scotland as the United Kingdom, so those can't be mapped.)
world_data_names <- c(
  "Bermuda"                  = "Bermudas",
  "Czechia"                  = "Czech Republic",
  "North Korea"              = "Democratic Peoples Republic of Korea (North Korea)",
  "Falkland Islands"         = "Falkland Islands (Malvinas)",
  "Guinea-Bissau"            = "Guinea Bissau",
  "Netherlands"              = "Holland",
  "South Korea"              = "Republic of Korea (South Korea)",
  "United States of America" = "United States",
  "Sint Maarten"             = "Sint Maarten (Dutch part)",
  "Virgin Islands"           = "United States Virgin Islands"
)

# ECLAC associate members (https://www.cepal.org/en/about/member-states-and-associate-members)
eclac_associates <- c("Anguilla", "Aruba", "Bermudas", "British Virgin Islands", "Cayman Islands", "Curaçao",
                      "Guadeloupe", "French Guiana", "Martinique", "Montserrat", "Puerto Rico",
                      "Sint Maarten (Dutch part)", "Turks and Caicos Islands", "United States Virgin Islands")

regional_iso3 <- c("LAA", "CAR", "SAA", "CAA")   # regional aggregates in lac_countries.xlsx

# CEPALSTAT countries (dimension 208), with English and Spanish names
cs_country <- get_dimension_table(208)
lac        <- read_excel(file.path(external_dir, "lac_countries.xlsx"))

# One row per CEPALSTAT country: standard name, ISO codes (matched by name) and flags
countries <- cs_country %>%
  select(cepalstat = id, std_name = name) %>%
  left_join(
    read_csv(file.path(external_dir, "code_iso_mp.csv"), show_col_types = FALSE, name_repair = "unique_quiet") %>%  # unnamed first column becomes ...1
      transmute(std_name = recode(`...1`, !!!world_data_names), iso2 = alpha2, iso3 = alpha3),
    by = "std_name", relationship = "many-to-many"
  ) %>%
  mutate(
    iso3   = if_else(std_name == "World", "WLD", iso3),
    in_lac = iso3 %in% lac$iso3,
    ECLAC  = if_else(in_lac & iso3 != "WLD", "Y", NA),
    ECLACa = if_else(ECLAC %in% "Y" | std_name %in% eclac_associates, "Y", NA),
    region = if_else(in_lac & iso3 %in% regional_iso3, "Y", NA),
    world  = if_else(in_lac & iso3 %in% "WLD", "Y", NA)
  ) %>%
  select(-in_lac)

# Every spelling of each country, in one list
spellings <- bind_rows(
  cs_country %>% select(cepalstat = id, name, name_es) %>% pivot_longer(-cepalstat, names_to = "type", values_to = "name"),
  lac %>%
    pivot_longer(c(english, english_short, spanish, spanish_short), names_to = "type", values_to = "name") %>%
    inner_join(countries %>% select(cepalstat, iso3), by = "iso3", relationship = "many-to-many"),
  manual_names %>% inner_join(countries %>% select(cepalstat, std_name), by = "std_name")
) %>%
  distinct(cepalstat, name)

iso_new <- spellings %>%
  left_join(countries, by = "cepalstat", relationship = "many-to-many") %>%
  select(cepalstat, name, std_name, ECLAC, ECLACa, region, world, iso2, iso3) %>%
  arrange(desc(ECLAC), std_name)


# ==== B. Compare with the saved files ====

# `meta` is the current Data/indicator_metadata.xlsx, loaded by utils.R

# Formats indicators as "id - name" for the summary, or just the id if the name isn't known.
# Names are shown in full unless a width is given (e.g. width = 45 gives "5417 - Mean sea surface
# temperature..."). Returns nothing for empty input.
label_ind <- function(id, name, width = Inf) {
  if (is.finite(width)) name <- str_trunc(name, width)
  glue("{id}{if_else(is.na(name), '', paste0(' - ', name))}")
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
duplicated <- env %>% filter(n() > 1, .by = id) %>% distinct(id, .keep_all = TRUE)

# Profile indicators missing from the tree; names are taken from the current table, where available
unmatched_profile <- tibble(id = setdiff(profile_ids, env$id)) %>%
  left_join(meta %>% select(id, indicator), by = "id")

# The saved indicator table. last_update changes with every data upload, so it's left out of the
# comparisons below.
old_indicators <- read_previous(paths$indicators)
old_indicators_compared <- if (is.null(old_indicators)) NULL else select(old_indicators, -any_of("last_update"))

# Changes to the reference tables in Data/Tables/, summarized as counts
table_changes <- bind_rows(
  compare_tables(old_indicators_compared, select(env, -last_update), key = "id", table = "indicators", label = "indicator"),
  compare_tables(read_previous(paths$sources, "sources"), sources, key = "source_id", table = "sources", label = "description"),
  compare_links(read_previous(paths$sources, "by_indicator"), indicator_source, item_col = "source_id", table = "sources by indicator"),
  compare_tables(read_previous(paths$notes, "notes"), notes, key = "note_id", table = "notes", label = "description"),
  compare_links(read_previous(paths$notes, "by_indicator"), indicator_note, item_col = "note_id", table = "notes by indicator"),
  compare_tables(read_previous(paths$dimensions, "dimensions"), dimensions, key = "dim_id", table = "dimensions", label = "name"),
  compare_links(read_previous(paths$dimensions, "by_indicator"), indicator_dimension, item_col = "dim_id", table = "dimensions by indicator")
)

# Unit changes, in English and Spanish (compared with the saved indicator table)
unit_changes <- tibble(id = integer(), indicator = character(), lang = character(), old_unit = character(), new_unit = character())
if (!is.null(old_indicators) && "unit" %in% names(old_indicators)) {
  unit_changes <- old_indicators %>%
    distinct(id, .keep_all = TRUE) %>%
    select(id, any_of(c("unit", "unit_es"))) %>%
    pivot_longer(-id, names_to = "column", values_to = "old_unit") %>%
    inner_join(
      env %>%
        distinct(id, .keep_all = TRUE) %>%
        select(id, indicator, unit, unit_es) %>%
        pivot_longer(c(unit, unit_es), names_to = "column", values_to = "new_unit"),
      by = c("id", "column")
    ) %>%
    filter(coalesce(old_unit, "") != coalesce(new_unit, "")) %>%
    filter(id %in% sheets$indicator_id) %>%   # indicators with no metadata sheet are listed separately
    mutate(lang = if_else(column == "unit", "EN", "ES"))
}

# Indicators with no metadata sheet in CEPALSTAT (no unit or text, so no snapshot)
no_sheet <- env %>%
  distinct(id, .keep_all = TRUE) %>%
  filter(!id %in% sheets$indicator_id)

# Metadata snapshots: the new text of each file, compared with the saved file (if any)
snapshot_rows <- sheets %>%
  left_join(
    env %>%
      distinct(id, .keep_all = TRUE) %>%
      select(indicator_id = id, indicator_name = indicator, indicator_name_es = indicator_es),
    by = "indicator_id"
  )

snapshots <- snapshot_rows %>%
  select(indicator_id, indicator_name) %>%
  mutate(
    path     = file.path(paths$snapshots, glue("{indicator_id}.md")),
    text     = map_chr(split(snapshot_rows, seq_len(nrow(snapshot_rows))), format_metadata_snapshot),
    is_new   = !file.exists(path),
    old_text = map_chr(path, function(f) if (file.exists(f)) read_file(f) else NA_character_),
    changed  = !is_new & old_text != text
  )

# Snapshots of indicators that no longer have metadata in CEPALSTAT (their files are deleted in part D)
removed_snapshots <- list.files(paths$snapshots, pattern = "^[0-9]+\\.md$", full.names = TRUE) %>%
  discard(~ .x %in% snapshots$path)

# Text changes in the six sections of each changed snapshot, sized by describe_text_change()
text_changes <- tibble(indicator_id = integer(), indicator_name = character(), header = character(), change = character())
if (any(snapshots$changed)) {
  text_changes <- snapshots %>%
    filter(changed) %>%
    mutate(sections = map2(old_text, text, function(old, new) {
      inner_join(parse_metadata_snapshot(old), parse_metadata_snapshot(new), by = "header", suffix = c("_old", "_new"))
    })) %>%
    select(indicator_id, indicator_name, sections) %>%
    unnest(sections) %>%
    filter(str_squish(text_old) != str_squish(text_new)) %>%
    mutate(change = map2_chr(text_old, text_new, describe_text_change))
}

# Dimension member changes (compared with the saved members sheet): one row per member added, removed
# or renamed, with its English and Spanish names together (e.g. "Methane (CH4); Metano (CH4)").
# Members of a dimension that is new or no longer used aren't listed one by one; those dimensions
# appear in the table changes instead.
member_changes <- tibble(dim_id = integer(), dim_name = character(), change = character(),
                         old_name = character(), new_name = character(), line = character())
old_members <- read_previous(paths$dimensions, "members")
if (!is.null(old_members)) {
  both_names  <- function(en, es) paste(coalesce(en, ""), coalesce(es, ""), sep = "; ")
  old_members <- old_members %>% mutate(across(c(dim_id, member_id), as.integer), names = both_names(name, name_es))
  new_members <- dim_members %>% mutate(names = both_names(name, name_es))

  members_added <- anti_join(new_members, old_members, by = c("dim_id", "member_id")) %>%
    filter(dim_id %in% old_members$dim_id) %>%
    transmute(dim_id, change = "added", old_name = NA_character_, new_name = names)
  members_removed <- anti_join(old_members, new_members, by = c("dim_id", "member_id")) %>%
    filter(dim_id %in% new_members$dim_id) %>%
    transmute(dim_id, change = "removed", old_name = names, new_name = NA_character_)
  members_renamed <- inner_join(old_members, new_members, by = c("dim_id", "member_id"), suffix = c("_old", "")) %>%
    filter(names_old != names) %>%
    transmute(dim_id, change = "renamed", old_name = names_old, new_name = names)

  member_changes <- bind_rows(members_added, members_removed, members_renamed) %>%
    left_join(dimensions %>% select(dim_id, dim_name = name), by = "dim_id") %>%
    mutate(line = case_when(
      change == "added"   ~ glue('Dimension {dim_id} ({dim_name}): added member "{new_name}"'),
      change == "removed" ~ glue('Dimension {dim_id} ({dim_name}): removed member "{old_name}"'),
      TRUE                ~ glue('Dimension {dim_id} ({dim_name}): renamed member "{old_name}" -> "{new_name}"')
    ))
}

# ---- Changelog rows (added to Data/indicator_changelog.xlsx in part D) ----

# Logged: indicators created or deprecated, and changes to an indicator's name, category, unit, sources,
# footnotes or dimensions, and to dimension members. Not logged: metadata text (its history is in the
# Metadata/{id}.md snapshots in git), latest years of data, and the text of sources or footnotes.
# The comparisons below only cover indicators in both the saved and the new indicator table; indicators
# that are new or no longer in the tree are logged as created or deprecated instead.

new_ind <- env %>% distinct(id, .keep_all = TRUE)
old_ind <- if (is.null(old_indicators)) new_ind else distinct(old_indicators, id, .keep_all = TRUE)
both_ids <- intersect(old_ind$id, new_ind$id)

# Compares columns of the saved and new indicator tables, e.g. the English and Spanish names. Their
# values are combined into one, separated by "; " (e.g. "tonnes; toneladas"), so a change in either
# language gives one row per indicator. Skipped if an older saved table lacks one of the columns.
compare_indicator_columns <- function(columns, change_type) {
  if (!all(columns %in% names(old_ind))) return(tibble())
  combine <- function(df) {
    df %>%
      filter(id %in% both_ids) %>%
      select(id, all_of(columns)) %>%
      mutate(across(-id, ~ coalesce(as.character(.x), ""))) %>%
      unite("value", all_of(columns), sep = "; ")
  }
  inner_join(combine(old_ind), combine(new_ind), by = "id", suffix = c("_old", "_new")) %>%
    filter(value_old != value_new) %>%
    transmute(indicator_id = id, change_type = change_type, old_value = value_old, new_value = value_new)
}

# Category: the indicator's place in the thematic tree, e.g. "Environmental conditions > Water", in
# English (category) and Spanish (category_es)
add_category_path <- function(df) {
  for (col in setdiff(c("cat1", "cat2", "cat3", "cat1_es", "cat2_es", "cat3_es"), names(df))) df[[col]] <- NA_character_
  path <- function(...) paste(na.omit(c(...)), collapse = " > ")
  df %>% mutate(category    = pmap_chr(list(cat1, cat2, cat3), path),
                category_es = pmap_chr(list(cat1_es, cat2_es, cat3_es), path))
}
old_ind <- add_category_path(old_ind)
new_ind <- add_category_path(new_ind)

# Sources, footnotes or dimensions linked to each indicator (one row per link added or removed)
compare_indicator_links <- function(old_links, new_links, item_col, labels, change_type) {
  if (is.null(old_links)) return(tibble())
  keep <- function(df) df %>% filter(indicator_id %in% both_ids) %>% distinct(indicator_id, .data[[item_col]]) %>%
    mutate(across(everything(), as.integer))
  old_links <- keep(old_links)
  new_links <- keep(new_links)
  bind_rows(
    anti_join(old_links, new_links, by = c("indicator_id", item_col)) %>% mutate(side = "old"),
    anti_join(new_links, old_links, by = c("indicator_id", item_col)) %>% mutate(side = "new")
  ) %>%
    left_join(labels, by = item_col) %>%
    transmute(indicator_id, change_type = change_type,
              old_value = if_else(side == "old", label, NA_character_),
              new_value = if_else(side == "new", label, NA_character_))
}

old_sources <- read_previous(paths$sources, "sources")
source_labels <- bind_rows(sources, old_sources) %>%
  mutate(source_id = as.integer(source_id)) %>%
  distinct(source_id, .keep_all = TRUE) %>%
  transmute(source_id, label = as.character(glue("{source_id} - {organization_acronym}: {description}")))

note_labels <- bind_rows(notes, read_previous(paths$notes, "notes")) %>%
  mutate(note_id = as.integer(note_id)) %>%
  distinct(note_id, .keep_all = TRUE) %>%
  transmute(note_id, label = as.character(glue("{note_id} - {str_trunc(description, 80)}")))

dim_labels <- bind_rows(dimensions, read_previous(paths$dimensions, "dimensions")) %>%
  mutate(dim_id = as.integer(dim_id)) %>%
  distinct(dim_id, .keep_all = TRUE) %>%
  transmute(dim_id, label = as.character(glue("{dim_id} - {name}")))

# Source changes: one row per indicator whose set of sources changed (not the text of a source)
source_rows <- tibble()
old_source_links <- read_previous(paths$sources, "by_indicator")
if (!is.null(old_source_links)) {
  source_sets <- function(links) links %>%
    mutate(across(c(indicator_id, source_id), as.integer)) %>%
    filter(indicator_id %in% both_ids) %>%
    left_join(source_labels, by = "source_id") %>%
    arrange(indicator_id, source_id) %>%
    summarise(ids = paste(source_id, collapse = ";"), value = paste(label, collapse = "; "), .by = indicator_id)
  source_rows <- inner_join(source_sets(old_source_links), source_sets(indicator_source),
                            by = "indicator_id", suffix = c("_old", "_new")) %>%
    filter(ids_old != ids_new) %>%
    transmute(indicator_id, change_type = "source", old_value = value_old, new_value = value_new)
}

# All changes that are logged on every run (starting from an empty table, so the columns always exist)
changelog_rows <- bind_rows(
  tibble(indicator_id = integer(), dimension_id = integer(), affected_indicators = character(),
         change_type = character(), old_value = character(), new_value = character()),
  compare_indicator_columns(c("indicator", "indicator_es"), "name"),
  compare_indicator_columns(c("category", "category_es"), "category"),
  compare_indicator_columns(c("unit", "unit_es"), "unit") %>%
    filter(indicator_id %in% sheets$indicator_id),   # indicators with no metadata sheet have no unit to compare
  source_rows,
  compare_indicator_links(read_previous(paths$notes, "by_indicator"), indicator_note, "note_id", note_labels, "footnote"),
  compare_indicator_links(read_previous(paths$dimensions, "by_indicator"), indicator_dimension, "dim_id", dim_labels, "dimension"),
  member_changes %>%
    left_join(indicator_dimension %>% summarise(affected_indicators = paste(sort(unique(indicator_id)), collapse = "; "),
                                                .by = dim_id), by = "dim_id") %>%
    transmute(dimension_id = dim_id, affected_indicators, change_type = "dimension_member", old_value = old_name, new_value = new_name)
) %>%
  left_join(new_ind %>% select(indicator_id = id, indicator_name = indicator), by = "indicator_id")

# Indicators created or deprecated: logged only when the metadata table is updated (part D), since a
# missing indicator is only dropped from it after confirmation
changelog_rows_created <- bind_rows(
  added   %>% transmute(indicator_id = id, indicator_name = indicator, change_type = "created",    new_value = indicator),
  removed %>% transmute(indicator_id = id, indicator_name = indicator, change_type = "deprecated", old_value = indicator)
)

# Country (ISO) table: compared with the saved version. Changes are rare, so each one is listed in full.
old_iso <- (read_previous(paths$iso) %||% iso_new) %>%
  filter(!is.na(cepalstat))
old_countries <- old_iso %>% select(any_of(names(countries))) %>% distinct()
old_spellings <- old_iso %>% distinct(cepalstat, name)

country_names <- bind_rows(countries, old_countries) %>%
  distinct(cepalstat, .keep_all = TRUE) %>%
  select(cepalstat, country = std_name)

country_changes <- compare_tables(old_countries, countries, key = "cepalstat", table = "countries") %>%
  mutate(cepalstat = as.numeric(id)) %>%
  left_join(country_names, by = "cepalstat") %>%
  mutate(line = if_else(
    change == "changed",
    glue("{cepalstat} - {country}: {field} {coalesce(old_value, '(blank)')} -> {coalesce(new_value, '(blank)')}"),
    glue("{cepalstat} - {country}: country {change}")
  ))

spelling_changes <- bind_rows(
  anti_join(spellings, old_spellings, by = c("cepalstat", "name")) %>% mutate(change = "added"),
  anti_join(old_spellings, spellings, by = c("cepalstat", "name")) %>% mutate(change = "removed")
) %>%
  filter(!cepalstat %in% country_changes$cepalstat[country_changes$change != "changed"]) %>%  # whole country already listed
  left_join(country_names, by = "cepalstat") %>%
  mutate(line = glue("{cepalstat} - {country}: {change} spelling \"{name}\""))

# Names in the reference files that don't match any CEPALSTAT country (left out of the table)
unmatched_names <- c(
  lac %>% filter(!iso3 %in% countries$iso3) %>% glue_data("{iso3} - {english} (lac_countries.xlsx)"),
  manual_names %>% filter(!std_name %in% countries$std_name) %>% glue_data("\"{name}\" -> \"{std_name}\" (manual entry)")
)


# ==== C. Summary ====

# Missing indicators; the metadata table is only updated after confirmation
review <- list(
  "Indicators no longer in the CEPALSTAT tree (will be dropped from the metadata table)" =
    label_ind(removed$id, removed$indicator)
)

# Other changes, listed for information
info <- list(
  "New indicators" = label_ind(added$id, added$indicator),
  "Renamed indicators" = glue("{label_ind(renamed$id, renamed$old_name)} -> {renamed$indicator}"),
  "Indicators with no source in CEPALSTAT" = label_ind(no_source$id, no_source$indicator),
  "Indicators whose main source changed" =
    glue("{label_ind(source_changed$id, source_changed$indicator, width = 45)}: {source_changed$old_source} -> {source_changed$source}"),
  "Indicators listed in more than one place in the tree" = label_ind(duplicated$id, duplicated$indicator),
  "Profile indicators not in the environmental tree" = label_ind(unmatched_profile$id, unmatched_profile$indicator),
  "Unit changes" =
    glue('{label_ind(unit_changes$id, unit_changes$indicator, width = 45)}: unit ({unit_changes$lang}) "{unit_changes$old_unit}" -> "{unit_changes$new_unit}"'),
  "Indicators with no metadata sheet in CEPALSTAT (no snapshot)" = label_ind(no_sheet$id, no_sheet$indicator),
  "Metadata text changes" =
    glue("{label_ind(text_changes$indicator_id, text_changes$indicator_name, width = 45)}: {text_changes$header}, {text_changes$change}"),
  "Dimension member changes" = member_changes$line,
  "Changes to the tables in Data/Tables/" = summarize_changes(table_changes),
  "Changes to the country (ISO) table" = c(country_changes$line, spelling_changes$line),
  "Country names that don't match a CEPALSTAT country (left out of the ISO table)" = unmatched_names
)

print_review_summary(review, info)


# ==== D. Save ====

## Reference tables: always updated ----
ensure_dir(here("Data/Tables"))
write_xlsx(list(indicators = env), paths$indicators)
write_xlsx(list(sources = sources, by_indicator = indicator_source), paths$sources)
write_xlsx(list(notes = notes, by_indicator = indicator_note), paths$notes)
write_xlsx(list(dimensions = dimensions, by_indicator = indicator_dimension, members = dim_members), paths$dimensions)
write_xlsx(list(iso = iso_new), paths$iso)
message("✅ Tables saved to Data/Tables/")

## Metadata snapshots: one file per indicator, always rewritten (git only shows files whose text changed) ----
ensure_dir(paths$snapshots)
walk2(snapshots$text, snapshots$path, function(text, path) write_file(text, path))
file.remove(removed_snapshots)

n_new     <- sum(snapshots$is_new)
n_changed <- sum(snapshots$changed)
n_removed <- length(removed_snapshots)
if (n_new + n_changed + n_removed > 0) {
  message(glue("📝 Metadata snapshots: {n_new} new, {n_changed} changed, {n_removed} removed. ",
               "Commit the {{id}}.md files in Metadata/ to keep the history of these changes."))
} else {
  message("✅ Metadata snapshots unchanged")
}

## Metadata table: updated without asking unless indicators are missing ----
needs_review <- any(lengths(review) > 0)

if (needs_review) {
  beep()  # sound alert, in case you've switched windows during the download
  update_meta <- ask_yes_no("⚠️ Indicators above are missing from CEPALSTAT. Update Data/indicator_metadata.xlsx?")
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

## Changelog: add the changes found in this run ----
new_log_rows <- bind_rows(changelog_rows, if (update_meta) changelog_rows_created)
# Which indicators use each dimension, to fill in affected_indicators (e.g. for rows added by hand)
dim_usage <- indicator_dimension %>%
  summarise(affected_indicators = paste(sort(unique(indicator_id)), collapse = "; "), .by = dim_id)
n_logged <- append_changelog(new_log_rows, paths$changelog, affected_lookup = dim_usage)
if (n_logged > 0) {
  message(glue("📝 {n_logged} new row(s) added to Data/indicator_changelog.xlsx. Add a reason where useful."))
} else {
  message("✅ No new changes for the changelog")
}
