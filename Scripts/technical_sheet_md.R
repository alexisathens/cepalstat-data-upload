# Generates draft metadata for CEPALSTAT indicators as Markdown (.md) files.
# Markdown version of technical_sheet.R: same three steps and prompts, but all files are .md
# (for use in a Markdown editor such as Obsidian) and are written to a single folder.
#
# Workflow (see run_meta.R):
#   1. suggest_metadata_en()   - archives published metadata ({id}_legacy.md) and drafts English ({id}_en.md)
#      -> human review: edit {id}_en.md
#   2. translate_metadata_es() - translates the reviewed English draft ({id}_es.md)
#   3. export_metadata_admin() - assembles both languages for copying into CEPALSTAT Admin ({id}_admin.md)
#
# Re-running steps 1 or 2 does NOT overwrite an existing draft, so reviewed edits are never lost.
# To regenerate a draft on purpose, pass overwrite = TRUE. Step 3 always overwrites, since the
# admin file is generated output and is not edited by hand.
#
# The system prompts and the translation glossary are kept as editable files in Metadata/Prompts/:
#   draft_en.md     - instructions for drafting the English metadata (step 1)
#   translate_es.md - instructions for translating into Spanish (step 2)
#   glossary.md     - required English to Spanish terms, added to the translation prompt (step 2)
#
# New indicators have no published metadata to revise. They are detected automatically (no published
# Definition, Methodology or Comments in CEPALSTAT), and the draft is instead written from the
# indicator's entry in Data/indicator_metadata.xlsx (name, source, dimensions) by adapting the content
# of the gold standard indicator(s). For these, choose a closely related published indicator (e.g.,
# same data source) as the gold standard. To override the detection, pass new_indicator = TRUE or FALSE.

# Development note: I experimented with attaching relevant UN and domain-specific methodological documents
# and pointing to the indicator's R code. In the end I found the greatest success with the least amount
# of token usage by only feeding Claude the current indicator metadata along with a "golden example" of a
# similar CEPALSTAT indicator.

# Requires utils.R to be sourced first (packages, `meta` table and get_indicator_metadata()).

# ---- settings ----

METADATA_DIR    <- here::here("Metadata")           # all .md files for every indicator are written here
PROMPTS_DIR     <- file.path(METADATA_DIR, "Prompts") # system prompts and glossary
ANTHROPIC_MODEL <- "claude-sonnet-4-6"              # model used for both drafting and translation


# ---- shared helpers ----

# File path for one of an indicator's metadata files.
# type is one of "legacy", "en", "es" or "admin". Sample usage: md_path(2487, "en") -> Metadata/2487_en.md
md_path <- function(indicator_id, type) {
  file.path(METADATA_DIR, glue("{indicator_id}_{type}.md"))
}

# TRUE if a file exists and has content. An empty file (e.g. one created by clicking an unresolved
# link in Obsidian) is treated as missing so it does not block the overwrite check.
has_content <- function(path) {
  file.exists(path) && file.size(path) > 0
}

# Strips any <br>/<br/>/<br /> tags that leaked into CEPALSTAT's published metadata (e.g. from a
# prior admin export), so the model never sees HTML in its reference material and the admin
# export's own <br> tags never compound on top of pre-existing ones.
strip_html_breaks <- function(text) {
  str_remove_all(text, "<br\\s*/?>")
}

# Returns one field from a get_indicator_metadata() table, or "" if the field is missing or empty.
# Sample usage: get_field(m, "definition")
get_field <- function(m, variable) {
  value <- m$value[m$variable == variable]
  if (length(value) == 0 || is.na(value[1])) return("")
  strip_html_breaks(value[1])
}

# Indicator name from Data/indicator_metadata.xlsx, falling back to the CEPALSTAT API.
get_indicator_name <- function(indicator_id) {
  name <- meta %>% filter(id == indicator_id) %>% pull(indicator)
  if (length(name) == 1) return(name)
  get_indicator_metadata(indicator_id) %>% get_field("indicator_name")
}

# Formats an indicator's published fields using the same ### DEFINITION / ### METHODOLOGY /
# ### COMMENTS headers required of the model's output.
format_published_fields <- function(indicator_id, lang = "en") {
  m <- get_indicator_metadata(indicator_id, lang = lang)
  paste0(
    "### DEFINITION\n",  get_field(m, "definition"), "\n\n",
    "### METHODOLOGY\n", get_field(m, "calculation_methodology"), "\n\n",
    "### COMMENTS\n",    get_field(m, "comments"), "\n"
  )
}

# Formats gold standard indicators as labelled example blocks for the system prompt.
get_formatted_metadata <- function(example_ids, lang = "en") {
  example_ids %>%
    map_chr(function(id) {
      name <- get_indicator_metadata(id, lang = lang) %>% get_field("indicator_name")
      paste0("--- indicator ", id, ": ", name, " ---\n\n", format_published_fields(id, lang))
    }) %>%
    paste(collapse = "\n\n")
}

# Lists an indicator's published metadata as "variable: value" lines for the user prompt.
format_existing_metadata <- function(indicator_id, lang = "en") {
  get_indicator_metadata(indicator_id, lang = lang) %>%
    mutate(value = strip_html_breaks(value)) %>%
    mutate(line = paste0(variable, ": ", value)) %>%
    pull(line) %>%
    paste(collapse = "\n")
}

# Builds the YAML front matter (Obsidian "Properties") block at the top of each file.
# Links in `related` point only to files that already exist, so Obsidian never shows broken links.
make_front_matter <- function(indicator_id, related_types, extra = list()) {
  name <- str_replace_all(get_indicator_name(indicator_id), '"', '\\\\"') # escape quotes for YAML

  fields <- c(
    glue("indicator_id: {indicator_id}"),
    glue('indicator_name: "{name}"'),
    imap_chr(extra, ~ glue("{.y}: {.x}"))
  )

  related <- related_types %>%
    keep(~ has_content(md_path(indicator_id, .x))) %>%
    map_chr(~ glue('  - "[[{indicator_id}_{.x}]]"'))
  if (length(related) > 0) fields <- c(fields, "related:", related)

  paste(c("---", fields, "---", ""), collapse = "\n")
}

# Writes a .md file (front matter + body) as UTF-8, creating the metadata folder if needed.
write_md <- function(front_matter, body, path) {
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  write_file(paste0(front_matter, "\n", str_trim(body), "\n"), path)
}

# Reads a .md file and returns its body without the front matter.
read_md_body <- function(path) {
  read_file(path) %>%
    str_remove_all("\r") %>%                       # normalize Windows line endings
    str_remove("^---\\n[\\s\\S]*?\\n---\\n") %>%   # drop the front matter block
    str_trim()
}

# Splits a draft into its three sections (### DEFINITION / ### METHODOLOGY / ### COMMENTS).
parse_fields <- function(text, source_label) {
  sections <- str_split(text, "(?=### (DEFINITION|METHODOLOGY|COMMENTS))")[[1]] %>%
    discard(~ !nzchar(trimws(.x)))

  field_map <- c(DEFINITION = "definition", METHODOLOGY = "methodology", COMMENTS = "comments")
  out <- list(definition = NA_character_, methodology = NA_character_, comments = NA_character_)

  for (sec in sections) {
    header <- str_match(sec, "^### (\\w+)")[, 2]
    body   <- str_remove(sec, "^### \\w+") %>% str_trim()
    if (header %in% names(field_map)) out[[field_map[[header]]]] <- body
  }

  missing <- names(out)[map_lgl(out, is.na)]
  if (length(missing) > 0) {
    stop(glue(
      "Could not find section(s) in {source_label} draft: {paste(missing, collapse = ', ')}. ",
      "Expected headers: ### DEFINITION / ### METHODOLOGY / ### COMMENTS"
    ))
  }

  out
}

# Reads a system prompt from Metadata/Prompts/ (without its front matter).
# Sample usage: read_prompt("draft_en")
read_prompt <- function(name) {
  path <- file.path(PROMPTS_DIR, glue("{name}.md"))
  assert_that(file.exists(path), msg = glue("Prompt file not found: {path}"))
  read_md_body(path)
}

# Reads the glossary table in Metadata/Prompts/glossary.md into a tibble (english, spanish, notes).
# Only rows of the Markdown table are read; any other text in the file is ignored.
read_glossary <- function() {
  path <- file.path(PROMPTS_DIR, "glossary.md")
  if (!file.exists(path)) return(tibble(english = character(), spanish = character(), notes = character()))

  cells <- read_md_body(path) %>%
    str_split("\n") %>%
    pluck(1) %>%
    str_trim() %>%
    keep(~ str_starts(.x, fixed("|"))) %>%              # table rows only
    discard(~ str_detect(.x, "^\\|[\\s|:-]+\\|$")) %>%  # drop the |---|---| separator line
    str_remove_all("^\\||\\|$") %>%                     # drop the outer pipes
    str_split_fixed(fixed("|"), 3)                      # one column per cell

  tibble(english = cells[, 1], spanish = cells[, 2], notes = cells[, 3]) %>%
    mutate(across(everything(), str_trim)) %>%
    slice(-1) %>%                                       # drop the header row
    filter(english != "", spanish != "")
}

# Formats the glossary as a list for the translation prompt, e.g.
# "- LULUCF -> LULUCF (Keep the English acronym. Do not use UTCUTS.)"
format_glossary <- function(glossary) {
  if (nrow(glossary) == 0) return("")
  lines <- glossary %>%
    mutate(line = if_else(
      notes == "",
      glue("- {english} -> {spanish}"),
      glue("- {english} -> {spanish} ({notes})")
    )) %>%
    pull(line)
  paste0("GLOSSARY (required terminology, English -> Spanish):\n", paste(lines, collapse = "\n"))
}

# Lists glossary terms that appear in the English text but whose Spanish term is missing from the
# translation, so they can be checked by hand. Matching ignores case and uses whole words.
check_glossary <- function(english_text, spanish_text, glossary) {
  has_term <- function(text, term) str_detect(text, regex(paste0("\\b", str_escape(term), "\\b"), ignore_case = TRUE))

  glossary %>%
    filter(map_lgl(english, ~ has_term(english_text, .x))) %>%
    filter(!map_lgl(spanish, ~ has_term(spanish_text, .x)))
}

# TRUE if the indicator has no published Definition, Methodology or Comments in CEPALSTAT, i.e. it is a
# new indicator. An indicator that CEPALSTAT does not recognize yet (HTTP 404) also counts as new.
# Any other API error stops the script, so a connection problem is never mistaken for a new indicator.
# Sample usage: is_new_indicator(5808)
is_new_indicator <- function(indicator_id) {
  url <- glue("https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{indicator_id}/metadata?lang=en&format=json")
  result <- tryCatch(fetch_cepalstat_json(url), httr2_http_404 = function(e) NULL)
  if (is.null(result)) return(TRUE)

  # A field counts as empty if it is missing, null or only whitespace/<br> tags
  is_blank <- function(x) {
    is.null(x) || length(x) == 0 || is.na(x[1]) || str_trim(strip_html_breaks(as.character(x[1]))) == ""
  }

  metadata <- result$body$metadata
  c("definition", "calculation_methodology", "comments") %>%
    map_lgl(~ is_blank(metadata[[.x]])) %>%
    all()
}

# Uses new_indicator if it was given; otherwise detects it and reports when an indicator is treated as new.
resolve_new_indicator <- function(indicator_id, new_indicator) {
  if (!is.null(new_indicator)) return(new_indicator)
  detected <- is_new_indicator(indicator_id)
  if (detected) message(glue("🆕 No published metadata found for {indicator_id}: drafting as a new indicator."))
  detected
}

# Calls the Anthropic API and returns the response text, starting at the first ### DEFINITION header.
call_claude <- function(system_prompt, user_prompt) {
  api_key <- Sys.getenv("ANTHROPIC_API_KEY")
  assert_that(nchar(api_key) > 0, msg = "ANTHROPIC_API_KEY not found. Please add it to your .Renviron file.")

  response <- request("https://api.anthropic.com/v1/messages") |>
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01",
      "content-type"      = "application/json"
    ) |>
    req_body_json(list(
      model       = ANTHROPIC_MODEL,
      max_tokens  = 4096,
      temperature = 0, # makes model more deterministic and reproducible
      system      = trimws(system_prompt),
      messages    = list(list(role = "user", content = user_prompt))
    )) |>
    req_timeout(180) |>
    req_retry(max_tries = 4, is_transient = \(r) resp_status(r) %in% c(429, 529),
              backoff = \(i) 30) |>
    req_error(body = \(r) resp_body_string(r)) |>
    req_perform()

  resp_body_json(response)$content[[1]]$text %>%
    str_remove("^[\\s\\S]*?(?=### DEFINITION)") %>% # strip any stray preamble before the first header
    strip_html_breaks()                             # strip any <br> echoed from reference material
}


# ---- step 1: English draft ----

suggest_metadata_en <- function(indicator_id, gold_standard_indicators = c(2487),
                                new_indicator = NULL, overwrite = FALSE) {
  # new_indicator = NULL (default) detects new indicators automatically; TRUE or FALSE overrides it.

  ## skip if a draft already exists, to protect reviewed edits
  draft_path <- md_path(indicator_id, "en")
  if (has_content(draft_path) && !overwrite) {
    message(glue("⏭️ Skipped {indicator_id}: {basename(draft_path)} already exists (use overwrite = TRUE to regenerate)"))
    return(invisible(NULL))
  }

  message(glue("Processing metadata for indicator {indicator_id}..."))
  new_indicator <- resolve_new_indicator(indicator_id, new_indicator)

  ## system prompt
  # Edit Metadata/Prompts/draft_en.md and the user prompts below to refine what the model generates.

  system_prompt <- paste(
    read_prompt("draft_en"),
    "The following are examples of high-quality CEPALSTAT metadata to use as a reference for style, structure, and level of detail:\n\n",
    get_formatted_metadata(gold_standard_indicators),
    sep = "\n\n"
  )

  ## user prompt

  if (!new_indicator) {
    # Existing indicator: revise its published metadata
    user_prompt <- paste0(
      "CURRENT METADATA FOR THIS INDICATOR (indicator_id ", indicator_id, "):\n",
      format_existing_metadata(indicator_id),
      "\n\nPlease revise the metadata fields (definition, calculation_methodology, comments) ",
      "based on the available inputs. Keep other metadata elements exactly as-is.",
      "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
      "— no introductory sentence, no preamble, no markdown bold headers, no closing remarks. ",
      "Respond with only the three ### sections, nothing else."
    )

  } else {
    # New indicator: no published metadata yet, so draft from its entry in the metadata table
    # and adapt the content of the closely related gold standard indicator(s)
    meta_row <- meta %>% filter(id == indicator_id)
    assert_that(
      nrow(meta_row) == 1,
      msg = glue("Indicator {indicator_id} not found in Data/indicator_metadata.xlsx. Add it before drafting metadata.")
    )

    user_prompt <- paste0(
      "NEW INDICATOR (no published metadata yet):\n",
      "indicator_id: ", indicator_id, "\n",
      "indicator_name: ", meta_row$indicator, "\n",
      "source: ", meta_row$source, "\n",
      "dimensions: ", coalesce(meta_row$dimensions, "none beyond country and year"), "\n", # blank = country x year only
      "classification: ", paste(na.omit(c(meta_row$cat1, meta_row$cat2, meta_row$cat3)), collapse = " > "),
      "\n\nThe reference indicator(s) above are closely related to this new indicator and share its data ",
      "source and general context. Draft the metadata fields (definition, calculation_methodology, comments) ",
      "for the new indicator by adapting their content: keep what applies (source description, coverage, ",
      "context, links) and change what differs, as indicated by the new indicator's name and dimensions. ",
      "Do not carry over details that are specific to the reference indicator(s).",
      "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
      "— no introductory sentence, no preamble, no markdown bold headers, no closing remarks. ",
      "Respond with only the three ### sections, nothing else."
    )
  }

  ## archive published (pre-AI) metadata, once per indicator (existing indicators only)
  legacy_path <- md_path(indicator_id, "legacy")
  if (!new_indicator && !has_content(legacy_path)) {
    legacy_body <- paste0(
      "> Snapshot of the published CEPALSTAT metadata before any AI-assisted revision. Do not edit.\n\n",
      "## English\n\n", format_published_fields(indicator_id, "en"), "\n",
      "## Spanish\n\n", format_published_fields(indicator_id, "es")
    )
    front_matter <- make_front_matter(indicator_id, related_types = character(),
                                      extra = list(retrieved = format(Sys.Date())))
    write_md(front_matter, legacy_body, legacy_path)
  }

  ## generate English draft
  # Review and edit Metadata/{indicator_id}_en.md before translating.
  english_text <- call_claude(system_prompt, user_prompt)

  front_matter <- make_front_matter(
    indicator_id,
    related_types = "legacy",
    extra = list(
      gold_standard = paste0("[", paste(gold_standard_indicators, collapse = ", "), "]"),
      generated     = format(Sys.Date())
    )
  )
  write_md(front_matter, english_text, draft_path)

  message(glue("✅ Exported (en) metadata for {indicator_id}: {basename(draft_path)}"))
  invisible(english_text)
}


# ---- step 2: Spanish translation ----

translate_metadata_es <- function(indicator_id, gold_standard_indicators = c(2487),
                                  new_indicator = NULL, overwrite = FALSE) {
  # Reads the reviewed English draft and translates it to Spanish, using this indicator's own
  # existing Spanish metadata as a terminology reference.
  # new_indicator = NULL (default) detects new indicators automatically; TRUE or FALSE overrides it.

  ## check inputs, and skip if a translation already exists
  en_path <- md_path(indicator_id, "en")
  es_path <- md_path(indicator_id, "es")

  assert_that(
    has_content(en_path),
    msg = glue("English draft not found: {en_path}\nRun suggest_metadata_en() first.")
  )

  if (has_content(es_path) && !overwrite) {
    message(glue("⏭️ Skipped {indicator_id}: {basename(es_path)} already exists (use overwrite = TRUE to re-translate)"))
    return(invisible(NULL))
  }

  message(glue("Processing metadata translation for indicator {indicator_id}..."))
  new_indicator <- resolve_new_indicator(indicator_id, new_indicator)

  ## system prompt
  # Edit Metadata/Prompts/translate_es.md and glossary.md to refine the translation.

  glossary <- read_glossary()

  system_prompt <- paste(
    read_prompt("translate_es"),
    format_glossary(glossary),
    "The following are examples of high-quality CEPALSTAT metadata in Spanish to use as a reference for style, structure, and terminology:\n\n",
    get_formatted_metadata(gold_standard_indicators, lang = "es"),
    sep = "\n\n"
  )

  ## user prompt (reviewed English draft, without its front matter)

  english_text <- read_md_body(en_path)

  if (!new_indicator) {
    # Existing indicator: ground terminology in its own published Spanish metadata
    user_prompt <- paste0(
      "ENGLISH TEXT TO TRANSLATE:\n", english_text,
      "\n\nEXISTING SPANISH METADATA FOR THIS INDICATOR (written by a human, not authoritative — prefer ",
      "its phrasing unless the glossary or a more internationally accepted Spanish term says otherwise):\n",
      format_existing_metadata(indicator_id, lang = "es"),
      "\n\nTranslate the English text above into Spanish, following the glossary and terminology guidance above.",
      "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
      "— no introductory sentence, no preamble, no closing remarks. Respond with only the three ### ",
      "sections (headers left untranslated), nothing else."
    )

  } else {
    # New indicator: no existing Spanish metadata, so rely on the Spanish reference examples
    user_prompt <- paste0(
      "ENGLISH TEXT TO TRANSLATE:\n", english_text,
      "\n\nThis is a new indicator with no existing Spanish metadata. Always use the glossary terms above. ",
      "Otherwise, use the Spanish reference examples for terminology, preferring internationally (UN) ",
      "accepted Spanish terms where they differ.",
      "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
      "— no introductory sentence, no preamble, no closing remarks. Respond with only the three ### ",
      "sections (headers left untranslated), nothing else."
    )
  }

  ## generate Spanish translation
  spanish_text <- call_claude(system_prompt, user_prompt)

  ## flag any glossary terms that were not followed, for manual review
  missed <- check_glossary(english_text, spanish_text, glossary)
  if (nrow(missed) > 0) {
    message(glue("⚠️ Glossary check for {indicator_id}: expected Spanish term(s) not found. Please review {basename(es_path)}:"))
    message(paste0("   ", missed$english, " -> ", missed$spanish, collapse = "\n"))
  }

  front_matter <- make_front_matter(
    indicator_id,
    related_types = c("legacy", "en"),
    extra = list(generated = format(Sys.Date()))
  )
  write_md(front_matter, spanish_text, es_path)

  message(glue("✅ Exported (es) metadata for {indicator_id}: {basename(es_path)}"))
  invisible(spanish_text)
}


# ---- step 3: CEPALSTAT Admin export ----

export_metadata_admin <- function(indicator_id) {
  # Assembles the English and Spanish drafts into the order used in CEPALSTAT Admin. Each field
  # is placed in a code block so Obsidian shows the <br> tags as-is and adds a copy button.
  # Always overwrites, since this file is generated output and is not edited by hand.

  ## functions

  to_html_breaks <- function(text) {
    # Adds <br>/<br><br> tags for CEPALSTAT's rich text field, keeping the real line breaks too
    # so the file stays readable. Paragraph breaks (blank line between) get <br><br>; single
    # breaks (e.g. within a formula or list) get <br>. Strips any existing <br> first, so the
    # tags never compound.
    paragraphs <- text %>%
      strip_html_breaks() %>%
      str_trim() %>%
      str_split("\n{2,}") %>%
      pluck(1) %>%
      map_chr(~ str_replace_all(.x, "\n", "<br>\n"))

    paste(paragraphs, collapse = "<br><br>\n\n")
  }

  admin_block <- function(header, text) {
    # One field as a header plus a fenced code block. Stops if the text contains ``` itself,
    # since that would end the code block early.
    if (str_detect(text, "```")) {
      stop(glue("Field '{header}' for indicator {indicator_id} contains ``` and cannot be placed in a code block."))
    }
    paste0("### ", header, "\n```html\n", to_html_breaks(text), "\n```\n")
  }

  ## read drafts

  en_path <- md_path(indicator_id, "en")
  es_path <- md_path(indicator_id, "es")

  assert_that(has_content(en_path), msg = glue("English draft not found: {en_path}\nRun suggest_metadata_en() first."))
  assert_that(has_content(es_path), msg = glue("Spanish draft not found: {es_path}\nRun translate_metadata_es() first."))

  en_fields <- read_md_body(en_path) %>% parse_fields("English")
  es_fields <- read_md_body(es_path) %>% parse_fields("Spanish")

  ## assemble in CEPALSTAT Admin order

  admin_body <- paste(
    "> Generated file for copying into CEPALSTAT Admin. Do not edit: changes are overwritten on the next export. Use the copy button in the corner of each block.\n",
    admin_block("DEFINICIÓN - ESPAÑOL",  es_fields$definition),
    admin_block("DEFINICIÓN - INGLÉS",   en_fields$definition),
    admin_block("METODOLOGÍA - ESPAÑOL", es_fields$methodology),
    admin_block("METODOLOGÍA - INGLÉS",  en_fields$methodology),
    admin_block("COMENTARIOS - ESPAÑOL", es_fields$comments),
    admin_block("COMENTARIOS - INGLÉS",  en_fields$comments),
    sep = "\n"
  )

  front_matter <- make_front_matter(
    indicator_id,
    related_types = c("en", "es"),
    extra = list(generated = format(Sys.Date()))
  )

  admin_path <- md_path(indicator_id, "admin")
  write_md(front_matter, admin_body, admin_path)
  message(glue("✅ Exported CEPALSTAT Admin metadata for {indicator_id}: {basename(admin_path)}"))

  invisible(admin_body)
}
