# Drafts revisions of CEPALSTAT indicator metadata sheets (definition, methodology, comments), with or
# without AI assistance, as one Markdown draft per indicator for use in a Markdown editor such as Obsidian.
#
# Files in Metadata/:
#   {id}.md        - snapshot of the published sheet, written by build_tables.R (read-only, tracked in git)
#   {id}_draft.md  - your draft (local only). It goes through two stages in the same file:
#                      stage: english draft - English only, to edit by hand
#                      stage: admin         - Spanish and English, in CEPALSTAT Admin order, each field in a
#                                             code block with <br> tags, ready to copy into Admin
#
# Workflow (see run_metadata.R):
#   1. start_draft()   - creates the English draft: AI suggestions, AI edits following your instructions,
#                        or a plain copy of the published English (ai_assist = FALSE)
#      -> edit the English in Metadata/{id}_draft.md
#   2. finish_draft()  - translates the English fields you changed (unchanged fields keep their published
#                        Spanish) and rewrites the draft in Admin format
#      -> copy the fields into CEPALSTAT Admin
#   3. build_tables.R  - updates the snapshot; a draft that now matches the published text is deleted
#   draft_status()     - lists your drafts and their stage
#
# Everything published is read from the snapshots, so run build_tables.R first if they may be out of
# date. Existing drafts are never overwritten unless overwrite = TRUE.
#
# Gold standard (gold): published indicators used as examples of style and structure. Required for new
# indicators (no published text yet), where the draft is written by adapting a closely related indicator
# (e.g. same data source). Optional otherwise: for an indicator that has already been revised, its own
# text is the best reference, and `instructions` (e.g. "Update the GDP base year to 2018 dollars") is
# usually more useful.
#
# The system prompts and the translation glossary are editable files in Metadata/Prompts/:
#   draft_en.md     - instructions for drafting the English metadata (start_draft)
#   translate_es.md - instructions for translating into Spanish (finish_draft)
#   glossary.md     - required English to Spanish terms, added to the translation prompt (finish_draft)

# Development note: I experimented with attaching relevant UN and domain-specific methodological documents
# and pointing to the indicator's R code. In the end I found the greatest success with the least amount
# of token usage by only feeding Claude the current indicator metadata along with a "golden example" of a
# similar CEPALSTAT indicator.

# Requires utils.R to be sourced (packages, `meta` table, the snapshot helpers, and the AI model set in
# config.R as ANTHROPIC_MODEL).

# ---- settings ----

PROMPTS_DIR <- here::here("Metadata", "Prompts")   # system prompts and glossary

# Section headers used in English drafts and in the AI's responses, by metadata field
english_headers <- c(definition = "DEFINITION", calculation_methodology = "METHODOLOGY", comments = "COMMENTS")


# ---- shared helpers ----

# TRUE if a file exists and has content. An empty file (e.g. one created by clicking an unresolved
# link in Obsidian) is treated as missing.
has_content <- function(path) {
  file.exists(path) && file.size(path) > 0
}

# Removes <br>/<br/>/<br /> tags (used before adding them again in to_html_breaks(), so they never
# compound).
strip_html_breaks <- function(text) {
  str_remove_all(text, "<br\\s*/?>")
}

# Adds <br>/<br><br> tags for CEPALSTAT's rich text field, keeping the real line breaks too so the file
# stays readable: a blank line between paragraphs becomes <br><br>, a single line break becomes <br>.
# The reverse conversion is clean_metadata_text() in utils.R.
to_html_breaks <- function(text) {
  paragraphs <- text %>%
    coalesce("") %>%
    strip_html_breaks() %>%
    str_trim() %>%
    str_split("\n{2,}") %>%
    pluck(1) %>%
    map_chr(~ str_replace_all(.x, "\n", "<br>\n"))

  paste(paragraphs, collapse = "<br><br>\n\n")
}

# Reads a .md file and returns its body without the front matter.
read_md_body <- function(path) {
  read_file(path) %>%
    str_remove_all("\r") %>%                       # normalize Windows line endings
    str_remove("^---\\n[\\s\\S]*?\\n---\\n") %>%   # drop the front matter block
    str_trim()
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

# Calls the Anthropic API and returns the response text, starting at the first ### header.
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
    str_remove("^[\\s\\S]*?(?=### )") %>%   # strip any stray preamble before the first header
    strip_html_breaks()                     # strip any <br> echoed from reference material
}


# ---- reading published text and drafts ----

# Published text of an indicator from its snapshot: one row per field, with English (en) and Spanish
# (es). NULL if the indicator has no snapshot.
# Sample usage: read_published_fields(3381)
read_published_fields <- function(indicator_id) {
  path <- snapshot_path(indicator_id)
  if (!file.exists(path)) return(NULL)
  sections <- read_metadata_snapshot(path)
  metadata_fields %>%
    transmute(field,
              en = sections$text[match(header_en, sections$header)],
              es = sections$text[match(header_es, sections$header)]) %>%
    mutate(across(c(en, es), ~ coalesce(.x, "")))
}

# TRUE if the indicator has no published text yet (no snapshot, or all fields empty)
is_new_indicator <- function(published) {
  is.null(published) || all(str_squish(c(published$en, published$es)) == "")
}

# Writes English fields (a named vector: definition, calculation_methodology, comments) as
# "### DEFINITION / ### METHODOLOGY / ### COMMENTS" sections. Only the fields given are written.
format_english_sections <- function(fields) {
  paste0("### ", english_headers[names(fields)], "\n", fields, collapse = "\n\n")
}

# Reads "### DEFINITION / ### METHODOLOGY / ### COMMENTS" sections (from an English draft or an AI
# response) into a named vector. Stops if one of the expected sections is missing.
parse_english_sections <- function(text, expected = names(english_headers), source_label = "draft") {
  sections <- parse_metadata_snapshot(paste0("\n", text))
  fields <- set_names(sections$text[match(english_headers[expected], sections$header)], expected)
  missing <- expected[is.na(fields)]
  if (length(missing) > 0) {
    stop(glue("Couldn't find the {paste(english_headers[missing], collapse = ', ')} section(s) in the {source_label}. ",
              "Expected headers: ### DEFINITION / ### METHODOLOGY / ### COMMENTS"))
  }
  fields
}

# English text of a draft, in either stage, as a named vector (definition, calculation_methodology, comments)
read_draft_english <- function(path, stage) {
  if (identical(stage, "admin")) {
    sections <- read_admin_draft(path)
    set_names(sections$text[match(metadata_fields$header_en, sections$header)], metadata_fields$field)
  } else {
    parse_english_sections(read_md_body(path), source_label = basename(path))
  }
}

# Name of an indicator, from Data/indicator_metadata.xlsx or else its snapshot
indicator_label <- function(indicator_id, lang = "en") {
  column <- if (lang == "en") "indicator" else "indicator_es"
  name <- if (column %in% names(meta)) meta[[column]][meta$id == indicator_id] else character()
  if (length(name) == 1 && !is.na(name)) return(name)
  path <- snapshot_path(indicator_id)
  if (file.exists(path)) read_front_matter(path)[[if (lang == "en") "indicator_name" else "indicator_name_es"]] else ""
}

# Gold standard indicators formatted as labelled examples for a system prompt, in English or Spanish
format_gold_examples <- function(gold, lang = "en") {
  gold %>%
    map_chr(function(id) {
      published <- read_published_fields(id)
      if (is_new_indicator(published)) stop(glue("Gold standard {id} has no published text in Metadata/{id}.md."))
      fields <- set_names(published[[lang]], published$field)
      paste0("--- indicator ", id, ": ", indicator_label(id, lang), " ---\n\n", format_english_sections(fields))
    }) %>%
    paste(collapse = "\n\n")
}

# Writes a draft file: front matter (a named list) and body
write_draft <- function(indicator_id, front_matter, body) {
  quote_yaml <- function(x) paste0('"', str_replace_all(as.character(x), '"', '\\\\"'), '"')
  lines <- c("---", imap_chr(front_matter, ~ paste0(.y, ": ", quote_yaml(.x))), "---")
  write_file(paste0(paste(lines, collapse = "\n"), "\n", str_trim(body), "\n"), draft_path(indicator_id))
}

# Runs one step for several indicators, reporting a failure without stopping the others
for_each_indicator <- function(indicator_ids, step, step_name) {
  walk(indicator_ids, function(id) {
    tryCatch(step(id), error = function(e) message(glue("❌ {step_name} failed for {id}: {conditionMessage(e)}")))
  })
}


# ---- step 1: English draft ----

# Creates Metadata/{id}_draft.md with the English metadata to edit. Accepts one indicator or several.
#   ai_assist = TRUE  - the AI suggests a revision of the published English (or, with `instructions`,
#                       makes only the changes you describe)
#   ai_assist = FALSE - the published English is copied as it is, for small edits by hand
# New indicators (no published text) always use the AI, and need a gold standard.
# Sample usage: start_draft(4183, instructions = "Update the GDP base year to 2018 dollars")
start_draft <- function(indicator_ids, gold = NULL, instructions = NULL, ai_assist = TRUE, overwrite = FALSE) {
  for_each_indicator(indicator_ids, step_name = "start_draft", function(indicator_id) {

    path <- draft_path(indicator_id)
    if (has_content(path) && !overwrite) {
      message(glue("⏭️ Skipped {indicator_id}: {basename(path)} already exists (use overwrite = TRUE to start over)"))
      return(invisible(NULL))
    }

    published <- read_published_fields(indicator_id)
    new_indicator <- is_new_indicator(published)
    if (new_indicator && is.null(gold)) {
      stop("it has no published text yet, so a gold standard is needed: start_draft(id, gold = <closely related indicator>)")
    }
    if (new_indicator && !ai_assist) stop("it has no published text to copy, so it needs ai_assist = TRUE and a gold standard")

    if (!ai_assist) {
      # Plain copy of the published English
      english <- set_names(published$en, published$field)
      message(glue("Copied the published English for {indicator_id}"))

    } else {
      message(glue("Drafting English for {indicator_id}{if (new_indicator) ' (new indicator)' else ''}..."))

      # System prompt: drafting instructions (Metadata/Prompts/draft_en.md), plus gold examples if given
      system_prompt <- read_prompt("draft_en")
      if (!is.null(gold)) {
        system_prompt <- paste(
          system_prompt,
          "The following are examples of high-quality CEPALSTAT metadata to use as a reference for style, structure, and level of detail:",
          format_gold_examples(gold, "en"),
          sep = "\n\n"
        )
      }

      response_rules <- paste0(
        "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
        "— no introductory sentence, no preamble, no markdown bold headers, no closing remarks. ",
        "Respond with only the three ### sections, nothing else."
      )

      if (new_indicator) {
        # New indicator: draft from its entry in the metadata table, adapting the gold standard(s)
        meta_row <- meta %>% filter(id == indicator_id)
        assert_that(nrow(meta_row) == 1,
                    msg = glue("Indicator {indicator_id} not found in Data/indicator_metadata.xlsx. Add it before drafting metadata."))
        user_prompt <- paste0(
          "NEW INDICATOR (no published metadata yet):\n",
          "indicator_id: ", indicator_id, "\n",
          "indicator_name: ", meta_row$indicator, "\n",
          "source: ", meta_row$source, "\n",
          "unit: ", if ("unit" %in% names(meta_row)) coalesce(meta_row$unit, "not specified") else "not specified", "\n",
          "dimensions: ", coalesce(meta_row$dimensions, "none beyond country and year"), "\n",
          "classification: ", paste(na.omit(c(meta_row$cat1, meta_row$cat2, meta_row$cat3)), collapse = " > "),
          "\n\nThe reference indicator(s) above are closely related to this new indicator and share its data ",
          "source and general context. Draft the metadata fields (definition, methodology, comments) ",
          "for the new indicator by adapting their content: keep what applies (source description, coverage, ",
          "context, links) and change what differs, as indicated by the new indicator's name and dimensions. ",
          "Do not carry over details that are specific to the reference indicator(s).",
          if (!is.null(instructions)) paste0("\n\nAlso follow these instructions: ", instructions) else "",
          response_rules
        )

      } else {
        # Existing indicator: revise its published English, or make only the requested changes
        snapshot_meta <- read_front_matter(snapshot_path(indicator_id))
        task <- if (is.null(instructions)) {
          "Please revise the metadata fields (definition, methodology, comments) based on the available inputs."
        } else {
          paste0("Make only the following changes, and keep everything else exactly as it is: ", instructions)
        }
        user_prompt <- paste0(
          "CURRENT METADATA FOR THIS INDICATOR (indicator_id ", indicator_id, "):\n",
          "indicator_name: ", indicator_label(indicator_id), "\n",
          "unit: ", coalesce(snapshot_meta$unit, ""), "\n\n",
          format_english_sections(set_names(published$en, published$field)),
          "\n\n", task,
          response_rules
        )
      }

      english <- parse_english_sections(call_claude(system_prompt, user_prompt), source_label = "AI response")
    }

    front_matter <- list(
      indicator_id   = indicator_id,
      indicator_name = indicator_label(indicator_id),
      stage          = "english draft",
      based_on       = snapshot_fingerprint(indicator_id),
      started        = format(Sys.Date()),
      ai_assist      = ai_assist,
      gold           = paste(gold, collapse = ", "),
      instructions   = coalesce(instructions, "")
    )
    note <- glue("> English draft. Edit the text below, then run finish_draft({indicator_id}) to translate it and format it for CEPALSTAT Admin.")
    write_draft(indicator_id, front_matter, paste0(note, "\n\n", format_english_sections(english)))
    message(glue("✅ English draft saved: Metadata/{basename(path)}"))
  })
}


# ---- step 2: Spanish translation and Admin format ----

# Translates the English fields that differ from the published English, keeps the published Spanish for
# the others, and rewrites Metadata/{id}_draft.md in CEPALSTAT Admin format. Accepts one indicator or
# several. A draft already in Admin format is skipped unless overwrite = TRUE (which re-translates from
# the English in it, e.g. after editing it there).
# Sample usage: finish_draft(c(4183, 4185))
finish_draft <- function(indicator_ids, overwrite = FALSE) {
  glossary <- read_glossary()

  for_each_indicator(indicator_ids, step_name = "finish_draft", function(indicator_id) {

    path <- draft_path(indicator_id)
    assert_that(has_content(path), msg = glue("no draft found at {path}. Run start_draft({indicator_id}) first."))
    front_matter <- read_front_matter(path)
    if (identical(front_matter$stage, "admin") && !overwrite) {
      message(glue("⏭️ Skipped {indicator_id}: the draft is already in Admin format (use overwrite = TRUE to translate again)"))
      return(invisible(NULL))
    }

    english   <- read_draft_english(path, front_matter$stage)
    published <- read_published_fields(indicator_id)
    if (is.null(published)) published <- tibble(field = names(english_headers), en = "", es = "")

    # Fields whose English differs from the published English are translated; the others keep the
    # published Spanish, so wording that hasn't changed isn't re-translated
    changed <- names(english)[str_squish(english) != str_squish(published$en[match(names(english), published$field)])]
    spanish <- set_names(published$es, published$field)[names(english)]

    if (length(changed) == 0) {
      message(glue("ℹ️ {indicator_id}: the English is the same as the published text, so the published Spanish is kept"))
    } else {
      message(glue("Translating {indicator_id} ({paste(english_headers[changed], collapse = ', ')})..."))

      gold <- str_split(coalesce(front_matter$gold, ""), ",\\s*")[[1]] %>% discard(~ .x == "")
      system_prompt <- paste(read_prompt("translate_es"), format_glossary(glossary), sep = "\n\n")
      if (length(gold) > 0) {
        system_prompt <- paste(
          system_prompt,
          "The following are examples of high-quality CEPALSTAT metadata in Spanish to use as a reference for style, structure, and terminology:",
          format_gold_examples(gold, "es"),
          sep = "\n\n"
        )
      }

      existing_spanish <- spanish[changed]
      reference <- if (all(str_squish(existing_spanish) == "")) {
        "\n\nThis text has no published Spanish yet. Always use the glossary terms above; otherwise prefer internationally (UN) accepted Spanish terms."
      } else {
        paste0("\n\nPUBLISHED SPANISH FOR THESE SECTIONS (written by a human, not authoritative — prefer its phrasing ",
               "unless the glossary or a more internationally accepted Spanish term says otherwise):\n",
               format_english_sections(existing_spanish))
      }

      user_prompt <- paste0(
        "ENGLISH TEXT TO TRANSLATE:\n", format_english_sections(english[changed]),
        reference,
        "\n\nTranslate the English text above into Spanish, following the glossary and terminology guidance above.",
        "\n\nYour response must begin immediately with the first ### header as its very first characters ",
        "— no introductory sentence, no preamble, no closing remarks. Respond with only the ### ",
        "sections given (headers left untranslated), nothing else."
      )

      translated <- parse_english_sections(call_claude(system_prompt, user_prompt), expected = changed, source_label = "AI response")
      spanish[changed] <- translated

      # Flag glossary terms that weren't followed, for manual review
      missed <- check_glossary(paste(english[changed], collapse = "\n"), paste(translated, collapse = "\n"), glossary)
      if (nrow(missed) > 0) {
        message(glue("⚠️ Glossary check for {indicator_id}: expected Spanish term(s) not found. Please review the draft:"))
        message(paste0("   ", missed$english, " -> ", missed$spanish, collapse = "\n"))
      }
    }

    # Admin format: each field in Spanish then English, in a code block with <br> tags
    admin_block <- function(header, text) {
      if (str_detect(text, "```")) stop(glue("the {header} text contains ``` and can't be placed in a code block"))
      paste0("### ", header, "\n```html\n", to_html_breaks(text), "\n```\n")
    }
    blocks <- metadata_fields %>%
      pmap_chr(function(field, header_es, header_en) {
        paste0(admin_block(header_es, spanish[[field]]), "\n", admin_block(header_en, english[[field]]))
      })

    front_matter$stage      <- "admin"
    front_matter$translated <- if (length(changed) > 0) paste(english_headers[changed], collapse = ", ") else "none"
    front_matter$finished   <- format(Sys.Date())
    note <- glue("> Ready for CEPALSTAT Admin: copy each block with the copy button in its corner. After publishing, ",
                 "run build_tables.R; this draft is deleted once the published text matches it.")
    write_draft(indicator_id, front_matter, paste0(note, "\n\n", paste(blocks, collapse = "\n")))
    message(glue("✅ Draft ready for CEPALSTAT Admin: Metadata/{basename(path)}"))
  })
}


# ---- draft overview ----

# Lists the drafts in Metadata/: stage, whether the published text changed since the draft was
# started, and which English fields differ from the published text.
# Sample usage: draft_status()
draft_status <- function() {
  paths <- list.files(here("Metadata"), pattern = "^[0-9]+_draft\\.md$", full.names = TRUE)
  if (length(paths) == 0) {
    message("No drafts in Metadata/")
    return(invisible(tibble()))
  }

  status <- map_dfr(paths, function(path) {
    front_matter <- read_front_matter(path)
    id <- as.integer(str_extract(basename(path), "^[0-9]+"))
    published <- read_published_fields(id)
    english <- tryCatch(read_draft_english(path, front_matter$stage), error = function(e) NULL)
    changed <- if (is.null(english) || is.null(published)) NA_character_ else {
      fields <- names(english)[str_squish(english) != str_squish(published$en[match(names(english), published$field)])]
      if (length(fields) == 0) "none" else paste(english_headers[fields], collapse = ", ")
    }
    tibble(
      id                      = id,
      indicator               = str_trunc(indicator_label(id), 45),
      stage                   = coalesce(front_matter$stage, ""),
      started                 = coalesce(front_matter$started, ""),
      english_changed         = changed,
      published_changed_since = !identical(front_matter$based_on, snapshot_fingerprint(id))
    )
  })

  print(as.data.frame(status), row.names = FALSE)
  if (any(status$published_changed_since)) {
    message("⚠️ The published text changed after some drafts were started (published_changed_since = TRUE). ",
            "Check them against Metadata/{id}.md before pasting into CEPALSTAT Admin.")
  }
  invisible(status)
}
