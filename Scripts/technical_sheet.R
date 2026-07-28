# Generates a draft metadata template for a given CEPALSTAT indicator.
# Fetches existing metadata from the CEPALSTAT API and calls the
# Anthropic API to produce an updated draft in English.

# Development note: I experimented with attaching relevant UN and domain-specific methodological documents
# and pointing to the indicator's R code. In the end I found the greatest success with the least amount 
# of token usage by only feeding Claude the current indicator metadata along with a "golden example" of a 
# similar CEPALSTAT indicator.

## testing variables
# indicator_id <- 3881
# gold_standard_indicators <- c(2487, 4174)
# -- simple indicator
# 2487 - Primary and secondary energy supply -- compound indicator
# 4174 - Energy intensity measured in terms of primary energy and GDP -- calculated indicator

suggest_metadata_en <- function(indicator_id, gold_standard_indicators = c(2487)) {
  
  ## setup
  
  PROJECT_ROOT  <- here::here()
  OUTPUT_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Outputs")
  LEGACY_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Legacy")
  
  CEPALSTAT_API_URL <- "https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{id}/metadata?lang={lang}&format=json"
  ANTHROPIC_MODEL   <- "claude-sonnet-4-6"

  ## general system prompt
  # Edit SYSTEM_PROMPT and the user_prompt block in main to refine what the model generates.
  
  SYSTEM_PROMPT <- "
You are an expert in statistical metadata standards for international development indicators.
Your task is to draft metadata for CEPALSTAT environmental indicators following UNSD best
practices, as illustrated in the reference documents provided.

For each indicator, you will revise or draft the following three fields only:
  1. Definition
  2. Methodology
  3. Comments / additional information

The Definition should be more general and clarify terms and concepts. Sometimes the indicator
can be a calculation with values in both the numerator and denominator. If so, define both elements
with a clear and technical definition. Depending on the complexity of the indicator, this can be anywhere
from 3-6 sentences (or 2-4 short paragraphs).

The Methodology is where details are included that gives the user sufficient information to recreate
the indicator themselves. Generally, this includes notes on the data source, key groupings or
filterings of the data (that aren't apparent from the indicator name), and any formulas or calculations.

The Comments are where general comments are made about the use of the data (if applicable), and
more importantly, includes links to relevant resources for further reading.

Keep in mind that the fields Data Source, Units, and Data Frequency are defined elsewhere in the
metadata and do not need to be explicitly outlined in the three fields above.

Also note that most of the metadata sheets were written originally in Spanish and translated. If there is an
internationally (UN) used and approved phrasing or terminology, use that.

Write with precision and professional tone appropriate for a UN statistical system.
Avoid vague language. Cite units, data sources, and methodological steps explicitly.

STYLE REQUIREMENTS:
- NEVER use em dashes (—) or en dashes (–) under any circumstances.
- Do not use HTML tags, special characters, or unicode subscripts/superscripts in formulas.
  Write formulas in plain text only, for example: VR_t = ((M_t - M_(t-1)) / M_(t-1)) x 100

OUTPUT FORMAT (required — the output is parsed programmatically downstream):
Format your entire response using exactly these three section headers, each alone on its own line,
with the field's content immediately below it. Do not add any other headers, preamble, or commentary
outside these three sections.

### DEFINITION
<definition text>

### METHODOLOGY
<methodology text>

### COMMENTS
<comments text>
"
  
  ## functions
  
  get_formatted_metadata <- function(example_ids, lang = "en") {
    # Fetches golden standard metadata entries and formats them as labelled example blocks, using
    # the SAME ### DEFINITION / ### METHODOLOGY / ### COMMENTS format required of the output — so
    # the examples demonstrate the required structure instead of contradicting it.
    example_ids %>%
      map(function(id) {
        m <- get_indicator_metadata(id, lang = lang)
        glue(
          "--- indicator {id}: {m$value[m$variable == 'indicator_name']} ---\n\n",
          "### DEFINITION\n{m$value[m$variable == 'definition']}\n\n",
          "### METHODOLOGY\n{m$value[m$variable == 'calculation_methodology']}\n\n",
          "### COMMENTS\n{m$value[m$variable == 'comments']}\n"
        )
      }) %>%
      paste(collapse = "\n\n")
  }
  
  save_legacy_metadata <- function(indicator_id) {
    # Archives the current (pre-AI) English and Spanish metadata to a single text file
    
    legacy_path <- file.path(LEGACY_DIR, glue("metadata_{indicator_id}.txt"))
    
    if (!file.exists(legacy_path)) {
      en_text <- get_formatted_metadata(indicator_id, lang = "en")
      es_text <- get_formatted_metadata(indicator_id, lang = "es")
      
      today <- format(Sys.Date(), "%Y-%m-%d")
      legacy_text <- glue(
        "--- ENGLISH METADATA ({today}) ---\n\n{en_text}\n\n",
        "--- SPANISH METADATA ({today}) ---\n\n{es_text}\n"
      )
      
      legacy_path <- file.path(LEGACY_DIR, glue("metadata_{indicator_id}.txt"))
      writeLines(legacy_text, legacy_path)
    }
    
  }
  
  generate_draft <- function(indicator_id, system_prompt, user_prompt) {
    # Calls the Anthropic API and writes the English draft to a .txt file for review.
    api_key <- Sys.getenv("ANTHROPIC_API_KEY")
    assert_that(nchar(api_key) > 0, msg = "ANTHROPIC_API_KEY not found. Please add it to your .Renviron file.")
    
    response <- request("https://api.anthropic.com/v1/messages") |>
      req_headers(
        "x-api-key"         = api_key,
        "anthropic-version" = "2023-06-01",
        "content-type"      = "application/json"
      ) |>
      req_body_json(list(
        model      = ANTHROPIC_MODEL,
        max_tokens = 4096,
        temperature = 0, # makes model more deterministic and reproducible
        system     = trimws(system_prompt),
        messages   = list(list(role = "user", content = user_prompt))
      )) |>
      req_timeout(180) |>
      req_retry(max_tries = 4, is_transient = \(r) resp_status(r) %in% c(429, 529),
                backoff = \(i) 30) |>
      req_error(body = \(r) resp_body_string(r)) |>
      req_perform()
    
    result        <- resp_body_json(response)
    response_text <- result$content[[1]]$text %>%
      str_remove("^[\\s\\S]*?(?=### DEFINITION)") # strip any stray preamble before the first required header
    
    draft_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_en.txt"))
    writeLines(response_text, draft_path)
    message(glue("English draft written to: {draft_path}"))
    
    response_text
  }
  
  # ---- main ----
  
  message(glue("Processing metadata for indicator {indicator_id}..."))
  
  ## update system prompt with good examples
  golden_examples <- get_formatted_metadata(gold_standard_indicators)
  
  system_prompt <- paste(
    SYSTEM_PROMPT,
    "The following are examples of high-quality CEPALSTAT metadata to use as a reference for style, structure, and level of detail:\n\n",
    golden_examples,
    sep = "\n\n"
  )
  
  ## specify user prompt with current metadata text
  
  existing_metadata <- get_indicator_metadata(indicator_id) %>%
    mutate(line = paste0(variable, ": ", value)) %>%
    pull(line) %>%
    paste(collapse = "\n")
  
  user_prompt <- paste0(
    existing_metadata,
    "\n\nPlease revise the metadata fields (definition, calculation_methodology, comments) ",
    "based on the available inputs. Keep other metadata elements exactly as-is.",
    "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
    "— no introductory sentence, no preamble, no markdown bold headers, no closing remarks. ",
    "Respond with only the three ### sections, nothing else."
  )
  
  ## store existing (pre-AI) metadata locally
  save_legacy_metadata(indicator_id)
  
  ## generate English draft
  # Review and edit Metadata/Outputs/metadata_{indicator_id}_en.txt before translating.
  # message("Calling Anthropic API (English draft)...")
  english_text <- generate_draft(indicator_id, system_prompt, user_prompt)
  # cat(english_text)

  message(glue("✅ Exported (en) metadata for {indicator_id}"))
}



translate_metadata_es <- function(indicator_id, gold_standard_indicators = c(2487)) {
  # Reads the reviewed English draft (written by suggest_metadata_en) and translates it to Spanish,
  # using this indicator's own existing Spanish metadata as a terminology reference.

  ## setup

  PROJECT_ROOT <- here::here()
  OUTPUT_DIR   <- file.path(PROJECT_ROOT, "Metadata", "Outputs")

  ANTHROPIC_MODEL <- "claude-sonnet-4-6"

  ## general system prompt

  SYSTEM_PROMPT <- "
You are a professional translator specializing in UN statistical documentation for Latin America.
Your task is to translate English statistical metadata into Spanish for the CEPALSTAT database,
maintained by ECLAC (Comision Economica para America Latina y el Caribe).

This indicator's existing Spanish metadata (provided below) was written by a human, so it is not an
authoritative source of terminology — but prefer its established phrasing over a fresh translation.
The exception: if there is a more internationally (UN) used and approved Spanish term or phrasing,
use that instead. This is the same standard used when drafting the English metadata.

Translation requirements:
- Prefer this indicator's existing Spanish terminology, deferring to internationally accepted
  terminology where it differs (see above).
- Use formal, precise language appropriate for a UN statistical system.
- Translate faithfully — do not add, remove, or summarize content.

STYLE REQUIREMENTS:
- NEVER use em dashes (—) or en dashes (–) under any circumstances.
- Do not use HTML tags, special characters, or unicode subscripts/superscripts in formulas.
  Write formulas in plain text only.

OUTPUT FORMAT (required — the output is parsed programmatically downstream):
The English text uses ### DEFINITION / ### METHODOLOGY / ### COMMENTS as section headers. Preserve
these exact headers unchanged (do not translate them) and translate only the content beneath each
one. Do not add any other headers, preamble, or commentary outside these three sections.
"

  ## functions

  get_formatted_metadata <- function(example_ids, lang = "en") {
    # Fetches golden standard metadata entries and formats them as labelled example blocks, using
    # the SAME ### DEFINITION / ### METHODOLOGY / ### COMMENTS format required of the output — so
    # the examples demonstrate the required structure instead of contradicting it.
    example_ids %>%
      map(function(id) {
        m <- get_indicator_metadata(id, lang = lang)
        glue(
          "--- indicator {id}: {m$value[m$variable == 'indicator_name']} ---\n\n",
          "### DEFINITION\n{m$value[m$variable == 'definition']}\n\n",
          "### METHODOLOGY\n{m$value[m$variable == 'calculation_methodology']}\n\n",
          "### COMMENTS\n{m$value[m$variable == 'comments']}\n"
        )
      }) %>%
      paste(collapse = "\n\n")
  }

  generate_translation <- function(indicator_id, system_prompt, user_prompt) {
    # Calls the Anthropic API and writes the Spanish translation to a .txt file for review.
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

    result        <- resp_body_json(response)
    response_text <- result$content[[1]]$text %>%
      str_remove("^[\\s\\S]*?(?=### DEFINITION)") # strip any stray preamble before the first required header

    draft_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_es.txt"))
    writeLines(response_text, draft_path)
    #message(glue("Spanish draft written to: {draft_path}"))

    response_text
  }

  # ---- main ----

  message(glue("Processing metadata translation for indicator {indicator_id}..."))

  ## update system prompt with good examples, in Spanish
  golden_examples <- get_formatted_metadata(gold_standard_indicators, lang = "es")

  system_prompt <- paste(
    SYSTEM_PROMPT,
    "The following are examples of high-quality CEPALSTAT metadata in Spanish to use as a reference for style, structure, and terminology:\n\n",
    golden_examples,
    sep = "\n\n"
  )

  ## read the reviewed English draft (written by suggest_metadata_en)
  draft_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_en.txt"))
  assert_that(
    file.exists(draft_path),
    msg = glue("English draft not found: {draft_path}\nRun suggest_metadata_en() first.")
  )
  english_text <- paste(readLines(draft_path, warn = FALSE), collapse = "\n")

  ## fetch this indicator's own existing Spanish metadata, for terminology grounding
  existing_es_metadata <- get_indicator_metadata(indicator_id, lang = "es") %>%
    mutate(line = paste0(variable, ": ", value)) %>%
    pull(line) %>%
    paste(collapse = "\n")

  user_prompt <- paste0(
    "ENGLISH TEXT TO TRANSLATE:\n", english_text,
    "\n\nEXISTING SPANISH METADATA FOR THIS INDICATOR (written by a human, not authoritative — prefer ",
    "its phrasing unless a more internationally accepted Spanish term exists):\n", existing_es_metadata,
    "\n\nTranslate the English text above into Spanish, following the terminology guidance above.",
    "\n\nYour response must begin immediately with '### DEFINITION' as its very first characters ",
    "— no introductory sentence, no preamble, no closing remarks. Respond with only the three ### ",
    "sections (headers left untranslated), nothing else."
  )

  ## generate Spanish translation
  spanish_text <- generate_translation(indicator_id, system_prompt, user_prompt)

  message(glue("✅ Exported (es) metadata for {indicator_id}"))
}


export_metadata_admin <- function(indicator_id) {
  # Reads the reviewed English and Spanish drafts (written by suggest_metadata_en and
  # translate_metadata_es) and assembles them into the CEPALSTAT Admin import format: labelled
  # Spanish/English pairs for Definicion, Metodologia, and Comentarios, with paragraph breaks
  # converted to <br><br> for CEPALSTAT's rich text fields.

  ## setup

  PROJECT_ROOT <- here::here()
  OUTPUT_DIR   <- file.path(PROJECT_ROOT, "Metadata", "Outputs")

  ## functions

  parse_fields <- function(text, source_label) {
    # Splits a draft into its three labelled sections (### DEFINITION / ### METHODOLOGY / ### COMMENTS).
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

  to_html_breaks <- function(text) {
    # Converts paragraph breaks (blank line between) to <br><br>, and single line breaks
    # (e.g. within a formula or list) to <br>, for CEPALSTAT's rich text fields.
    text %>%
      str_trim() %>%
      str_replace_all("\n{2,}", "<br><br>") %>%
      str_replace_all("\n", "<br>")
  }

  # ---- main ----

  message(glue("Assembling CEPALSTAT Admin export for indicator {indicator_id}..."))

  en_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_en.txt"))
  es_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_es.txt"))

  assert_that(file.exists(en_path), msg = glue("English draft not found: {en_path}\nRun suggest_metadata_en() first."))
  assert_that(file.exists(es_path), msg = glue("Spanish draft not found: {es_path}\nRun translate_metadata_es() first."))

  en_fields <- paste(readLines(en_path, warn = FALSE), collapse = "\n") %>% parse_fields("English")
  es_fields <- paste(readLines(es_path, warn = FALSE), collapse = "\n") %>% parse_fields("Spanish")

  admin_text <- paste0(
    "***Definición - español:***\n", to_html_breaks(es_fields$definition), "\n\n",
    "***Definición - inglés:***\n", to_html_breaks(en_fields$definition), "\n\n",
    "***Metodología - español:***\n", to_html_breaks(es_fields$methodology), "\n\n",
    "***Metodología - inglés:***\n", to_html_breaks(en_fields$methodology), "\n\n",
    "***Comentarios - español:***\n", to_html_breaks(es_fields$comments), "\n\n",
    "***Comentarios - inglés:***\n", to_html_breaks(en_fields$comments), "\n"
  )

  admin_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_admin.txt"))
  writeLines(admin_text, admin_path)
  message(glue("✅ Exported CEPALSTAT Admin metadata for {indicator_id}: {admin_path}"))

  invisible(admin_text)
}
