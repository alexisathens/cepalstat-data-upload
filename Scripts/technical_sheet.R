# Generates a draft metadata template for a given CEPALSTAT indicator.
# Fetches existing metadata from the CEPALSTAT API and calls the
# Anthropic API to produce an updated draft in English.

# Development note: I experimented with attaching relevant UN and domain-specific methodological documents
# and pointing to the indicator's R code. In the end I found the greatest success with the least amount 
# of token usage by only feeding Claude the current indicator metadata along with a "golden example" of a 
# similar CEPALSTAT indicator.

library(tidyverse)
library(readxl)
library(writexl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(assertthat)
library(CepalStatR)
library(pdftools)



## testing variables
# indicator_id <- 3881
# gold_standard_indicators <- c(2487, 4174)
# 2487 - Primary and secondary energy supply
# 4174 - Energy intensity measured in terms of primary energy and GDP

suggest_metadata_en <- function(indicator_id = 3881,
                                gold_standard_indicators = c(2487, 4174)) {
  
  ## setup
  
  PROJECT_ROOT  <- here::here()
  OUTPUT_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Outputs")
  LEGACY_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Legacy")
  
  CEPALSTAT_API_URL <- "https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{id}/metadata?lang={lang}&format=json"
  ANTHROPIC_MODEL   <- "claude-sonnet-4-6"
  
  ind_meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

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
"
  
  ## functions
  
  get_formatted_metadata <- function(example_ids, lang = "en") {
    # Fetches golden standard metadata entries and formats them as labelled example blocks.
    example_ids %>%
      map(function(id) {
        m <- get_indicator_metadata(id, lang = lang)
        glue(
          "--- indicator {id}: {m$value[m$variable == 'indicator_name']} ---\n\n",
          "definition:\n{m$value[m$variable == 'definition']}\n\n",
          "calculation_methodology:\n{m$value[m$variable == 'calculation_methodology']}\n\n",
          "comments:\n{m$value[m$variable == 'comments']}\n"
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
    response_text <- result$content[[1]]$text
    
    draft_path <- file.path(OUTPUT_DIR, glue("metadata_{indicator_id}_en.txt"))
    writeLines(response_text, draft_path)
    message(glue("English draft written to: {draft_path}"))
    
    response_text
  }
  
  # ---- main ----
  
  # message(glue("Processing indicator {indicator_id}..."))
  
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
    "based on the available inputs. Keep other metadata elements exactly as-is."
  )
  
  ## store existing (pre-AI) metadata locally
  save_legacy_metadata(indicator_id)
  
  
  ## generate English draft
  # Review and edit Metadata/Outputs/metadata_{indicator_id}_en.txt before translating.
  # message("Calling Anthropic API (English draft)...")
  english_text <- generate_draft(indicator_id, system_prompt, user_prompt)
  # cat(english_text)
}



# TRANSLATION_SYSTEM_PROMPT <- "
# You are a professional translator specializing in UN statistical documentation for Latin America.
# Your task is to translate English statistical metadata into Spanish for the CEPALSTAT database,
# maintained by ECLAC (Comision Economica para America Latina y el Caribe).
# 
# Translation requirements:
# - Use established ECLAC/CEPALSTAT Spanish terminology, as shown in the reference examples provided.
# - Do not translate proper names of organizations, data sources, or internationally recognized
#   acronyms that appear in their English form in Spanish UN documents (e.g., OLADE, GDP, CO2,
#   UNSD, IPCC, PIB is acceptable for GDP in Spanish contexts).
# - Preserve the exact structure and section labels of the original (DEFINITION:, METHODOLOGY:,
#   COMMENTS:).
# - Use formal, precise language appropriate for a UN statistical system.
# - Translate faithfully — do not add, remove, or summarize content.
# 
# STYLE REQUIREMENTS:
# - NEVER use em dashes (—) or en dashes (–) under any circumstances.
# - Do not use HTML tags or special unicode characters.
#   Write formulas in plain text only.
# "


# translate_to_spanish <- function(indicator_id, use_existing_spanish = TRUE) {
#   # Reads the reviewed English draft, translates it to Spanish, and writes the result
#   # to a .txt file. Returns the Spanish text.
#   api_key <- Sys.getenv("ANTHROPIC_API_KEY")
#   assert_that(nchar(api_key) > 0, msg = "ANTHROPIC_API_KEY not found. Please add it to your .Renviron file.")
# 
#   draft_path <- file.path(OUTPUT_DIR, glue("{indicator_id}_english_draft.txt"))
#   assert_that(
#     file.exists(draft_path),
#     msg = glue("English draft not found: {draft_path}\nRun generate_draft() first.")
#   )
#   english_text <- paste(readLines(draft_path, warn = FALSE), collapse = "\n")
# 
#   # Build translation prompt sections
#   translate_sections <- list()
#   translate_sections[["ENGLISH TEXT TO TRANSLATE"]] <- english_text
# 
#   if (use_existing_spanish) {
#     message("Fetching existing Spanish metadata for terminology reference...")
#     es_meta <- fetch_cepalstat_metadata(indicator_id, lang = "es")
#     es_meta_text <- toJSON(es_meta, pretty = TRUE, auto_unbox = TRUE)
#     translate_sections[["EXISTING SPANISH METADATA (terminology reference)"]] <- es_meta_text
#   }
# 
#   message("Fetching Spanish golden standard examples...")
#   es_examples <- fetch_example_metadata(c(2487, 4174), lang = "es")
# 
#   system_prompt <- paste(
#     TRANSLATION_SYSTEM_PROMPT,
#     "The following are examples of high-quality CEPALSTAT metadata in Spanish to use as a reference for style and terminology:\n\n",
#     es_examples,
#     sep = "\n\n"
#   )
# 
#   user_prompt <- paste0(
#     "Indicator ID: ", indicator_id, "\n\n",
#     translate_sections %>% imap(~ glue("--- {.y} ---\n{.x}")) %>% paste(collapse = "\n\n"),
#     "\n\nTranslate the English text above into Spanish."
#   )
# 
#   response <- request("https://api.anthropic.com/v1/messages") |>
#     req_headers(
#       "x-api-key"         = api_key,
#       "anthropic-version" = "2023-06-01",
#       "content-type"      = "application/json"
#     ) |>
#     req_body_json(list(
#       model      = ANTHROPIC_MODEL,
#       max_tokens = 4096,
#       system     = trimws(system_prompt),
#       messages   = list(list(role = "user", content = list(list(type = "text", text = user_prompt))))
#     )) |>
#     req_timeout(180) |>
#     req_retry(max_tries = 4, is_transient = \(r) resp_status(r) %in% c(429, 529),
#               backoff = \(i) 30) |>
#     req_error(body = \(r) resp_body_string(r)) |>
#     req_perform()
# 
#   result       <- resp_body_json(response)
#   spanish_text <- result$content[[1]]$text
# 
#   spanish_path <- file.path(OUTPUT_DIR, glue("{indicator_id}_spanish_draft.txt"))
#   writeLines(spanish_text, spanish_path)
#   message(glue("Spanish draft written to: {spanish_path}"))
# 
#   spanish_text
# }

# write_output <- function(indicator_id, english_text, spanish_text = NULL) {
#   output_path <- file.path(OUTPUT_DIR, paste0("metadata_draft_", indicator_id, ".xlsx"))
# 
#   # TODO: parse text into individual fields (definition, methodology, comments) once
#   # the bulk upload column structure is confirmed.
#   rows <- list(data.frame(indicator_id = indicator_id, lang = "en", metadata = english_text))
#   if (!is.null(spanish_text))
#     rows <- c(rows, list(data.frame(indicator_id = indicator_id, lang = "es", metadata = spanish_text)))
# 
#   write_xlsx(bind_rows(rows), output_path)
#   message("Output written to: ", output_path)
# }




# Step 2: Translate to Spanish
# spanish_text <- translate_to_spanish(indicator_id)
# cat(spanish_text)

# Step 3: Write output
# Pass spanish_text once translation is done; omit it to write English only.
# write_output(indicator_id, english_text)
# write_output(indicator_id, english_text, spanish_text)
# message("Done.")
