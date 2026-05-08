# Generates a draft metadata template for a given CEPALSTAT indicator.
# Fetches existing metadata from the CEPALSTAT API, reads the associated
# R processing script and any source documentation PDFs, and calls the
# Anthropic API to produce an updated draft in English.

library(tidyverse)
library(readxl)
library(writexl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(assertthat)
library(CepalStatR)


# ---- setup ----

PROJECT_ROOT  <- here::here()
GLOBAL_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Inputs", "Global")
SOURCE_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Inputs", "By Source")
INDICATOR_DIR <- file.path(PROJECT_ROOT, "Metadata", "Inputs", "By Indicator")
SCRIPTS_DIR   <- file.path(PROJECT_ROOT, "Scripts")
OUTPUT_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Outputs")

# Local cache mapping local PDF paths -> Anthropic file IDs.
# PDFs are uploaded once; subsequent runs reuse the stored IDs.
PDF_FILE_ID_CACHE <- file.path(PROJECT_ROOT, "Metadata", "pdf_file_ids.json")

CEPALSTAT_API_URL <- "https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{id}/metadata?lang=en&format=json"
ANTHROPIC_MODEL   <- "claude-sonnet-4-6"

indicator_id <- 4183

ind_meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))


# ---- prompts ----
# Edit SYSTEM_PROMPT and the user_prompt block in main to refine what the model generates.

SYSTEM_PROMPT <- "
You are an expert in statistical metadata standards for international development indicators.
Your task is to draft metadata for CEPALSTAT environmental indicators following UNSD best
practices, as illustrated in the reference documents provided.

For each indicator, you will revise or draft the following three fields only:
  1. Definition
  2. Methodology
  3. Comments / additional information

The Definition should be more general and clarify terms and concepts. Depending on the complexity
of the indicator, this can be anywhere from 3-6 sentences (or 2-4 short paragraphs).

The Methodology is where details are included that gives the user sufficient information to recreate
the indicator themselves. Generally, this includes notes on the data source, key groupings or
filterings of the data, and any formulas or calculations.

The Comments are where general comments are made about the use of the data (if applicable), and
more importantly, includes links to relevant resources for further reading.

Keep in mind that the fields Data Source, Units, and Data Frequency are defined elsewhere in the
metadata and do not need to be explicitly outlined in the three fields above.

Write with precision and professional tone appropriate for a UN statistical system.
Avoid vague language. Cite units, data sources, and methodological steps explicitly.
"


# ---- functions ----

define_indicator_paths <- function(indicator_id) {
  ind_source <- ind_meta %>% filter(id == indicator_id) %>% pull(source)
  ind_dim    <- ind_meta %>% filter(id == indicator_id) %>% pull(dimensions)
  ind_dim    <- if (is.na(ind_dim)) "" else ind_dim

  if (ind_source == "OLADE") {
    code_cleaning_instr <- file.path(SCRIPTS_DIR, "01_olade_instructions.qmd")
    code_cleaning       <- file.path(SCRIPTS_DIR, "01_olade.R")
    code_processing     <- file.path(SCRIPTS_DIR, "02_olade.R")
    meta_source         <- file.path(SOURCE_DIR, "olade")

    crosswalk     <- file.path(PROJECT_ROOT, "Data", "Raw", "olade", "energy_dimensions_crosswalk.xlsx")
    crosswalk_tab <- if (ind_dim == "Type of energy__Primary_and_Secondary") {
      "dimensions_crosswalk_44966"
    } else if (ind_dim == "Energy intensity_Economic activity") {
      "dimensions_crosswalk_78134"
    } else {
      NULL
    }
  } else {
    stop(glue("No path logic defined for source: {ind_source}"))
  }

  list(
    code_cleaning_instr = if (file.exists(code_cleaning_instr)) code_cleaning_instr else NULL,
    code_cleaning       = if (file.exists(code_cleaning)) code_cleaning else NULL,
    code_processing     = if (file.exists(code_processing)) code_processing else NULL,
    meta_source         = if (dir.exists(meta_source)) meta_source else NULL,
    crosswalk           = if (!is.null(crosswalk_tab)) crosswalk else NULL,
    crosswalk_tab       = crosswalk_tab
  )
}

fetch_cepalstat_metadata <- function(indicator_id) {
  url      <- gsub("\\{id\\}", indicator_id, CEPALSTAT_API_URL)
  response <- request(url) |> req_perform()
  resp_body_json(response)
}

fetch_example_metadata <- function(example_ids) {
  # Fetches golden standard metadata entries and formats them as labelled example blocks.
  example_ids %>%
    map(function(id) {
      m <- fetch_cepalstat_metadata(id)
      glue(
        "--- EXAMPLE OUTPUT ---\n\n",
        "DEFINITION:\n{m$body$metadata$definition}\n\n",
        "METHODOLOGY:\n{m$body$metadata$calculation_methodology}\n\n",
        "COMMENTS:\n{m$body$metadata$comments}\n"
      )
    }) %>%
    paste(collapse = "\n\n")
}

extract_indicator_section <- function(script_path, indicator_id) {
  # Extracts the indicator-specific section plus general setup from a processing script.
  lines <- readLines(script_path, warn = FALSE)

  any_header_pattern  <- "^# ----"
  this_header_pattern <- glue("(?i)# ---- indicator {indicator_id}")

  all_header_idx  <- which(str_detect(lines, any_header_pattern))
  this_header_idx <- which(str_detect(lines, this_header_pattern))

  if (length(this_header_idx) == 0) {
    warning(glue("No section found for indicator {indicator_id} in {basename(script_path)}"))
    return(NULL)
  }

  this_header_idx <- this_header_idx[1]

  # General content = everything before the third section header (setup + data reads)
  general_lines <- if (all_header_idx[1] > 1) lines[1:(all_header_idx[3] - 1)] else character(0)

  next_header_idx <- all_header_idx[all_header_idx > this_header_idx]
  section_end_idx <- if (length(next_header_idx) > 0) next_header_idx[1] - 1 else length(lines)
  indicator_lines <- lines[this_header_idx:section_end_idx]

  paste(c(general_lines, indicator_lines), collapse = "\n")
}

read_scripts <- function(paths, indicator_id) {
  scripts <- list()

  if (!is.null(paths$code_cleaning_instr))
    scripts$code_cleaning_instr <- paste(readLines(paths$code_cleaning_instr, warn = FALSE), collapse = "\n")

  if (!is.null(paths$code_cleaning))
    scripts$code_cleaning <- paste(readLines(paths$code_cleaning, warn = FALSE), collapse = "\n")

  if (!is.null(paths$code_processing))
    scripts$code_processing <- extract_indicator_section(paths$code_processing, indicator_id)

  keep(scripts, ~ !is.null(.x))
}

read_crosswalk <- function(paths) {
  if (is.null(paths$crosswalk)) return(NULL)
  read_xlsx(paths$crosswalk, sheet = paths$crosswalk_tab) %>%
    format_tsv() %>%
    paste(collapse = "\n")
}

upload_pdf <- function(pdf_path, api_key) {
  # Uploads a PDF to the Anthropic Files API and returns its file_id.
  response <- request("https://api.anthropic.com/v1/files") |>
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01",
      "anthropic-beta"    = "files-api-2025-04-14"
    ) |>
    req_body_multipart(file = curl::form_file(pdf_path, type = "application/pdf")) |>
    req_error(body = \(r) resp_body_string(r)) |>
    req_perform()
  resp_body_json(response)$id
}

load_pdfs <- function(pdf_dir, api_key) {
  # Returns document blocks referencing Anthropic file IDs.
  # Uploads any PDFs not already in the local cache; skips ones that are.
  if (is.null(pdf_dir) || !dir.exists(pdf_dir)) return(list())

  pdf_files <- list.files(pdf_dir, pattern = "\\.pdf$", full.names = TRUE)
  if (length(pdf_files) == 0) return(list())

  cache   <- if (file.exists(PDF_FILE_ID_CACHE)) fromJSON(PDF_FILE_ID_CACHE, simplifyVector = FALSE) else list()
  changed <- FALSE

  for (pdf_path in pdf_files) {
    if (is.null(cache[[pdf_path]])) {
      message(glue("  Uploading {basename(pdf_path)}..."))
      cache[[pdf_path]] <- upload_pdf(pdf_path, api_key)
      changed <- TRUE
    } else {
      message(glue("  Using cached file ID for {basename(pdf_path)}"))
    }
  }

  if (changed) {
    dir.create(dirname(PDF_FILE_ID_CACHE), showWarnings = FALSE, recursive = TRUE)
    writeLines(toJSON(cache, auto_unbox = TRUE, pretty = TRUE), PDF_FILE_ID_CACHE)
  }

  lapply(pdf_files, function(pdf_path) {
    list(
      type          = "document",
      source        = list(type = "file", file_id = cache[[pdf_path]]),
      title         = basename(pdf_path),
      cache_control = list(type = "ephemeral")
    )
  })
}

generate_draft <- function(indicator_id, api_key, system_prompt, user_prompt, pdf_blocks) {
  # Calls the Anthropic API and writes the English draft to a .txt file for review.
  # Returns the response text.
  content <- c(pdf_blocks, list(list(type = "text", text = user_prompt)))

  response <- request("https://api.anthropic.com/v1/messages") |>
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01",
      "anthropic-beta"    = "files-api-2025-04-14",
      "content-type"      = "application/json"
    ) |>
    req_body_json(list(
      model      = ANTHROPIC_MODEL,
      max_tokens = 4096,
      system     = trimws(system_prompt),
      messages   = list(list(role = "user", content = content))
    )) |>
    req_retry(max_tries = 3) |>
    req_error(body = \(r) resp_body_string(r)) |>
    req_perform()

  result        <- resp_body_json(response)
  response_text <- result$content[[1]]$text

  draft_path <- file.path(OUTPUT_DIR, glue("english_draft_{indicator_id}.txt"))
  writeLines(response_text, draft_path)
  message(glue("English draft written to: {draft_path}"))
  message("Review and edit the file, then call translate_to_spanish() to continue.")

  response_text
}

write_output <- function(indicator_id, generated_text) {
  output_path <- file.path(OUTPUT_DIR, paste0("metadata_draft_", indicator_id, ".xlsx"))

  # TODO: parse generated_text into individual fields once bulk upload column structure is confirmed
  output_df <- data.frame(
    indicator_id       = indicator_id,
    generated_metadata = generated_text
  )

  write_xlsx(output_df, output_path)
  message("Output written to: ", output_path)
}


# ---- main ----

message(glue("Processing indicator {indicator_id}..."))

api_key <- Sys.getenv("ANTHROPIC_API_KEY")
assert_that(nchar(api_key) > 0, msg = "ANTHROPIC_API_KEY not found. Please add it to your .Renviron file.")

paths             <- define_indicator_paths(indicator_id)
existing_metadata <- fetch_cepalstat_metadata(indicator_id)

message("Reading scripts...")
scripts <- read_scripts(paths, indicator_id)
message(glue("Loaded {length(scripts)} script(s): {paste(names(scripts), collapse = ', ')}"))

crosswalk_text <- read_crosswalk(paths)
if (!is.null(crosswalk_text)) {
  scripts$crosswalk <- crosswalk_text
  message("Crosswalk loaded.")
}

r_script_content <- scripts %>%
  imap(~ glue("--- {.y} ---\n{.x}")) %>%
  paste(collapse = "\n\n")

message("Loading PDF reference documents...")
global_pdf_blocks    <- load_pdfs(GLOBAL_DIR, api_key)
source_pdf_blocks    <- load_pdfs(paths$meta_source, api_key)
indicator_pdf_blocks <- load_pdfs(file.path(INDICATOR_DIR, indicator_id), api_key)
pdf_blocks <- c(global_pdf_blocks, source_pdf_blocks, indicator_pdf_blocks)
message(glue(
  "Loaded {length(global_pdf_blocks)} global PDF(s), ",
  "{length(source_pdf_blocks)} source PDF(s), and ",
  "{length(indicator_pdf_blocks)} indicator PDF(s)."
))

# Build prompts — edit these blocks to adjust what the model receives
message("Fetching golden standard metadata examples...")
example_block <- fetch_example_metadata(c(2487, 4174))

system_prompt <- paste(
  SYSTEM_PROMPT,
  "The following are examples of high-quality CEPALSTAT metadata to use as a reference for style, structure, and level of detail:\n\n",
  example_block,
  sep = "\n\n"
)

existing_metadata_text <- toJSON(existing_metadata, pretty = TRUE, auto_unbox = TRUE)

user_prompt <- paste0(
  "Indicator ID: ", indicator_id, "\n\n",
  "--- EXISTING METADATA ---\n", existing_metadata_text, "\n\n",
  "--- R PROCESSING SCRIPT ---\n", r_script_content, "\n\n",
  "Please revise the metadata fields (definition, calculation_methodology, comments) ",
  "based on the existing metadata, the R processing script, and the reference documents provided. ",
  "Keep other metadata elements exactly as-is."
)

# Call the Anthropic API
message("Calling Anthropic API...")
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
generated_text <- generate_draft(indicator_id, api_key, system_prompt, user_prompt, pdf_blocks)

cat(generated_text)

write_output(indicator_id, generated_text)
message("Done.")
