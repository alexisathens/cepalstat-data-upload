# Generates a draft metadata template for a given CEPALSTAT indicator.
# Fetches existing metadata from the CEPALSTAT API, reads the associated
# R processing script and any source documentation PDFs, and calls the
# Anthropic API to produce an updated draft in English and Spanish.

library(tidyverse)
library(readxl)
library(writexl)
library(httr2)
library(jsonlite)
library(glue)
library(here)
library(assertthat)
library(CepalStatR)
library(base64enc)  # Encoding PDFs for the Anthropic API


# =============================================================================
# Configuration
# Adjust paths to match your project directory structure.
# =============================================================================

PROJECT_ROOT  <- here::here()   # Assumes script is run from the project root

UNSD_PDF_DIR  <- file.path(PROJECT_ROOT, "Metadata", "Inputs", "UNSD references")     # Static UNSD reference PDFs
SOURCE_DIR <- file.path(PROJECT_ROOT, "Metadata", "Inputs", "By Source")          # Source-specific PDFs
INDICATOR_DIR <- file.path(PROJECT_ROOT, "Metadata", "Inputs", "By Indicator")          # Indicator-specific PDFs
SCRIPTS_DIR <- file.path(PROJECT_ROOT, "Scripts")          # Scripts path
OUTPUT_DIR    <- file.path(PROJECT_ROOT, "Metadata", "Outputs")                       # Where output Excel files are written

CEPALSTAT_API_URL <- "https://api-cepalstat.cepal.org/cepalstat/api/v1/indicator/{id}/metadata?lang=en&format=json"

ANTHROPIC_MODEL   <- "claude-sonnet-4-6"

# --- Set your indicator ID here ---
indicator_id <- 2487

# get metadata
meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))


# =============================================================================
# Step 1: Resolve indicator file paths
# Replace the internals with your actual path logic once known.
# =============================================================================

resolve_indicator_paths <- function(indicator_id) {
  # Returns a list of file paths associated with a given indicator.
  
  ind_source <- meta %>% filter(id == indicator_id) %>% pull(source)
  ind_dim <- meta %>% filter(id == indicator_id) %>% pull(dimensions)
  
  if(ind_source == "OLADE") {
    code_cleaning_instr <- file.path(SCRIPTS_DIR, "01_olade_instructions.qmd")
    code_cleaning <- file.path(SCRIPTS_DIR, "01_olade.R")
    code_processing <- file.path(SCRIPTS_DIR, "02_olade.R")
    
    meta_source <- file.path(SOURCE_DIR, "olade")
    meta_ind <- file.path(INDICATOR_DIR, indicator_id)
    
    crosswalk <- file.path(PROJECT_ROOT, "Data", "Raw", "olade", "energy_dimensions_crosswalk.xlsx")
    
    crosswalk_tab <- if (ind_dim == "Type of energy__Primary_and_Secondary") {
      "dimensions_crosswalk_44966"
    } else if (indicator_dim == "Energy intensity_Economic activity") {
      "dimensions_crosswalk_78134"
    } else {
      NULL  # No crosswalk for this indicator
    }
    
  } else {
    stop(glue("No path logic defined for source: {ind_source}"))
  }
  
  list(
    code_cleaning_instr = if (file.exists(code_cleaning_instr)) code_cleaning_instr else NULL,
    code_cleaning = if (file.exists(code_cleaning)) code_cleaning else NULL,
    code_processing = if (file.exists(code_processing)) code_processing else NULL,
    meta_source = if (dir.exists(meta_source)) meta_source else NULL,
    meta_ind = if (dir.exists(meta_ind)) meta_ind else NULL,
    crosswalk = if (!is.null(crosswalk_tab)) crosswalk else NULL,
    crosswalk_tab  = crosswalk_tab
  )
}


# =============================================================================
# Step 2: Fetch existing metadata from the CEPALSTAT API
# =============================================================================

fetch_existing_metadata <- function(indicator_id) {
  # Fetches current metadata for the indicator from the CEPALSTAT public API.
  
  url <- gsub("\\{id\\}", indicator_id, CEPALSTAT_API_URL)
  
  response <- request(url) |>
    req_perform()
  
  resp_body_json(response)
}


# =============================================================================
# Step 3: Read R scripts and cleaning instructions
# Reads all code files associated with the indicator.
# For the processing script, only the section relevant to the indicator is
# extracted. The cleaning and instructions files are passed in full since
# their content is not cleanly separated by indicator.
# =============================================================================

extract_indicator_section <- function(script_path, indicator_id) {
  # Extracts the portion of the processing script relevant to a given indicator.
  # Returns a string containing:
  #   - Any general content at the top of the script (before the THIRD section header)
  #   - The indicator-specific section identified by its header comment
  
  lines <- readLines(script_path, warn = FALSE)
  
  # Pattern matching any section header (e.g. "# ---- indicator 2487 ---- ")
  any_header_pattern    <- regex("^# ----", ignore_case = TRUE)
  
  # Pattern matching this indicator's specific section header
  this_header_pattern <- glue("(?i)# ---- indicator {indicator_id}")
  
  all_header_idx  <- which(str_detect(lines, any_header_pattern))
  this_header_idx <- which(str_detect(lines, this_header_pattern))
  
  # Warn and return NULL if the indicator section is not found
  if (length(this_header_idx) == 0) {
    warning(glue("No section found for indicator {indicator_id} in {basename(script_path)}"))
    return(NULL)
  }
  
  this_header_idx <- this_header_idx[1]
  
  # General content = everything before the first few section headers (setup; download files)
  general_lines <- if (all_header_idx[1] > 1) {
    lines[1:(all_header_idx[3] - 1)]
  } else {
    character(0)
  }
  
  # Indicator section = from this header until the line before the next header (or end of file)
  next_header_idx <- all_header_idx[all_header_idx > this_header_idx]
  section_end_idx <- if (length(next_header_idx) > 0) next_header_idx[1] - 1 else length(lines)
  indicator_lines <- lines[this_header_idx:section_end_idx]
  
  # Combine general content and indicator section
  paste(c(general_lines, indicator_lines), collapse = "\n")
}


read_scripts <- function(paths, indicator_id) {
  # Reads all code files associated with the indicator.
  # The processing script is filtered to the relevant indicator section;
  # the cleaning script and instructions file are passed in full.
  
  scripts <- list()
  
  # Cleaning instructions (.qmd) — passed in full; general documentation
  if (!is.null(paths$code_cleaning_instr)) {
    scripts$code_cleaning_instr <- paste(
      readLines(paths$code_cleaning_instr, warn = FALSE),
      collapse = "\n"
    )
  }
  
  # Cleaning script — passed in full; download logic is not separated by indicator
  if (!is.null(paths$code_cleaning)) {
    scripts$code_cleaning <- paste(
      readLines(paths$code_cleaning, warn = FALSE),
      collapse = "\n"
    )
  }
  
  # Processing script — extract only the relevant indicator section
  if (!is.null(paths$code_processing)) {
    scripts$code_processing <- extract_indicator_section(paths$code_processing, indicator_id)
  }
  
  # Drop any entries that came back NULL (e.g. section not found in processing script)
  keep(scripts, ~ !is.null(.x))
}

read_crosswalk <- function(paths) {
  # Reads the indicator-specific tab from the OLADE dimensions crosswalk.
  # Returns a formatted string representation of the table for inclusion
  # in the prompt, or NULL if no crosswalk exists for this indicator.
  
  if (is.null(paths$crosswalk)) return(NULL)
  
  read_xlsx(paths$crosswalk, sheet = paths$crosswalk_tab) %>%
    format_tsv() %>%                # Converts to tab-separated text for clean model input
    paste(collapse = "\n")
}


# =============================================================================
# Step 4: Load PDFs as base64 for the Anthropic API
# The Anthropic API accepts PDFs as base64-encoded document inputs.
# =============================================================================

load_pdfs <- function(pdf_dir) {
  # Loads all PDFs in a directory and returns them as a list of
  # Anthropic API-compatible document content blocks.
  # Returns an empty list if the directory does not exist.
  
  if (is.null(pdf_dir) || !dir.exists(pdf_dir)) return(list())
  
  pdf_files <- list.files(pdf_dir, pattern = "\\.pdf$", full.names = TRUE)
  
  lapply(pdf_files, function(pdf_path) {
    encoded <- base64encode(pdf_path)
    list(
      type   = "document",
      source = list(
        type       = "base64",
        media_type = "application/pdf",
        data       = encoded
      ),
      title = basename(pdf_path)   # Labels the document so the model can reference it by name
    )
  })
}


# =============================================================================
# Step 5: Build the prompt and call the Anthropic API
# =============================================================================

SYSTEM_PROMPT <- "
You are an expert in statistical metadata standards for international development indicators.
Your task is to draft metadata for CEPALSTAT environmental indicators following UNSD best
practices, as illustrated in the reference documents provided.

For each indicator, you will revise or draft the following four fields only:
  1. Definition
  2. Methodology
  3. Comments / additional information

The field 'Data characteristics' should always be left exactly as: 'Frequency: Annual / Coverage: National'

Write with precision and professional tone appropriate for a UN statistical system.
Avoid vague language. Cite units, data sources, and methodological steps explicitly.

Provide output first in English, then in Spanish.
"


call_anthropic_api <- function(indicator_id,
                               existing_metadata,
                               r_script_content,
                               unsd_pdf_blocks,
                               source_pdf_blocks) {
  # Assembles the full prompt and calls the Anthropic API.
  # Returns the model's response as a string.
  
  # Retrieve API key from environment (set in .Renviron)
  api_key <- Sys.getenv("ANTHROPIC_API_KEY")
  if (api_key == "") stop("ANTHROPIC_API_KEY not found. Please add it to your .Renviron file.")
  
  # Format existing metadata as text for the prompt
  existing_metadata_text <- toJSON(existing_metadata, pretty = TRUE, auto_unbox = TRUE)
  
  r_script_section <- if (nchar(r_script_content) > 0) {
    r_script_content
  } else {
    "No R script available for this indicator."
  }
  
  user_prompt <- paste0(
    "Indicator ID: ", indicator_id, "\n\n",
    "--- EXISTING METADATA ---\n", existing_metadata_text, "\n\n",
    "--- R PROCESSING SCRIPT ---\n", r_script_section, "\n\n",
    "Please revise the metadata fields (definition, calculation_methodology, comments) ",
    "based on the existing metadata, the R processing script, and the reference documents provided. ",
    "Keep other metadata elements exactly as-is. ",
    "Provide the output in English first, then in Spanish."
  )
  
  # Combine all document inputs: UNSD reference first, then indicator-specific
  all_documents <- c(unsd_pdf_blocks, source_pdf_blocks)
  
  # Build content block: documents first, then the text prompt
  content <- c(
    all_documents,
    list(list(type = "text", text = user_prompt))
  )
  
  # Build and send the API request
  response <- request("https://api.anthropic.com/v1/messages") |>
    req_headers(
      "x-api-key"         = api_key,
      "anthropic-version" = "2023-06-01",
      "content-type"      = "application/json"
    ) |>
    req_body_json(list(
      model      = ANTHROPIC_MODEL,
      max_tokens = 4096,
      system     = trimws(SYSTEM_PROMPT),
      messages   = list(
        list(role = "user", content = content)
      )
    )) |>
    req_retry(max_tries = 3) |>
    req_perform()
  
  # Extract the text response
  result <- resp_body_json(response)
  result$content[[1]]$text
}


# =============================================================================
# Step 6: Write output to Excel
# Format mirrors the CEPALSTAT metadata API structure, one field per row.
# Adjust column names once the bulk upload template is confirmed.
# =============================================================================

write_output <- function(indicator_id, generated_text) {
  # Writes the generated metadata to an Excel file.
  # Currently stores the full model response as a single cell — this will
  # be restructured into the bulk upload format once that template is confirmed.
  
  output_path <- file.path(OUTPUT_DIR, paste0("metadata_draft_", indicator_id, ".xlsx"))
  
  # TODO: Parse generated_text into individual fields and language rows
  # once the bulk upload column structure is confirmed.
  output_df <- data.frame(
    indicator_id       = indicator_id,
    generated_metadata = generated_text
  )
  
  write_xlsx(output_df, output_path)
  message("Output written to: ", output_path)
}


# =============================================================================
# Main
# =============================================================================

message(glue("Processing indicator {indicator_id}..."))

# Resolve file paths for this indicator
paths <- resolve_indicator_paths(indicator_id)

# Fetch existing metadata from CEPALSTAT API
message("Fetching existing metadata from CEPALSTAT API...")
existing_metadata <- fetch_existing_metadata(indicator_id)

# Read R scripts (processing script filtered to indicator section; others in full)
message("Reading scripts...")
scripts <- read_scripts(paths, indicator_id)
message(glue("Loaded {length(scripts)} script(s): {paste(names(scripts), collapse = ', ')}"))

# Read crosswalk if one exists for this indicator, and append to scripts list
crosswalk_text <- read_crosswalk(paths)
if (!is.null(crosswalk_text)) {
  scripts$crosswalk <- crosswalk_text
  message("Crosswalk loaded.")
}

# Collapse named scripts list into a single labelled string for the prompt
r_script_content <- scripts %>%
  imap(~ glue("--- {.y} ---\n{.x}")) %>%
  paste(collapse = "\n\n")

# Load PDFs from all three input directories
# Order: UNSD references first, then source-specific, then indicator-specific
message("Loading PDF reference documents...")
unsd_pdf_blocks <- load_pdfs(UNSD_PDF_DIR)
source_pdf_blocks <- c(
  load_pdfs(paths$meta_source),
  load_pdfs(paths$meta_ind)
)
message(glue(
  "Loaded {length(unsd_pdf_blocks)} UNSD PDF(s) and ",
  "{length(source_pdf_blocks)} source/indicator PDF(s)."
))

# Call the Anthropic API
message("Calling Anthropic API...")
generated_text <- call_anthropic_api(
  indicator_id      = indicator_id,
  existing_metadata = existing_metadata,
  r_script_content  = r_script_content,
  unsd_pdf_blocks   = unsd_pdf_blocks,
  source_pdf_blocks = source_pdf_blocks
)

cat(generated_text)

# Write output
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)
write_output(indicator_id, generated_text)
message("Done.")

