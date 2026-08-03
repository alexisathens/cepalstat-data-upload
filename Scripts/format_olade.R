library(tidyverse)
library(magrittr)
library(readxl)
library(httr2)
library(jsonlite)
library(glue)
library(writexl)
library(here)
library(assertthat)
library(CepalStatR)

# This script processes OLADE energy indicators using the automated process_indicator() function

input_path <- here("Data/Raw/olade")
output_path <- here("Data/Raw/olade")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

# ---- cleaning functions ----

# Generic cleaning function for files with countries as rows
clean_olade_standard <- function(input_file, output_file) {

  # Read data
  data <- read_excel(paste0(input_path, "/", input_file), col_names = FALSE)
  
  # Extract and apply column headers from row 3; rename first column to "Country"
  header_row <- unlist(data[3, ], use.names = FALSE) %>% str_trim()
  header_row[1] <- "Country"
  
    # Remove header and unit rows, then apply column names
  data <- data[-c(3,4), ]
  colnames(data) <- header_row
  
  # Extract year from rows where Country field contains a 4-digit year,
  # fill downward to tag each country block, then remove those year header rows
  data %<>%
    mutate(Years = str_extract(Country, "\\b\\d{4}$")) %>%
    fill(Years, .direction = "down") %>%
    select(Country, Years, everything()) %>%
    filter(!str_detect(Country, "\\b\\d{4}$"), Country != "Supply and demand series")
  
  # Standardize country names against ISO reference table
  data %<>%
    left_join(iso %>% select(name, std_name), by = c("Country" = "name")) %>%
    mutate(Country = coalesce(std_name, Country)) %>%
    select(-std_name)
  
  # Check which countries don't match to ISO (uncomment to diagnose)
  # data %>%
  #   filter(!Country %in% iso$name) %>%
  #   distinct(Country) %>%
  #   print()
  
  # Filter to LAC countries, excluding regional aggregates
  data %<>%
    filter(Country %in% iso$name, !Country %in% c("Central America", "South America", "Caribbean"))
  
  # Pivot to long format and drop rows with no reported value
  data %<>%
    pivot_longer(cols = -c(Country, Years), names_to = "Type", values_to = "value") %>%
    mutate(value = as.numeric(value), Years = as.numeric(Years)) %>%
    filter(!is.na(value))
  
  # Export
  write.csv(data, file = file.path(output_path, output_file), row.names = FALSE)
  message(glue("✓ Exported {output_file}"))
  return(data)
}


# ---- total energy production ----

production <- clean_olade_standard("energy_production_raw.xlsx", "energy_production_clean.csv")

# ---- total energy supply ----

supply <- clean_olade_standard("energy_supply_raw.xlsx", "energy_supply_clean.csv")

# ---- total final consumption ----

consumption <- clean_olade_standard("energy_consumption_raw.xlsx", "energy_consumption_clean.csv")

# ---- final consumption by sector ----

consumption_sector <- clean_olade_standard("energy_consumption_sector_raw.xlsx", "energy_consumption_sector_clean.csv")

# ---- electricity losses ----

losses <- clean_olade_standard("electricity_losses_raw.xlsx", "electricity_losses_clean.csv")

# ---- electricity infrastructure ----
# This needs a separate cleaning function because the rows are energy types, columns are countries, and tabs are years

# Cleaning function for OLADE files structured with energy types as rows and
# countries as columns, with one tab per year (e.g., installed capacity).
# Iterates over all tabs, reshapes each to the standard country-as-row format,
# stacks into a single long data frame, and passes through the generic cleaner.
clean_olade_infra <- function(input_file, output_file) {
  
  file_path <- paste0(input_path, "/", input_file)
  sheet_names <- excel_sheets(file_path)
  
  # --- Step 1: Read and reshape each tab, then stack into one data frame -----
  
  combined <- map(sheet_names, function(sheet) {
    
    raw <- read_excel(file_path, sheet = sheet, col_names = FALSE)
    
    # Row 2 contains year metadata (e.g., "1991 - Electricity - Installed capacity")
    year <- str_extract(raw[[1]][2], "\\b\\d{4}\\b")
    
    # Row 3 is the header: "Description", "Unit", then country names
    # Rows 4 onward are energy type data rows
    headers <- unlist(raw[3, ], use.names = FALSE) %>% str_trim()
    data <- raw[4:nrow(raw), ]
    colnames(data) <- headers
    
    # Drop rows without a valid energy type label (footnotes, blanks, source row)
    #data %<>% filter(!is.na(Description), str_trim(Description) != "")
    data %<>% 
      filter(str_trim(Description) %in% c("Nuclear", "Non-renewable thermal (combustion)", "Renewable thermal (combustion)", "Hydro", "Geothermal", "Wind", "Solar"))
    
    # Trim leading whitespace from energy type labels (some are indented in source)
    data %<>% mutate(Description = str_trim(Description))
    
    # Drop the Unit column — unit metadata is tracked in indicator_metadata.xlsx
    data %<>% select(-Unit)
    
    # Transpose: pivot so countries become rows and energy types become columns,
    # matching the country-as-row layout expected by clean_olade_standard()
    data %<>%
      pivot_longer(cols = -Description, names_to = "Country", values_to = "value") %>%
      pivot_wider(names_from = Description, values_from = value)
    
    # Prepend a year header row in the format clean_olade_standard() expects:
    # a row where the Country field holds the 4-digit year, signaling a year block
    year_row <- tibble(Country = year) %>%
      bind_cols(tibble(!!!setNames(
        rep(list(NA_character_), ncol(data) - 1),
        colnames(data)[-1]
      )))
    
    bind_rows(year_row, data)
    
  }) %>% bind_rows()
  
  # --- Step 2: Write to a temp file in the expected structure -----------------
  
  # clean_olade_standard() reads column names from row 3 of the raw Excel file.
  # write_xlsx() always writes the data frame header as row 1, so we embed the
  # column names as an explicit data row and suppress the automatic header,
  # placing rows 1-2 as blanks and the column names in row 3 as expected.
  header_row <- tibble(!!!setNames(
    as.list(colnames(combined)),
    colnames(combined)
  ))
  
  dummy_row <- tibble(!!!setNames(
    rep(list("_"), ncol(combined)),
    colnames(combined)
  ))
  
  combined_padded <- bind_rows(dummy_row, dummy_row, header_row, combined)
  
  temp_file <- file.path(input_path, "_temp_infra.xlsx")
  write_xlsx(combined_padded, path = temp_file, col_names = FALSE)
  
  # --- Step 3: Pass through the generic cleaner, then remove the temp file ---
  
  result <- clean_olade_standard(
    input_file = "_temp_infra.xlsx",
    output_file = output_file
  )
  
  on.exit(file.remove(temp_file), add = TRUE)
  
  return(result)
}

# Run custom function
infra <- clean_olade_infra("electricity_infra_raw.xlsx", "electricity_infra_clean.csv")
