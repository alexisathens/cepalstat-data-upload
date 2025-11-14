library(httr2)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(readxl)
library(here)
library(magrittr)

utils::globalVariables(c(
  "emissions", "year", "value",
  "iso3", "wb_iso_code3", "wb_iso3",
  "population", "gdp_constant_2015_usd",
  "countryiso3code", "country", ".", "lookup_iso3"
))

# Visit: https://www.climatewatchdata.org/data-explorer/historical-emissions? 
# For information on the API and bulk data downloads

# Read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>% 
  filter(ECLACa == "Y" | name == "World") %>% 
  mutate(
    iso3 = ifelse(.data$name == "World", "WORLD", .data$iso3),
    wb_iso3 = ifelse(.data$iso3 == "WORLD", "WLD", .data$iso3)
  ) %>% 
  distinct(across(all_of(c("iso3", "wb_iso3", "std_name"))))

# Lookup table for converting between WB and CEPAL iso codes
iso_lookup_wb <- iso %>% select(all_of(c("iso3", "wb_iso3")))

# Get ISO codes for LAC countries and World
regions_iso <- iso %>% filter(!.data$iso3 %in% c("LAA", "CAA", "CAR")) %>% pull("iso3")

# Get base API URL
base <- "https://www.climatewatchdata.org/api/v1/data/historical_emissions"

# Get available filter options
data_sources <- request(paste0(base, "/data_sources")) %>%
  req_perform() %>%
  resp_body_json(simplifyVector = TRUE) %>%
  pluck("data") %>%
  as_tibble()

sectors <- request(paste0(base, "/sectors")) %>%
  req_perform() %>%
  resp_body_json(simplifyVector = TRUE) %>%
  pluck("data") %>%
  as_tibble()

gases <- request(paste0(base, "/gases")) %>%
  req_perform() %>%
  resp_body_json(simplifyVector = TRUE) %>%
  pluck("data") %>%
  as_tibble()

# Helper function to download data with filters
# Note: The API is paginated, so we may need to handle multiple pages
cw_get_data <- function(source_ids = NULL,
                        gas_ids = NULL,
                        sector_ids = NULL,
                        regions = NULL,
                        start_year = NULL,
                        end_year = NULL,
                        max_pages = 1000) {
  
  # Build URL - use base endpoint
  url <- base
  
  # Build query string manually to handle array parameters
  query_parts <- c()
  
  # Add array parameters - repeat key for each value
  if (!is.null(source_ids)) {
    # Ensure it's a vector
    source_ids <- as.vector(source_ids)
    for (id in source_ids) {
      query_parts <- c(query_parts, paste0("source_ids[]=", id))
    }
  }
  if (!is.null(gas_ids)) {
    gas_ids <- as.vector(gas_ids)
    for (id in gas_ids) {
      query_parts <- c(query_parts, paste0("gas_ids[]=", id))
    }
  }
  if (!is.null(sector_ids)) {
    sector_ids <- as.vector(sector_ids)
    for (id in sector_ids) {
      query_parts <- c(query_parts, paste0("sector_ids[]=", id))
    }
  }
  if (!is.null(regions)) {
    regions <- as.vector(regions)
    for (region in regions) {
      # Don't URL encode if it's already a simple ISO code
      query_parts <- c(query_parts, paste0("regions[]=", region))
    }
  }
  if (!is.null(start_year)) {
    query_parts <- c(query_parts, paste0("start_year=", start_year))
  }
  if (!is.null(end_year)) {
    query_parts <- c(query_parts, paste0("end_year=", end_year))
  }
  
  # Build full URL with query string
  if (length(query_parts) > 0) {
    query_string <- paste(query_parts, collapse = "&")
    url <- paste0(url, "?", query_string)
  }
  
  # Collect all data across pages
  all_data <- list()
  page <- 1
  has_next_page <- TRUE
  
  # Print URL for debugging
  print(paste("Fetching data from:", url))
  
  while (has_next_page && page <= max_pages) {
    # Add page parameter
    page_url <- url
    if (page > 1) {
      separator <- ifelse(grepl("\\?", url), "&", "?")
      page_url <- paste0(url, separator, "page=", page)
    }
    
    # Make request and parse response
    tryCatch({
      resp <- request(page_url) %>%
        req_perform()
      
      result <- resp %>%
        resp_body_json(simplifyVector = TRUE)
      
      # Check if we have data
      if (is.null(result$data) || length(result$data) == 0) {
        has_next_page <- FALSE
        break
      }
      
      all_data[[page]] <- result$data
      
      # Check Link header for pagination info
      link_header <- resp_headers(resp)$Link
      
      if (!is.null(link_header)) {
        # Parse Link header to check for "next" relation
        # Link header format: <url>; rel="next", <url>; rel="last"
        has_next <- grepl('rel="next"', link_header) || grepl("rel='next'", link_header)
        has_next_page <- has_next
      } else {
        # If no Link header, check if we got a full page (50 items)
        # If less than 50, assume we're done
        has_next_page <- length(result$data) >= 50
      }
      
      if (has_next_page) {
        page <- page + 1
      }
      
    }, error = function(e) {
      # If we get an error (like 404 on next page), stop pagination
      if (page == 1) {
        stop("API request failed: ", e$message, "\nURL: ", page_url)
      }
      has_next_page <<- FALSE
      return(NULL)
    })
  }
  
  # Combine all pages
  if (length(all_data) > 0) {
    data <- bind_rows(all_data) %>% 
      as_tibble()
    
    # Filter by regions if specified (in case API filtering didn't work)
    if (!is.null(regions) && "iso_code3" %in% names(data)) {
      regions_vec <- as.vector(regions)
      data <- data %>%
        filter(.data$iso_code3 %in% regions_vec)
    }
    
    # Process the emissions array into year/value columns
    # The API returns emissions as a nested list
    if ("emissions" %in% names(data)) {
      data <- data %>%
        tidyr::unnest(cols = all_of("emissions"), keep_empty = TRUE) %>%
        pivot_wider(names_from = all_of("year"), values_from = all_of("value"), names_prefix = "")
    }
    
    return(data)
  } else {
    return(tibble())
  }
}

# Find IDs for common filters
source_id_climate_watch <- data_sources %>% 
  filter(.data$name == "Climate Watch") %>% 
  pull(id) %>% 
  first()

gas_id_all_ghg <- gases %>% 
  filter(str_detect(tolower(.data$name), "all ghg|all greenhouse")) %>% 
  pull(id) %>% 
  first()

gas_id_co2 <- gases %>% 
  filter(str_detect(tolower(.data$name), "^co2$|carbon dioxide")) %>% 
  pull(id) %>% 
  first()

sector_id_total_no_lucf <- sectors %>% 
  filter(data_source_id == source_id_climate_watch) %>% 
  filter(str_detect(tolower(.data$name), "total excluding lucf")) %>% 
  pull(id) %>% 
  first()

# Create output directory if it doesn't exist
output_dir <- "Data/Raw/climate watch"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# ============================================================================
# World Bank Data Functions for Population and GDP
# ============================================================================

# Function to download World Bank data
# Indicators: SP.POP.TOTL (population), NY.GDP.MKTP.KD (GDP constant 2015 US$)
wb_get_data <- function(indicator, countries = NULL, start_date = 1990, end_date = NULL) {
  base_url <- "https://api.worldbank.org/v2/country"
  
  # Build country list - use all if not specified
  if (is.null(countries) || length(countries) == 0) {
    wb_countries <- iso_lookup_wb$wb_iso3
  } else {
    wb_countries <- iso_lookup_wb$wb_iso3[iso_lookup_wb$iso3 %in% countries] %>% 
      unique()
    
    # Fallback to provided countries if not found in lookup
    if (length(wb_countries) == 0) {
      wb_countries <- unique(countries)
    }
  }
  wb_countries <- wb_countries[!is.na(wb_countries)]
  country_list <- ifelse(length(wb_countries) > 0, paste(wb_countries, collapse = ";"), "all")
  
  # Build URL
  url <- paste0(base_url, "/", country_list, "/indicator/", indicator, "?format=json&date=", 
                start_date, ":", ifelse(is.null(end_date), format(Sys.Date(), "%Y"), end_date),
                "&per_page=10000")
  
  # Make request
  result <- request(url) %>%
    req_perform() %>%
    resp_body_json(simplifyVector = TRUE)
  
  # Extract data from second element (first is metadata)
  if (length(result) >= 2 && !is.null(result[[2]])) {
    data <- result[[2]] %>%
      as_tibble() %>%
      select(.data$countryiso3code, .data$country, .data$date, .data$value) %>%
      rename(
        wb_iso_code3 = .data$countryiso3code, 
        country_name = .data$country,
        year = .data$date
      ) %>%
      mutate(year = as.numeric(.data$year),
             value = as.numeric(.data$value))
    
    # Convert WB iso codes back to CEPAL iso codes
    iso_lookup_wb_join <- iso_lookup_wb
    iso_lookup_wb_join$lookup_iso3 <- iso_lookup_wb_join[["iso3"]]
    iso_lookup_wb_join[["iso3"]] <- NULL
    
    data <- data %>%
      left_join(iso_lookup_wb_join, by = c("wb_iso_code3" = "wb_iso3")) %>%
      mutate(
        iso_code3 = dplyr::coalesce(.data$lookup_iso3, .data$wb_iso_code3)
      ) %>%
      select(.data$iso_code3, .data$country_name, .data$year, .data$value)
    
    # Filter by countries if specified
    if (!is.null(countries)) {
      countries_vec <- as.vector(countries)
      data <- data %>%
        filter(.data$iso_code3 %in% countries_vec | is.na(.data$iso_code3))
    }
    
    return(data)
  } else {
    return(tibble(iso_code3 = character(), country_name = character(), 
                  year = numeric(), value = numeric()))
  }
}

# Function to append population and GDP denominators to emissions data
prepare_emissions_with_denominators <- function(emissions_data,
                                                include_population = TRUE,
                                                include_gdp = TRUE) {
  
  if (!"iso_code3" %in% names(emissions_data)) {
    warning("No iso_code3 column found in emissions data")
    return(emissions_data)
  }
  
  year_cols <- names(emissions_data)[sapply(names(emissions_data), function(x) {
    suppressWarnings(!is.na(as.numeric(x)))
  })]
  
  if (length(year_cols) == 0) {
    warning("No year columns found in emissions data")
    return(emissions_data)
  }
  
  years <- as.numeric(year_cols)
  countries <- unique(emissions_data$iso_code3[!is.na(emissions_data$iso_code3)])
  
  pop_data <- NULL
  gdp_data <- NULL
  
  if (isTRUE(include_population)) {
    message("Downloading World Bank population data...")
    pop_data <- wb_get_data("SP.POP.TOTL",
                            countries = countries,
                            start_date = min(years, na.rm = TRUE),
                            end_date = max(years, na.rm = TRUE)) %>%
      rename(population = .data$value) %>%
      select(.data$iso_code3, .data$year, .data$population)
  }
  
  if (isTRUE(include_gdp)) {
    message("Downloading World Bank GDP data...")
    gdp_data <- wb_get_data("NY.GDP.MKTP.KD",
                            countries = countries,
                            start_date = min(years, na.rm = TRUE),
                            end_date = max(years, na.rm = TRUE)) %>%
      rename(gdp_constant_2015_usd = .data$value) %>%
      select(.data$iso_code3, .data$year, .data$gdp_constant_2015_usd)
  }
  
  static_cols <- setdiff(names(emissions_data), year_cols)
  
  emissions_long <- emissions_data %>%
    select(all_of(static_cols), all_of(year_cols)) %>%
    pivot_longer(cols = all_of(year_cols), names_to = "year", values_to = "emissions") %>%
    mutate(year = as.numeric(.data$year))
  
  combined_long <- emissions_long %>%
    left_join(pop_data, by = c("iso_code3", "year")) %>%
    left_join(gdp_data, by = c("iso_code3", "year"))
  
  combined_long %>%
    mutate(year = as.numeric(.data$year)) %>%
    select(any_of(c(
      "iso_code3",
      "country",
      "sector",
      "gas",
      "unit",
      "year",
      "emissions",
      "population",
      "gdp_constant_2015_usd"
    )))
}

# Helper to reshape indicator output before export
format_indicator_output <- function(data) {
  if (is.null(data) || nrow(data) == 0) {
    return(tibble())
  }
  
  data <- data %>%
    select(-any_of(c("id", "iso", "data_source")))
  
  if ("year" %in% names(data)) {
    data <- data %>%
      mutate(
        year = as.integer(.data$year),
        value = as.numeric(.data$value)
      )
  } else {
    year_cols <- names(data)[grepl("^\\d{4}$", names(data))]
    
    if (length(year_cols) == 0) {
      warning("No year columns found when formatting output")
      return(data)
    }
    
    data <- data %>%
      pivot_longer(cols = all_of(year_cols), names_to = "year", values_to = "value") %>%
      mutate(
        year = as.integer(.data$year),
        value = as.numeric(.data$value)
      )
  }
  
  data
}

## ---- indicator 3351 - greenhouse gas (ghg) emissions by sector ----

# Find sector IDs
sector_id_energy <- sectors %>% 
  filter(str_detect(tolower(.data$name), "^energy$|^energy sector$")) %>% 
  pull(id) %>% 
  first()

sector_id_industrial <- sectors %>% 
  filter(str_detect(tolower(.data$name), "industrial process")) %>% 
  pull(id) %>% 
  first()

sector_id_agriculture <- sectors %>% 
  filter(str_detect(tolower(.data$name), "^agriculture$|agricultural")) %>% 
  pull(id) %>% 
  first()

sector_id_waste <- sectors %>% 
  filter(str_detect(tolower(.data$name), "^waste$")) %>% 
  pull(id) %>% 
  first()

sector_ids_3351 <- c(sector_id_energy, sector_id_industrial, sector_id_agriculture, sector_id_waste)

indicator_3351 <- cw_get_data(
  source_ids = source_id_climate_watch,
  regions = regions_iso,
  sector_ids = sector_ids_3351,
  gas_ids = gas_id_all_ghg
)

# Reshape and clean output
indicator_3351 <- format_indicator_output(indicator_3351)

write.csv(indicator_3351, 
          file = file.path(output_dir, "3351_raw.csv"),
          row.names = FALSE)

## ---- indicator 3158 - carbon dioxide (co₂) emissions, total excluding land-use change and forestry (lucf) ----

indicator_3158_raw <- cw_get_data(
  source_ids = source_id_climate_watch,
  regions = regions_iso,
  sector_ids = sector_id_total_no_lucf,
  gas_ids = gas_id_co2
)

# Reshape and clean output
indicator_3158 <- format_indicator_output(indicator_3158_raw)

write.csv(indicator_3158, 
          file = file.path(output_dir, "3158_raw.csv"),
          row.names = FALSE)

## ---- indicator 3159 - share of carbon dioxide (co₂) emissions relative to the global total ----

# Use same data as 3158
indicator_3159 <- format_indicator_output(indicator_3158_raw)

write.csv(indicator_3159, 
          file = file.path(output_dir, "3159_raw.csv"),
          row.names = FALSE)

## ---- indicator 2027 - carbon dioxide (co₂) emissions (total, per capita, and per gdp) ----

indicator_2027_raw <- cw_get_data(
  source_ids = source_id_climate_watch,
  regions = regions_iso,
  sector_ids = sector_id_total_no_lucf,
  gas_ids = gas_id_co2
)

# Append population and GDP denominators
indicator_2027 <- prepare_emissions_with_denominators(
  indicator_2027_raw,
  include_population = TRUE,
  include_gdp = TRUE
)

write.csv(indicator_2027, 
          file = file.path(output_dir, "2027_raw.csv"),
          row.names = FALSE)


## end anuario exports, check the rest later


# ## ---- indicator 4463 - greenhouse gas (ghg) emissions per gdp ----
# 
# indicator_4463 <- cw_get_data(
#   source_ids = source_id_climate_watch,
#   regions = regions_iso,
#   sector_ids = sector_id_total_no_lucf,
#   gas_ids = gas_id_all_ghg
# )
# 
# # Calculate per GDP values
# indicator_4463_per_gdp <- calculate_per_capita_gdp(indicator_4463, calculation_type = "per_gdp")
# 
# # Combine total and per GDP
# indicator_4463 <- bind_rows(
#   indicator_4463 %>% mutate(calculation = "Total"),
#   indicator_4463_per_gdp %>% mutate(calculation = "Per GDP")
# )
# 
# # Reshape and clean output
# indicator_4463 <- format_indicator_output(indicator_4463)
# 
# write.csv(indicator_4463, 
#           file = file.path(output_dir, "4463_raw.csv"),
#           row.names = FALSE)
# 
# ## ---- indicator 4462 - greenhouse gas (ghg) emissions from the energy sector ----
# 
# # Find sector IDs for energy subsectors
# sector_ids_energy <- sectors %>% 
#   filter(str_detect(tolower(.data$name), "electricity|heat|manufacturing|construction|transport|fuel combustion|fugitive") &
#          str_detect(tolower(.data$name), "energy")) %>% 
#   pull(id)
# 
# # If no subsectors found, try finding "Energy" sector parent
# if (length(sector_ids_energy) == 0) {
#   sector_ids_energy <- sectors %>% 
#     filter(str_detect(tolower(.data$name), "^energy$|^energy sector$")) %>% 
#     pull(id) %>% 
#     first()
# }
# 
# indicator_4462 <- cw_get_data(
#   source_ids = source_id_climate_watch,
#   regions = regions_iso,
#   sector_ids = sector_ids_energy,
#   gas_ids = gas_id_all_ghg
# )
# 
# # Reshape and clean output
# indicator_4462 <- format_indicator_output(indicator_4462)
# 
# write.csv(indicator_4462, 
#           file = file.path(output_dir, "4462_raw.csv"),
#           row.names = FALSE)
# 
# ## ---- indicator 4461 - greenhouse gas (ghg) emissions per capita ----
# 
# indicator_4461 <- cw_get_data(
#   source_ids = source_id_climate_watch,
#   regions = regions_iso,
#   sector_ids = sector_id_total_no_lucf,
#   gas_ids = gas_id_all_ghg
# )
# 
# # Calculate per capita values
# indicator_4461_per_capita <- calculate_per_capita_gdp(indicator_4461, calculation_type = "per_capita")
# 
# # Combine total and per capita
# indicator_4461 <- bind_rows(
#   indicator_4461 %>% mutate(calculation = "Total"),
#   indicator_4461_per_capita %>% mutate(calculation = "Per capita")
# )
# 
# # Reshape and clean output
# indicator_4461 <- format_indicator_output(indicator_4461)
# 
# write.csv(indicator_4461, 
#           file = file.path(output_dir, "4461_raw.csv"),
#           row.names = FALSE)
# 
# 
# 
# 
# 
# ## ---- indicator 3387 - share of greenhouse gas (ghg) emissions relative to the global total ----
# 
# indicator_3387 <- cw_get_data(
#   source_ids = source_id_climate_watch,
#   regions = regions_iso,
#   sector_ids = sector_id_total_no_lucf,
#   gas_ids = gas_id_all_ghg
# )
# 
# # Reshape and clean output
# indicator_3387 <- format_indicator_output(indicator_3387)
# 
# write.csv(indicator_3387, 
#           file = file.path(output_dir, "3387_raw.csv"),
#           row.names = FALSE)


