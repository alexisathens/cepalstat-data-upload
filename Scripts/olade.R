# This script processes OLADE energy indicators using the automated process_indicator() function
# See data cleaning notes at: cepalstat-data-upload\Data\Raw\olade\energy_indicators_overview.xlsx

# ---- setup ----

library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# Define last year of full OLADE data
max_year_olade <- 2025

# read energy type dimension mappings
input_path <- here("Data/Raw/olade")
energy_types <- read_excel(paste0(input_path, "/energy_dimensions_crosswalk.xlsx"))

# get mappings from olade energy sectors to cepalstat energy sectors
energy_econ_sectors <- read_excel(paste0(input_path, "/energy_dimensions_crosswalk.xlsx"), sheet = "dimensions_crosswalk_78134")


# ---- read downloaded files ----

data_prod <- read_csv(paste0(input_path, "/energy_production_clean.csv"))
data_supply <- read_csv(paste0(input_path, "/energy_supply_clean.csv"))
data_cons <- read_csv(paste0(input_path, "/energy_consumption_clean.csv"))
data_cons_sec <- read_csv(paste0(input_path, "/energy_consumption_sector_clean.csv"))
data_losses <- read_csv(paste0(input_path, "/electricity_losses_clean.csv"))
data_infra <- read_csv(paste0(input_path, "/electricity_infra_clean.csv"))

# ---- shared functions ----

# Shared no-op filter
filter_none <- function(data) data

# ---- indicator 5672 — energy production ----

dim_config_5672 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name")
)

transform_5672 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # summarize by energy type
    group_by(Country, Years, type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(Type = type)
}

spec_5672 <- indicator_spec(
  indicator_id = 5672,
  data = data_prod,
  max_year = max_year_olade,
  dim_config = dim_config_5672,
  filter_data = filter_none,
  transform_data = transform_5672,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 2487 — primary and secondary energy supply ----

dim_config_2487 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name")
)

transform_2487 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # summarize by energy type
    group_by(Country, Years, type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(Type = type)
}

footnotes_2487 <- list(
  "5896" = function(df) df$Type == "Total primaries", # Includes the following energy resources: petroleum, natural gas, coal, hydroenergy, geothermal, nuclear, firewood, cane bagasse, wind, solar, ethanol, biodiesel, biogas, other biomass and other primary sources.
  "5897" = function(df) df$Type == "Total secondaries" # Includes the following energy resources: electricity, liquefied petroleum gas, gasoline/alcohol, kerosene/jet fuel, diesel oil, fuel oil, coke, charcoal, gases, other secondary and non-energy sources.
)

spec_2487 <- indicator_spec(
  indicator_id = 2487,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_2487,
  filter_data = filter_none,
  transform_data = transform_2487,
  calculate_regional = maintain_regional, # keep source LAC data from OLADE
  footnotes = footnotes_2487
)

# ---- indicator 5730 — final energy consumption ----

dim_config_5730 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44966"),
  pub_col = c("208_name", "29117_name", "44966_name")
)

transform_5730 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # summarize by energy type
    group_by(Country, Years, type) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(Type = type)
}

spec_5730 <- indicator_spec(
  indicator_id = 5730,
  data = data_cons,
  max_year = max_year_olade,
  dim_config = dim_config_5730,
  filter_data = filter_none,
  transform_data = transform_5730,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 2486 — primary energy supply from renewable and non-renewable sources, by type of energy ----

dim_config_2486 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name")
)

transform_2486 <- function(data) {
  data %<>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # create clean/renewable groupings
    filter(order == "Primary") %>% 
    mutate(cat = ifelse(is.na(renewable), "Non-renewable energy",
                        ifelse(is.na(clean), "Renewable energy not clean", "Clean renewable energy"))) %>%
    # summarize to country-year-type level
    group_by(Country, Years, type, cat) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
  
  # create category sub-totals
  subtotals <- data %>% 
    group_by(Country, Years, cat) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    rename(type = cat)
  
  # join together and format
  data %>% 
    select(-cat) %>% 
    bind_rows(subtotals) %>% 
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    mutate(type = ifelse(type == "Other primary", "Other clean", type)) %>% # adjust one label for this dimension
    rename(Type = type) %>% 
    filter(!is.na(value))
}

spec_2486 <- indicator_spec(
  indicator_id = 2486,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_2486,
  filter_data = filter_none,
  transform_data = transform_2486,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 3154 — renewable energy share of primary energy supply ----

dim_config_3154 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

transform_3154 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% select(type, olade_type, renewable) %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    # keep OLADE subtotals and totals (since Total can't be calculated directly from data)
    mutate(type = ifelse(is.na(type), olade_type, type)) %>% 
    # filter on renewables or total primaries
    filter(type == "Total primaries" | renewable == "Y") %>% 
    mutate(renewable = ifelse(!is.na(renewable), "Renewable", "Total")) %>%
    # calculate renewable share of total primary energy
    group_by(Country, Years, renewable) %>%
    summarize(value = sum_or_na(value), .groups = "drop") %>%
    pivot_wider(names_from = renewable) %>%
    mutate(value = Renewable / Total * 100) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

spec_3154 <- indicator_spec(
  indicator_id = 3154,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_3154,
  filter_data = filter_none,
  transform_data = transform_3154,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4236 — composition of renewable primary energy supply, by type of energy ----

dim_config_4236 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "44959"),
  pub_col = c("208_name", "29117_name", "44959_name")
)

transform_4236 <- function(data) {
  data %>% 
    # merge CEPALSTAT energy labels
    rename(olade_type = Type) %>% 
    left_join(energy_types %>% fill(type, .direction = "down"),
              by = c("olade_type")) %>% 
    filter(renewable == "Y") %>% 
    # Calculate share of renewable primary energy, broken out by type
    group_by(Country, Years) %>%
    mutate(total = sum(value, na.rm = TRUE)) %>%
    ungroup() %>% 
    mutate(share = round(value / total * 100, 1)) %>%
    mutate(type = ifelse(type == "Other primary", "Other clean", type)) %>% # adjust one label for this dimension
    select(Country, Years, type, share) %>%
    rename(Type = type, value = share) %>%
    filter(!is.na(value))
}

spec_4236 <- indicator_spec(
  indicator_id = 4236,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_4236,
  filter_data = filter_none,
  transform_data = transform_4236,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4174 — energy intensity (primary energy supply / GDP) ----

dim_config_4174 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4174 <- function(data) {
  data %>%
    filter(Type == "Total primaries") %>%
    select(-Type)
}

transform_4174 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dolllars
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join PIB data and calculate energy intensity
  data %>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(supply = value) %>%
    mutate(value = supply / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

spec_4174 <- indicator_spec(
  indicator_id = 4174,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_4174,
  filter_data = filter_4174,
  transform_data = transform_4174,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4183 — change in energy intensity (primary energy supply / GDP) ----

dim_config_4183 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4183 <- function(data) {
  data %>%
    filter(Type == "Total primaries") %>%
    select(-Type)
}

transform_4183 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dollars (millions)
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join GDP data and calculate energy intensity
  data %<>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(supply = value) %>%
    mutate(value = supply / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
  
  # Calculate variation rate: ((Mt - Mt-1) / Mt-1) * 100
  data %>%
    arrange(Country, Years) %>%
    group_by(Country) %>%
    mutate(
      value_prev = lag(value),
      value = ((value - value_prev) / value_prev) * 100
    ) %>%
    ungroup() %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

spec_4183 <- indicator_spec(
  indicator_id = 4183,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_4183,
  filter_data = filter_4183,
  transform_data = transform_4183,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)


# ---- indicator 2023 — energy intensity (final energy consumption / GDP) ----

dim_config_2023 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2023 <- function(data) {
  data %>%
    filter(Type == "Total") %>%
    select(-Type)
}

transform_2023 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dolllars
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join PIB data and calculate energy intensity
  data %>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(cons = value) %>%
    mutate(value = cons / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

spec_2023 <- indicator_spec(
  indicator_id = 2023,
  data = data_cons,
  max_year = max_year_olade,
  dim_config = dim_config_2023,
  filter_data = filter_2023,
  transform_data = transform_2023,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4184 — change in energy intensity (final energy consumption / GDP) ----

dim_config_4184 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4184 <- function(data) {
  data %>%
    filter(Type == "Total") %>%
    select(-Type)
}

transform_4184 <- function(data) {
  # Obtain PIB data from CEPALSTAT
  # 2204 - Total Annual Gross Domestic Product (GDP) at constant prices in (2018) dollars
  pib <- call.data(id.indicator = 2204) %>% as_tibble()
  
  pib %<>%
    mutate(Years = as.numeric(Years)) %>%
    select(Country, Years, pib = value)
  
  # Join PIB data and calculate energy intensity
  data %<>%
    left_join(pib, by = c("Country", "Years")) %>%
    filter(as.numeric(Years) >= 1990) %>% # this is the start of the pib series
    mutate(value = value / 1e12) %>% # convert from joules to terajoules
    rename(cons = value) %>%
    mutate(value = cons / pib) %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
  
  # Calculate variation rate: ((Mt - Mt-1) / Mt-1) * 100
  data %>%
    arrange(Country, Years) %>%
    group_by(Country) %>%
    mutate(
      value_prev = lag(value),
      value = ((value - value_prev) / value_prev) * 100
    ) %>%
    ungroup() %>%
    select(Country, Years, value) %>%
    filter(!is.na(value))
}

spec_4184 <- indicator_spec(
  indicator_id = 4184,
  data = data_cons,
  max_year = max_year_olade,
  dim_config = dim_config_4184,
  filter_data = filter_4184,
  transform_data = transform_4184,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4243 — energy intensity (final energy consumption / GDP), by economic activity ----

dim_config_4243 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "78134"),
  pub_col = c("208_name", "29117_name", "78134_name")
)

filter_4243 <- function(data) {
  data %<>%
    filter(Years >= 1990) %>%  # remove data prior to 1990 as that's when the econ series starts
    filter(Type != "Residential") # olade classifies sectors by consumption and so includes residential use; cepalstat calculates gdp by production and so doesn't include
}

transform_4243 <- function(data) {
  data %<>% 
    left_join(energy_econ_sectors %>% distinct(olade_label, dim_label), by = c("Type" = "olade_label")) %>% 
    group_by(Country, Years, dim_label) %>% 
    summarize(cons = sum(value, na.rm = TRUE), .groups = "drop")
  
  # Obtain GDP by economic activity from CEPALSTAT (indicator 2216)
  pib_sector <- call.data(id.indicator = 2216) %>% as_tibble()
  # 2216 - Annual Gross Domestic Product (GDP) by activity at constant prices in dollars (Millions of dollars, 2018$)
  
  pib_sector %<>% 
    distinct(Country, Years, Type = Rubro__Sector_Cuentas_nacionales_anuales, value) %>% # there are currently exact duplicates in cepalstat, take distinct values until this is fixed (issue confirmed by Patricia)
    left_join(energy_econ_sectors %>% distinct(econ_label, dim_label), by = c("Type" = "econ_label")) %>% 
    group_by(Country, Years, dim_label) %>% 
    summarize(pib = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    filter(!is.na(dim_label)) # remove extra econ categories
  
  data %>% 
    left_join(pib_sector, by = c("Country", "Years", "dim_label")) %>% 
    mutate(cons = cons / 1e12) %>% # convert from joules to terajoules
    mutate(value = cons / pib) %>% 
    rename(Type = dim_label) %>% 
    select(-cons, -pib) %>% 
    filter(!is.na(value))
}

spec_4243 <- indicator_spec(
  indicator_id = 4243,
  data = data_cons_sec,
  max_year = max_year_olade,
  dim_config = dim_config_4243,
  filter_data = filter_4243,
  transform_data = transform_4243,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4150 — installed electricity-generating capacity ----

dim_config_4150 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "77605"),
  pub_col = c("208_name", "29117_name", "77605_name")
)

filter_4150 <- function(data) {
  data %>%
    filter(Years >= 2000) %>% # more complete series begins in 2000
    mutate(Type = case_when(
      Type == "Hidro" ~ "Hidroeléctrica",
      Type == "Térmica no renovable (combustión)" ~ "Térmica no renovable",
      Type == "Térmica renovable (combustión)" ~ "Térmica renovable",
      TRUE ~ Type
    ))
}

transform_4150 <- function(data) {
  data %>% 
    mutate(Type = case_when( # format labels
      Type == "Hydro" ~ "Hydroelectric",
      Type == "Non-renewable thermal (combustion)" ~ "Non-renewable Thermal",
      Type == "Renewable thermal (combustion)" ~ "Renewable Thermal",
      TRUE ~ Type
    )) %>% 
    bind_rows(
      group_by(., Country, Years) %>%  # Create summary row
        summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
        mutate(Type = "Total")) %>%
    arrange(Country, Years, Type)
}

spec_4150 <- indicator_spec(
  indicator_id = 4150,
  data = data_infra,
  max_year = max_year_olade,
  dim_config = dim_config_4150,
  filter_data = filter_4150,
  transform_data = transform_4150,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 1755 — installed electricity-generating capacity (historical series) ----

# This OLADE series runs from 1970 to 2015. The data shows the total electrical capacity for countries.
# This series was replaced by indicator 4150 — Installed capacity for producing electricity, by source, which includes more detailed data.

# ---- indicator 1754 — electricity consumption ----

dim_config_1754 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_1754 <- function(data) {
  data %>%
    filter(Type == "Electricity") %>% 
    select(-Type)
}

transform_1754 <- function(data) {
  data %>% 
    mutate(value = value / 3.6e12) # convert from joules to GWh
}

spec_1754 <- indicator_spec(
  indicator_id = 1754,
  data = data_cons,
  max_year = max_year_olade,
  dim_config = dim_config_1754,
  filter_data = filter_1754,
  transform_data = transform_1754,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4234 — electricity losses ----

dim_config_4234 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4234 <- function(data) {
  data %>%
    filter(Type == "Electricity") %>% 
    select(-Type)
}

transform_4234 <- function(data) {
  data
  # note this data source is in its original units (GWh), so no need to convert anything
}

spec_4234 <- indicator_spec(
  indicator_id = 4234,
  data = data_losses,
  max_year = max_year_olade,
  dim_config = dim_config_4234,
  filter_data = filter_4234,
  transform_data = transform_4234,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)

# ---- indicator 4235 - proportion of electricity losses ----

dim_config_4235 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4235 <- function(data) {
  data %>%
    filter(Type == "Electricity") %>% 
    select(-Type)
}

transform_4235 <- function(data) {
  data %<>% 
    mutate(supply = value / 3.6e12) %>%  # convert from joules to GWh
    select(-value)
  
  losses <- data_losses
  
  losses %<>% 
    filter(Years <= max_year_olade) %>% 
    filter(Type == "Electricity") %>% 
    select(-Type) %>% 
    rename(losses = value)
  
  data %>% 
    full_join(losses, by = c("Country", "Years")) %>% 
    mutate(value = losses / supply * 100) %>% 
    filter(!is.na(value)) %>% 
    select(-supply, -losses)
}

spec_4235 <- indicator_spec(
  indicator_id = 4235,
  data = data_supply,
  max_year = max_year_olade,
  dim_config = dim_config_4235,
  filter_data = filter_4235,
  transform_data = transform_4235,
  calculate_regional = maintain_regional # keep source LAC data from OLADE
)
