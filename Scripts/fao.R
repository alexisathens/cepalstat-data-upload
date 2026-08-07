# This script downloads, cleans, and standardizes FAO indicators

# ---- setup ----

library(here)
source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

# Define max year of reliable FAO data
max_year_fao <- 2025 # as of July 2026
# missing Land area item from RL dataset for year 2025 as of July 2026...
# hopefully all FAO data will be available for 2025 in time for the yearbook

# ---- download data ----

# Load information about all datasets into a data frame
fao_metadata <- FAOmetaTable$domainTable %>% as_tibble()
# Alternatively go here to see data areas: https://www.fao.org/faostat/en/#data

## create custom bulk download function while API is broken
get_fao_bulk <- function(filename) {
  data_folder <- here("Data/Raw/fao")
  download_faostat_bulk(url_bulk = paste0("https://bulks-faostat.fao.org/production/", filename), 
                        data_folder = data_folder)
  read_faostat_bulk(file.path(data_folder, filename)) %>% as_tibble()
}

# download land use (RL) data
use <- get_fao_bulk("Inputs_LandUse_E_All_Data_(Normalized).zip") %>% filter(!is.na(value))

# download climate change (ET) data
clim <- get_fao_bulk("Environment_Temperature_change_E_All_Data_(Normalized).zip") %>% filter(!is.na(value))

# download land cover (LC) data
cover <- get_fao_bulk("Environment_LandCover_E_All_Data_(Normalized).zip") %>% filter(!is.na(value))

# download crops and livestock products (QCL) data
crop <- get_fao_bulk("Production_Crops_Livestock_E_All_Data_(Normalized).zip") %>% filter(!is.na(value))

# download fertilizers by Nutrient (RFN) data
fert <- get_fao_bulk("Inputs_FertilizersNutrient_E_All_Data_(Normalized).zip") %>% filter(!is.na(value))

# download pesticide use (RP) data
pest <- get_fao_bulk("Inputs_Pesticides_Use_E_All_Data_(Normalized).zip") %>% filter(!is.na(value))

## fishstat & aquastat downloads
# imports from the fishstat package. See documentation here: https://cran.r-universe.dev/fishstat/doc/manual.html
fish <- capture %>%
  inner_join(country, by = "country") %>%
  inner_join(species, by = "species") %>% 
  as_tibble()

fish <- fish %>% 
  mutate(
    Years = as.integer(year),
    Country = country_name,
    Species = species_name,
    Species_Group = isscaap
  )

aqua <- aquaculture %>%
  inner_join(country, by = "country") %>%
  inner_join(species, by = "species") %>% 
  inner_join(area, by = "area") %>% 
  inner_join(environment, by = "environment") %>% 
  as_tibble()

aqua <- aqua %>% 
  mutate(
    Years = as.integer(year),
    Country = country_name,
    Area = inlandmarine
  )

## **note: still need to download aquastat (water resources) manually? or is there an API now?


# ---- shared functions ----

# Shared filter and dimension rename across all forest indicators
filter_forest <- function(data) {
  data %>% 
    filter(item %in% c("Forest land", "Naturally regenerating forest", "Planted Forest", "Land area")) %>% 
    filter(element == "area") %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "CuraÃ§ao")) %>% 
    mutate(item = case_when(
      item == "Forest land" ~ "Total forest",
      item == "Naturally regenerating forest" ~ "Natural forest",
      item == "Planted Forest" ~ "Forest plantations",
      TRUE ~ item
    )) %>% 
    rename(Country = area, Years = year, Type = item) %>% 
    select(Country, Years, Type, value)
}

# Intermediate dataset: cropland area, used as the denominator for the fertilizer/pesticide use
# intensity indicators (2022, 3382)
result_cropland <- use %>%
  filter(item == "Cropland") %>% # Cropland = Arable Land + Permanent Crops
  filter(element == "area") %>%
  filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla")) %>%
  rename(Country = area, Years = year) %>%
  mutate(Years = as.character(Years)) %>%
  select(Country, Years, area = value)

# Regional strategy for "intensity" indicators (value per unit of cropland area): same shape as
# calculate_regional_wgt_avg, but without the *100 -- these are per-hectare rates, not percentages
calculate_regional_intensity <- function(df) {
  df <- df %>%
    filter(!Country %in% c("South America", "Central America", "Caribbean",
                           "Latin America and the Caribbean", "Latin America"))
  
  lac_total <- df %>%
    filter(Country != "World") %>%
    group_by(across(all_of(setdiff(names(df), c("Country", "value", "area"))))) %>%
    summarise(value = sum(value, na.rm = TRUE),
              area = sum(area, na.rm = TRUE), .groups = "drop") %>%
    mutate(Country = "Latin America and the Caribbean")
  
  df %>%
    bind_rows(lac_total) %>%
    mutate(value = value / area) %>%
    select(-area)
}


## ---- indicator 3381 - mean annual temperature change ----

dim_config_3381 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3381 <- function(data) {
  data %>% 
    filter(element == "temperature_change" & months == "Meteorological year") %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao"))
}

transform_3381 <- function(data) {
  data %>%
    rename(Country = area, Years = year) %>%
    select(Country, Years, value)
}

# note: this indicator has a special exception in the standardize_country() function that lets it keep the subregional data
spec_3381 <- indicator_spec(
  indicator_id = 3381,
  data = clim,
  max_year = max_year_fao,
  dim_config = dim_config_3381,
  filter_data = filter_3381,
  transform_data = transform_3381,
  calculate_regional = maintain_regional # keep FAO sub/regional calculations as-is
)

## ---- indicator 2035 - country area ----

dim_config_2035 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "21899"),
  pub_col = c("208_name", "29117_name", "21899_name")
)

filter_2035 <- function(data) {
  data %>% 
    filter(item %in% c("Country area", "Land area", "Inland waters")) %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_2035 <- function(data) {
  data %>% 
    mutate(item = ifelse(item == "Inland waters", "Area of inland waters", item),
           item = ifelse(item == "Country area", "Total area", item)) %>% 
    rename(Country = area, Type = item, Years = year) %>% 
    select(Country, Years, Type, value)
}

spec_2035 <- indicator_spec(
  indicator_id = 2035,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_2035,
  filter_data = filter_2035,
  transform_data = transform_2035,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 2054 - inland waters area ----

dim_config_2054 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2054 <- function(data) {
  data %>% 
    filter(item %in% c("Inland waters")) %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_2054 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

spec_2054 <- indicator_spec(
  indicator_id = 2054,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_2054,
  filter_data = filter_2054,
  transform_data = transform_2054,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 3355 - area covered by permanent snow and glaciers ----

dim_config_3355 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3355 <- function(data) {
  data %>% 
    filter(element == "area_from_cci_lc" & item == "Permanent snow and glaciers") %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao"))
}

transform_3355 <- function(data) {
  data %>% 
    mutate(value = value * 1000) %>% # transform from 1,000 hectares into hectares
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

spec_3355 <- indicator_spec(
  indicator_id = 3355,
  data = cover,
  max_year = max_year_fao,
  dim_config = dim_config_3355,
  filter_data = filter_3355,
  transform_data = transform_3355,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 4176 - area covered by mangroves ----

dim_config_4176 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4176 <- function(data) {
  data %>%
    filter(element == "area_from_cci_lc" & item == "Mangroves") %>%
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao"))
}

transform_4176 <- function(data) {
  data %>%
    rename(Country = area, Years = year) %>%
    select(Country, Years, value)
}

spec_4176 <- indicator_spec(
  indicator_id = 4176,
  data = cover,
  max_year = max_year_fao,
  dim_config = dim_config_4176,
  filter_data = filter_4176,
  transform_data = transform_4176,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 2036 - forest area ----

dim_config_2036 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20722"),
  pub_col = c("208_name", "29117_name", "20722_name")
)

transform_2036 <- function(data) {
  data %>% 
    filter(Type %in% c("Total forest", "Natural forest", "Forest plantations"))
}

spec_2036 <- indicator_spec(
  indicator_id = 2036,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_2036,
  filter_data = filter_forest,
  transform_data = transform_2036,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 2021 - proportion of forest area ----

dim_config_2021 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20722"),
  pub_col = c("208_name", "29117_name", "20722_name")
)

# note: "land area" data tends to lag behind forest by 1-2 years, but this filters incomplete cases
transform_2021 <- function(data) {
  data %>% 
    group_by(Country, Years) %>%
    filter(any(Type == "Land area")) %>%  # Keep only groups that have "Land area"
    ungroup() %>% 
    mutate(denom = if_else(Type == "Land area", value, NA)) %>% 
    group_by(Country, Years) %>% 
    fill(denom, .direction = "downup") %>%
    ungroup() %>% 
    filter(Type != "Land area") %>% 
    rename(num = value)
}

spec_2021 <- indicator_spec(
  indicator_id = 2021,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_2021,
  filter_data = filter_forest,
  transform_data = transform_2021,
  calculate_regional = calculate_regional_wgt_avg
)


## ---- indicator 2530 - natural forest proportion of total forest ----

dim_config_2530 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

transform_2530 <- function(data) { 
  data %>% 
    filter(Type %in% c("Total forest", "Natural forest")) %>% 
    filter(Country != "Anguilla") %>% # Remove Anguilla because they're the only country without natural or plantation forests detailed
    mutate(Type = case_when(
      Type == "Natural forest" ~ "num", # Define numerator and denominator of proportion
      Type == "Total forest" ~ "denom",
      TRUE ~ "",
    )) %>% 
    pivot_wider(names_from = Type)
}

spec_2530 <- indicator_spec(
  indicator_id = 2530,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_2530,
  filter_data = filter_forest,
  transform_data = transform_2530,
  calculate_regional = calculate_regional_wgt_avg
)

## ---- indicator 2531 - forest plantations proportion of total forest ----

dim_config_2531 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# note: many small countries have "NA" values for Forest plantations, while 100% of their forest is natural.
# i think it's safe to infer that these cases should be imputed with 0s
transform_2531 <- function(data) { 
  data %>% 
    filter(Type %in% c("Total forest", "Forest plantations")) %>% 
    filter(Country != "Anguilla") %>% # Remove Anguilla because they're the only country without natural or plantation forests detailed
    mutate(Type = case_when(
      Type == "Forest plantations" ~ "num", # Define numerator and denominator of proportion
      Type == "Total forest" ~ "denom",
      TRUE ~ "",
    )) %>% 
    pivot_wider(names_from = Type) %>% 
    mutate(num = replace_na(num, 0))  # Assume NAs to mean no planted forests (generally true)
}

spec_2531 <- indicator_spec(
  indicator_id = 2531,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_2531,
  filter_data = filter_forest,
  transform_data = transform_2531,
  calculate_regional = calculate_regional_wgt_avg
)

## ---- indicator 1739 - irrigated area ----

dim_config_1739 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_1739 <- function(data) {
  data %>% 
    filter(item %in% c("Land area equipped for irrigation")) %>% 
    filter(element == "area") %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_1739 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

spec_1739 <- indicator_spec(
  indicator_id = 1739,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_1739,
  filter_data = filter_1739,
  transform_data = transform_1739,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 1869 - ag area by land type use ----

dim_config_1869 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "26646"),
  pub_col = c("208_name", "29117_name", "26646_name")
)

filter_1869 <- function(data) {
  data %>% 
    filter(item %in% c("Arable land", "Permanent crops", "Permanent meadows and pastures")) %>% 
    filter(element == "area") %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_1869 <- function(data) {
  data %<>% 
    mutate(item = case_when(
      item == "Arable land" ~ "Area of arable land",
      item == "Permanent crops" ~ "Area of permanent crops",
      item == "Permanent meadows and pastures" ~ "Area of permanent meadows and pastures",
      TRUE ~ item
    )) %>% 
    rename(Country = area, Type = item, Years = year) %>% 
    select(Country, Years, Type, value)
  
  # create the summed "Agricultural area"
  agri_sum <- data %>%
    group_by(Country, Years) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Agricultural area")
  
  bind_rows(data, agri_sum)
}

spec_1869 <- indicator_spec(
  indicator_id = 1869,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_1869,
  filter_data = filter_1869,
  transform_data = transform_1869,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 4049 - prop of ag area with organic agriculture ----

dim_config_4049 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4049 <- function(data) {
  data %>% 
    filter(item %in% c("Agriculture area under organic agric.")) %>% 
    filter(element == "share_in_agricultural_land") %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_4049 <- function(data) {
  data %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

# note: maintain FAO's regional LAC calculation since the data is lacked to recompute it
spec_4049 <- indicator_spec(
  indicator_id = 4049,
  data = use,
  max_year = max_year_fao,
  dim_config = dim_config_4049,
  filter_data = filter_4049,
  transform_data = transform_4049,
  calculate_regional = maintain_regional
)


## ---- indicator 1740 - harvested area of main crops ----

dim_config_1740 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20721"),
  pub_col = c("208_name", "29117_name", "20721_name")
)

filter_1740 <- function(data) {
  data %>% 
    filter(element == "area_harvested") %>% 
    filter(item %in% c("Cereals, primary", "Sugar Crops Primary", "Fibre Crops, Fibre Equivalent",
                       "Oilcrops, Oil Equivalent", "Fruit Primary", "Vegetables Primary",
                       "Pulses, Total", "Treenuts, Total", "Roots and Tubers, Total")) %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermudas", "Curaçao", "Anguilla"))
}

transform_1740 <- function(data) {
  data %>% 
    mutate(item = case_when(
      item == "Cereals, primary" ~ "Cereals",
      item == "Sugar Crops Primary" ~ "Sugar crops",
      item == "Fibre Crops, Fibre Equivalent" ~ "Fibre crops",
      item == "Oilcrops, Oil Equivalent" ~ "Oilcrops",
      item == "Fruit Primary" ~ "Fruit",
      item == "Vegetables Primary" ~ "Vegetables",
      item == "Pulses, Total" ~ "Pulses",
      item == "Treenuts, Total" ~ "Treenuts",
      item == "Roots and Tubers, Total" ~ "Roots and tubers",
      TRUE ~ item
    )) %>% 
    mutate(value = value / 1000) %>% # transform into 1000s of hectares
    rename(Country = area, Type = item, Years = year) %>% 
    select(Country, Years, Type, value)
}

spec_1740 <- indicator_spec(
  indicator_id = 1740,
  data = crop,
  max_year = max_year_fao,
  dim_config = dim_config_1740,
  filter_data = filter_1740,
  transform_data = transform_1740,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 2038 - fertilizer consumption ----

dim_config_2038 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2038 <- function(data) {
  data %>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Nutrient nitrogen N (total)", "Nutrient phosphate P2O5 (total)", "Nutrient potash K2O (total)")) %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_2038 <- function(data) {
  data %>% 
    group_by(area, year) %>% # sum across fertilizer types (items)
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value)
}

footnotes_2038 <- list(
  "7177" = function(df) df$Years == "2002" # 7177/ La serie de datos de 1961 a 2001 y la serie de 2002 a la fecha deberán analizarse por separado...
)

spec_2038 <- indicator_spec(
  indicator_id = 2038,
  data = fert,
  max_year = max_year_fao,
  dim_config = dim_config_2038,
  filter_data = filter_2038,
  transform_data = transform_2038,
  calculate_regional = calculate_regional_sum,
  footnotes = footnotes_2038
)


## ---- indicator 2022 - fertilizer use intensity ----

dim_config_2022 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2022 <- function(data) {
  data %>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Nutrient nitrogen N (total)", "Nutrient phosphate P2O5 (total)", "Nutrient potash K2O (total)")) %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_2022 <- function(data) {
  data %>% 
    group_by(area, year) %>% # sum across fertilizer types (items)
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value) %>% 
    mutate(Years = as.character(Years)) %>% 
    left_join(result_cropland, by = c("Country", "Years")) %>% 
    arrange(Country, Years)
}

spec_2022 <- indicator_spec(
  indicator_id = 2022,
  data = fert,
  max_year = max_year_fao,
  dim_config = dim_config_2022,
  filter_data = filter_2022,
  transform_data = transform_2022,
  calculate_regional = calculate_regional_intensity
)

## ---- indicator 2039 - pesticide consumption ----

dim_config_2039 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "20723"),
  pub_col = c("208_name", "29117_name", "20723_name")
)

filter_2039 <- function(data) {
  data %>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Insecticides", "Herbicides", "Fungicides and Bactericides")) %>% 
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla")) %>% 
    select(Country = area, Years = year, Type = item, value)
}

transform_2039 <- function(data) {
  data %<>% 
    mutate(Type = ifelse(Type == "Fungicides and Bactericides", "Fungicides and bactericides", Type))
  
  pest_totals <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(Type = "Total")
  
  data %>% 
    bind_rows(pest_totals)
}

spec_2039 <- indicator_spec(
  indicator_id = 2039,
  data = pest,
  max_year = max_year_fao,
  dim_config = dim_config_2039,
  filter_data = filter_2039,
  transform_data = transform_2039,
  calculate_regional = calculate_regional_sum
)

## ---- indicator 3382 - pesticide use intensity ----

dim_config_3382 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_3382 <- function(data) {
  data %<>% 
    filter(element == "agricultural_use") %>% 
    filter(item %in% c("Insecticides", "Herbicides", "Fungicides and Bactericides")) %>%
    filter(!area %in% c("Sint Maarten (Dutch part)", "Bermuda", "Curaçao", "Anguilla"))
}

transform_3382 <- function(data) {
  data %<>% 
    group_by(area, year) %>% # sum across pesticide types (items)
    summarize(value = sum(value, na.rm = T), .groups = "drop") %>% 
    rename(Country = area, Years = year) %>% 
    select(Country, Years, value) %>% 
    mutate(Years = as.character(Years)) %>% 
    left_join(result_cropland, by = c("Country", "Years"))
}

spec_3382 <- indicator_spec(
  indicator_id = 3382,
  data = pest,
  max_year = max_year_fao,
  dim_config = dim_config_3382,
  filter_data = filter_3382,
  transform_data = transform_3382,
  calculate_regional = calculate_regional_intensity
)


## ---- indicator 2019 - fish capture production ----

dim_config_2019 <- tibble(
  data_col = c("Country", "Years", "Species"),
  dim_id = c("208", "29117", "20720"),
  pub_col = c("208_name", "29117_name", "20720_name")
)

filter_2019 <- function(data) {
  whales <- c("Blue-whales, fin-whales", "Sperm-whales, pilot-whales", "Eared seals, hair seals, walruses", "Miscellaneous aquatic mammals")
  
  data %>% 
    filter(!Species_Group %in% whales) %>% 
    filter(!Country %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country, Years, Species, Species_Group, value)
}

transform_2019 <- function(data) {
  data %<>% 
    mutate(
      Species_Division = case_when(
        # 1 Freshwater fishes
        Species_Group %in% c(
          "Carps, barbels and other cyprinids",
          "Tilapias and other cichlids",
          "Miscellaneous freshwater fishes"
        ) ~ "Freshwater fishes",
        
        # 2 Diadromous fishes
        Species_Group %in% c(
          "Sturgeons, paddlefishes",
          "River eels",
          "Salmons, trouts, smelts",
          "Shads",
          "Miscellaneous diadromous fishes"
        ) ~ "Diadromous fishes",
        
        # 3 Marine fishes
        Species_Group %in% c(
          "Flounders, halibuts, soles",
          "Cods, hakes, haddocks",
          "Miscellaneous coastal fishes",
          "Miscellaneous demersal fishes",
          "Herrings, sardines, anchovies",
          "Tunas, bonitos, billfishes",
          "Miscellaneous pelagic fishes",
          "Sharks, rays, chimaeras",
          "Marine fishes not identified"
        ) ~ "Marine fishes",
        
        # 4 Crustaceans
        Species_Group %in% c(
          "Freshwater crustaceans",
          "Crabs, sea-spiders",
          "Lobsters, spiny-rock lobsters",
          "King crabs, squat-lobsters",
          "Shrimps, prawns",
          "Krill, planktonic crustaceans",
          "Miscellaneous marine crustaceans"
        ) ~ "Crustaceans",
        
        # 5 Molluscs
        Species_Group %in% c(
          "Freshwater molluscs",
          "Abalones, winkles, conchs",
          "Oysters",
          "Mussels",
          "Scallops, pectens",
          "Clams, cockles, arkshells",
          "Squids, cuttlefishes, octopuses",
          "Miscellaneous marine molluscs"
        ) ~ "Molluscs",
        
        # 6 Whales, seals and other aquatic mammals
        Species_Group %in% c(
          "Blue-whales, fin-whales",
          "Sperm-whales, pilot-whales",
          "Eared seals, hair seals, walruses",
          "Miscellaneous aquatic mammals"
        ) ~ "Whales, seals and other aquatic mammals",
        
        # 7 Miscellaneous aquatic animals
        Species_Group %in% c(
          "Frogs and other amphibians",
          "Turtles",
          "Crocodiles and alligators",
          "Sea-squirts and other tunicates",
          "Horseshoe crabs and other arachnoids",
          "Sea-urchins and other echinoderms",
          "Miscellaneous aquatic invertebrates"
        ) ~ "Miscellaneous aquatic animals",
        
        # 8 Miscellaneous aquatic animal products
        Species_Group %in% c(
          "Pearls, mother-of-pearl, shells",
          "Corals",
          "Sponges"
        ) ~ "Miscellaneous aquatic animal products",
        
        # 9 Aquatic plants
        Species_Group %in% c(
          "Brown seaweeds",
          "Red seaweeds",
          "Green seaweeds",
          "Miscellaneous aquatic plants"
        ) ~ "Aquatic plants",
        
        TRUE ~ NA_character_
      )
    ) %>% 
    select(-Species, -Species_Group) %>% 
    # map to cepalstat labels
    mutate(Species = case_when(
      Species_Division %in% c("Freshwater fishes") ~ "Freshwater fish",
      Species_Division %in% c("Marine fishes") ~ "Marine fish",
      # keep Molluscs and Crustaceans and Aquatic plans as is
      Species_Division %in% c("Diadromous fishes", "Miscellaneous aquatic animals", "Miscellaneous aquatic animal products") ~ "Other",
      TRUE ~ Species_Division
    )) %>% select(-Species_Division)
  
  data %<>% 
    group_by(Country, Years, Species) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1000) # change units from tons to 1000s of tons
  
  total <- data %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Species = "TOTAL")
  
  data %>% 
    bind_rows(total)
}

footnotes_2019 <- list(
  "6545" = function(df) rep(TRUE, nrow(df)), # Incluye la captura en áreas marinas y en aguas continentales. [applies to everyone]
  "7777" = function(df) df$Species == "TOTAL", # El total no incluye ballenas, focas y otros mamíferos acuáticos
  "5518" = function(df) df$Species == "Other" # Incluye peces diádromos, varios animales acuáticos y varios productos de animales acuáticos.
)

spec_2019 <- indicator_spec(
  indicator_id = 2019,
  data = fish,
  max_year = max_year_fao,
  dim_config = dim_config_2019,
  filter_data = filter_2019,
  transform_data = transform_2019,
  calculate_regional = calculate_regional_sum,
  footnotes = footnotes_2019
)


## ---- indicator 2020 - aquaculture production ----

dim_config_2020 <- tibble(
  data_col = c("Country", "Years", "Area"),
  dim_id = c("208", "29117", "26819"),
  pub_col = c("208_name", "29117_name", "26819_name")
)

filter_2020 <- function(data) {
  data %>% 
    filter(environment_name %in% c("Freshwater", "Marine")) %>% # remove "Brackishwater"
    filter(Area %in% c("Inland waters", "Marine areas")) %>% # select all
    filter(!Country %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country, Years, Area, value)
}

transform_2020 <- function(data) {
  data %<>% 
    group_by(Country, Years, Area) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(value = value / 1000) # change units from tons to 1000s of tons
  
  total <- data %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Area = "Total")
  
  data %>% 
    bind_rows(total)
}

footnotes_2020 <- list(
  "5899" = function(df) rep(TRUE, nrow(df)) # Incluye la producción en áreas marinas y en aguas continentales.
)

spec_2020 <- indicator_spec(
  indicator_id = 2020,
  data = aqua,
  max_year = max_year_fao,
  dim_config = dim_config_2020,
  filter_data = filter_2020,
  transform_data = transform_2020,
  calculate_regional = calculate_regional_sum,
  footnotes = footnotes_2020
)

## ---- indicator 4185 - water withdrawal by sector ----

dim_config_4185 <- tibble(
  data_col = c("Country", "Years", "Sector"),
  dim_id = c("208", "29117", "59252"),
  pub_col = c("208_name", "29117_name", "59252_name")
)

filter_4185 <- function(data) {
  data %>%
    filter(str_detect(Variable, "as \\% of total")) %>% 
    filter(!Area %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country = Area, Years = Year, Sector = Variable, value = Value)
}

# ** check if bolivia issue still exists this year
transform_4185 <- function(data) {
  bol_data <- data %>% filter(str_detect(Country, "Bolivia") & Years %in% c(2020, 2021))
  
  if(any(bol_data$value > 100)) { # fix data issue for years 2020/2021 where entries were seemingly reported in units rather than %
    bol_data %<>% 
      group_by(Years) %>% 
      mutate(perc = value/sum(value) * 100) %>% 
      ungroup() %>% 
      select(-value, value = perc)
    
    data %<>% # remove old data and attach corrected
      filter(!(str_detect(Country, "Bolivia") & Years %in% c(2020, 2021))) %>% 
      bind_rows(bol_data)
  }
  
  data %>% 
    mutate(Sector = case_when(
      str_detect(Sector, "Agricultural") ~ "Agricultural",
      str_detect(Sector, "Industrial") ~ "Industrial",
      str_detect(Sector, "Municipal") ~ "Municipal",
      TRUE ~ Sector
    ))
}

# ** double check maintain regional calc
spec_4185 <- indicator_spec(
  indicator_id = 4185,
  data = aquastat, # check **
  max_year = max_year_fao,
  dim_config = dim_config_4185,
  filter_data = filter_4185,
  transform_data = transform_4185,
  calculate_regional = maintain_regional
)


## ---- indicator 4186 - water intensity of agriculture value added ----

dim_config_4186 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_4186 <- function(data) {
  data %>% 
    filter(Variable == "Agricultural water withdrawal") %>% 
    filter(!Area %in% c("Sint Maarten (Dutch part)")) %>% 
    select(Country = Area, Years = Year, value = Value)
}

transform_4186 <- function(data) {
  # get annual GDP by economic activity from CEPALSTAT, in constant 2018 dollars
  gdp <- CepalStatR::call.data(2216) %>% as_tibble()
  
  gdp %<>% 
    rename(Sector = Rubro__Sector_Cuentas_nacionales_anuales) %>% 
    filter(Sector %in% c("Agriculture, hunting, forestry and fishing")) %>% 
    select(Country, Years, gdp = value)
  
  data %>% 
    left_join(gdp, by = c("Country", "Years")) %>% 
    filter(!is.na(gdp)) %>%
    mutate(intensity = value / gdp * 1e3) %>% 
    select(Country, Years, value = intensity)
}

spec_4186 <- indicator_spec(
  indicator_id = 4186,
  data = aquastat,
  max_year = max_year_fao,
  dim_config = dim_config_4186,
  filter_data = filter_4186,
  transform_data = transform_4186,
  calculate_regional = maintain_regional # no ECLAC average **
)
