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

# This script processes "other" indicators from small sources using the automated process_indicator() function
# See the data download instructions at: 01_other_instructions.qmd

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))

input_path <- here("Data/Raw/other")

# read in ISO with cepalstat ids
iso <- read_xlsx(here("Data/iso_codes.xlsx"))

iso %<>%
  filter(ECLACa == "Y") %>%
  select(cepalstat, name, std_name)

# read in indicator metadata
# meta <- read_xlsx(here("Data/indicator_metadata.xlsx"))

max_year <- 2024 # define most recent year with full data


# ---- read downloaded files ----

data_irena <- read_xlsx(paste0(input_path, "/irena_raw.xlsx"))

data_iso_2024 <- read_xlsx(paste0(input_path, "/iso_2024_raw.xlsx"), sheet = "ISO 14001", skip = 1) # in future years, we'll only need to download one year at a time
data_iso_2023 <- read_xlsx(paste0(input_path, "/iso_2023_raw.xlsx"), sheet = "ISO 14001", skip = 1)
data_iso_2022 <- read_xlsx(paste0(input_path, "/iso_2022_raw.xlsx"), sheet = "ISO 14001", skip = 1)
data_iso_2021 <- read_xlsx(paste0(input_path, "/iso_2021_raw.xlsx"), sheet = "ISO 14001", skip = 1)

## read these in instead in indicator 2037 code with function
# data_odp_bcm <- read_xlsx(paste0(input_path, "/odp_bcm_raw.xlsx"), skip = 1)
# data_odp_cfc <- read_xlsx(paste0(input_path, "/odp_cfc_raw.xlsx"), skip = 1)
# data_odp_ctc <- read_xlsx(paste0(input_path, "/odp_ctc_raw.xlsx"), skip = 1)
# data_odp_halcfc <- read_xlsx(paste0(input_path, "/odp_halcfc_raw.xlsx"), skip = 1)
# data_odp_halons <- read_xlsx(paste0(input_path, "/odp_halons_raw.xlsx"), skip = 1)
# data_odp_hbfc <- read_xlsx(paste0(input_path, "/odp_hbfc_raw.xlsx"), skip = 1)
# data_odp_hcfc <- read_xlsx(paste0(input_path, "/odp_hcfc_raw.xlsx"), skip = 1)
# data_odp_mb <- read_xlsx(paste0(input_path, "/odp_mb_raw.xlsx"), skip = 1)
# data_odp_tca <- read_xlsx(paste0(input_path, "/odp_tca_raw.xlsx"), skip = 1)

data_ramsar <- read_xlsx(paste0(input_path, "/ramsar_raw.xlsx"))

data_mea <- read_csv(here("Data/Raw/informea/parties_raw.csv"))
data_mea_meta <- read_csv(here("Data/Raw/informea/treaties_raw.csv"))


# ---- indicator 4244 — Public investment trends in renewable energy ----

indicator_id <- 4244

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_4244 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "78139"),
  pub_col = c("208_name", "29117_name", "78139_name")
)

data <- data_irena
names(data) # check if units for the amount change -- it seems like the base year is updated every so often

filter_4244 <- function(data) {
  data %>%
    filter(Category == "Renewables")
}

transform_4244 <- function(data) {
  data %<>% 
    rename(Country = `Country/Area`,
           Years = Year,
           Type = Technology,
           value = `Amount (2022 Constant USD million)`) %>% 
    mutate(Type = case_when(
      Type == "Renewable hydropower" ~ "Hydroelectric",
      Type == "Solar energy" ~ "Solar",
      Type == "Wind energy" ~ "Wind",
      Type == "Geothermal energy" ~ "Geothermal",
      TRUE ~ Type
    )) %>% 
    group_by(Country, Years, Type) %>% 
    summarize(value = sum(value, na.rm = T), .groups = "drop")
  
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value), .groups = "drop") %>% 
    mutate(Type = "Total")
  
  data %>% bind_rows(total)
}

footnotes_4244 <- function(data) {
  data # keep footnotes_id as empty
}

source_4244 <- function() {
  10686 # Calculations made by ECLAC based on data from the International Renewable Energy Agency (IRENA) 
}

result_4244 <- process_indicator(
  indicator_id = indicator_id,
  data = data_irena,
  dim_config = dim_config_4244,
  filter_fn = filter_4244,
  transform_fn = transform_4244,
  remove_lac = TRUE, # remove and recalculate LAC as simple sum
  footnotes_fn = footnotes_4244,
  source_fn = source_4244,
  diagnostics = TRUE,
  export = TRUE,
  ind_notes = "Updated source to link to IRENA website"
)


# ---- indicator 1763 - ISO 14001-certified enterprises ----

indicator_id <- 1763

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_1763 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# --- data compilation for ISO surveys
# note that this data source is a bit different since it's an annual survey
# we want to take the pre-existing data from CEPALSTAT and just append the new year's survey to it
hist_1763 <- tibble(call.data(1763))
hist_1763 %<>% select(Country, Years, value)

# add new year of data
data_iso_2024 %<>% mutate(Years = 2024) %>% select(Country, Years, value = certificates)
data_iso_2023 %<>% mutate(Years = 2023) %>% select(Country, Years, value = certificates)
data_iso_2022 %<>% mutate(Years = 2022) %>% select(Country, Years, value = certificates)
data_iso_2021 %<>% mutate(Years = 2021) %>% select(Country, Years, value = certificates)

# combine together
data_iso <- hist_1763 %>% bind_rows(data_iso_2024, data_iso_2023, data_iso_2022, data_iso_2021)
data_iso %<>% arrange(Country, Years) %>% filter(!str_detect(Country, "Compared")) # order and remove footnote
# ---

filter_1763 <- function(data) {
  data %>% 
    filter(!is.na(value))
}

transform_1763 <- function(data) {
  zero_countries <- data %>% 
    group_by(Country) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    filter(value == 0) %>% 
    pull(Country)
  
  data %>% filter(!Country %in% zero_countries)
}

footnotes_1763 <- function(data) {
  data %>%
    mutate(
      footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id))
}

result_1763 <- process_indicator(
  indicator_id = indicator_id,
  data = data_iso,
  dim_config = dim_config_1763,
  filter_fn = filter_1763,
  transform_fn = transform_1763,
  remove_lac = TRUE, # remove and recalculate LAC as simple sum
  footnotes_fn = footnotes_1763,
  diagnostics = TRUE,
  export = TRUE
)



# ---- indicator 2029 - ISO 14001-certified enterprises per GDP ----

indicator_id <- 2029

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2029 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# --- data compilation for ISO surveys
# NEED TO update 1763 data on CEPALSTAT before processing this indicator
# because it takes the raw values from the published 1763
base_1763 <- tibble(call.data(1763)) %>% select(Country, Years, value)
data_iso <- base_1763
# ---

filter_2029 <- function(data) {
  data
}

transform_2029 <- function(data) {
  data
}

regional_2029 <- function(data) {
  zero_countries <- data %>% 
    group_by(Country) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    filter(value == 0) %>% 
    pull(Country)
  
  data %<>% 
    filter(!Country %in% zero_countries)
  
  gdp <- tibble(call.data(2204)) %>% 
    select(Country, Years, gdp = value) %>% 
    mutate(Years = as.character(Years))
  
  data %<>% 
    left_join(gdp, by = c("Country", "Years")) %>% 
    filter(!is.na(value) & !is.na(gdp))
  
  lac <- data %>% 
    group_by(Years) %>% 
    summarize(value = sum(value),
              gdp = sum(gdp), .groups = "drop") %>% 
    mutate(Country = "Latin America and the Caribbean")
  
  data %<>% 
    bind_rows(lac) %>% 
    mutate(per = round(value / gdp * 1000, 1)) %>% 
    select(Country, Years, value = per) %>%
    arrange(Country, Years)
  
  data
}

footnotes_2029 <- function(data) {
  data %>%
    mutate(
      footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id))
}

result_2029 <- process_indicator(
  indicator_id = indicator_id,
  data = data_iso,
  dim_config = dim_config_2029,
  filter_fn = filter_2029,
  transform_fn = transform_2029,
  remove_lac = TRUE, # remove and recalculate LAC with custom function
  regional_fn = regional_2029,
  footnotes_fn = footnotes_2029,
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2037 - Consumption of ozone depleting substances (ODS) ----

indicator_id <- 2037

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2037 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "26657"),
  pub_col = c("208_name", "29117_name", "26657_name")
)

# --- data cleaning/compilation
ods_files <- c(
  "Chlorofluorocarbons (CFCs)"      = "odp_cfc_raw.xlsx",
  "Halons"                          = "odp_halons_raw.xlsx",
  "Other fully halogenated CFCs"    = "odp_halcfc_raw.xlsx",
  "Carbon tetrachloride"            = "odp_ctc_raw.xlsx",
  "Methyl chloroform"            = "odp_tca_raw.xlsx", # methyl cloroform / Trichloroethane (TCA) are the same
  "Hydrochlorofluorocarbons (HCFCs)"= "odp_hcfc_raw.xlsx",
  "Hydrobromofluorocarbons (HBFCs)" = "odp_hbfc_raw.xlsx",
  "Bromochloromethane"              = "odp_bcm_raw.xlsx",
  "Methyl bromide"                  = "odp_mb_raw.xlsx"
)

clean_ods_data <- function(data, substance) {
  data %>%
    select(
      Country,
      matches("^\\d{4}$")   # only 4-digit year column names
    ) %>% 
    pivot_longer(
      cols      = -Country,
      names_to  = "Years",
      values_to = "value"
    ) %>%
    mutate(
      Type = substance,
      Years = as.integer(Years)
    ) %>% 
    select(Country, Years, Type, value)
}

data_ods <- imap_dfr(
  ods_files,
  ~ read_xlsx(file.path(input_path, .x), skip = 1) %>%
    clean_ods_data(substance = .y)
)
# ---

filter_2037 <- function(data) {
  data %>% 
    filter(!is.na(value)) %>% 
    filter(Years >= 1989)
}

transform_2037 <- function(data) {
  total <- data %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>% 
    mutate(Type = "Total")
  
  data %>% 
    bind_rows(total) %>% 
    arrange(Country, Years, Type)
}

footnotes_2037 <- function(data) {
  data %>%
    mutate(
      footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "6970"), footnotes_id),
      footnotes_id = if_else(Type == "Total", append_footnote(footnotes_id, "5792"), footnotes_id), # 5792/ Incluye todas las sustancias controladas por el Protocolo de Montreal.
      )
}

result_2037 <- process_indicator(
  indicator_id = indicator_id,
  data = data_ods,
  dim_config = dim_config_2037,
  filter_fn = filter_2037,
  transform_fn = transform_2037,
  remove_lac = TRUE, # remove and recalculate LAC as simple sum
  footnotes_fn = footnotes_2037,
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2016 - Surface area of Ramsar designated wetlands ----

indicator_id <- 2016

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2016 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

filter_2016 <- function(data) {
  data %>% 
    select(Country, Date = `Designation date`, value = `Area (ha)`)
}

transform_2016 <- function(data) {
  data %>% 
    # expend year data since data source only lists the start year it was designated
    mutate(start_year = year(Date)) %>% 
    rowwise() %>%
    mutate(Years = list(seq(start_year, max_year))) %>%
    ungroup() %>%
    unnest(Years) %>% 
    group_by(Country, Years) %>% 
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
}

footnotes_2016 <- function(data) {
  data %>%
    mutate(
      footnotes_id = if_else(Country == "Latin America and the Caribbean", append_footnote(footnotes_id, "12417"), footnotes_id))
  # 12417/ Valor total calculado por CEPAL en base a la información disponible en la fuente.
}

result_2016 <- process_indicator(
  indicator_id = indicator_id,
  data = data_ramsar,
  dim_config = dim_config_2016,
  filter_fn = filter_2016,
  transform_fn = transform_2016,
  remove_lac = TRUE, # remove and recalculate LAC as simple sum
  footnotes_fn = footnotes_2016,
  diagnostics = TRUE,
  export = TRUE
)


# ---- indicator 2031 - Multilateral environmental agreements ----

indicator_id <- 2031

# Fill out dim config table by matching the following info:
# get_indicator_dimensions(indicator_id)
# print(pub <- get_cepalstat_data(indicator_id) %>% match_cepalstat_labels())

dim_config_2031 <- tibble(
  data_col = c("Country", "MEA", "Phase"),
  dim_id = c("208", "26667", "26689"),
  pub_col = c("208_name", "26667_name", "26689_name")
)

filter_2031 <- function(data) {
  data %>% 
    pivot_longer(cols = c(Signature, Ratification, Force), names_to = "Phase") %>% 
    select(Country = Party, cs_id, title, Phase, value)
}

transform_2031 <- function(data) {
  data %>% 
    mutate(cs_id = str_to_title(cs_id)) %>% 
    mutate(MEA = case_when(
      cs_id == "Cites" ~ "CITES",
      cs_id == "Cms" ~ "CMS",
      cs_id == "Law Of The Sea" ~ "Law of the Sea",
      cs_id == "Biological Diversity" ~ "Biological diversity",
      cs_id == "Paris" ~ "Paris-UNFCCC",
      cs_id == "Cartagena-Conv" ~ "Marine Environment",
      TRUE ~ cs_id
    )) %>% 
    mutate(Phase = case_when(
      Phase == "Signature" ~ "Year of signature",
      Phase == "Ratification" ~ "Year of ratification, acceptance, approval or adhesion",
      Phase == "Force" ~ "Year of entry into force",
      TRUE ~ Phase
    )) %>% 
    select(Country, MEA, Phase, value) %>% 
    filter(!is.na(value))
}

footnotes_2031 <- function(data) {
  data %>%
      mutate(
        footnotes_id = if_else(MEA == "Biological diversity", append_footnote(footnotes_id, "5496"), footnotes_id), # 5496/ Convenio sobre la Diversidad Biológica (1992).
        footnotes_id = if_else(MEA == "Basel", append_footnote(footnotes_id, "5495"), footnotes_id), # 5495/ Convenio de Basilea sobre el Control de los Movimientos Transfronterizos de los Desechos Peligrosos y su Eliminación (1989).
        footnotes_id = if_else(MEA == "CITES", append_footnote(footnotes_id, "5490"), footnotes_id), # 5490/ Convención sobre el Comercio Internacional de Especies Amenazadas de Fauna y Flora Silvestres (1973).
        footnotes_id = if_else(MEA == "CMS", append_footnote(footnotes_id, "5491"), footnotes_id), # 5491/ Convención sobre la conservación de las especies migratorias de animales silvestres (1979).
        footnotes_id = if_else(MEA == "Stockholm", append_footnote(footnotes_id, "5502"), footnotes_id), # 5502/ Convenio de Estocolmo sobre Contaminantes Orgánicos Persistentes (2001).
        footnotes_id = if_else(MEA == "Vienna", append_footnote(footnotes_id, "5493"), footnotes_id), # 5493/ Convenio de Viena para la Protección de la Capa de Ozono (1985).
        footnotes_id = if_else(MEA == "Montreal", append_footnote(footnotes_id, "5494"), footnotes_id), # 5494/ Protocolo de Montreal relativo a las Sustancias que Agotan la Capa de Ozono (1987).
        footnotes_id = if_else(MEA == "Cartagena", append_footnote(footnotes_id, ""), footnotes_id), # 5501/ Protocolo de Cartagena sobre Seguridad de la Biotecnología (2000).
        footnotes_id = if_else(MEA == "Climate Change", append_footnote(footnotes_id, "5497"), footnotes_id), # 5497/ Convención Marco de las Naciones Unidas sobre el Cambio Climático (1992).
        footnotes_id = if_else(MEA == "Kyoto", append_footnote(footnotes_id, "5499"), footnotes_id), # 5499/ Protocolo de Kyoto de la Convención Marco de las Naciones Unidas sobre el Cambio Climático (1997).
        footnotes_id = if_else(MEA == "Ramsar", append_footnote(footnotes_id, "5488"), footnotes_id), # 5488/ Convención Relativa a los Humedales de Importancia Internacional, Especialmente como Hábitat de Aves Acuáticas (1971).
        footnotes_id = if_else(MEA == "Desertification", append_footnote(footnotes_id, "5498"), footnotes_id), # 5498/ Convención de las Naciones Unidas de lucha contra la Desertificación en los Países Afectados por Sequía Grave o Desertificación, en particular en África (1994).
        footnotes_id = if_else(MEA == "Rotterdam", append_footnote(footnotes_id, "5500"), footnotes_id), # 5500/ Convenio de Rotterdam sobre el Procedimiento de Consentimiento Fundamentado Previo Aplicable a Ciertos Plaguicidas y Productos Químicos Peligrosos Objeto de Comercio Internacional (1998).
        footnotes_id = if_else(MEA == "Marine Environment", append_footnote(footnotes_id, "16773"), footnotes_id), # [16773]Convenio para la protección y el desarrollo del medio marino en la región del Gran Caribe (1983)
        footnotes_id = if_else(MEA == "Minamata", append_footnote(footnotes_id, "8788"), footnotes_id), # 8788/ Convenio de Minamata sobre el Mercurio (2013)
        footnotes_id = if_else(MEA == "Paris-UNFCCC", append_footnote(footnotes_id, "7780"), footnotes_id), # 7780/ Acuerdo de París en el marco de la Convención Marco de de las Naciones Unidas sobre el Cambio Climático (UNFCCC) (2015)
        footnotes_id = if_else(MEA == "Escazu", append_footnote(footnotes_id, "8789"), footnotes_id), # 8789/ Acuerdo Regional (Escazú) sobre el Acceso a la Información, la Participación Pública y el Acceso a la Justicia en Asuntos Ambientales en América Latina y el Caribe (2018)
        footnotes_id = if_else(MEA == "Law of the Sea", append_footnote(footnotes_id, "5492"), footnotes_id), # 5492/ Convención de las Naciones Unidas sobre el Derecho del Mar (1982).
        footnotes_id = if_else(MEA == "Heritage", append_footnote(footnotes_id, "5489"), footnotes_id) # 5489/ Convenio sobre la Protección del Patrimonio Mundial, Cultural y Natural (1972).
        )
}

result_2031 <- process_indicator(
  indicator_id = indicator_id,
  data = data_mea,
  dim_config = dim_config_2031,
  filter_fn = filter_2031,
  transform_fn = transform_2031,
  regional_fn = FALSE,
  footnotes_fn = footnotes_2031,
  diagnostics = TRUE,
  export = FALSE
)

# export csv to do self checks -- meas don't have the year field and don't work with the qc_checks script -- do just this one manually instead
self <- result_2031$clean

self %<>%
  mutate(Phase = factor(Phase, levels = c(
    "Year of signature",
    "Year of ratification, acceptance, approval or adhesion",
    "Year of entry into force"
  ))) %>%
  pivot_wider(
    names_from = c(MEA, Phase),
    values_from = value,
    names_sep = "_"
  )

write_csv(self, here("QC Reports/qc_table_2031.csv"))  

