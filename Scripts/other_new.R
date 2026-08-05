# This script processes "other" indicators from small/miscellaneous sources using the
# indicator_spec/process_indicator architecture
# See the data download instructions at: download_other.qmd

# ---- setup ----

source(here("Scripts/utils.R"))
source(here("Scripts/process_indicator_fn.R"))
library(lubridate) # for year() in indicator 2016's transform (not loaded by utils.R)

input_path <- here("Data/Raw/other")

max_year_other <- 2025 # define most recent year with full data


# ---- read downloaded files ----

data_irena <- read_xlsx(paste0(input_path, "/irena_raw.xlsx"))

data_iso_2024 <- read_xlsx(paste0(input_path, "/iso_2024_raw.xlsx"), sheet = "ISO 14001", skip = 1) # in future years, we'll only need to download one year at a time
data_iso_2023 <- read_xlsx(paste0(input_path, "/iso_2023_raw.xlsx"), sheet = "ISO 14001", skip = 1)
data_iso_2022 <- read_xlsx(paste0(input_path, "/iso_2022_raw.xlsx"), sheet = "ISO 14001", skip = 1)
data_iso_2021 <- read_xlsx(paste0(input_path, "/iso_2021_raw.xlsx"), sheet = "ISO 14001", skip = 1)

data_ramsar <- read_xlsx(paste0(input_path, "/ramsar_raw.xlsx"))

data_mea <- read_csv(here("Data/Raw/informea/parties_raw.csv"))
data_mea_meta <- read_csv(here("Data/Raw/informea/treaties_raw.csv"))

# ---- shared functions ----

filter_none <- function(data) data
transform_none <- function(data) data

# Shared by 1763's transform and 2029's regional calc: drop countries with zero certificates
# across every year on record, so they don't clutter either indicator.
remove_zero_countries <- function(data) {
  zero_countries <- data %>%
    group_by(Country) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    filter(value == 0) %>%
    pull(Country)

  data %>% filter(!Country %in% zero_countries)
}


## ---- indicator 4244 - public investment trends in renewable energy ----

dim_config_4244 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "78139"),
  pub_col = c("208_name", "29117_name", "78139_name")
)

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
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")

  total <- data %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop") %>%
    mutate(Type = "Total")

  data %>% bind_rows(total)
}

define_source_4244 <- function(df, indicator_id) {
  df %>% mutate(source_id = 10686) # Calculations made by ECLAC based on data from the International Renewable Energy Agency (IRENA)
}

spec_4244 <- indicator_spec(
  indicator_id = 4244,
  data = data_irena,
  max_year = max_year_other,
  dim_config = dim_config_4244,
  filter_data = filter_4244,
  transform_data = transform_4244,
  calculate_regional = calculate_regional_sum,
  define_source = define_source_4244
)


## ---- indicator 1763 - ISO 14001-certified enterprises ----

dim_config_1763 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# this data source is a bit different since it's an annual survey -- take the pre-existing data
# from CEPALSTAT and append the new year's survey to it
hist_1763 <- tibble(call.data(1763)) %>% select(Country, Years, value)

data_iso_2024 %<>% mutate(Years = 2024) %>% select(Country, Years, value = certificates)
data_iso_2023 %<>% mutate(Years = 2023) %>% select(Country, Years, value = certificates)
data_iso_2022 %<>% mutate(Years = 2022) %>% select(Country, Years, value = certificates)
data_iso_2021 %<>% mutate(Years = 2021) %>% select(Country, Years, value = certificates)

data_iso_1763 <- hist_1763 %>%
  bind_rows(data_iso_2024, data_iso_2023, data_iso_2022, data_iso_2021) %>%
  arrange(Country, Years) %>%
  filter(!str_detect(Country, "Compared")) # remove footnote row

filter_1763 <- function(data) {
  data %>%
    filter(!is.na(value))
}

transform_1763 <- function(data) {
  data %>% remove_zero_countries()
}

spec_1763 <- indicator_spec(
  indicator_id = 1763,
  data = data_iso_1763,
  max_year = max_year_other,
  dim_config = dim_config_1763,
  filter_data = filter_1763,
  transform_data = transform_1763,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 2029 - ISO 14001-certified enterprises per GDP ----

dim_config_2029 <- tibble(
  data_col = c("Country", "Years"),
  dim_id = c("208", "29117"),
  pub_col = c("208_name", "29117_name")
)

# NEED TO update 1763 data on CEPALSTAT before processing this indicator, because it takes the raw
# values from the published 1763 -- not the data_iso_1763 combined above, deliberately a separate
# variable so the two indicators can't accidentally share/overwrite each other's input
data_iso_2029 <- tibble(call.data(1763)) %>% select(Country, Years, value)

# Bundles filtering + a GDP join + the regional calc together (matching the old regional_2029
# exactly) rather than splitting across filter_data/transform_data, since the zero-country removal
# and GDP join both need to happen before the LAC total can be computed correctly.
calculate_regional_2029 <- function(data) {
  data %<>% remove_zero_countries()

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

  data %>%
    bind_rows(lac) %>%
    mutate(value = round(value / gdp * 1000, 1)) %>%
    select(Country, Years, value) %>%
    arrange(Country, Years)
}

spec_2029 <- indicator_spec(
  indicator_id = 2029,
  data = data_iso_2029,
  max_year = max_year_other,
  dim_config = dim_config_2029,
  filter_data = filter_none,
  transform_data = transform_none,
  calculate_regional = calculate_regional_2029
)


## ---- indicator 2037 - consumption of ozone depleting substances (ODS) ----

dim_config_2037 <- tibble(
  data_col = c("Country", "Years", "Type"),
  dim_id = c("208", "29117", "26657"),
  pub_col = c("208_name", "29117_name", "26657_name")
)

ods_files <- c(
  "Chlorofluorocarbons (CFCs)"       = "odp_cfc_raw.xlsx",
  "Halons"                           = "odp_halons_raw.xlsx",
  "Other fully halogenated CFCs"     = "odp_halcfc_raw.xlsx",
  "Carbon tetrachloride"             = "odp_ctc_raw.xlsx",
  "Methyl chloroform"                = "odp_tca_raw.xlsx", # methyl cloroform / Trichloroethane (TCA) are the same
  "Hydrochlorofluorocarbons (HCFCs)" = "odp_hcfc_raw.xlsx",
  "Hydrobromofluorocarbons (HBFCs)"  = "odp_hbfc_raw.xlsx",
  "Bromochloromethane"               = "odp_bcm_raw.xlsx",
  "Methyl bromide"                   = "odp_mb_raw.xlsx"
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

footnotes_2037 <- list(
  "5792" = function(df) df$Type == "Total" # Incluye todas las sustancias controladas por el Protocolo de Montreal.
)

spec_2037 <- indicator_spec(
  indicator_id = 2037,
  data = data_ods,
  max_year = max_year_other,
  dim_config = dim_config_2037,
  filter_data = filter_2037,
  transform_data = transform_2037,
  calculate_regional = calculate_regional_sum,
  footnotes = footnotes_2037
)


## ---- indicator 2016 - surface area of Ramsar designated wetlands ----

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
    # expand year data since data source only lists the start year it was designated
    mutate(start_year = year(Date)) %>%
    rowwise() %>%
    mutate(Years = list(seq(start_year, max_year_other))) %>%
    ungroup() %>%
    unnest(Years) %>%
    group_by(Country, Years) %>%
    summarize(value = sum(value, na.rm = TRUE), .groups = "drop")
}

# note: the old code used footnote 12417 here instead of the standard 6970 ("Valor total calculado
# por CEPAL en base a la información disponible en la fuente" vs. 6970's "Calculado a partir de la
# información disponible de los países de la región"). There's no per-indicator override for the
# auto-applied LAC footnote, so this now gets the standard 6970 like every other recalculated-LAC
# indicator -- flag if 12417 specifically needs to be preserved instead.

spec_2016 <- indicator_spec(
  indicator_id = 2016,
  data = data_ramsar,
  max_year = max_year_other,
  dim_config = dim_config_2016,
  filter_data = filter_2016,
  transform_data = transform_2016,
  calculate_regional = calculate_regional_sum
)


## ---- indicator 2031 - multilateral environmental agreements ----

# ** NEEDS CONFIRMATION -- see chat: utils.R's assert_data_reqs() already has a standing exception
# ("if (indicator_id == 2031) { data %<>% mutate(Years = as.integer(Years)) }") implying this
# indicator is expected to have a Years column where Years == value (the signature/ratification/
# entry-into-force year IS the value). The old dim_config below has no Years dimension at all, which
# would make assert_data_reqs's unconditional `filter(Years <= max_year)` error immediately. Added
# Years as a 4th dimension and duplicated value into it as a best guess -- please confirm this
# matches what's actually published for 2031 before running it.

dim_config_2031 <- tibble(
  data_col = c("Country", "MEA", "Phase", "Years"),
  dim_id = c("208", "26667", "26689", "29117"),
  pub_col = c("208_name", "26667_name", "26689_name", "29117_name")
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
      cs_id == "Cartagena" ~ "Biosafety", # change labels here to clarify cartagena agreements
      cs_id == "Cartagena-Conv" ~ "Cartagena",
      TRUE ~ cs_id
    )) %>%
    mutate(Phase = case_when(
      Phase == "Signature" ~ "Year of signature",
      Phase == "Ratification" ~ "Year of ratification, acceptance, approval or adhesion",
      Phase == "Force" ~ "Year of entry into force",
      TRUE ~ Phase
    )) %>%
    select(Country, MEA, Phase, value) %>%
    filter(!is.na(value)) %>%
    mutate(Years = value) # ** best guess -- see NEEDS CONFIRMATION note above
}

footnotes_2031 <- list(
  "5496" = function(df) df$MEA == "Biological diversity", # Convenio sobre la Diversidad Biológica (1992).
  "5495" = function(df) df$MEA == "Basel", # Convenio de Basilea sobre el Control de los Movimientos Transfronterizos de los Desechos Peligrosos y su Eliminación (1989).
  "5490" = function(df) df$MEA == "CITES", # Convención sobre el Comercio Internacional de Especies Amenazadas de Fauna y Flora Silvestres (1973).
  "5491" = function(df) df$MEA == "CMS", # Convención sobre la conservación de las especies migratorias de animales silvestres (1979).
  "5502" = function(df) df$MEA == "Stockholm", # Convenio de Estocolmo sobre Contaminantes Orgánicos Persistentes (2001).
  "5493" = function(df) df$MEA == "Vienna", # Convenio de Viena para la Protección de la Capa de Ozono (1985).
  "5494" = function(df) df$MEA == "Montreal", # Protocolo de Montreal relativo a las Sustancias que Agotan la Capa de Ozono (1987).
  "5501" = function(df) df$MEA == "Biosafety", # Protocolo de Cartagena sobre Seguridad de la Biotecnología (2000).
  "5497" = function(df) df$MEA == "Climate Change", # Convención Marco de las Naciones Unidas sobre el Cambio Climático (1992).
  "5499" = function(df) df$MEA == "Kyoto", # Protocolo de Kyoto de la Convención Marco de las Naciones Unidas sobre el Cambio Climático (1997).
  "5488" = function(df) df$MEA == "Ramsar", # Convención Relativa a los Humedales de Importancia Internacional, Especialmente como Hábitat de Aves Acuáticas (1971).
  "5498" = function(df) df$MEA == "Desertification", # Convención de las Naciones Unidas de lucha contra la Desertificación en los Países Afectados por Sequía Grave o Desertificación, en particular en África (1994).
  "5500" = function(df) df$MEA == "Rotterdam", # Convenio de Rotterdam sobre el Procedimiento de Consentimiento Fundamentado Previo Aplicable a Ciertos Plaguicidas y Productos Químicos Peligrosos Objeto de Comercio Internacional (1998).
  "16773" = function(df) df$MEA == "Cartagena", # Convenio para la protección y el desarrollo del medio marino en la región del Gran Caribe (1983)
  "8788" = function(df) df$MEA == "Minamata", # Convenio de Minamata sobre el Mercurio (2013)
  "7780" = function(df) df$MEA == "Paris-UNFCCC", # Acuerdo de París en el marco de la Convención Marco de de las Naciones Unidas sobre el Cambio Climático (UNFCCC) (2015)
  "8789" = function(df) df$MEA == "Escazu", # Acuerdo Regional (Escazú) sobre el Acceso a la Información, la Participación Pública y el Acceso a la Justicia en Asuntos Ambientales en América Latina y el Caribe (2018)
  "5492" = function(df) df$MEA == "Law of the Sea", # Convención de las Naciones Unidas sobre el Derecho del Mar (1982).
  "5489" = function(df) df$MEA == "Heritage" # Convenio sobre la Protección del Patrimonio Mundial, Cultural y Natural (1972).
)

define_source_2031 <- function(df, indicator_id) {
  df %>% mutate(source_id = 11306) # Portal de las Naciones Unidas sobre acuerdos multilaterales sobre el medio ambiente (InforMEA)
}

spec_2031 <- indicator_spec(
  indicator_id = 2031,
  data = data_mea,
  max_year = max_year_other,
  dim_config = dim_config_2031,
  filter_data = filter_2031,
  transform_data = transform_2031,
  calculate_regional = maintain_regional, # no ECLAC average
  footnotes = footnotes_2031,
  define_source = define_source_2031
)
