library(httr2)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)

base <- "https://www.climatewatchdata.org/api/v1/data/historical_emissions"

cw_get <- function(url) {
  request(url) %>%
    req_perform() %>%
    resp_body_json(simplifyVector = TRUE)
}


data_sources <- cw_get(paste0(base, "/data_sources"))$data %>% as_tibble()
data_sources
