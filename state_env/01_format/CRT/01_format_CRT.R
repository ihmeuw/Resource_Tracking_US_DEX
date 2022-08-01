## ---------------------------
## Purpose: Process Census of Retail Trade data
## Author: USERNAME
## Date modified: 4/13/2021
## ---------------------------
rm(list = ls())

source("FILEPATH/currency_conversion.R")
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

source("FILEPATH/currency_conversion.R")

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, openxlsx, arrow)
in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

## ---------------------------
## NAICS codes of interest
## ---------------------------

## the below are type of care speficic. 
pharma_codes <- c("44611")
dme_other_codes <- c()

## ---------------------------
## Get location info
## ---------------------------

states <- fread("FILEPATH/states.csv")

## pad FIPS codes
states[,state := str_pad(state,width = 2, side = "left", pad = "0")]

## ---------------------------
## Read datafiles
## ---------------------------

## 2017
crt_17 <- fread(paste0(in_dir, "2017/USA_ECONOMIC_CENSUS_2017_SECTOR_44_EC1744BASIC_Y2021M09D20.DAT"))
## 2012 
crt_12 <- fread(paste0(in_dir, "2012/USA_ECONOMIC_CENSUS_2012_SECTOR_44_EC1244A1_Y2021M09D20.DAT"))
## 2007
crt_07 <- fread(paste0(in_dir, "2007/USA_ECONOMIC_CENSUS_2007_COUNTY_EC0744A1_Y2021M09D20.DAT"))
## 2002
crt_02 <- fread(paste0(in_dir, "2002/USA_ECONOMIC_CENSUS_2002_CRT_COUNTY_EC0244A1_Y2021M04D12.DAT"))

## ---------------------------
## Clean each file
## ---------------------------

## Filter to state and county (not all have "geo_type_meaning")
## geotype 2,3 = state,county 
## "& COUNTY + PLACE + CSA + MSA == 0" is included to make sure we only get state level totals
crt_02 <- crt_02[(GEOTYPE == 2 & COUNTY + PLACE + CSA + MSA == 0) | GEOTYPE == 3]
crt_07 <- crt_07[(GEOTYPE == 2 & COUNTY + PLACE + CSA + MSA == 0) | GEOTYPE == 3]
crt_12 <- crt_12[(GEOTYPE == 2 & COUNTY + PLACE + CSA + MSA == 0) | GEOTYPE == 3]
crt_17 <- crt_17[(GEOTYPE == 2 & COUNTY + PLACE + CSA + MSA == 0) | GEOTYPE == 3]

## select and standardize column names to be able to bind data
crt_02 <- crt_02[,.(
  YEAR, GEOTYPE, ST, COUNTY, ## demographic stuff
  NAICS = NAICS2002, NAICS_TTL = NAICS2002_MEANING, ## business codes
  RCPTOT,  ## revenue ($1000)
  RCPTOT_F ## flag
)]
crt_07 <- crt_07[,.(
  YEAR, GEOTYPE, ST, COUNTY, ## demographic stuff
  NAICS = NAICS2007, NAICS_TTL = NAICS2007_MEANING, ## business codes
  RCPTOT,  ## revenue ($1000)
  RCPTOT_F ## flag
)]
crt_12 <- crt_12[,.(
  YEAR, GEOTYPE, ST, COUNTY, ## demographic stuff
  NAICS = NAICS2012, NAICS_TTL = NAICS2012_TTL, ## business codes
  RCPTOT,  ## revenue ($1000)
  RCPTOT_F ## flag
)]
crt_17 <- crt_17[,.(
  YEAR, GEOTYPE, ST, COUNTY, ## demographic stuff
  NAICS = NAICS2017, NAICS_TTL = NAICS2017_TTL, ## business codes
  RCPTOT,  ## revenue ($1000)
  RCPTOT_F ## flag
)]

## ---------------------------
## combine and clean up
## ---------------------------

## bind
CRT <- rbindlist(
  list(crt_02, crt_07, crt_12, crt_17),
  use.names = T, 
  fill = T
)

## recode geotype
CRT[,GEOTYPE := plyr::revalue(as.character(GEOTYPE), c("2" = "state", "3" = "county"))]

## rename
setnames(
  CRT, 
  c("YEAR", "GEOTYPE", "ST", "COUNTY", 
    "NAICS", "NAICS_TTL", 
    "RCPTOT", 
    "RCPTOT_F"), 
  c("year_id", "geography_type", "state", "cnty", 
    "NAICS", "NAICS_title", 
    "revenue", 
    "revenue_flag")
)

## pad and fill FIPS codes
CRT[,state := str_pad(state,width = 2, side = "left", pad = "0")]
CRT[,cnty := paste0(state, str_pad(cnty,width = 3, side = "left", pad = "0"))]

## clean up flag
stopifnot(CRT[revenue_flag != "",unique(revenue_flag) == "D"])
CRT[,flag_D := ifelse(revenue_flag == "D", 1, 0)]
CRT[,revenue_flag := NULL]

## filter to codes we care about
CRT <- CRT[NAICS %in% c(pharma_codes, dme_other_codes)]

## check state data is square
stopifnot(
  nrow(CRT[geography_type == "state"]) == CRT[geography_type == "state", uniqueN(year_id)*uniqueN(state)]
)

## check no missing data
stopifnot(
  CRT[geography_type == "state", sum(is.na(revenue) | revenue == 0 | flag_D == 1)] == 0
)

## ---------------------------
## assign types of care
## ---------------------------

CRT[NAICS %in% as.numeric(pharma_codes), type := "pharma"]
CRT[NAICS %in% as.numeric(dme_other_codes), type := "dme_other"]

## ---------------------------
## add NID
## ---------------------------
nid <- fread("FILEPATH") %>% select(-dataset)
CRT <- merge(CRT, nid, by="year_id",all.x = T)

## ---------------------------
## merge in location columns and split state/county
## ---------------------------

## split by state/county
crt_state <- CRT[geography_type == "state", -c("geography_type", "cnty")]
crt_county <- CRT[geography_type == "county", -c("geography_type", "state")]

## note what we're dropping
if(length(diff(crt_state$state, states$state)) > 0) warning("Dropping states: ", paste0(diff(crt_county$cnty, counties$cnty), collapse = ", "))
if(length(diff(crt_county$cnty, counties$cnty)) > 0) warning("Dropping counties: ", paste0(diff(crt_county$cnty, counties$cnty), collapse = ", "))

## merge in location stuff
crt_state <- merge(crt_state, states, by = "state")
crt_county <- merge(crt_county, counties, by = "cnty")

## ---------------------------
## aggregate across types of care
## ---------------------------

## there are no health sector codes here (no 62* codes)
## ...

## sum by type of care
state <- crt_state[!is.na(type), .(
    revenue = sum(revenue, na.rm = T),
    flag_D = round(mean(flag_D),2) ## proportion of obs with flag D
  ), 
  by = c("nid", "year_id", names(states), "type")
]

county <- crt_county[!is.na(type), .(
    revenue = sum(revenue, na.rm = T),
    flag_D = round(mean(flag_D),2) ## proportion of obs with flag D
  ), 
  by = c("nid", "year_id", names(counties), "type")
]

## order
setcolorder(county, c("year_id", names(counties)))
setcolorder(state, c("year_id", names(states)))
county <- county[order(year_id, cnty)]
state <- state[order(year_id, state)]

## convert to millions
county[,revenue := (revenue*1000)/10^6]
state[,revenue := (revenue*1000)/10^6]

## ---------------------------
## Currency convert
## ---------------------------

## Currency convert state data
state[, iso3 := "USA"][, year := year_id]
state <- currency_conversion(
  data = state, 
  col.loc = "iso3",
  col.value = "revenue",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)
state[,iso3 := NULL]

## Currency convert county data
county[, iso3 := "USA"][, year := year_id]
county <- currency_conversion(
  data = county, 
  col.loc = "iso3",
  col.value = "revenue",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)
county[,iso3 := NULL]

## ---------------------------
## write
## ---------------------------
write_feather(state, paste0(out_dir, "CRT_state.feather"))
write_feather(county, paste0(out_dir, "CRT_county.feather"))
