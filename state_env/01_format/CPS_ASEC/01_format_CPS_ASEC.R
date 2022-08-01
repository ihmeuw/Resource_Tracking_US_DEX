## ---------------------------
## Purpose: Process Current Population Survey Annual Social and Economic Supplement
## Author: USERNAME
## Date modified: 9/15/2021
## ---------------------------

# https://ceprdata.org/cps-uniform-data-extracts/march-cps-supplement/march-cps-data/
# https://ceprdata.org/cps-uniform-data-extracts/march-cps-supplement/march-cps-documentation/
# http://ceprdata.org/cps-uniform-data-extracts/march-cps-supplement/march-cps-programs/

rm(list = ls())
pacman::p_load(data.table, arrow, tidyverse)
d <- data.table

NIDs <- c(
  "2018" = "476404",
  "2017" = "476403",
  "2016" = "476402",
  "2015" = "476401",
  "2014" = "476400",
  "2013" = "476399",
  "2012" = "476398",
  "2011" = "483851"
)

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

load("FILEPATH/states.RData") 
source("FILEPATH/currency_conversion.R")

in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

## get data
yrs <- 2011:2018

cps <- rbindlist(
  lapply(
    yrs, 
    function(x){
      file <- Sys.glob(paste0(in_dir, "*", x, "*MARCH*FEATHER"))
      dat <- d(read_feather(file))
    }
  ), 
  use.names = T, 
  fill = T
)

## confirm our expectation about the data
## PMOOP is the sum of: 
##   - PHIP_VAL: Out of pocket expenditures for comprehensive and noncomprehensive health insurance premiums 
##   - POTC_VAL: Out of pocket expenditures for over the counter health related spending 
##   - PMED_VAL: Out of pocket expenditures for non-premium medical care 
cps <- cps[,.(fnlwgt, pmoop, phipval, potcval, pmedval, state_name = state, year)]
cps[pmoop != phipval + potcval + pmedval]

## create OOP excluding premiums
cps[,oop := pmoop - phipval]

## sum by weight
cps_w <- cps[,.(oop = sum(oop*fnlwgt, na.rm = T)), by = .(state_name, year)]

## merge in states
diff(cps_w$state_name, states$state_name)
cps_w <- merge(cps_w, states, by = "state_name")

## currency convert
cps_w[, iso3 := "USA"][, year_id := year]
cps_w <- currency_conversion(
  data = cps_w, 
  col.loc = "iso3",
  col.value = "oop",
  currency = "usd",
  col.currency.year = "year_id",
  base.year = 2020
)
cps_w[,iso3 := NULL]

## add NID
cps_w[,nid := plyr::revalue(as.character(year), NIDs)]

## write
write_feather(cps_w, paste0(out_dir, "CPS_ASEC.feather"))
