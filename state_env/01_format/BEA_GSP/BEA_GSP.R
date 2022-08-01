## ---------------------------
## Purpose: Process GDP by state (GSP)
## Author: USERNAME
## Date modified: 10/11/2021
## ---------------------------

rm(list = ls())

pacman::p_load(data.table, tidyverse)
source("FILEPATH") # currency conversion function
load("FILEPATH") # states lookup

## get data
setwd("FILEPATH")
dt <- fread("FILEPATH", skip = 4, nrows = 51) 

## reshape
dt[,GeoFips := NULL]
dt <- melt(dt, id.vars = "GeoName")

## Take average GDP across the quarters
dt[,year := as.numeric(str_extract(variable, "[:digit:]{4}"))]
dt <- dt[,.(gdp = mean(value)), by = .(state_name = GeoName, year_id = year)]

## currency convert out of 2021 dollars
dt[,iso3 := "USA"][,year := year_id]
dt <- currency_conversion(
  data = dt, 
  col.loc = "iso3",
  col.value = "gdp",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)
dt[,iso3 := NULL]

## remove years > 2019
dt <- dt[year_id <= 2019]

## merge in states
dt <- merge(dt, states, by = "state_name", all.x = T)
stopifnot(dt[is.na(state), .N == 0])

## set name to match unit
setnames(dt, "gdp", "gdp_millions")

## add NID
dt[,nid := "490410"]

## write out
fwrite(dt, "FILEPATH")
