## ---------------------------
## Purpose: Process AHA hospital data from KFF
## Author: USERNAME
## Date modified: 4/12/2021
## ---------------------------
rm(list = ls())

## ---------------------------
## Setup
## ---------------------------

source("FILEPATH/get_population.R")
source("FILEPATH/currency_conversion.R")
load("FILEPATH/states.RData")

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

## get NIDS
nids <- fread("FILEPATH/KFF_AHA.csv")

## ---------------------------
## Get data
## ---------------------------

## list files
cost_files <- Sys.glob(paste0(in_dir, "KAISER_HOSPITAL_EXPENSES/*/*CSV"))
days_files <- Sys.glob(paste0(in_dir, "KAISER_HOSPITAL_BEDS/*/*CSV"))

## function to read data and year from awkward CSV files
read_fcn <- function(x){
  dt <- fread(x,skip = 2, nrow = 52)
  yr <- str_extract(read_lines(x)[2], "[:digit:]+")
  dt[,year_id := as.integer(yr)]
}

## read!
cost <- rbindlist(lapply(cost_files, read_fcn))
days <- rbindlist(lapply(days_files, read_fcn))

## clean up data
setnames(cost, c("Location", "Expenses per Inpatient Day"), c("state_name", "cost_per_IP_day"))
setnames(days, c("Location", "Total"), c("state_name", "IP_days_per_1000_pop"))
days <- days[,.(state_name, IP_days_per_1000_pop, year_id)]
cost[,cost_per_IP_day := as.numeric(str_remove(cost_per_IP_day, "\\$"))]
days[,IP_days_per_1000_pop := as.numeric(IP_days_per_1000_pop)]

## merge days and cost
hosp <- merge(cost, days, by= c("state_name", "year_id"), all = T)

## ensure state names are standard
hosp <- hosp[state_name != "United States"]
diff(unique(hosp$state_name), states$state_name)

## merge in location ID
hosp <- merge(hosp, states, by = "state_name")

## ---------------------------
## Convert to our total spending
## ---------------------------

## convert to total spending per 1000 pop
hosp[,spend_per_1000_pop := cost_per_IP_day*IP_days_per_1000_pop]

## convert to spending per capita
hosp[,spend_per_pop := spend_per_1000_pop/1000]
hosp[,c("cost_per_IP_day", "IP_days_per_1000_pop", "spend_per_1000_pop") := NULL]

## get population
s_id <- unique(states$location_id)
pop <- get_population(
  gbd_round_id =7, 
  decomp_step = "iterative",
  location_id = s_id,
  age_group_id = 22,
  sex_id = 3, 
  year_id = unique(hosp$year_id)
)
pop <- pop[,.(location_id, year_id, population)]

## convert to total spending
lapply(list(hosp, pop), function(x) x[,location_id := as.character(location_id)])
hosp <- merge(hosp, pop, by = c("location_id", "year_id"))
hosp[,hospital_spending := spend_per_pop*population]
hosp[,c("spend_per_pop", "population") := NULL]

## currency convert
hosp[, iso3 := "USA"][, year := year_id]
hosp <- currency_conversion(
  data = hosp, 
  col.loc = "iso3",
  col.value = "hospital_spending",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)

## ---------------------------
## add NID
## ---------------------------
hosp[,nid := nids[1,dex_nid]]

## ---------------------------
## write
## ---------------------------
setcolorder(hosp, c("year_id", names(states)))
arrow::write_feather(hosp, paste0(out_dir, "kff_aha_hospital_spending.feather"))