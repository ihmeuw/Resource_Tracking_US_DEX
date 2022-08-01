## ---------------------------
## Purpose: calculate N federal employees by state 
## Author: USERNAME
## Date modified: 4/8/2021
## ---------------------------
rm(list = ls())

pacman::p_load(data.table, tidyverse, arrow)

## get QCEW proportions
qcew <- data.table(read_feather("FILEPATH/QCEW_prop_national_employees.feather"))

## get total federal employees
federal_employees <- fread("FILEPATH/FEDERAL_RESERVE_ECONOMIC_DATA_FRED/USA_FRED_1980_2019_Y2021M10D07.CSV")
federal_employees <- federal_employees[,.(
  year_id = as.numeric(str_extract(DATE, "[:digit:]{4}")),
  n_employees = CES9091000001*1000
)]
federal_employees <- federal_employees[year_id %in% qcew$year_id]

## check N states by year
stopifnot(federal_employees[,.N, by = year_id][,unique(N)] == 12)

## take mean employees across months
federal_employees <- federal_employees[,.(n_employees = mean(n_employees)), by = year_id]

## merge
state_fed <- merge(qcew, federal_employees, by = c("year_id"))

## calculate 
state_fed <- state_fed[,.(
  year_id, 
  abbreviation, 
  state_name, 
  location_id, 
  state, 
  region, 
  division, 
  n_employees = n_employees*prop_national_employees
)]

## add NID
nid <- fread("FILEPATH/fed_emp.csv")$dex_nid %>% unique()
state_fed[,nid := nid]

## write out
write_feather(state_fed, "FILEPATH/N_employees_by_state.feather")
