## ---------------------------
## Purpose: Prep QCEW NUMBER OF EMPLOYEES as covariate 
## Author: USERNAME
## Date modified: 4/8/2021
## ---------------------------
rm(list = ls())

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, openxlsx, arrow, lme4)
dir <- "FILEPATH"
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

load('FILEPATH/states.RData')

## get QCEW data
qcew_files <- Sys.glob("FILEPATH/employment_pct_by_state/*")
qcew <- rbindlist(
  lapply(
    qcew_files, 
    function(x) data.table(arrow::read_feather(x))
  )
)

## simplify
qcew <- qcew[,.(location_id, year_id, own_title, industry_title, annual_avg_emplvl)]

## sum federal employees
qcew <- qcew[own_title %like% "Federal",.(annual_avg_emplvl = sum(annual_avg_emplvl)), by = .(location_id, year_id)]

## convert to proportion national
qcew[,ntl_annual_avg_emplvl := sum(annual_avg_emplvl), by = .(year_id)]
qcew[,prop_national_employees := annual_avg_emplvl/ntl_annual_avg_emplvl]

## add in location info
qcew[,location_id := as.character(location_id)]
states[,location_id := as.character(location_id)]
qcew <- merge(qcew, states, by = "location_id", all = T)

## simplify and write4
qcew <- qcew[,c(names(states), "year_id", "prop_national_employees"), with = F]
arrow::write_feather(qcew, paste0(dir, "/QCEW_prop_national_employees.feather"))
