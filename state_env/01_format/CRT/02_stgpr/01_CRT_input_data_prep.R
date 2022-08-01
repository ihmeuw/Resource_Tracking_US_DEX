################################################
#' @description save input data for CRT 
################################################

rm(list=ls())


# if we want to model in per capita space
per_capita <- T



library(data.table)
library(tidyverse)
library(arrow)

in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_population.R")

## ------------------------------------
## prep input data
## ------------------------------------

state_data <- data.table(read_feather(paste0(in_dir,"CRT_state.feather")))

## check that flags are exclusive
stopifnot(state_data[flag_S > 0 & flag_D > 0, .N == 0])

#keep key variables and create necessary variables
#data must include nid, location_id, sex_id, year_id, age_group_id, measure_id, val, variance, sample_size, and is_outlier.
state_data <- state_data[,.(
  type, 
  nid, 
  location_id, 
  sex_id = 3, 
  year_id, 
  age_group_id=22, 
  measure_id = 19,
  val = revenue,
  variance = var(revenue)/10^6,
  sample_size = .N,
  is_outlier = 0, 
  ds_flag = flag_D ## flag outlier if we have any data suppression
)]

## update variance based on data suppression flag
## increase variance by a percentage equal to the percentage of data points with suppression flags
state_data[,variance := variance * (1+ds_flag)]

## get population
s_id <- unique(state_data$location_id)
pop <- get_population(
  gbd_round_id =7, 
  decomp_step = "iterative",
  location_id = s_id,
  age_group_id = 22,
  sex_id = 3, 
  year_id = c(2002,2007,2012,2017)
)

## if model in per capita, convert
if (per_capita == T) {
  state_data <- merge(state_data, pop, all.x = T)
  state_data[,val := (val*10^6)/population][,population := NULL]
}

## split and write input for each type
out <- split(state_data, state_data$type)
lapply(
  names(out), 
  function(x){
    fwrite(out[[x]], paste0(out_dir, x, ".csv"))
  }
)

## ------------------------------------
## get covariates
## ------------------------------------

qcew <- data.table(arrow::read_feather("FILEPATH"))

## write file
fwrite(qcew, paste0(out_dir, "covariates.csv"))

