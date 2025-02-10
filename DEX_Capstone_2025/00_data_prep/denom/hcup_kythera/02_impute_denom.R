## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Imputes denominator data for private data sources
##          using MSCAN enrollment rates as a reference
## ==================================================

rm(list = ls())

PRI <- "RUN_ID"

## --------------------------------------------
## Setup
## --------------------------------------------
## packages
library(tidyverse)
library(data.table)
library(arrow)
library(reticulate)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

# location info
load("[repo_root]/static_files/GEOGRAPHY/states.RData")
load("[repo_root]/static_files/GEOGRAPHY/merged_counties.RData")
counties <- merge(counties[,.(mcnty, state_name, current)], states[,.(state_name, abbreviation)], by="state_name")
counties <- counties[current == 1,.(abbreviation, mcnty_resi = mcnty)]
counties <- unique(counties)
counties <- rbind(counties, data.table(abbreviation = NA, mcnty_resi = NA))

## config
raw_config <- get_config()
config <- parsed_config(raw_config, "SAMPLE_DENOM", run_id = PRI)
config <- config$data_output_dir

## --------------------------------------------
## Read in utilization data
## --------------------------------------------

# Read util
util <- open_dataset(paste0("FILEPATH")) %>% ## out_dir from 01_prep_util_rates.R
  collect() %>% 
  data.table()

## set broad ages
util[age_group_years_start <= 25, broad_age_start := 0]
util[age_group_years_start > 25 & age_group_years_start < 65, broad_age_start := 30]
util[age_group_years_start >= 65, broad_age_start := 65]

## --------------------------------------------
## Aggregate util rates from MSCAN
## --------------------------------------------

# get mscan util rates
mscan_rates <- util[
  i = source == "mscan",
  j = .(enrollment = sum(enrollment, na.rm = TRUE), util = sum(util, na.rm = TRUE)), 
  by = .(toc, pri_payer, state, broad_age_start)
]
## add in national (to calculate denom for NIS/NEDS)
mscan_rates_ntl <- mscan_rates[
  j = .(enrollment = sum(enrollment, na.rm = TRUE), util = sum(util, na.rm = TRUE), state = "USA"), 
  by = .(toc, pri_payer, broad_age_start)
]
mscan_rates <- rbind(mscan_rates, mscan_rates_ntl, use.names = T, fill = T)

## now, remove st_resi == -1 (it should be included when calculating national rates (above), but not left at the state level
mscan_rates <- mscan_rates[state != "-1"]

## and NOW (somewhat confusingly) rename "USA" to "-1" again (haha)
## this is because we want to use the NATIONAL rate to inform unknown states below
## so in MSCAN rates, "-1" means national, whereas in KYTHERA, it means "we don't know the state"
## -- we want these two to be merged together
mscan_rates[state == "USA", state := "-1"]

## calculate util rates
mscan_rates[, util_rate := util/enrollment]

## make sure no dups in demographic categories
stopifnot(sum(duplicated(mscan_rates[,.(toc, pri_payer, state, broad_age_start)])) == 0)

## drop possible inf (impossible) util_rate
mscan_rates <- mscan_rates[enrollment > 0] ## would lead to inf or NA util_rate

## remove unneeded columns
mscan_rates[, c("enrollment", "util") := NULL]

## --------------------------------------------
## Calculate HCUP/KYTHERA denom
## --------------------------------------------
##
## _kh = KYTHERA, HCUP
## _m = MSCAN
## ...
## denom_kh = util_kh * (denom_m / util_m)
## ...
## ^ so, assume KYTHERA/HCUP have same utilization/denom rates as MSCAN

## get Kythera/hcup
impute <- util[source %in% c("kyth","sids","sedd","nis","neds")]


impute <- impute[
  j = .(util = sum(util, na.rm = T)), 
  by = .(source, toc, year_id, pri_payer, state, mcnty, age_group_years_start, broad_age_start, sex_id)
]
impute_merge_cols <- c("toc","pri_payer", "state", "broad_age_start")

## merge data to impute with with priv util rates
impute <- merge(
  impute, mscan_rates, 
  by = impute_merge_cols, 
  all.x = TRUE
)

## drop remaining NAs/0s in util_rate
impute <- impute[util_rate > 0]

## make sure no NA rates
stopifnot(sum(is.na(impute$util_rate)) == 0)

## make sure no NA util
stopifnot(sum(is.na(impute$util)) == 0)

## make sure no 0 util
stopifnot(sum(impute$util == 0) == 0)

## calculate denom for kyth/hcup assi\uming it has the same ratio of util/enrollment as MSCAN
impute[, denom := util/util_rate] # denom_kh = util_kh * (denom_m / util_m) = util_kh / (util_m / denom_m)

# clean up
ids <- c("source", "toc", "year_id", "pri_payer", "state", "mcnty", "age_group_years_start", "sex_id", "util", "util_rate", "denom")
impute <- impute[,ids, with = F]

## check how much is NA by source
impute[,.(prop_missing_state = mean(state == "-1"), prop_missing_county = mean(mcnty == -1)), by = source][order(-prop_missing_county)]  

## use full name for final data
impute[source == "kyth", source := "kythera"]

## check for no demographic duplicates
stopifnot(sum(duplicated(impute[,.(source, toc, year_id, pri_payer, state, mcnty, age_group_years_start, sex_id)])) == 0)  

## make sure only 1 na mcnty per group
## (every data source with county data has a single "NA" county that represents unkonwn county denom)
## -- it's left in so that it can be included when denom for that state (or national) is calculated
n_na_mcnty <- impute[,.(n = sum(mcnty == -1)), by = .(source, toc, year_id, pri_payer, state, age_group_years_start, sex_id)]
stopifnot(max(n_na_mcnty$n) == 1)

# Quick fix for weird thing where we have mcnty but not state in Kythera
impute <- impute[(state == "-1" & mcnty == "-1") | state != "-1"]

## --------------------------------------------
## Save out
## --------------------------------------------
## write out
lapply(unique(impute$source), function(sc){
  print(sc)
  fpath <- config[[toupper(sc)]]
  print(fpath)
  unlink(fpath, recursive = TRUE)
  out_data <- impute[source == sc]
  if(sc %in% c("nis", "neds")) out_data[,c("state", "mcnty") := NULL]
  out_data %>%
    group_by(year_id, age_group_years_start, sex_id) %>%
    write_dataset(fpath, format = "parquet", existing_data_behavior = "overwrite")
})
