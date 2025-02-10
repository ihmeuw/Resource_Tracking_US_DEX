## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Compiles utilization and enrollment data for 
##          imputing private-insurance sample denominators.
## ==================================================

rm(list = ls())

run_id <- "RUN_ID"

out_dir <- "FILEPATH"

## --------------------------------------------
## Setup
## --------------------------------------------

## packages
library(data.table)
library(tidyverse)
library(arrow)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

## location info
## states   : state_name | state
load("[repo_root]/static_files/GEOGRAPHY/states.RData")
states <- states[,.(state_name, state = abbreviation)]

## set directories
print('config')
config <- get_config()
private_denom_dirs <- parsed_config(config, "PRIV_UTIL", run_id)$data_output_dir ## for util counts
sample_denom_dirs <- parsed_config(config, "SAMPLE_DENOM")$data_output_dir ## for sample denom
detrunc_config <- parsed_config(config, "DETRUNC", run_id)

## years
ages <- c(0,1,seq(5,95, by = 5))

## --------------------------------------------
## get MSCAN data
## state-specific
## --------------------------------------------
print('MSCAN denom')
mscan_denom <- open_dataset(sample_denom_dirs[["MSCAN"]], format = "parquet") %>% collect() %>% data.table()
mscan_denom <- mscan_denom[
  j = .(enrollment = sum(pop)), 
  by = .(year_id, location, age_group_years_start, sex_id)
]
setnames(mscan_denom, "location", "st_resi")

## remove possible demographic NAs
mscan_denom <- mscan_denom[!is.na(age_group_years_start) & age_group_years_start != -1]
mscan_denom <- mscan_denom[!is.na(sex_id) & sex_id != -1]

print('MSCAN data')
mscan <- open_dataset(paste0(paste0(private_denom_dirs[["MSCAN"]], "data")), format = "parquet") %>% 
  group_by(toc, pri_payer, year_id, st_resi, age_group_years_start, sex_id) %>%
  summarize(util = sum(util, na.rm = TRUE)) %>%
  collect() %>% 
  data.table()

## ensure pri_payers are correct
stopifnot(all(mscan$pri_payer %in% c(2, 23))) 
## priv, mdcr_priv

## ensure ages are correct
stopifnot(all(mscan$age_group_years_start %in% ages))

## any age 65+ pri_payer 2 (priv) should be recoded to mdcr_priv
mscan[age_group_years_start >= 65 & pri_payer == 2, pri_payer := 23] 

## re-aggregate post-recoding (before merging with denom)
mscan <- mscan[,.(util = sum(util, na.rm = T)), by = .(toc, pri_payer, year_id, st_resi, age_group_years_start, sex_id)]

## remove possible demographic NAs
mscan <- mscan[!is.na(age_group_years_start) & age_group_years_start != -1]
mscan <- mscan[!is.na(sex_id) & sex_id != -1]

## cross denom with TOC
mscan_denom <- data.table(crossing(
  mscan_denom, 
  toc = unique(mscan$toc),
  pri_payer = unique(mscan$pri_payer)
))

## remove denom for pri_payer 2 (priv) post-age 65
## (denom isn't actually pri_payer specific, so we're just removing that crossing)
mscan_denom <- mscan_denom[!(pri_payer == 2 & age_group_years_start >= 65)]

## merge with denom
mscan <- merge(mscan, mscan_denom, by=c("toc", "year_id", "pri_payer", "st_resi", "age_group_years_start","sex_id"), all = T)

## final cleanup
mscan[is.na(util), util := 0]
mscan[is.na(enrollment), enrollment := 0]
mscan[enrollment > 0 & enrollment < 1, enrollment := 1]

## re-aggregate again (just in case)
mscan <- mscan[
  j = .(util = sum(util), enrollment = sum(enrollment)), 
  by = .(toc, pri_payer, year_id, st_resi, age_group_years_start, sex_id)
]
mscan <- mscan[,.(source = "mscan", toc, year_id, pri_payer, state = st_resi, mcnty = -1, age_group_years_start, sex_id, enrollment, util)]
stopifnot(sum(duplicated(mscan)) == 0)

## --------------------------------------------
## get KYTHERA data
## state- and county-specific
## --------------------------------------------

print('KYTHERA data')
kyth <- open_dataset(paste0(private_denom_dirs[["KYTHERA"]], "data"), format = "parquet") %>%
  filter(year_id >= 2016) %>%
  group_by(toc, pri_payer, year_id, st_resi, mcnty_resi, age_group_years_start, sex_id) %>%
  summarize(util = sum(util, na.rm = TRUE)) %>%
  collect() %>% 
  data.table()

## any age 65+ pri_payer 2 (priv) should be recoded to mdcr_priv
kyth[age_group_years_start >= 65 & pri_payer == 2, pri_payer := 23]

## remove possible age/sex NAS 
kyth <- kyth[!is.na(age_group_years_start) & age_group_years_start != -1]
kyth <- kyth[!is.na(sex_id) & sex_id != -1]

## ensure ages are correct
stopifnot(all(kyth$age_group_years_start %in% ages))

## standardize mcnty/state NAs
kyth[is.na(st_resi), st_resi := "-1"]
kyth[is.na(mcnty_resi), mcnty_resi := -1]

## re-aggregate
kyth <- kyth[
  j = .(util = sum(util)), 
  by = .(toc, pri_payer, year_id, st_resi, mcnty_resi, age_group_years_start, sex_id)
]
kyth <- kyth[,.(source = "kyth", toc, year_id, pri_payer, state = st_resi, mcnty = as.character(mcnty_resi), age_group_years_start, sex_id)]
stopifnot(sum(duplicated(kyth)) == 0)

## --------------------------------------------
## get HCUP data
## state- and county-specific (consider st_serv and st_resi)
## --------------------------------------------

print('HCUP data')
hcup <- lapply(c("SIDS","SEDD","NIS","NEDS"), function(source){
  
  print(source)
  
  if(source %in% c("SIDS", "SEDD")){
    ## read data
    df <- open_dataset(paste0(private_denom_dirs[[source]], "data"), format = "parquet") %>%
      group_by(toc, pri_payer, year_id, st_resi, mcnty_resi, age_group_years_start, sex_id) %>%
      summarize(util = sum(util, na.rm = TRUE)) %>%
      collect() %>%
      data.table()
    
  }else{
    ## NEDS being finicky with error: "Unsupported cast from string to null using function cast_null"
    ## fixing schema with our function
    data_schema <- update_nulls_schema(paste0(private_denom_dirs[[source]], "data")) 
    ## read data
    df <- open_dataset(paste0(private_denom_dirs[[source]], "data"), schema = data_schema, format = "parquet") %>%
      group_by(toc, pri_payer, year_id, age_group_years_start, sex_id) %>%
      summarize(util = sum(util, na.rm = TRUE)) %>%
      collect() %>%
      data.table() %>%
      mutate(st_resi = "-1", mcnty_resi = "-1")
  }
  
  ## standardize any mcnty/state NAs
  df[is.na(st_resi), st_resi := "-1"]
  df[is.na(mcnty_resi), mcnty_resi := -1]
  
  ## remove possible age/sex NAS
  df <- df[!is.na(age_group_years_start) & age_group_years_start != -1]
  df <- df[!is.na(sex_id) & sex_id != -1]
  
  ## ensure ages are correct
  stopifnot(all(df$age_group_years_start %in% ages))
  
  ## ensure pri_payers are correct
  stopifnot(all(df$pri_payer %in% c(2, 23)))
  ## mdcr, priv, mdcd, oop, mdcr_mdcd, mdcr_priv
  ## ^ not going to use all the above necessarily
  
  ## any age 65+ pri_payer 2 (priv) should be recoded to mdcr_priv
  df[age_group_years_start >= 65 & pri_payer == 2, pri_payer := 23]
  
  ## re-aggregate (after combining NA/unknowns)
  df <- df[,.(util = sum(util)), by = .(toc, pri_payer, year_id, age_group_years_start, sex_id, st_resi, mcnty_resi)]

  ## add source as a column
  df[, source := tolower(source)]
  
  return(df)
  
}) %>% rbindlist(use.names = T, fill = T)

## reorder columns
hcup <- hcup[,.(source, toc, year_id, pri_payer, state = st_resi, mcnty = as.character(mcnty_resi), age_group_years_start, sex_id, util)]
stopifnot(sum(duplicated(hcup)) == 0)

## --------------------------------------------
## combine and write out
## --------------------------------------------

## combine
data <- rbindlist(list(mscan, kyth, hcup), fill = T, use.names = T)

## make sure no NAs in places they shouldn't be
stopifnot(data[is.na(toc) | is.na(year_id) | is.na(pri_payer) | is.na(age_group_years_start) | is.na(sex_id), .N] == 0)

## check for any incorrect NA values
stopifnot(data[age_group_years_start == -1, .N] == 0)
stopifnot(data[sex_id == -1, .N] == 0)
stopifnot(data[is.na(mcnty), .N] == 0)
stopifnot(data[is.na(state), .N] == 0)

print('writing')
unlink(out_dir, recursive = TRUE)
data %>%
  group_by(source, year_id, toc) %>%
  write_dataset(
    path = out_dir, 
    format = "parquet",
    existing_data_behavior = "overwrite"
  )
