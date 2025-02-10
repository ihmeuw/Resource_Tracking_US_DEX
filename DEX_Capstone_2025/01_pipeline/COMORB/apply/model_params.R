##--------------------------------------------------
#  Since comorb is last step before modeling, save out list of combinations (geo/toc/pri_payer/payer/acause/sex) that model is to be parallelized over
#     - References expected dataset combinations to check outputs
# 
#
# Author: Haley Lescinsky + Sawyer Crosby
#           
##--------------------------------------------------

rm(list = ls())

## Setup
print("Importing libraries")
pacman::p_load(data.table, tidyverse, argparse)
source('/FILEPATH/get_age_metadata.R')
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, tidyverse, sparklyr, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

n_encounters_threshold <- 4000
use_threshold <- F

## parse arguments
print("Parsing arguments")
if(interactive()){
  PRI <- "76"
  by_race <- 'yes' #options: yes, no
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  PRI <- args[1]
  by_race <- args[2]
}

## get config and paths
raw_config <- get_config()
config <- parsed_config(raw_config, key = "COMORB", run_id = PRI)
out_dir <- config$data_output_dir_final

## define function
get_params <- function(out_dir, by_race){ 
  print("Getting params")
  filepath <- paste0(out_dir,"/data/")
  if (by_race == 'yes'){
    filepath <- paste0(out_dir, '/data_race/')
  }
  print("  > enumerating filenames (takes a minute)")
  possibles <- system(paste0("find ", filepath, " -type f"), intern = T)
  print("  > parsing filenames")
  possibles <- str_remove(possibles, filepath)
  possibles <- data.table(fp = possibles)
  col_order <- c("metric", "geog", "toc", "pri_payer", "pay", "acause")
  possibles[, (col_order) := data.table::tstrsplit(fp, "/", fixed = TRUE)]
  possibles[, sex_id := as.numeric(ifelse(acause %like% "sex", str_extract(str_extract(acause, "sex[:digit:]"), "[:digit:]"), str_extract(str_extract(acause, "_[:digit:]_"), "[:digit:]")))]
  possibles[, acause := str_remove(str_remove(acause, "acause_"), paste0("_", sex_id, ".*$"))]
  possibles[grepl('sex', acause), acause := str_remove(acause, "_sex.*")]
  
  possibles[,fp := NULL]
  # Add sex_id to col order list since we've added it
  col_order <- c(col_order, "sex_id")
  possibles[,(col_order) := lapply(.SD, function(x) str_remove(x, ".+="))]
  print("  > done")
  return(unique(possibles))
}

## run function and save file
param_map <- get_params(out_dir, by_race)
setnames(param_map, c("geog", "pay"), c("geo", "payer"))
setDT(param_map)
param_map[, have_in_collapse_output := 1]

## save summary of combinations
if (by_race == 'no'){
  fwrite(param_map[, .(acause, toc, metric, pri_payer, sex_id, geo, payer)], paste0(out_dir, "complete_params_in_data.csv"))
  expected_data_path <- raw_config$METADATA$collapse_expected_combos_path
} else if (by_race == 'yes') {
  fwrite(param_map[, .(acause, toc, metric, pri_payer, sex_id, geo, payer)], paste0(out_dir, "complete_params_in_data_race.csv"))
  expected_data_path <- raw_config$METADATA$collapse_expected_combos_race_path
}

# now pull in dataset of what we expect
expected_df <- fread(expected_data_path)

# we aren't tracking dataset coming out of collapse, so drop dataset and merge onto param_map
expected_df <- unique(expected_df[,.(geo, toc, pri_payer, payer, include = 1)])
param_map <- merge(param_map, expected_df, by = c("toc", "pri_payer", "geo", "payer"), all = T)

param_map[is.na(include), include := 0]

# SAVE FOR MODELING
if (by_race == 'no'){
  fwrite(param_map[include == 1 & have_in_collapse_output == 1, .(acause, toc, metric, pri_payer, sex_id, geo, payer)], paste0(out_dir, "params_for_model.csv"))
} else if (by_race == 'yes') {
  fwrite(param_map[include == 1 & have_in_collapse_output == 1, .(acause, toc, metric, pri_payer, sex_id, geo, payer)], paste0(out_dir, "params_for_model_race.csv"))
} 


# MISSING
param_map[include == 1 & is.na(have_in_collapse_output), missing_expected_data:= 1]
param_map[(include == 0 | is.na(include)) & have_in_collapse_output==1, have_output_but_dont_expect:=1]
investigate_output <- param_map[missing_expected_data == 1 | have_output_but_dont_expect == 1]

if (by_race == 'no'){
  write.xlsx(list("missing_expected_data" = unique(param_map[missing_expected_data == 1,.(toc,geo,pri_payer,payer, missing_expected_data)]),
                  "have_output_but_dont_expect"  =  unique(param_map[have_output_but_dont_expect == 1,.(toc,geo,pri_payer,payer, have_output_but_dont_expect)]),
                  "full_list_with_acause" = investigate_output),
             paste0(out_dir, "/investigate_outputs.xlsx"))
} else if (by_race == 'yes') {
  write.xlsx(list("missing_expected_data" = unique(param_map[missing_expected_data == 1,.(toc,geo,pri_payer,payer, missing_expected_data)]),
                  "have_output_but_dont_expect"  =  unique(param_map[have_output_but_dont_expect == 1,.(toc,geo,pri_payer,payer, have_output_but_dont_expect)]),
                  "full_list_with_acause" = investigate_output),
             paste0(out_dir, "/investigate_outputs_race.xlsx"))
}


print("Saved!")
