## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script for the DETRUNC step that applies the map to MEPS data
## ==================================================

rm(list = ls())
library(lbd.loader, lib.loc = sprintf("FILEPATH"))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

pacman::p_load(data.table, tidyverse, arrow, argparse, reticulate)

## source primary cause function
source_python(paste0(here, "/../../../PRIMARY_CAUSE/main/helpershelpers.py"))

## -------------------------
## setup
## -------------------------

## get arguments
parser <- ArgumentParser(description='Get args')
parser$add_argument("--sub_dataset", help="MEPS sub dataset")
parser$add_argument("--age", help="MEPS sub dataset")
parser$add_argument("--sex", help="MEPS sub dataset")
parser$add_argument("--PRI", help="Phase run ID")
parser$add_argument("--MVI", help="Map version ID")
parser$add_argument("--CAUSEMAP_MVI", help="Map version ID")
args <- parser$parse_args()
print(args)
sub_dataset <- args$sub_dataset 
age <- as.numeric(args$age)
sex <- as.numeric(args$sex)
PRI <- as.character(args$PRI)
MVI <- as.character(args$MVI)
CAUSEMAP_MVI <- as.character(args$CAUSEMAP_MVI)


## get config and paths
raw_config <- get_config()
config <- parsed_config(raw_config, key = "DETRUNC", run_id = PRI, map_version_id = MVI)
root_input_dir <- config$data_input_dir$MEPS
input_path <- Sys.glob(paste0(root_input_dir, "*MEPS_", sub_dataset, "*"))
stopifnot(length(file) == 1)
map_dir <- paste0(config$map_output_dir, "maps/")
output_path <- paste0(config$data_output_dir$MEPS, "data")
missed_path <- paste0(config$data_output_dir$MEPS, "missed_icds/", sub_dataset, "_age", age, "_sex", sex, ".feather")
nas_path <- paste0(config$data_output_dir$MEPS, "nas_dropped/", sub_dataset, "_age", age, "_sex", sex, ".feather")
lvl_flag_path <- paste0(config$data_output_dir$MEPS, "lvl_flag/", sub_dataset, "_age", age, "_sex", sex, ".feather")

## -------------------------
## read and clean data
## -------------------------

## read data
schema <- update_nulls_schema(input_path)
data <- arrow::open_dataset(input_path, schema = schema) %>%
  filter(age_group_years_start == age & sex_id == sex & year_id >= 2000) %>% 
  collect() %>%
  data.table()

if(nrow(data) == 0){
  print("No data here")
  
}else if(uniqueN(data$toc) > 1){
  stop("Multiple TOCs in file")
  
}else{
  
  TOC <- unique(data$toc)
  
  ## check for _gc in DV
  if(TOC == "DV") stopifnot(data[acause == "_gc", .N] == 0)
  
  ## select expected columns
  setnames(data, "claim_id", "encounter_id")
  expected_cols <- c(
    "bene_id", "encounter_id",
    "toc", "pri_payer", "year_id", "year_dchg", "age_group_years_start", "age_group_id", "sex_id",
    "survey_wt", "varpsu", "varstr",
    "code_system", "dx", "dx_level", 
    "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt",
    "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "priv_fac_pay_amt", "oop_fac_pay_amt", 
    "tot_pay_amt", "tot_chg_amt", "tot_fac_pay_amt", "tot_fac_chg_amt",
    "los",
    "nid"
  )
  if(TOC == "DV") expected_cols <- c(expected_cols, "primary_cause", "acause")
  if(TOC == "RX") expected_cols <- c(expected_cols, "days_supply")
  for(i in expected_cols){
    if(!i %in% colnames(data)){
      data[,(i) := NA]
    }
  }
  data <- data[,expected_cols, with = F]
  
  ## drop rows we're not interested in (just in case)
  data <- data[toc != "OTH"]
  
  ## fix any 0 LOS
  data[toc %in% c("IP", "NF") & los == 0, los := 1]
  
  ## add admission_count (just to standardize with C2E outputs)
  data[,admission_count := 1]
  
  ## ensure days_supply is numeric
  if(TOC == "RX") data[,days_supply := as.integer(days_supply)]
  
  ## -------------------------
  ## Small validations on input data
  ## -------------------------
  
  if(nrow(data) > nrow(unique(data))){
    print("There are duplicates in the data, taking unique of the data!")
    data <- unique(data)
  }
  
  if(nrow(data[,.N, by = c('dx_level', 'encounter_id')][N > 1]) > 0){
    
    print(data[,.N, by = c('dx_level', 'encounter_id')][N > 1])
    print("Multiple rows have the same dx_level and encounter_id! This will likely cause the pri cause assertion error to go off. If so, check data + data processing!")
    message("Multiple rows have the same dx_level and encounter_id! This will likely cause the pri cause assertion error to go off. If so, check data + data processing!")
    
  }
  
  ## -------------------------
  ## count and drop NAs
  ## -------------------------
  
  drop_cols <- c(
    "toc",
    "age_group_years_start",
    "age_group_id",
    "pri_payer",
    "sex_id",
    "code_system",
    "dx_level",
    "dx"
  )
  if(TOC == "DV") drop_cols <- drop_cols[!drop_cols %in% c("code_system", "dx_level", "dx")]
  na_vals <- list(-1, "-1", "None", "NA", "<NA>", "UNK", "UNKNOWN", "unknown")
  
  ## count NAs
  na_counts <- data.table()
  for(j in colnames(data)){
    n <- nrow(data[is.na(get(j)) | get(j) %in% na_vals])
    na_counts <- rbind(
      na_counts, 
      data.table(variable = j, n_na = n)
    )
  }
  na_counts[,c("source", "sub_dataset", "n_rows") := .("MEPS", sub_dataset, nrow(data))]
  
  ## drop NAs
  for(j in drop_cols){
    data <- data[!is.na(get(j)) & !get(j) %in% na_vals]
  }
  
  na_counts[,dropped_nas := "NO"]
  na_counts[variable %in% drop_cols, dropped_nas := "YES"]
  
  if(TOC != "DV"){
    ## -------------------------
    ## read map
    ## -------------------------
    
    ## read maps at different agg levels
    map_TAS <- data.table(arrow::read_feather(paste0(map_dir, "detrunc_toc_age_sex.feather")))
    map_TS <- data.table(arrow::read_feather(paste0(map_dir, "detrunc_toc_sex.feather")))
    map_T <- data.table(arrow::read_feather(paste0(map_dir, "detrunc_toc.feather")))
    map_0 <- data.table(arrow::read_feather(paste0(map_dir, "detrunc.feather")))
    map_TAS <- map_TAS[toc == TOC & age_group_years_start == age & sex_id == sex, .(code_system, dx = icd3, acause, prop, N)]
    map_TS <- map_TS[toc == TOC & sex_id == sex, .(code_system, dx = icd3, acause, prop, N)]
    map_T <- map_T[toc == TOC, .(code_system, dx = icd3, acause, prop, N)]
    map_0 <- map_0[,.(code_system, dx = icd3, acause, prop, N)]
    
    ## get all-toc maps for RX
    map_AS <- data.table(arrow::read_feather(paste0(map_dir, "detrunc_age_sex.feather")))
    map_S <- data.table(arrow::read_feather(paste0(map_dir, "detrunc_sex.feather")))
    map_AS <- map_AS[age_group_years_start == age & sex_id == sex, .(code_system, dx = icd3, acause, prop, N)]
    map_S <- map_S[sex_id == sex, .(code_system, dx = icd3, acause, prop, N)]
    
    ## get restrictions
    restrictions <- fread(raw_config$METADATA$toc_cause_restrictions_PRE_COLLAPSE_path)
    restrictions <- restrictions[include == 1 & gc_nec == 0, -c("include", "gc_nec")]
    restrictions <- restrictions[toc == TOC, -"toc"]
    restrictions <- restrictions[age_start <= age & age_end >= age, -c("age_start", "age_end")]
    if(sex == 1) restrictions <- restrictions[male == 1, -c("male", "female")]
    if(sex == 2) restrictions <- restrictions[female == 1, -c("male", "female")]
    
    ## re-apply restrictions to aggregated maps and regenerate proportions
    ## map_TAS is already restricted
    lapply(
      list("map_TS", "map_T", "map_0", "map_AS", "map_S"), 
      function(x){
        tmp <- get(x)
        tmp <- tmp[acause %in% restrictions$acause]
        tmp[,denom := sum(N), by = .(code_system, dx)]
        tmp[,prop := N/denom]
        tmp[,c("denom", "N") := NULL]
        assign(x, tmp, envir = .GlobalEnv)
      }
    )
    
    ## -------------------------
    ## apply map
    ## -------------------------
    
    ## temporarily concat code_system and icd_code
    lapply(list(map_TAS, map_TS, map_T, map_0, map_AS, map_S, data), function(x){
      x[,CODE := paste0(code_system, "_", dx)]
    })
    
    ## separate out codes that aren't in the map
    missed <- data[!CODE %in% map_0$CODE, .N, by = .(code_system, dx)]
    missed[,c("toc", "age_group_years_start", "sex_id") := .(data[,unique(toc)], age, sex)]
    data <- data[CODE %in% map_0$CODE]
    
    ## run custom mapping fcn
    if(nrow(data) > 0){
      codes <- data[,unique(CODE)]
      invisible({
        lapply(
          codes, 
          function(x){
            
            n <- data[CODE == x, .N]
            
            if(data[,unique(toc)] == "RX"){
              
              ## use all-toc map for RX
              prop_AS <- map_AS[CODE == x] 
              prop_S <- map_S[CODE == x] 
              prop_0 <- map_0[CODE == x]
              
              if(nrow(prop_AS) > 0){
                data[CODE == x, c("acause", "map_lvl") := .(prop_AS[,sample(acause, size = n, prob = prop, replace = T)], "AS")]
              }else if(nrow(prop_S) > 0){
                data[CODE == x, c("acause", "map_lvl") := .(prop_S[,sample(acause, size = n, prob = prop, replace = T)], "S")]
              }else{
                data[CODE == x, c("acause", "map_lvl") := .(prop_0[,sample(acause, size = n, prob = prop, replace = T)], "0")]
              }
              
            } else{
              
              prop_TAS <- map_TAS[CODE == x]
              prop_TS <- map_TS[CODE == x]
              prop_T <- map_T[CODE == x]
              prop_0 <- map_0[CODE == x]
              
              if(nrow(prop_TAS) > 0) {
                data[CODE == x, c("acause", "map_lvl") := .(prop_TAS[,sample(acause, size = n, prob = prop, replace = T)], "TAS")]
              }else if(nrow(prop_TS) > 0){
                data[CODE == x, c("acause", "map_lvl") := .(prop_TS[,sample(acause, size = n, prob = prop, replace = T)], "TS")]
              }else if(nrow(prop_T) > 0){
                data[CODE == x, c("acause", "map_lvl") := .(prop_T[,sample(acause, size = n, prob = prop, replace = T)], "T")]
              }else{
                data[CODE == x, c("acause", "map_lvl") := .(prop_0[,sample(acause, size = n, prob = prop, replace = T)], "0")]
              }
            }
          }
        )
      })
      data[,CODE := NULL]
      
      ## -------------------------
      ## Flag any that used an aggregated map
      ## -------------------------
      map_lvl_flag <- data[, .(N = .N), by = .(toc, age_group_years_start, sex_id, map_lvl)]
      data[,map_lvl := NULL]
      
      ## -------------------------
      ## apply primary cause
      ## -------------------------
      causemap_config <- parsed_config(raw_config, key = "CAUSEMAP", run_id = PRI, map_version_id = CAUSEMAP_MVI)
      ## split by year 
      data <- split(data, data$year_id)

      data <- rbindlist(lapply(
        data, 
        function(x) {
          #print( unique(x$year_id) )
          data.table(add_primary_cause_indicator(x, TOC, age, causemap_config, CAUSEMAP_MVI))
        }
      ), use.names = TRUE)
    }
  }
  
  data[,primary_cause := as.integer(primary_cause)]
  
  ## fix columns that are TRUE instead of NA (oddity from reticulate)
  types <- unname(unlist(lapply(data, class)))
  wrongly_bool <- names(data)[types == "logical"] ## no columns should be boolean
  data[,(wrongly_bool) := NA]
  
  ## -------------------------
  ## save out data
  ## -------------------------
  if(nrow(data) > 0){
    data %>% 
      group_by(toc, year_id, age_group_years_start, sex_id) %>%
      write_dataset(output_path, basename_template = paste0(sub_dataset, "_age", age, "_sex", sex, "-part{i}.parquet"))
  }
  dir.create(dirname(nas_path), recursive = T, showWarnings = F)
  dir.create(dirname(missed_path), recursive = T, showWarnings = F)
  dir.create(dirname(lvl_flag_path), recursive = T, showWarnings = F)
  arrow::write_feather(na_counts, nas_path) ## only want to track sub_dataset
  if(exists("missed")) if(nrow(missed) > 0) arrow::write_feather(missed, missed_path) ## want to track toc/age/sex
  if(exists("map_lvl_flag")) if(nrow(map_lvl_flag) > 0) arrow::write_feather(map_lvl_flag, lvl_flag_path) ## want to track toc/age/sex 
}
