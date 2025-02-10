##----------------------------------------------------------------
## Title: 01_apply_f2t.R
## Purpose: Use ratios calculated in 01_model_f2t.R to calculate total charge amount when we only have facility charges.
## Author: Azalea Thomson
##----------------------------------------------------------------

## --------------------
## 0. Setup 
## --------------------
Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr)
username <- Sys.getenv('USER')

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(str_remove(here, 'apply_map'),'/payer_dictionary.R'))

library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())
'%ni%' <- Negate('%in%')

# Arguments
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
for (i in names(tasks)){
  assign(i, tasks[, get(i)])
  message(i,':', tasks[, get(i)])
}
use_weights <- args[2]


message(paste0('target_',target,'_toc_', care,'_yr_', yr,'_family_', f))

f2t_map_path <- paste0(map_version,'maps/weights_',use_weights,'/')

level_causelist <- fread(cause_map_path)[,.(acause,family,cause_name)]
cl <- level_causelist[!(acause %like% '_NEC') & !(acause %like% '_gc')]
causes <- unique(cl[family == f]$acause)
print(causes)



##' --------------------
##' Read in the data to adjust
##' --------------------
# schema <- update_nulls_schema(indir)

if (target == 'MDCR' & care %in% c('AM','ED')){
  
  mypipe <-. %>%filter(toc == care &year_id == yr &acause %in% causes) 
  
}else{
  
  mypipe <-. %>%filter(toc == care &year_id == yr &acause %in% causes)%>%
  collect() %>% data.table() 
  
}


target_data <- arrow::open_dataset(indir) %>% mypipe
print('Got data to adjust')

if (is.null(target_data)){
  target_data <- data.table()
}

## The only MDCR data that does not need adjusting is the 5% sample from the years where we have carrier data 
if (target == 'MDCR'){
  # Only using AM and ED data from the 5% sample which already contains facility and total charges
  if (care %in% c('AM','ED') ){ 
    good_data <- target_data
    data_to_adj <- data.table()
  }else{
    if (care == 'IP' | care == 'HH' | (care == 'NF' & yr == 2019 )){ 
      good_data <- target_data[ENHANCED_FIVE_PERCENT_FLAG == 'Y']
      data_to_adj <- target_data[ENHANCED_FIVE_PERCENT_FLAG != 'Y']
    }else (care == 'NF' & yr != 2019){
      good_data <- data.table()
      data_to_adj <- target_data[,ENHANCED_FIVE_PERCENT_FLAG := 'N']
    }
  }
}else{
  good_data <- data.table()
  data_to_adj <- target_data
}


if (nrow(data_to_adj)>0){
  ## Merge on cause family
  level_causelist <- fread(cause_map_path)[,.(acause,family)]
  data_to_adj <- merge(data_to_adj, level_causelist, by = 'acause', all.x=T)
  
  ##' --------------------
  ##' Pull in the map
  ##' payment = charge * adj_factor
  ##' --------------------
  if (yr >2019){
    pull_yr <- 2019
  }else{
    pull_yr <- yr
  }
  
  ratios <- arrow::open_dataset(f2t_map_path) %>% 
    filter(toc == care &
             year_id == pull_yr) %>%
    collect() %>% data.table() 
  ratios$n <- NULL
  ratios$toc <- NULL
  print('Got ratios')
  
  if (yr >2019){
    ratios[,year_id:=yr]
  }

  
  ## Merge map on target data
  adj_data <- merge(data_to_adj, ratios, by = c('year_id','family'), all.x = T)
  
  
  print('Merged on map')
  ## --------------------
  ## Apply map
  ## --------------------
  
  adj_data[,tot_chg_amt:=tot_chg_amt*f2t_fit]
  adj_data$f2t_fit <- NULL
  
  if (target == 'MDCR'){
    adj_data <- rbind(good_data,adj_data, fill = T)
  }
  adj_data <- adj_data[order(year_id,age_group_years_start)]
}else{
  print('No data to adjust')
  adj_data <- good_data
}

## --------------------
## Save out data
## --------------------
if (nrow(target_data) >0){

  print('Saving out data')
  arrow::write_dataset(adj_data, 
                       path =outdir,
                       basename_template = paste0("family",f,"-{i}.parquet"),
                       partitioning = c( "toc","year_id","code_system","age_group_years_start"))
  
  
  
  print(Sys.time() - t0)
  print('Done')
 
}






