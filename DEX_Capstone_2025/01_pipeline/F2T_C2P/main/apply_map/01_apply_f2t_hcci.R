##----------------------------------------------------------------
## Title: 01_apply_f2t_hcci.R
## Purpose: Use ratios calculated in 01_model_f2t.R to calculate total charge, when we only have facility charge.
## Author: Azalea Thomson
##----------------------------------------------------------------


## --------------------
## 0. Setup 
## --------------------
Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr)
username <- Sys.getenv('USER')
'%ni%' <- Negate('%in%')

library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(str_remove(here, 'apply_map'),'/payer_dictionary.R'))

# Arguments
args <- commandArgs(trailingOnly = TRUE)
run_v <- args[1]
map_v <- args[2]
use_weights <- args[3]
types_of_care <- c('IP','HH','ED','AM')


f2t_map_path <- paste0("FILEPATH",map_v,"/maps/weights_",use_weights,"/")
indir <- paste0('FILEPATH/run_',run_v, '/RDP/data/')
outdir <- paste0('FILEPATH/run_',run_v,'/F2T/data/')

cause_map_path <- "FILEPATH/"
level_causelist <- fread(cause_map_path)[,.(acause,family,cause_name)]
cl <- level_causelist[!(acause %like% '_NEC') & !(acause %like% '_gc')]

for (care in types_of_care){
  
  print(care)
  ##' --------------------
  ##' Read in the data to adjust
  ##' --------------------
  schema <- update_nulls_schema(indir)
  
  mypipe <-. %>%filter(toc == care) %>% 
    collect() %>% data.table() 
  
  target_data <- arrow::open_dataset(indir) %>% mypipe
  print('Got data to adjust')
  
  data_to_adj <- target_data[metric %ni% c('encounters_per_person','days_per_encounter')]
  good_data <- target_data[metric %in% c('encounters_per_person','days_per_encounter')]
  
  if (nrow(data_to_adj)>0){
    ## Merge on cause family
    level_causelist <- fread(cause_map_path)[,.(acause,family)]
    data_to_adj <- merge(data_to_adj, level_causelist, by = 'acause', all.x=T)
    
    ##' --------------------
    ##' Pull in the map
    ##' payment = charge * adj_factor
    ##' --------------------
    ratios <- arrow::open_dataset(f2t_map_path) %>% 
      filter(toc == care ) %>%
      collect() %>% data.table() 
    
    ##duplicate ratios from 2019 through 2022
    ratios_extend_2020 <- copy(ratios[year_id==2019])[,year_id:=2020]
    ratios_extend_2021 <- copy(ratios[year_id==2019])[,year_id:=2021]
    ratios_extend_2022 <- copy(ratios[year_id==2019])[,year_id:=2022]
    ratios <- rbind(ratios,ratios_extend_2020,ratios_extend_2021,ratios_extend_2022)
    
    ratios$n <- NULL
    ratios$toc <- NULL
    print('Got ratios')
    
    ## Merge map on target data
    adj_data <- merge(data_to_adj, ratios, by = c('year_id','family'), all.x = T)
    
    
    print('Merged on map')
    ## --------------------
    ## Apply map
    ## --------------------
    
    adj_data[,raw_val:=raw_val*f2t_fit]
    adj_data$f2t_fit <- NULL
    adj_data$family <- NULL
    
    print('Adjusted')
    
  }else{
    print('No data to adjust')
    adj_data <- good_data
  }
  
  if (nrow(good_data) >0){
    adj_data <- rbind(adj_data,good_data)
  }
  
  if (nrow(target_data) != nrow(adj_data)){
    message('Input data not same as output data')
    stop()
  }
  ## --------------------
  ## Save out data
  ## --------------------
  adj_data <- adj_data[order(year_id,age_group_years_start)]
  print('Saving out data')
  arrow::write_dataset(adj_data, 
                       path =outdir,
                       basename_template = paste0("toc_",care,"-{i}.parquet"),
                       partitioning = c( "toc","year_id","age_group_years_start"))
  
}

print(Sys.time() - t0)
print('Done')








