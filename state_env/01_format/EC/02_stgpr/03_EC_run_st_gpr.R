################################################
#' @description Launch ST-GPR for Economics census data
################################################

rm(list = ls())

## *************************************************
## pick types to update (which do we want to re-run the models for?)

types_to_update <- c(
  "envelope",
  "dental",
  "dme_optometry",
  "home",
  "hospital",
  "nursing"
  "other_prof",
  "other",
  "phys_clin_service"
) 
## *************************************************

dir <- "FILEPATH"

## check if we already ran any of these models
for(i in types_to_update){
  
  ## get config
  config <- paste0(dir, "/02_config/", i, ".csv") 
  
  ## examine config
  config_data <- fread(config)
  
  ## run latest model
  index_id <- config_data[, max(model_index_id)]
  
  ## check for index in runinfo
  ## (we don't want to re-run something we've run already)
  if(file.exists(paste0(dir, "/run_info.csv"))){
    RI <- fread(paste0(dir, "/run_info.csv"))
    RI <- RI[type == i]
  }else{
    RI <- data.table("model_index_id")
  }
  
  if(index_id %in% RI$model_index_id){
    warning(paste0("We already ran the latest model in the config for ", i, " - will skip launch"))
  }else{
    print(paste0("OK to run model for ", i))
  }
}

## --------------------------
## setup
## --------------------------

## source STGPR functions
central_root <- 'FILEPATH'
setwd(central_root)
source('FILEPATH/register.R')
source('FILEPATH/sendoff.R')
source('FILEPATH/utility.r')

## --------------------------
## for the given type, run stgpr, save runinfo
## --------------------------

for(i in types_to_update){
  
  ## get config
  config <- paste0(dir, "/02_config/", i, ".csv")
  
  ## examine config
  config_data <- fread(config)
  
  ## run latest model
  index_id <- config_data[, max(model_index_id)]
  
  ## check for index in runinfo
  ## (we don't want to re-run something we've run already)
  if(file.exists(paste0(dir, "/run_info.csv"))){
    RI <- fread(paste0(dir, "/run_info.csv"))
    RI <- RI[type == i]
  }else{
    RI <- data.table("model_index_id")
  }
  
  if(index_id %in% RI$model_index_id){
    
    warning(paste0("We already ran this model for ", i, " - skipping launch"))
    
  }else{
    
    ## register model and get run_id
    run_id <- register_stgpr_model(config, model_index_id = index_id)
    
    ## set log path
    log_path <- paste0(dir, "/logs/", run_id)
    
    ## launch model
    stgpr_sendoff(run_id, 'proj_dex', log_path = log_path)
    
    ## create run info data for use later
    config_info <- fread(config)
    config_info <- config_info[model_index_id == index_id]
    config_info[, run_id := run_id]
    config_info[, date_run := str_remove(as.character(format(Sys.Date(),'%m/%d/%Y')), "^0")]
    config_info[, time_run := as.character(format(Sys.time(), '%H:%M'))]
    
    ## append to run_info_file
    if(!file.exists(paste0(dir, "/run_info.csv"))){
      fwrite(config_info, "run_info.csv")
    }else{
      runinfo <- fread(paste0(dir, "/run_info.csv"))
      runinfo[, date_run := as.character(date_run)]
      runinfo[, time_run := as.character(time_run)]
      runinfo <- rbind(runinfo, config_info, use.names = T, fill = T)
      fwrite(runinfo, paste0(dir, "/run_info.csv"))
    }
    
    ## print
    print(paste0(i, " launched"))
  }
  
  ## give a moment
  Sys.sleep(2)
}

