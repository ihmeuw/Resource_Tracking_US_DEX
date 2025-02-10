#--------------------------------------------------------------
#   Create 'key' of all possible model combinations and whether they converged and have expected outputs for a given model version
#
#   Authors: Haley Lescinsky and Azalea Thomson
#
#--------------------------------------------------------------

username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile()) 
setwd(here)
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
'%ni%' <- Negate('%in%')

config <- get_config()

check_corrupted <- T

if(interactive()){
  model_version <- "XX"
  include_race <- F
  dir <- paste0("/FILEPATH/model_version_", model_version, "/")
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  model_version <- args[1]
  include_race <- args[2]
  dir <- paste0("/FILEPATH/model_version_", model_version, "/")
}

# identify run id to get full list of job params
run_id <- get_model_version(model_version_id = list(paste0(model_version)))$phase_run_id
if (include_race == T){
  job_params <- fread(paste0(gsub("#", run_id, config$COLLAPSE$pipeline_output_dir), "/params_for_model_race.csv"))
}else{
  job_params <- fread(paste0(gsub("#", run_id, config$COLLAPSE$pipeline_output_dir), "/params_for_model.csv"))
}

job_params <-  job_params[,.(acause, toc, metric, pri_payer, payer, sex_id, geo)]


schema <- update_nulls_schema(paste0(dir, "/convergence"))
schema[["rho_a"]] <- arrow::float()
schema[["rho_t"]] <- arrow::float()

# load convergance outputs saved during modeling
metadata <- open_dataset(paste0(dir, "/convergence"), schema = schema) %>% collect() %>% as.data.table()

## merge in total list of all parameter combos
metadata[,sex_id := as.character(sex_id)]
job_params[,sex_id := as.character(sex_id)]
job_params_with_metadata <- merge(job_params, metadata, all.x = T, by = c("acause", "toc", 'metric', "pri_payer", "payer", "sex_id", "geo"))

## set NAs
job_params_with_metadata[is.na(convergence), `:=`(convergence = 1, convergence_note = "no solution reached or not launched")]

## order 
job_params_with_metadata <- job_params_with_metadata[order(acause, toc, metric, pri_payer, payer, geo )]

# check output actually exists so we dont break compile
job_params_with_metadata[, output_path := paste0('/FILEPATH/model_version_', model_version,
             '/draws/geo=', geo,
             '/toc=', toc,
             '/metric=', metric,
             '/pri_payer=', pri_payer,
             '/payer=', payer,
             '/acause_', acause,
             '_sex', sex_id,
             '-0.parquet')]
job_params_with_metadata[, output_exists := file.exists(output_path)]

if(check_corrupted){
  # this takes a while, but can be necessary if jobs went over memory while writing output
  
  output_corrupted <- lapply(job_params_with_metadata[output_exists == TRUE]$output_path, function(p){
    
    tmp <- try(open_dataset(p))
    if(class(tmp)[1] == "try-error"){
      return(p)
    }else{
      return()
    }
    
  })
  output_corrupted <- unlist(output_corrupted)
  if(length(output_corrupted)  > 0){
    message("some files were corrupted!")
  }
  job_params_with_metadata[output_path %in% output_corrupted, output_exists := FALSE]

}

## recode 
job_params_with_metadata[,convergence := plyr::revalue(as.character(convergence), c("0" = "Yes", "1" = "No"))]

if ('percent_final_draws_fixed' %ni% names(job_params_with_metadata) ){
  job_params_with_metadata[,percent_final_draws_fixed:=0]
}

## save out
write.csv(job_params_with_metadata, paste0(dir, "/post_run_job_params.csv"), row.names = F)
