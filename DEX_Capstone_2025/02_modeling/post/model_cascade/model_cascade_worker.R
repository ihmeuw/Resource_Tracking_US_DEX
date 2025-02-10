###########################################################
# ------------Launch Model Cascade------------------------#
# Authors: Meera Beauchamp
#
# Purpose: Multiple models of different complexity may be run, (ie, simple, intermediate, complex)
# We want to take the output from the most preferred model that converged for each combination
# making a composite of results across different models, called a model set
# This script save all the preferred outputs to another folder.
#
# This script executes Step 2 and is launched by launch_model_cascade.R
#
# Step 1 - Create model set: Use job param files to identify which data to use from which model version
# Step 2 - Combine models: Read in the desired partitions of data from the desired model and move to a new
#          model set folder
# ----------------------------------------------------------------------#
###########################################################

rm(list = ls())
pacman::p_load(data.table, tidyverse, arrow, openxlsx)

Sys.setenv("RETICULATE_PYTHON" = '/FILEPATH/python')
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
config <- get_config()
#-------------------------------------------------------------------------------------------------
# Get arguments from launch_model_cascade.R 
#-------------------------------------------------------------------------------------------------
# Arguments
if(interactive()){
  
  cause<-'skin'
  model_set_id<-'setXX'
  outdir<- parsed_config(config, key = "MODEL", model_version = paste0(model_set_id))$output_dir
  model_set_dir<-paste0(outdir,'post_run_job_params.csv') 
  run_id<-'XX'
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  cause <- fread(args[1],header=TRUE, select=c('acause','task_id'))[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  cause<-cause$acause
  print(cause)
  
  model_set_dir <- args[2]
  outdir<-args[3] #Where to save the combine model set output
  model_set_id<-args[4] #model set id of the model cascade
  run_id<-args[5]
  
}

#-------------------------------------------------------------------------------------------------
#STEP 2: Combine models
#-------------------------------------------------------------------------------------------------

#Read in model_set
df_mod_set<-fread(model_set_dir)[convergence == 'Yes']

#Use the model set to create a list of partitions we want to move
df_use_partitions<-df_mod_set %>%
  drop_na(convergence, model_version_id)  %>%
  select(model_version_id, geo, toc, metric, pri_payer, payer,acause, sex_id) %>%
  filter(acause==cause)

#create list of lists
partitions<- apply(df_use_partitions,1,as.list)

lapply(1:length(partitions), function (i) {
  #get the parameters from the list of lists
  model_version = partitions[[i]]$model_version_id
  geo = partitions[[i]]$geo
  toc = partitions[[i]]$toc
  metric = partitions[[i]]$metric
  pri_payer = partitions[[i]]$pri_payer
  payer = partitions[[i]]$payer
  acause = partitions[[i]]$acause
  sex = partitions[[i]]$sex_id
  #MODEL DATA--------------------------------------------------------------------------------------
  #Create the path of where to read the model data from
  path<-paste0('/FILEPATH/model_version_', model_version,
               '/draws/geo=', geo,
               '/toc=', toc,
               '/metric=', metric,
               '/pri_payer=', pri_payer,
               '/payer=', payer,
               '/acause_', acause,
               '_sex', sex,
               '-0.parquet')
  print(path)
  #Get data and add a column with the model version
  data <- arrow::open_dataset(path) %>%
    mutate(#Reading in by partition removes column, so add back, acause and sex aren't partitioned
      geo = geo,
      toc = toc,
      metric = metric,
      pri_payer = pri_payer,
      payer= payer,
      #Add which model the data came from
      model_version_id=model_version)
  
  #Save the data in a new location
  arrow::write_dataset(data,
                       path = paste0(outdir,'draws'),
                       partitioning = c('geo','toc', 'metric', 'pri_payer', 'payer'),
                       existing_data_behavior = "overwrite",
                       basename_template = paste0("acause_",acause,"_sex",sex,"-{i}.parquet"))
  #SHINY DATA--------------------------------------------------------------------------------------
  #Create the path of where to read the shiny data from
  shiny_dir<-paste0('/FILEPATH/run_',run_id,'/data/')
  path_shiny<-paste0(shiny_dir, 
                     'model=', model_version,
                     '/acause=', acause,
                     '/toc=', toc,
                     '/metric=', metric,
                     '/geo=', geo,
                     '/payer=', payer,
                     '/pri_pay_', pri_payer,
                     '_sex', sex,
                     '-0.parquet')
  print(path_shiny)
  
  #Get data and add a column with the new model set id, shiny data won't retain the old model version id
  data_shiny <- arrow::open_dataset(path_shiny) %>% collect()
  
  data_shiny <- data_shiny %>% mutate(#Reading in by partition removes column, so add back, acause and sex aren't partitioned
    geo = geo,
    acause = acause,
    toc = toc,
    metric = metric,
    payer= payer,
    #Add what the model set is
    model=model_set_id,
    #Add which model the data came from
    model_version_id = model_version)
  
  
  shiny_dir<-paste0('/FILEPATH/run_',run_id,'/data/')
  
  #Save the shiny data in a new location
  arrow::write_dataset(data_shiny,
                       path = shiny_dir,
                       partitioning = c('model','acause', 'toc', 'metric', 'geo','payer'),
                       existing_data_behavior = "overwrite",
                       basename_template = paste0("pri_pay_",pri_payer,"_sex",sex,"-{i}.parquet"))
})

