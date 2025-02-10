##----------------------------------------------------------------
## Title: launch_compile_estimates.R
## Purpose: launch the jobs to compile utilization and spending estimates from model outputs and population denominators
## Authors: Azalea Thomson and Haley Lescinsky
## Note: Use test= T argument to test just a single task before running full job (upwards of 20k tasks for some jobs!)
##----------------------------------------------------------------


##----------------------------------------------------------------
Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)
username <- Sys.getenv('USER')

## --------------------
## Setup
## --------------------
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

source(paste0(here,'/function_get_compile_params.R'))

library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
library(arrow) 
options(arrow.skip_nul = TRUE)
library(tidyverse)
library(data.table)


## --------------------
## Specify model set
## --------------------

model_set_id <- 'XX'
mset <- paste0("set", model_set_id)
mc_adjust <- T # Keep at T to use spend envelopes to combine mc and ffs
rake_state_to_national <- T 
run_at_draw_level <- T  #Set to T if you want draws 1:n_draws, otherwise F returns just means
national_models_only <- F # Usually F unless a test
n_draws <- 50
use_imputation <- T 
days_metrics <- F
min_model_year <- 2010
max_model_year <- 2019

if (days_metrics == T){
  days_metrics_tocs <- 'RX'
}
run_queue <- 'all.q'

raw_config <- get_config()

model_set_data <- get_model_set(model_set_id = list(model_set_id))
run_v <- get_model_version(model_version_id= list(model_set_data$model_version_id[1]))[, phase_run_id]

denoms_path <- raw_config$POST_MODEL$scaling$pop_denom_dir
shea_dir <- raw_config$POST_MODEL$scaling$shea_env_path
denoms_version <- which_version_best(denoms_path)
shea_env_version <- which_version_best(shea_dir)

## --------------------
## Pull versions, assign paths
## --------------------


note <- "DESCRIPTION" 
description_use <- paste0("compile+scaling for set", model_set_id, 
                          ifelse(run_at_draw_level, ", draws",   ", means only"), 
                          ifelse(rake_state_to_national, ",raking to national", ',not raking to national'), 
                          ifelse(use_imputation, ",w imputation",",no imputation"), note)

if(national_models_only){
  description_use <- paste0(description_use, " ( national models only)")
}
print(description_use)

scaling_version <- register_scaled_version(model_set_id = model_set_id,
                                           pop_denom_version = as.integer(denoms_version),
                                           envelope_version = as.integer(shea_env_version),
                                           description = description_use)
print(scaling_version)


print(paste0('Starting scaling for model version(s) ',mset))

scaling_config <- parsed_config(raw_config, key = "POST_MODEL",scaled_version = scaling_version)

root_dir <- paste0("/FILEPATH/scaled_version_",scaling_version,"/")
param_dir <- paste0(root_dir,"params/") 
shea_path <- paste0(shea_dir, 'shea_adjusted_envelopes_allpayer.csv')

compile_dir <- scaling_config$scaling$compile_dir
stepwise_dir <- paste0(compile_dir, "../stepwise/")
diagnostic_dir <- paste0(compile_dir, "../diagnostics/")
missings_dir <- paste0(compile_dir, "../missings/")

toc_cause_restrictions_path <- parsed_config(raw_config, key = "METADATA")$toc_cause_restrictions_path

cl <- fread(toc_cause_restrictions_path)[include == 1 & gc_nec == 0]

causes <- unique(cl$acause)
causes <- causes[!(causes == 'exp_well_newborn')]
if (length(causes) != 148){
  stop('You do not have the expected 148 causes')
}


## --------------------
## Delete old output/make dir
## --------------------
dirs <- c(param_dir, missings_dir,diagnostic_dir, compile_dir)

for (d in dirs){
  if(dir.exists(d)){
    print(d)
    check <- readline('Are you sure you want to delete the existing output?')
    if (check == 'yes'){
      system(paste0("rm -r ", d))
    }
  }
  dir.create(d, recursive = T)
  
}

## --------------------
## Get job params
## --------------------

tocs <- c('AM','ED','IP','HH','NF','RX','DV')

geogs <- c('county','state', "national")



if(national_models_only == T){
  geogs <- c('national')
}


## COMPILE PARAMS
for (g in geogs){
  print(g)
  compile_params <- get_compile_params(scaling_version,mset, model_set_data, tocs, geog = g, 
                                       draws = run_at_draw_level, 
                                       n_draws = n_draws, 
                                       min_model_year = min_model_year,
                                       max_model_year = max_model_year,
                                       add_mem_sets = T, 
                                       rake_state_to_national = rake_state_to_national)
  
  if (days_metrics == T){
    compile_params[[1]][,days_metrics:= ifelse(care %in% days_metrics_tocs, T, F)]
  }else{
    compile_params[[1]][,days_metrics:= F]
  }
  
  assign(paste0("compile_param_dt_", g), compile_params[1] %>% as.data.table())
  assign(paste0("compile_param_path_", g), compile_params[2] %>% as.character())
  

  
  fwrite(get(paste0("compile_param_dt_", g)), get(paste0("compile_param_path_", g)))
}

## ------------------------------------------------
## Clear error/output log dirs/make them
## ------------------------------------------------
log_dir <- paste0("FILEPATH")

##' --------------------
##' 
##' 
##' 1. LAUNCH COMPILE - parallelized by draw, type of care, and states (county only)
##' Partitions on c("geo","toc","state","draw")
##' 
##' --------------------


##' --------------------
##' COMPILE NATIONAL 
##' --------------------

if (rake_state_to_national == T | national_models_only == T){
  

  nat_compile_mem <- '5G' # for both all pop 

  
  nat_compile_jid <- SUBMIT_ARRAY_JOB(
    name = 'compile_national', 
    script = paste0(here,'/1_draw_compile_estimates.R'), 
    queue = run_queue, 
    memory = nat_compile_mem, 
    threads = "1", 
    time = "0:30:00", # jobs take ~1-2 min
    throttle = nrow(compile_param_dt_national),
    error_dir = log_dir,  
    output_dir = log_dir, 
    archive = F,
    n_jobs = nrow(compile_param_dt_national), 
    user_email = paste0(Sys.info()['user'], "@uw.edu"),
    hold = NULL,
    args = c(compile_param_path_national, rake_state_to_national, use_imputation, mc_adjust)) 
  
  nat_compile_jid
}else{
  nat_compile_jid <- c()
}

if(national_models_only == T){
  cat(paste0("scaled version ", scaling_version,"\n", description_use, "\n---\nnational-compile: ", nat_compile_jid))
  stop("National only!") 
}


##' --------------------
##' COMPILE STATE
##' --------------------



compile_mem <- '50G' 


st_compile_jid <- SUBMIT_ARRAY_JOB(
  name = 'compile_state', 
  script = paste0(here,'/1_draw_compile_estimates.R'), 
  queue = run_queue, 
  memory = compile_mem, 
  threads = "1", 
  time = "1:00:00", 
  throttle = '500',
  error_dir = log_dir,  
  output_dir = log_dir, 
  archive = F,
  n_jobs = nrow(compile_param_dt_state), 
  user_email = paste0(Sys.info()['user'], "@uw.edu"),
  hold = nat_compile_jid, 
  args = c(compile_param_path_state, rake_state_to_national, use_imputation, mc_adjust)) 

st_compile_jid 


##' --------------------
##' COMPILE COUNTY  
##' Max run time per task is 1h30, max mem is 142GB (for non dental)
##' --------------------

  
cty_throttle <- 400 

cty_compile_jids <- c()

for(m in unique(compile_param_dt_county$mem_set)){
  
  print(m)
  cty_compile_jid <- SUBMIT_ARRAY_JOB(
    name = paste0('compile_county'), 
    script = paste0(here,'/1_draw_compile_estimates.R'), 
    queue = run_queue, 
    memory = m, 
    threads = "1",
    time = "5:00:00",
    throttle = cty_throttle, 
    error_dir = log_dir,  
    output_dir = log_dir, 
    archive = F,
    n_jobs = nrow(compile_param_dt_county[mem_set == m]), 
    user_email = paste0(Sys.info()['user'], "@uw.edu"),
    hold = c(nat_compile_jid, st_compile_jid),
    args = c(compile_param_path_county, rake_state_to_national, use_imputation, mc_adjust, m)) 
 
  job_note <- paste0("mem_set: ", m," --> ", cty_compile_jid, " --> ", nrow(compile_param_dt_county[mem_set == m]), ' tasks')
  print(job_note)
  cty_compile_jids <- c(cty_compile_jids, cty_compile_jid)
}
  


## --------------------
## PRINT JOB IDS
## --------------------


cat(paste0("scaled version", scaling_version,
           "\n---\nnational: ", nat_compile_jid,
           "\nnational-oop: ", nat_uninsured_compile_jid, 
           "\nstate: ", st_compile_jid,
           "\ncounty:\n", paste0(cty_compile_jid, collapse = '\n')))

