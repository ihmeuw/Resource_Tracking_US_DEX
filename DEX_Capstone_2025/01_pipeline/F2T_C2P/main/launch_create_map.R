##----------------------------------------------------------------
## Title: launch_create_map.R 
## Purpose: Launch jobs to calculate F2T and C2P ratios
## Author: Azalea Thomson
##----------------------------------------------------------------
reticulate::use_python('FILEPATH')
Sys.umask(mode = 002)
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')
username <- Sys.getenv('USER')
## --------------------
## Setup
## --------------------

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')

library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)


source(paste0(here, '/create_map/c2p_f2t_functions.R'))
source(paste0(here, '/launch_functions.R'))
## --------------------
## Define vars
## --------------------
raw_config <- get_config()
run_v <- get_phase_run_id(status = "best") 
causemap_config <- parsed_config(get_config(), key = "CAUSEMAP", run_id = run_v)
cause_map_path <- causemap_config$causelist_path

toc_cause_restrictions_path <- parsed_config(raw_config, key = "METADATA")$toc_cause_restrictions_PRE_COLLAPSE_path

## Register map versions
f2t_map_v <- register_map_version(related_map = "f2t", phase_run_id = run_v) %>% print()
f2t_use_weights <- F
f2t_config <- parsed_config(raw_config, "F2T", run_id = run_v, map_version_id = f2t_map_v)
f2t_map_path <- f2t_config$map_output_dir
f2t_map_path


c2p_map_v <- register_map_version(related_map = "C2P", phase_run_id = run_v) %>% print()
c2p_use_weights <- T
c2p_config <- parsed_config(raw_config, "C2P", run_id = run_v, map_version_id = c2p_map_v)
c2p_map_path <- c2p_config$map_output_dir
c2p_map_path



##----------------------------------------------------------------------
## Create output directories
##----------------------------------------------------------------------  

map_paths <- c(c2p_map_path, f2t_map_path)

for (m in map_paths){
  print(m)
  if (m %like% 'F2T'){
    use_weights <- f2t_use_weights
  }else{
    use_weights <- c2p_use_weights
  }
  if(dir.exists(m)){
    check <- readline('Are you sure you want to delete the existing output?')
    if (check == 'yes'){
      system(paste0("rm -r ", m))
    }
  }
  dir.create(paste0(m), recursive = T)
  dir.create(paste0(m,'/diagnostics/',use_weights,'/'), recursive = T)
  dir.create(paste0(m, '/maps/',use_weights,'/'), recursive = T)
}


## Create the metadata

f2t_sources <- c(f2t_config$map_inputs) 
f2t_source_meta <- data.table()
for (s in f2t_sources){
  meta <- data.table()
  meta$source <- s
  meta$indir <- paste0(f2t_config$data_input_dir[[s]],'data/')
  f2t_source_meta <- rbind(f2t_source_meta,meta)
}

fwrite(f2t_source_meta, paste0(f2t_map_path, 'metadata.csv'))


c2p_sources <- c(c2p_config$map_inputs) 
c2p_source_meta <- data.table()
for (s in c2p_sources){
  meta <- data.table()
  meta$source <- s
  meta$indir <- paste0(c2p_config$data_input_dir[[s]],'data/')
  c2p_source_meta <- rbind(c2p_source_meta,meta)
}

fwrite(c2p_source_meta, paste0(c2p_map_path, 'metadata.csv'))



## -----------------------------------------------------------
## Pull in valid payer combos with the sources we want to use 
## -----------------------------------------------------------
valid_payers_path <- parsed_config(raw_config, key = "METADATA")$toc_payer_restrictions_path
full_expected_path <- parsed_config(raw_config, key = "METADATA")$collapse_expected_data_path

f2t_payer_sources <- create_pay_combos(map = 'f2t',
                                       sources = f2t_sources, 
                                       path = full_expected_path, 
                                       tocs = c('IP','ED','HH','NF','AM'))
fwrite(f2t_payer_sources, paste0(f2t_map_path, 'payer_sources.csv'))

c2p_payer_sources <- create_pay_combos(map = 'c2p',
                                       sources = c2p_sources, 
                                       path = full_expected_path, 
                                       tocs = c('IP','ED')) #C2P is only relevant for IP and ED
fwrite(c2p_payer_sources, paste0(c2p_map_path, 'payer_sources.csv'))


## --------------------
## Determine params for jobs
## --------------------
cl <- fread(toc_cause_restrictions_path)[include == 1 & gc_nec == 0] 
level_causelist <- fread(cause_map_path)[,.(acause,family)]
cl <- merge(cl, level_causelist, by = 'acause', all.x=T)
families <- unique(cl$family)

## MODEL F2T RATIOS PARAMS
f2t_tocs <- c('IP','ED','HH','NF','AM') 

model_f2t_params <- expand.grid(map_dir = f2t_map_path,
                                care = f2t_tocs,
                                cause_map_path = cause_map_path,
                                f = families,
                                save_dir = paste0(f2t_map_path,'maps/weights_',f2t_use_weights,'/')) %>% data.table()
fwrite(model_f2t_params, paste0(f2t_map_path,"mod_f2t_params.csv"))


## MODEL C2P RATIOS PARAMS
c2p_tocs <- c('IP','ED')
model_c2p_params <- expand.grid(map_dir = c2p_map_path,
                            care = c2p_tocs,
                            cause_map_path = cause_map_path,
                            f = families,
                            save_dir = paste0(c2p_map_path,'maps/weights_',c2p_use_weights,'/')) %>% data.table()

fwrite(model_c2p_params, paste0(c2p_map_path,"mod_c2p_params.csv"))



## ------------------------------------------------
## Clear error/output log dirs/make them
## ------------------------------------------------
log_dir <- "FILEPATH"
if(dir.exists(log_dir)){
  system(paste0("rm -r ", log_dir))
}
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

## --------------------
## Submit jobs
## --------------------


launched_f2t_dir <- paste0(f2t_map_path, 'launched/')
if(!(dir.exists(launched_f2t_dir))){
  dir.create(paste0(launched_f2t_dir, "/"), recursive = T)
}


job_sets <- data.table(job = c('F2T','F2T_med','F2T_big'),
                       mem = c('50G','200G','500G'),
                       thrd = c('3','3','3'),
                       throt = c('200','50','10'))

relaunch <- F 

print(job_sets)

job_list <- c()

jobs <- unique(job_sets$job)
for (j in jobs ){
  
  job_inputs <- job_sets[job == j]
  
  if (relaunch ==T){
    prev_jid <- job_inputs$job_id
    meta <- pull_relaunch(dir=launched_f2t_dir,job_id=prev_jid)
    
  }else{
    
    if (j == 'F2T_big'){
      meta <- model_f2t_params[care=='AM' & f %in% c('fam_well','fam_mental','fam_msk')]
    }
    if (j == 'F2T_med'){
      meta <- model_f2t_params[care=='AM' & f %ni% c('fam_well','fam_mental','fam_msk')]
    }
    if (j == 'F2T'){
      meta <- model_f2t_params[care != 'AM']
    }
  }
  
  if (nrow(meta) >0){
    print(paste0('Submitting ',j))
    print(nrow(meta))
    param_path <- paste0(f2t_map_path,j,".csv")
    
    job_sets[job == j, n_tasks:= nrow(meta)]
    fwrite(meta, param_path)
    

    ## Model F2T ~ 2 hours to run
    model_f2t_jid <- SUBMIT_ARRAY_JOB(
      name = 'mod_f2t', 
      script = paste0(here,'/create_map/01_model_f2t.R'), 
      queue = "all.q", 
      memory = job_inputs$mem, 
      threads = job_inputs$thrd, 
      time = "5:00:00", 
      throttle = job_inputs$throt,
      n_jobs = nrow(meta), 
      error_dir = log_dir,
      output_dir =  log_dir,
      args = c(param_path,f2t_use_weights)) 
    model_f2t_jid
    
    meta[,job_id:=model_f2t_jid]
    fwrite(meta, paste0(launched_f2t_dir, model_f2t_jid,".csv"))
    job_sets[job == j, job_id:= model_f2t_jid]
  }

}
  
  

## Model C2P
model_c2p_jid <- SUBMIT_ARRAY_JOB(
  name = 'mod_c2p', 
  script = paste0(here,'/create_map/02_model_c2p.R'), 
  queue = "all.q", 
  memory = "150G", 
  threads = "2", 
  time = "3:00:00", 
  throttle = '60',
  n_jobs = nrow(model_c2p_params), 
  error_dir = log_dir,
  output_dir =  log_dir,
  args = c(paste0(c2p_map_path,"mod_c2p_params.csv"), c2p_use_weights)) 
model_c2p_jid



##----------------------------------------------------------------------
## Mark maps best
##----------------------------------------------------------------------  

## Link map application for F2T
mark_best(table = "map_version", table_id= f2t_map_v)
update_phase_run(phase_run_id = run_v, map_version_id = f2t_map_v)

## Link map application for C2P
mark_best(table = "map_version", table_id= c2p_map_v)
update_phase_run(phase_run_id = run_v, map_version_id = c2p_map_v)





