##----------------------------------------------------------------
##' Title: launch_apply_map.R
##' Purpose: Apply the F2T and C2P ratios to data sources that lack either total charges (& only have facility charges)
##'          or only have charges (and no payments)
##' Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')

## --------------------
## Setup
## --------------------

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())


username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

source(paste0(here, '/launch_functions.R'))
## --------------------
## Define vars
## --------------------

raw_config <- get_config()
run_v <- get_phase_run_id(status = "best")
f2t_map_v <- get_map_metadata(maps = list("F2T"))[status == "Best"]$map_version_id %>% as.character()
f2t_config <- parsed_config(raw_config, "F2T", run_id = run_v, map_version_id = f2t_map_v)
f2t_map_path <- f2t_config$map_output_dir
f2t_map_path
f2t_use_weights <- F
message('Applying F2T map version ',f2t_map_v, ', weights = ', f2t_use_weights)

c2p_map_v <- get_map_metadata(maps = list("C2P"))[status == "Best"]$map_version_id  %>% as.character()
c2p_config <- parsed_config(raw_config, "C2P", run_id = run_v, map_version_id = c2p_map_v)
c2p_map_path <- c2p_config$map_output_dir
c2p_map_path
c2p_use_weights <- T
message('Applying C2P map version ',c2p_map_v, ', weights = ', c2p_use_weights)

## Get source targets to adjust
f2t_targets <- f2t_config$targets 
c2p_targets <- c2p_config$targets

## Get the cause list
causemap_config <- parsed_config(raw_config, key = "CAUSEMAP", run_id = run_v)
cause_map_path <- causemap_config$causelist_path

toc_cause_restrictions_path <- parsed_config(raw_config, key = "METADATA")$toc_cause_restrictions_PRE_COLLAPSE_path
cl <- fread(toc_cause_restrictions_path)[include == 1 & gc_nec == 0] 
level_causelist <- fread(cause_map_path)[,.(acause,family)]
cl <- merge(cl, level_causelist, by = 'acause', all.x=T)
families <- unique(cl$family)

## ------------------------------------------------
## 3. Get params
## ------------------------------------------------

param_dir <- paste0(f2t_map_path,'/params/')
if(!(dir.exists(param_dir))){
  dir.create(paste0(param_dir, "/"), recursive = T)
}
launched_f2t_dir <- paste0(f2t_map_path, 'launched/')
if(!(dir.exists(launched_f2t_dir))){
  dir.create(paste0(launched_f2t_dir, "/"), recursive = T)
}

## F2T apply params
f2t_apply_params <- get_f2t_apply_params(f2t_targets[f2t_targets!='HCCI'])

## ------------------------------------------------
## 4. Clear output directories/make them 
## ------------------------------------------------

##F2T
for (target in f2t_targets){
  print(target)
  outdir <- paste0(f2t_config$data_output_dir[[target]],'data/')
  
  if(dir.exists(outdir)){
    check <- readline(paste0('Are you sure you want to delete the existing output in ', outdir))
    if (check == 'yes'){
      system(paste0("rm -r ", outdir))
    }
  }
  dir.create(paste0(outdir, "/"), recursive = T)
}




## ------------------------------------------------
## 5. Clear error/output log dirs/make them
## ------------------------------------------------
log_dir <- 'FILEPATH'
if(dir.exists(log_dir)){
  system(paste0("rm -r ", log_dir))
}
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

## ------------------------------------------------
## 6. Submit jobs
## Job arrays are separated out by toc and source in order to more accurately allocate resources.
## Note that IP and ED (for non CMS sources) run together in order to ensure that ED visits that
## have the same person-date-DX1 as a IP visit get grouped together with the IP visit and reclassified as IP ToC.
## ------------------------------------------------
job_sets <- data.table(job = c('HCUP','MDCR','MDCR_big'),
                       mem = c('50G','10G','175G','50G'),
                       thrd = c('2','2','2','2'),
                       throt = c('500','500','500','200'))

hold_for_jid <- NULL

relaunch <- F 

print(job_sets)

job_list <- c()
jobs <- unique(job_sets$job)
for (j in jobs ){
  
  job_inputs <- job_sets[job == j]
  
  if (relaunch ==T){
    prev_jid <- job_inputs$job_id
    meta <- pull_relaunch(dir=launched_f2t_dir,job_id=prev_jid)
    job_inputs[,mem:='200G']
  }else{


    if (j == 'HCUP'){
      meta <- f2t_apply_params[target%in% c('SIDS','SEDD','NIS','NEDS')]
    }
    if (j == 'MDCR'){
      meta <- f2t_apply_params[target== 'MDCR' & care %in% c('AM','ED')]
    }
    if (j == 'MDCR_big'){
      meta <- f2t_apply_params[target== 'MDCR' & care %ni% c('AM','ED')]
    }
  }
  
  if (nrow(meta) >0){
    print(paste0('Submitting ',j))
    print(nrow(meta))
    param_path <- paste0(param_dir,j,'.csv')
    
    job_sets[job == j, n_tasks:= nrow(meta)]
    fwrite(meta, param_path)
    
    ## Launch apply F2T
    apply_f2t_jid <- SUBMIT_ARRAY_JOB(
      name = 'f2t_apply',
      script = paste0(here,'/apply_map/01_apply_f2t.R'),
      queue = "all.q",
      memory = job_inputs$mem,
      threads = job_inputs$thrd,
      time = "12:00:00",
      throttle = job_inputs$throt,
      n_jobs = nrow(meta),
      hold = hold_for_jid,
      error_dir = log_dir,
      output_dir =  log_dir,
      args = c(param_path,f2t_use_weights) )
    print(apply_f2t_jid)
    meta[,job_id:=apply_f2t_jid]
    fwrite(meta, paste0(launched_f2t_dir, apply_f2t_jid,".csv"))
    job_sets[job == j, job_id:= apply_f2t_jid]
  }
}
print(job_sets)


apply_f2t_hcci_jid <- SUBMIT_JOB(
  name = 'f2t_apply_hcci',
  script = paste0(here,'/apply_map/01_apply_f2t_hcci.R'), 
  queue = "all.q", 
  memory = "50G", 
  threads = "1", 
  time = "4:00:00", 
  error_dir = log_dir,  
  output_dir = log_dir, 
  hold = hold_for_jid,
  archive = F,
  args = c(run_v, f2t_map_v,f2t_use_weights)) 
apply_f2t_hcci_jid 



##----------------------------------------------------------------------
## APPLY C2P - must hold for F2T HCUP (doesn't need to hold for F2T MDCR)
##----------------------------------------------------------------------  

## C2P apply params
c2p_apply_params <-get_c2p_apply_params(c2p_targets)
c2p_param_path <- paste0(c2p_map_path,"apply_c2p_params.csv")
fwrite(c2p_apply_params, c2p_param_path)


##C2P
for (target in c2p_targets){
  print(target)
  outdir <- paste0(c2p_config$data_output_dir[[target]],'data/')
  
  ### Need to make sure that new files get written over instead of appended in
  if(dir.exists(outdir)){
    check <- readline(paste0('Are you sure you want to delete the existing output in ', outdir))
    if (check == 'yes'){
      system(paste0("rm -r ", outdir))
    }
  }
  dir.create(paste0(outdir, "/"), recursive = T)
}


## Launch apply C2P
apply_c2p_jid <- SUBMIT_ARRAY_JOB(
  name = 'c2p_apply', 
  script = paste0(here,'/apply_map/02_apply_c2p.R'),
  queue = "all.q", 
  memory = "200G", 
  threads = "1", 
  time = "6:00:00", 
  throttle = '100',
  n_jobs = nrow(c2p_apply_params), 
  error_dir = log_dir,
  output_dir =  log_dir,
  user_email = paste0(Sys.info()['user'], "@uw.edu"),
  args = c(c2p_param_path,c2p_use_weights) ) 
apply_c2p_jid


