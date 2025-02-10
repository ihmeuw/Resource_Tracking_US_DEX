##############################################
#  Launcher for prevalence prediction at county level
# 
#  Authors: Drew DeJarnatt and Haley Lescinsky
#
##############################################

# HEADER
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

code_dir <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_dir)


library(data.table)
library(arrow)
library(dplyr)
'%nin%' <- Negate('%in%')

save_dir <- "/FILEPATH"

#------------------------
# make a new versioned folder to save the outputs in
#------------------------
draws <- T # T or F
override_version <- 'XX' # F or 'v3' for example
model_type <- "lasso"

if(override_version == F){
  version_id <- versioned_folder(dir = save_dir, make_folder = T)
  print(paste0("Saving to new version: ", version_id))
}else{
  version_id <- override_version
  print(paste0("Saving to existing version: ", version_id))
}

save_dir <- paste0(save_dir,  version_id, "/")
if(!dir.exists(save_dir)){dir.create(save_dir, recursive = T)}

if(draws == T){
  
  # Parallelize by cause when at draw level
  
  shared_causelist <- fread("FILEPATH/ushd_dex_causelist.csv")
  dex_causes <- shared_causelist[shared == 1]$dex_acause
  
  dir.create(paste0(save_dir, "/viz_by_cause/"))
  
  
  # < 15 minutes per task!
  jid <- SUBMIT_ARRAY_JOB(
    name = 'pred_prev_draws', 
    script = paste0(code_dir, '/pred_prev_county_simple.R'), 
    queue = "long.q", 
    memory = "175G",
    threads = "10", 
    time = "1:00:00", 
    archive = F,
    n_jobs = length(dex_causes),
    hold = NULL,
    args = c(save_dir, 'draws', 'specific', model_type))
  
  print(jid)
  
}else{
  

  jid <- SUBMIT_JOB(
    name = 'pred_prev', 
    script = paste0(code_dir, '/pred_prev_county_simple.R'), 
    queue = "long.q",
    memory = "50G", 
    threads = "2", 
    time = "3:00:00", 
    archive = F,
    hold = NULL,
    args = c(save_dir, 'no_draws', 'all', model_type))
  print(jid)
  
}
