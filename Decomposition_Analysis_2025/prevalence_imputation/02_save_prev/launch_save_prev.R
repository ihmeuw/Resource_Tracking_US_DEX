##############################################
#  Launcher for prevalence compiling 
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

save_dir <- "FILEPATH/"

#------------------------
# Enter version number with reg predictions
#------------------------

version <- 'XX'
draws <- F 
cap_rho <- T

save_jid <- SUBMIT_ARRAY_JOB(
  name = paste0('save_prev_', version), 
  script = paste0(code_dir, '/save_simple_prev_incidence.R'), 
  queue = "long.q",
  memory = "250G",
  threads = "2", 
  time = "3:00:00",
  archive = F,
  n_jobs = ifelse(draws, 51, 1), 
  hold = NULL,
  args = c(version, as.character(draws),  as.character(cap_rho)))

# If draws, launch a job to summarize the draws with a hold on earlier job!
if(draws == T){
  
  SUBMIT_JOB(
    name = paste0('summarize_draws', version), 
    script = paste0(code_dir, '/summarize_draws.R'), 
    queue = "long.q", 
    memory = "200G", 
    threads = "1",
    time = "6:00:00", 
    archive = F,
    hold = save_jid,
    args = c(version))
  
}


