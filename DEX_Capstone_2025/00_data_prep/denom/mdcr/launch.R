##################################################
# Launcher for MDCR sample denoms
# AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
##################################################
rm(list = ls())
library(data.table)
library(tidyverse)
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
user<-Sys.info()[["user"]]
'%ni%' <- Negate('%in%')

###############################################
# USER INPUTS - UPDATE THESE!
###############################################
# specify if running for chia
chia <- 1
if(chia == 1){
  dataset <- "CHIA_MDCR"
} else {
  dataset <- "MDCR"
}

timestamp <- Sys.Date()
log_dir <- paste0('FILEPATH')

## delete outputs
check <- readline('Are you sure you want to delete the existing output?')
if (check == 'yes'){
  unlink(paste0('FILEPATH'), recursive = TRUE) ## denom itself - disagg version
  unlink(paste0('FILEPATH'), recursive = TRUE) ## denom itself
  unlink(paste0('FILEPATH'), recursive = TRUE) ## intermediary MDCR for stage 2
}
#delete logs
unlink(log_dir, recursive = TRUE) 

if(chia != 1){
  states = c('1','2','3','4','5','6','7','8','9','10',
             '11','12','13','14','15','16','17','18','19','20',
             '21','22','23','24','25','26','27','28','29','30',
             '31','32','33','34','35','36','37','38','39','41',
             '42','43','44','45','46','47','49','50','51','52',
             '53',
             '67','68','69','70','71','72','73','74','80','99'
  )#20 is Maine, good for testing
  dataset <- "MDCR"
  years <- c(2000,2008:2017,2019)
  mem <- "45G" 
  t <- "2:00:00"
} else {
  states = c('22')
  dataset <- "CHIA_MDCR"
  years <- c(2015, 2016, 2017, 2018, 2019:2022)
  mem <- "40G"
  t <- "00:30:00"
}

for(state in states){
  num_thread = 2
  if (state %in% c('5','14','23','33')){ #request more for CA/NY and IL and MI
    num_thread=3
  }
  for(y in years){
    SUBMIT_JOB(
      paste0(dataset, state, y),
      script = paste0(here, "/MDCR.R"), 
      error_dir = errors,
      output_dir = outputs,
      queue = "long.q",
      memory = mem,
      threads = num_thread,
      time = t,
      archive = T,
      args = c(y, state, timestamp, chia)
    )
  }
}

MDCR_job_path <- paste0('FILEPATH')
write.csv(MDCR_grid, MDCR_job_path, row.names = F)

jid <- SUBMIT_ARRAY_JOB(
  paste0("MDCR"),
  script = paste0(here, "/MDCR.R"),
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "long.q",
  n_jobs = nrow(MDCR_grid),
  memory = mem,
  threads = 2,
  time = "08:00:00",
  user_email = paste0(Sys.info()['user'], "@uw.edu"),
  archive = F,
  args = c(MDCR_job_path, timestamp, chia)
)
print(paste0("MDCR", ": ",nrow(MDCR_grid),  ' tasks', " ---> ", jid, ' mem: ', mem))
