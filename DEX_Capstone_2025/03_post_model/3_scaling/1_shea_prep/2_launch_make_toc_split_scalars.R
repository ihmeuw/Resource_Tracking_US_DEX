##----------------------------------------------------------------
## Title: launch_make_toc_split.R
## Purpose: Use sources with facility/prof distinction to create scalars 
##          for splitting SHEA envelope into DEX types of care
## Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

setwd(here)
username <- Sys.getenv('USER')

## --------------------
## Set vars
## --------------------

run_ids <- get_phase_run_id()
run_v <- as.character(run_ids[status=='Best']$phase_run_id) %>% print()


c2e_config <- parsed_config(get_config(), key = "C2E", run_id = run_v)
f2t_config <- parsed_config(get_config(), key = "F2T", run_id = run_v)

sources <- c('MDCR','MSCAN','MDCD','CHIA_MDCR') 

shea_root_dir <- 'FILEPATH'
outdir <- paste0(shea_root_dir, 'raw_toc_scalars/run_',run_v,'/')
source_metapath <- 'FILEPATH'

if(!(dir.exists(outdir))){
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
}

source_meta <- data.table()
for(source in sources){
  print(source)
  meta <- data.table()
  
  if (source == 'MDCR'){
    meta <- crossing(source = 'MDCR',
                     indir = paste0(f2t_config$data_output_dir[[source]],'data/'), #pull from f2t since F2T gets applied to MDCR
                     toc = c('ED','AM','IP'),
                     year= c(2000,2010,2015,2016,2019), 
                     payers = c('mdcr','oop')) %>% as.data.table()
  }
  
  if (source == 'CHIA_MDCR'){
    meta <- crossing(source = 'CHIA_MDCR',
                     indir = paste0(c2e_config$data_output_dir[[source]],'data/'), #pull from c2e since F2T doesn't get applied to CHIA_MDCR
                     toc = c('ED','AM','IP'),
                     year= c(2015:2019,2021,2022), 
                     payers = c('mdcr','oop')) %>% as.data.table()
  }
  
  if (source == 'MSCAN'){
    meta <- crossing(source = 'MSCAN',
                     indir = paste0(c2e_config$data_output_dir[[source]],'data/'), 
                     year = c(2010:2022),
                     toc = c('ED','AM','IP'),
                     payers = c('priv'))%>% as.data.table()
  }
  
  if (source == 'MDCD'){
    meta <- crossing(source = 'MDCD', 
                     indir = paste0(c2e_config$data_output_dir[[source]],'data/'),
                     year = c(2000,2010,2014,2016,2019),
                     toc = c('ED','AM','IP'),
                     payers = c('mdcd','oop'))%>% as.data.table()
  }
  
  source_meta <- rbind(source_meta,meta)
}
  


fwrite(source_meta, source_metapath)

tasks <- data.table(state = c("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", 
                              "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", 
                              "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", 
                              "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
                              "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"), 
                    source_metapath = source_metapath,
                    outdir = outdir)

task_path <- paste0(here, '/task_list.csv')
fwrite(tasks, task_path)



SUBMIT_ARRAY_JOB(
  name = 'make_toc_scalars', 
  script = paste0(here, '/2a_make_toc_split_scalars.R'), 
  queue = "all.q", # string "all.q" or "long.q"
  memory = "100G", # string "#G"
  threads = "2", # string "#"
  time = "4:00:00", # string "##:##:##"
  throttle = '51',
  n_jobs = nrow(tasks), 
  hold = NULL,
  args = task_path)






