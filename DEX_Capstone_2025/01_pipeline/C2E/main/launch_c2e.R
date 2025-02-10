##----------------------------------------------------------------
## Title: launch_c2e.R 
## Author: Azalea Thomson
##----------------------------------------------------------------

## ------------------------------------------------
## 1. Set environment
## ------------------------------------------------
username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)
source(paste0('FILEPATH', '/cluster_utils.R'))
source(paste0(here, '/functions_c2e.R'))
source(paste0(here,'/launch_c2e_functions.R'))

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

'%ni%' <- Negate('%in%')

script_dir <- paste0(here, '/1_claims_to_encounter.R')

## ------------------------------------------------
## 2. Define vars
## ------------------------------------------------
run_v <- get_phase_run_id(status=list("best"))$phase_run_id %>% as.character()

message('Run version is: ', run_v)

c2e_config <- parsed_config(get_config(), key = "C2E", run_id = run_v)


sources <- c2e_config$targets
pipeline_dir <- c2e_config$pipeline_dir
param_dir <- paste0(pipeline_dir,"c2e_params/")  
launched_param_dir <- paste0(param_dir,'/launched/')
if(!(dir.exists(param_dir))){
  dir.create(paste0(param_dir, "/"), recursive = T)
}
if(!(dir.exists(launched_param_dir))){
  dir.create(paste0(launched_param_dir, "/"), recursive = T)
}
## ------------------------------------------------
## 3. Get params
## ------------------------------------------------

metadata <- get_job_params(sources)
print(unique(metadata$name))

## Temporarily dropping until we receive full data from CHIA
metadata <- metadata[!(year ==2020 & name == 'CHIA_MDCR')]

## ------------------------------------------------
## 4. Clear output directories/make them 
## ------------------------------------------------

for (source in sources){
  print(source)
  outdir <- unique(metadata[name == source]$outdir) %>% as.character()
  print(outdir)
  diagnostics_outdir <- unique(metadata[name == source]$diagnostics_outdir) %>% as.character()
  if( dir.exists(outdir) ){
    check <- readline(paste0('Are you sure you want to delete the existing output in ', outdir))
    if (check == 'yes'){
        system(paste0("rm -r ", outdir))
        system(paste0("rm -r ", diagnostics_outdir))
    }
  }
  dir.create(outdir, recursive = T)
  dir.create(diagnostics_outdir, recursive = T)
}

## ------------------------------------------------
## 5. Clear error/output log dirs/make them
## ------------------------------------------------
error_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")
log_paths <- c(error_dir, output_dir)
for (lp in log_paths){
  if(dir.exists(lp)){
    system(paste0("rm -r ", lp))
  }
  dir.create(lp, recursive = TRUE, showWarnings = FALSE)
}

## ------------------------------------------------
## 6. Submit jobs
## Job arrays are separated out by toc and source in order to more accurately allocate resources.
## Note that there are no ED rows for Kythera and MarketScan sources because they get
## pulled in with IP automatically in C2E to ensure ED to IP transfers are grouped together
## ------------------------------------------------
metadata[, job:= ifelse(toc %in% c('HH','NF'), paste0(name, '_OTH'), paste0(name, '_',toc))]
metadata[toc == 'AM' & state %in%c('NY','CA','TX','FL','OH','-1'), job:= paste0(job, '_big')]

job_sets <- crossing(source = sources,
                     care = c('AM','AM_big','IP','ED','OTH'), 
                     mem = '100G',
                     thrd = '2',
                     throt = '400') %>% as.data.table()
job_sets[,job :=paste0(source,'_',care)]
job_sets[job %like% 'big' | source == 'MDCD', `:=` (mem = '180G', throt = '200')]
job_sets[source == 'CHIA_MDCR',`:=` (mem = '30G', throt = '600')]
job_sets[job == 'MDCR_AM', `:=` (mem = '15G', throt = '600')]
job_sets[job == 'KYTHERA_IP' | job == 'MSCAN_IP', `:=` (mem = '200G', throt = '200')]

print(job_sets)

job_list <- c()

relaunch <- F

jobs <- unique(job_sets$job)
for (j in jobs ){
  
  job_inputs <- job_sets[job == j]
  
  if (relaunch == T){
    meta <- pull_relaunch(dir = launched_param_dir, job_id = job_inputs$job_id, username)
  }else{
    meta <- metadata[job == j]
  }

  if (nrow(meta) >0){
    print(paste0('Submitting ',j))
    print(nrow(meta))
    param_path <- paste0(param_dir,j,'.csv')

    fwrite(meta, param_path)
  
    submitted_j <- SUBMIT_ARRAY_JOB(
      name = paste0(j, '_C2E'),
      script = script_dir,
      queue = "all.q", 
      memory = job_inputs$mem,
      threads = job_inputs$thrd,
      time = "12:00:00",
      throttle = job_inputs$throt,
      error_dir = error_dir,
      output_dir = output_dir,
      test = F,
      n_jobs = nrow(meta),
      args = c(param_path,run_v))

    meta$job_id <- submitted_j
    fwrite(meta, paste0(launched_param_dir,submitted_j,".csv"))
    job_list <- c(job_list, submitted_j)
    job_sets[job == j, job_id:= submitted_j][job ==j,tasks:=nrow(meta)]
  }
    


}
job_list

fwrite(job_sets, paste0(launched_param_dir,'job_sets.csv'))

