## ==================================================
## Author(s): Haley Lescinsky, Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Launches ZIPFIX step in parallel on a SLURM cluster
## ==================================================

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = sprintf("FILEPATH"))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)
load("[repo_root]/static_files/GEOGRAPHY/states.RData")
log_dir <- "FILEPATH"

# Get and set up versions and arguments
#-----------------------------------------------------------------------------

sources_to_run <- c('KYTHERA')
RX_only <- T
wipe_all_outputs <- F
by_state <- T

phase_run_id <- get_phase_run_id()[status %in% c("Active", "Best"), max(phase_run_id)] ## uses latest, set manually as needed

# Launch ZIPFIX apply
#--------------------------------------------------------------------------------------------
config <- get_config()
zip_config <- parsed_config(config, "ZIPFIX", run_id = as.integer(phase_run_id))

for(data_source in sources_to_run){

  input_data_path <- paste0(zip_config$data_input_dir[[data_source]], "/data/")
  output_data_path <- zip_config$data_output_dir[[data_source]]
  zip3path <- zip_config$zip3_map_path
  zip5path <- zip_config$zip5_map_path

  # make sure output_data_path is empty
  tmp_log_dir <- paste0(log_dir, "/zipfix_logs/run_", phase_run_id, "/", data_source, "/" )

  if(wipe_all_outputs){
    if(dir.exists(output_data_path)){
      system(paste0("rm -r ", output_data_path))
    }
    if(dir.exists(tmp_log_dir)){
      system(paste0("rm -r ", tmp_log_dir))
    }
  }

  dir.create(paste0(output_data_path, "/data/"), showWarnings = F, recursive = T)
  dir.create(tmp_log_dir, showWarnings = F, recursive = T)

  # loop through tocs
  if(RX_only){
    tocs <- c("RX")  
  }else{
    tocs <- config$METADATA$tocs[[data_source]]
  }

  job_key <- data.table()

  for(toc in tocs){

    # get years
    data_files <- list.files(paste0(input_data_path, "/toc=", toc, "/"), pattern = "year", full.names = F)
    year_vec <- data_files %>% str_match_all("[0-9]{4}") %>% unlist %>% as.numeric
    job_key_tmp <- data.table(source_data_path = input_data_path,
                              source = data_source,
                              year_id = year_vec,
                              toc = toc)

    # now cross with states too 
    if(by_state){
        job_key_tmp <- as.data.table(tidyr::crossing(job_key_tmp, data.table("state_id"=c("-1", states$abbreviation))))
    }else{
      job_key_tmp$state_id <- NA
    }

    job_key <- rbind(job_key, job_key_tmp)

  }

  job_key[, task_id := 1:.N]
  task_key_path <- paste0(output_data_path,  "apply_task_id_key.csv")
  write.csv(job_key, task_key_path, row.names = F)

  #  Submit jobs
  mem <- "150G"
  thr <- 15

  # launch apply map 
  apply_jid <- SUBMIT_ARRAY_JOB(paste0(data_source, "_apply_zip_map"),
                                script = paste0(here, "/apply.R"),
                                error_dir = tmp_log_dir,
                                output_dir = tmp_log_dir,
                                queue = "long.q",
                                memory = mem,
                                threads = thr,
                                time = "3:00:00",
                                n_jobs = max(job_key$task_id),
                                archive = F,
                                args = c(task_key_path, output_data_path, zip3path, zip5path, data_source)
  )
  print(apply_jid)
}
