#-------------------------------------------------
#
#  Launch the apply comorb!
#  
#    Two steps: 1) apply comorb, which needs to be ACROSS CAUSES. 2) repartition, which saves the data by cause as expected for modeling
#
#  Author: Haley Lescinsky
#           
#---------------------------------------------------

rm(list = ls())
Sys.umask(mode = 002)
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH/python')
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")

#--------------------------------------------------------------
# Load in config and get versions
config <- get_config()

# determine phase_run_id
phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

by_race<-'no'  #Options: yes, no

# get new comorb version 
comorb_map_version_id <- get_map_metadata(maps = list("COMORB"), status = list("Best"))[, map_version_id]

comorb_config <- parsed_config(config, "COMORB", run_id = as.character(phase_run_id), map_version_id = as.character(comorb_map_version_id))

map_path <- comorb_config$map_output_dir

data_input_dir <- comorb_config$data_input_dir
data_output_dir <- comorb_config$data_output_dir_tmp
final_data_output_dir <- comorb_config$data_output_dir_final


if (by_race == 'yes'){
  dir.create(paste0(data_output_dir, "/tmp_race/"), recursive = T)
}else{
  dir.create(paste0(data_output_dir, "/tmp/"), recursive = T)
}
#------------------------------------------------------------

# make job key
maps <- arrow::open_dataset(paste0(map_path, "/maps/")) %>% collect() %>% as.data.table()

job_key <- unique(maps[,.(age_group_min, age_group_max, toc)])
job_key <- tidyr::crossing(job_key, 
                           "sex_id" = c(1,2), 
                           "year" = seq(2000, 2019, by = 1)) %>% as.data.table()
job_key[, task_id := 1:.N]
job_task_path <- paste0(map_path, "/_apply_comorb_task_id.csv")
write.csv(job_key, job_task_path, row.names = F)

# make job key

rp_job_key <- tidyr::crossing("toc" = unique(job_key$toc), 
                           "sex_id" = c(1,2), 
                           "payer" = c("mdcd", "mdcr", "oop", "priv"),
                           "geo" = c("county", "state", "national")) %>% as.data.table()
rp_job_key[, task_id := 1:.N]
rp_job_key_path <- paste0(map_path, "/_repartition_comorb_task_id.csv")

write.csv(rp_job_key, rp_job_key_path, row.names = F)

#-----------------------------------------
#  SUBMIT JOBS
##----------------------------------------

tmp_log_dir <- paste0(log_dir, "/COMORB_apply/run_id", phase_run_id, "_", "mvid", comorb_map_version_id,ifelse(by_race =='yes', '_byrace', ''),"/")
if(dir.exists(tmp_log_dir)){
  system(paste0("rm -r ", tmp_log_dir))
}
dir.create(tmp_log_dir, recursive = T)

# apply comorb
step1jid <- SUBMIT_ARRAY_JOB(paste0("comorb_apply_", comorb_map_version_id),
                 script = paste0(code_path, "/apply/apply_comorb.R"),
                 error_dir = tmp_log_dir,
                 output_dir = tmp_log_dir,
                 queue = "all.q",
                 memory = "60G", 
                 throttle = 10000,
                 threads = 4, 
                 time = "04:00:00",
                 n_jobs = max(job_key$task_id),
                 archive = F,
                 user_email = paste0(Sys.info()['user'], "@uw.edu"),
                 args = c(job_task_path, map_path, data_input_dir, data_output_dir, by_race))

# repartition comorb output to fit with util collapse output ready for model
rp_jobid <- SUBMIT_ARRAY_JOB(paste0("comorb_repartition"),
                 script = paste0(code_path, "/apply/repartition_comorb.R"),
                 error_dir = tmp_log_dir,
                 output_dir = tmp_log_dir,
                 queue = "all.q",
                 memory = "30G", # max 10G
                 throttle = 1000,
                 threads = 1, 
                 time = "01:00:00",
                 n_jobs = max(rp_job_key$task_id),
                 hold = step1jid,
                 archive = F,
                 user_email = paste0(Sys.info()['user'], "@uw.edu"),
                 args = c(rp_job_key_path, data_output_dir, final_data_output_dir, by_race))


# model param list that the modeling step uses
model_params <- SUBMIT_JOB(paste0("model_params", comorb_map_version_id),
           script = paste0(code_path, "/apply/model_params.R"),
           error_dir = tmp_log_dir,
           output_dir = tmp_log_dir,
           queue = "all.q",
           memory = "30G", # max 10G
           threads = 3, 
           time = "03:00:00",
           hold = rp_jobid,
           archive = F,
           args = c(phase_run_id, by_race))


# can only link map_version to phase_run if map_version is best.
mark_best(table = 'map_version',
          table_id = comorb_map_version_id)

# won't work if you've made copy. If a different map version is used than that in database, correct!
update_phase_run(map_version_id = comorb_map_version_id, 
                 phase_run_id = phase_run_id)

cat(paste0("Comorb apply (map version ", comorb_map_version_id, ") run id ", phase_run_id, ifelse(by_race == 'yes', ' by race', ''),
           "\n - job id1: ", step1jid, 
           "\n - job id2: ", rp_jobid,
           "\n - model params: ", model_params))
