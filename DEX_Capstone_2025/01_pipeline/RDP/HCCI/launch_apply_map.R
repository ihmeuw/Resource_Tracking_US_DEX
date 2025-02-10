#-------------------------------------------------
#
#  Launch the steps to apply the special injury+RDP HCCI maps to HCCI
#
#
#  Author: Haley Lescinsky
#
#-----------------------------------------------------
rm(list = ls())
Sys.umask(mode = 002)
pacman::p_load(dplyr, openxlsx, RMySQL,data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")

#--------------------------------------------------------------
config <- get_config()

# determine phase_run_id
phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

parsed_config <- parsed_config(config, "RDP", run_id = as.character(phase_run_id))   

map_version <- "/FILEPATH/INJURY_RDP/"
input_data_path <- paste0(parsed_config$data_input_dir['HCCI'], "/data/")
output_data_path <- paste0(parsed_config$data_output_dir['HCCI'])
if(dir.exists(input_data_path)){
  confirm_delete_directory(input_data_path)
}
if(dir.exists(output_data_path)){
  confirm_delete_directory(output_data_path)
}
dir.create(input_data_path,  recursive = T)
dir.create(output_data_path)

#--------------------------------
# Copy HCCI collapsed data over
#--------------------------------
hcci_data_path <- "/FILEPATH/HCCI_collapsed.parquet/"
hcci_data <- open_dataset(hcci_data_path) %>% filter(pri_payer!="unk" & payer!="unk" & toc!="UNK")

arrow::write_dataset(hcci_data, 
                     path = paste0(input_data_path), 
                     partitioning = c("toc", "year_id", "age_group_years_start", "sex_id"), 
                     existing_data_behavior = "overwrite")

#--------------------------------
# Make job key across sources!
#--------------------------------
data_source <- "HCCI"
if(!dir.exists(input_data_path)){stop("input_data_dir doesn't exist!")}
  
# demographic grid
demographic_grid <- data.table("age_start" = c(0, 1, seq(5, 95, by = 5)))
toc <- c("AM", "ED", "HH", "IP")
  
# get years
data_files <- list.files(paste0(input_data_path, "/toc=", toc), pattern = "year", full.names = T)
toc_vec <- data_files %>% str_match_all(paste0(toc, collapse = "|")) %>% unlist
data <- data.table(source_data_path = input_data_path,
                     source = data_source,
                     toc = unique(toc_vec))
job_key <- as.data.table(tidyr::crossing(data, demographic_grid))


# Job key for the pulling of data
job_key[, task_id := 1:.N]
task_key_path <- paste0(map_version,"/_apply_task_id_key_",phase_run_id,".csv")

write.csv(job_key, task_key_path, row.names = F)

#-----------------------------------------
#  SUBMIT JOBS
##----------------------------------------

tmp_log_dir <- paste0(log_dir, "/HCCI_INJ_RDP_apply/", phase_run_id, "/")
if(dir.exists(tmp_log_dir)){
  system(paste0("rm -r ", tmp_log_dir))
}
dir.create(tmp_log_dir, recursive = T)


# LAUNCH PULL DATA AS ARRAY
SUBMIT_ARRAY_JOB(paste0("hcci_rdp", phase_run_id),
                                  script = paste0(code_path, "/apply/apply_rdp_hcci.R"),
                                  error_dir = tmp_log_dir,
                                  output_dir = tmp_log_dir,
                                  queue = "all.q",
                                  memory = "15G",
                                  threads = 8, 
                                  time = "01:00:00",
                                  n_jobs = max(job_key$task_id),
                                  archive = F,
                                  args = c(task_key_path, map_version, output_data_path))
