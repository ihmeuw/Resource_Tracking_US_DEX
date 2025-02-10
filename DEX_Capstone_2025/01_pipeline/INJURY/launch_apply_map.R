#-------------------------------------------------
#
#  Launch the apply map for injury adjust
#     To-run: best to run the script in RStudio!
#
#  Dependencies: primary cause must have finished running!
#
#  Author: Haley Lescinsky
#---------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")
load("/FILEPATH/states.RData")

#--------------------------------------------------------------
# locate config
config <- get_config()

# determine latest phase_run_id
phase_run_id <- max(get_phase_run_id()[status == "Best", phase_run_id])

# determine injury version 
inj_map_version_id <- get_map_metadata(maps = list("INJURY"))[status == "Best", map_version_id]

#------------------------------------------
#  Set paths
#-----------------------------------------
inj_config <- parsed_config(config, "INJURY", run_id = as.integer(phase_run_id), map_version_id = as.integer(inj_map_version_id))
inj_map_path <- inj_config$map_output_dir

#------------------------------------------------------------
sources_to_run <- inj_config$targets 
tocs_included <- c("IP", "ED", "AM", "HH", "NF") 

#--------------------------------
# SUBMIT JOBS BY SOURCE
#--------------------------------

for(data_source in sources_to_run){ 
  
  message(data_source)
  tocs <- config$METADATA$tocs[[data_source]]
  tocs <- intersect(tocs, tocs_included)
  
  input_data_path <- paste0(inj_config$data_input_dir[[data_source]], "/data/")
  if(!dir.exists(input_data_path)){stop("input_data_dir doesn't exist!")}
  
  output_data_path <- inj_config$data_output_dir[[data_source]]
  
  # make sure output_data_bath is empty
  if(dir.exists(output_data_path)){
    confirm_delete_directory(output_data_path)
  }

  tmp_log_dir <- paste0(log_dir, "/INJ_apply/run_id", phase_run_id, "_","INJ_mvid", inj_map_version_id,"/", data_source, "/" )
  if(dir.exists(tmp_log_dir)){
    confirm_delete_directory(tmp_log_dir)
  }
  dir.create(tmp_log_dir, recursive = T)
  
  # code system map (count 2015 for both years)
  code_system <- data.table(year_id = c(1991:2015, 2015:2023),
                            code_system = c(rep("icd9", length(1991:2015)),
                                            rep("icd10", length(2015:2023))))
  
  job_key <- data.table()
  for(toc in tocs){
    
    # get years
    data_files <- list.files(paste0(input_data_path, "/toc=", toc), pattern = "year", full.names = F)
    year_vec <- data_files %>% str_match_all("[0-9]{4}") %>% unlist %>% as.numeric
    data <- data.table(source_data_path = input_data_path,
                       output_data_path = output_data_path,
                       source = data_source,
                       year_id = year_vec,
                       toc = toc,
                       toc_use = toc)
    tmp_job_key <- merge(data, code_system, by = "year_id", all.x = T)
    
    # Parallelize jobs by state to speed up run time
    tmp_job_key <- as.data.table(tidyr::crossing(tmp_job_key, data.table("state_id"=c(states$abbreviation,"-1"))))
    job_key <- rbind(job_key, tmp_job_key)
  
  }
    
    # Job key for the pulling of data
    job_key[, task_id := 1:.N]
    
    task_key_path <- paste0(inj_map_path,"/",data_source,"_apply_data_task_id_key__run", phase_run_id,".csv")
    
    
    write.csv(job_key, task_key_path, row.names = F)
    
    
    # LAUNCH APPLY DATA AS ARRAY
    apply_jid <- SUBMIT_ARRAY_JOB(paste0(data_source, "_INJ_apply"),
                                  script = paste0(code_path, "/apply_map/apply_injury.R"),
                                  error_dir = tmp_log_dir,
                                  output_dir = tmp_log_dir,
                                  queue = "all.q",
                                  memory = "50G",  
                                  threads = 1,  
                                  throttle = 500,
                                  time = "02:00:00",
                                  n_jobs = max(job_key$task_id),
                                  archive = F,
                                  args = c(task_key_path, inj_map_path),
                                  user_email = paste0(Sys.info()['user'], "@uw.edu"))
    
    print(data_source)
    print(apply_jid)
    

  
}

#-----------------------------------------
#  SUBMIT ERROR CATCHER
##----------------------------------------


tmp_log_dir <- paste0(log_dir, "/INJ_apply/run_id", phase_run_id, "_", "INJ_mvid", inj_map_version_id,"/")


# LAUNCH ERROR CATCHER
inj_log_check <- SUBMIT_JOB(paste0("INJ_log_check"),
                            script = paste0(code_path, "/make_map/06_check_logs.R"),
                            error_dir = tmp_log_dir, 
                            output_dir = tmp_log_dir, 
                            queue = "long.q",
                            memory = "4G",
                            threads = 3,
                            time = "00:10:00",
                            archive = F,
                            args = c(inj_map_path, tmp_log_dir, 1),
                            hold = apply_jid)


# Update database
update_phase_run(map_version_id = inj_map_version_id ,
                 phase_run_id = phase_run_id)



