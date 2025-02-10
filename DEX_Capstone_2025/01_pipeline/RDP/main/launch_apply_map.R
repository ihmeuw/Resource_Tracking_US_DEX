##--------------------------------------------------
#  Launch RDP 
#     - Apply map to adjust data
# 
#  Dependencies: Injury adjust and finished RDP map version
#
#  Author: Haley Lescinsky
#           
##--------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")
load("/FILEPATH/states.RData")


# Get and set up versions and arguments
#-----------------------------------------------------------------------------
wipe_all_outputs <- T

# locate config
config <- get_config()

# determine phase_run_id
phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

# determine rdp package version
packages_map_version_id <- get_map_metadata(maps = list("RDP_PACKAGES"))[status == "Best", map_version_id]

# determine causemap  version
cm_version <- get_map_metadata(maps = list("CAUSEMAP"))[status == "Best", map_version_id]

# determine rdp map version
rdp_map_version_id <- get_map_metadata(maps = list("RDP"))[status == "Best", map_version_id]

# Sources and Types of care for adjustment
sources_to_run <- config$RDP$map_inputs
tocs_included <- c("IP", "ED", "AM", "HH", "NF")

#------------------------------------------
#   Set paths
#-----------------------------------------
dex_icd_map_path <- parsed_config(config, key = "CAUSEMAP", map_version_id = as.integer(cm_version))$icd_map_path
packages_path <- parsed_config(config, key = "RDP", map_version_id = as.character(packages_map_version_id))$package_dir
rdp_map_path <- parsed_config(config, key = "RDP", map_version_id = as.integer(rdp_map_version_id))$map_output_dir
rdp_config <- parsed_config(config, "RDP", run_id = as.integer(phase_run_id))

if(!dir.exists(rdp_map_path)){
  stop("Map version does not exist. Cannot apply maps without maps!!")
}

# Launch RDP
#--------------------------------------------------------------------------------------------
visual_hold_jids <- c()

for(data_source in sources_to_run){
  
  message(data_source)
  
  input_data_path <- paste0(rdp_config$data_input_dir[[data_source]], "/data/")
  output_data_path <- rdp_config$data_output_dir[[data_source]]
  
  # make sure output_data_bath is empty
  tmp_log_dir <- paste0(log_dir, "/RDP_apply/run_id", phase_run_id, "_","rdp_mvid", rdp_map_version_id,"/", data_source, "/" )
  
  if(wipe_all_outputs){
    if(dir.exists(output_data_path)){
      confirm_delete_directory(output_data_path)
    }
    if(dir.exists(tmp_log_dir)){
      confirm_delete_directory(tmp_log_dir)
    }
  }

  dir.create(paste0(output_data_path, "/data/"), recursive = T)
  dir.create(tmp_log_dir, recursive = T)
  
  # code system map (count 2015 for both years)
  code_system <- data.table(year_id = c(1991:2015, 2015:2023),
                            code_system = c(rep("icd9", length(1991:2015)),
                                            rep("icd10", length(2015:2023))))
  
  # loop through tocs
  tocs <- config$METADATA$tocs[[data_source]]
  tocs <- intersect(tocs, tocs_included)
  
  job_key <- data.table()
  
  for(toc in tocs){
    
    # get years
    data_files <- list.files(paste0(input_data_path, "/toc=", toc, "/"), pattern = "year", full.names = F)
    year_vec <- data_files %>% str_match_all("[0-9]{4}") %>% unlist %>% as.numeric
    data <- data.table(source_data_path = input_data_path,
                       source = data_source,
                       year_id = year_vec,
                       toc = toc)
    job_key_tmp <- merge(data, code_system, by = "year_id", all.x = T)
    
    # now cross with states too if big dataset
    if(data_source %in% c("MDCD", "MSCAN", "MDCR", "KYTHERA")){
      job_key_tmp <- as.data.table(tidyr::crossing(job_key_tmp, data.table("state_id"=c("-1", states$abbreviation))))
    }else{
      job_key_tmp$state_id <- NA
    }
    
    job_key <- rbind(job_key, job_key_tmp)
    
  }
  
  job_key[, task_id := 1:.N]
  task_key_path <- paste0(rdp_map_path, data_source, "_apply_task_id_key_",phase_run_id, "_RETRY.csv")
  
  # add on source_use
  job_key[, source_use := source]
  
  write.csv(job_key, task_key_path, row.names = F)
  # 
  #
  #  Submit jobs
  #

  # launch apply map with a hold on the make map
  apply_jid <- SUBMIT_ARRAY_JOB(paste0(data_source, "_apply_RDP_map"),
                   script = paste0(code_path, "/apply_map/apply_map.R"),
                   error_dir = tmp_log_dir,
                   output_dir = tmp_log_dir,
                   queue = "all.q",
                   memory = ifelse(data_source %in% c("MDCD", "MDCR", "KYTHERA"), "70G", "22G"),
                   threads = 5,
                   time = "20:00:00",
                   throttle = 2000,
                   n_jobs = max(job_key$task_id),
                   archive = F,
                   user_email = paste0(Sys.info()['user'], "@uw.edu"),
                   args = c(task_key_path, packages_path, rdp_map_path, output_data_path, dex_icd_map_path)
  )
  print(apply_jid)

  # Launch diagnostic plots
  diagnostic_jid <- SUBMIT_JOB(name = paste0(data_source, "_viz_RDP"),
             script = paste0(code_path, "/../post/visualize_data.R"),
             error_dir = tmp_log_dir,
             output_dir = tmp_log_dir,
             queue = "long.q",
             memory = ifelse(data_source %in% c("KYTHERA"), "250G", "150G"),
             threads = 5,
             time = "02:00:00",
             archive = F,
             args = c(task_key_path, packages_path, rdp_map_path, output_data_path, dex_icd_map_path),
             hold = apply_jid
  )

  visual_hold_jids <- c(visual_hold_jids, diagnostic_jid)
}

tmp_log_dir <- paste0(log_dir, "/RDP_apply/run_id", phase_run_id, "_","rdp_mvid", rdp_map_version_id,"/")

# LAUNCH ERROR CATCHER
rdp_log_check <- SUBMIT_JOB(paste0("RDP_log_check"),
                            script = paste0(code_path, "/make_map/check_logs.R"),
                            error_dir = tmp_log_dir, 
                            output_dir = tmp_log_dir, 
                            queue = "long.q",
                            memory = "4G",
                            threads = 3,
                            time = "00:10:00",
                            archive = F,
                            args = c(rdp_map_path, tmp_log_dir, 1, phase_run_id),  # 1 for apply map as opposed to apply
                            hold = paste0(visual_hold_jids, collapse = ","))


diagnostic_jid <- SUBMIT_JOB(name = paste0("viz_all_RDP"),
                             script = paste0(code_path, "/../post/visualize_across_sources.R"),
                             error_dir = tmp_log_dir,
                             output_dir = tmp_log_dir,
                             queue = "all.q",
                             memory = "300G",
                             threads = 10, 
                             time = "03:00:00",
                             archive = F,
                             args = c(packages_path, rdp_map_path, phase_run_id, dex_icd_map_path),
                             hold = paste0(visual_hold_jids, collapse = ",")
)


update_phase_run(map_version_id = packages_map_version_id ,
                 phase_run_id = phase_run_id)

update_phase_run(map_version_id = rdp_map_version_id ,
                 phase_run_id = phase_run_id)

