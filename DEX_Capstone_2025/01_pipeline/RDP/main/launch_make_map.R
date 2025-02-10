##--------------------------------------------------
#   Launch RDP 
#          - making the map 
#          - map consists of probabilities between input garbage/nec codes and target codes
# 
# Dependencies: RDP Package version
#
#   Author: Haley Lescinsky
#           
##--------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")


# Get and set up versions and arguments
#-----------------------------------------------------------------------------
wipe_maps <- T

# locate config
config <- get_config()

# determine phase_run_id
phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

# determine rdp package version
packages_map_version_id <- get_map_metadata(maps = list("RDP_PACKAGES"))[status == "Best", map_version_id]

# make new map version
rdp_map_version_id <- register_map_version(related_map = "RDP",
                                             phase_run_id = phase_run_id,
                                             description = "DESCRIPTION")

# Sources and Types of care for adjustment
sources_to_run <- config$RDP$map_inputs
tocs_included <- c("IP", "ED", "AM", "HH", "NF")

#------------------------------------------
#  Set paths
#-----------------------------------------

packages_path <- parsed_config(config, key = "RDP", map_version_id = as.character(packages_map_version_id))$package_dir
map_path <- parsed_config(config, key = "RDP", map_version_id = as.character(rdp_map_version_id))$map_output_dir
rdp_config <- parsed_config(config, "RDP", run_id = as.integer(phase_run_id))

if(wipe_maps){
  confirm_delete_directory(map_path)
}else{
  dir.create(map_path)
}

# make some paths
dir.create(paste0(map_path, "/maps/"), recursive = T)

#--------------------------------
# Make job key across sources!
#--------------------------------
job_key <- data.table()
for(data_source in sources_to_run){ 
  
  input_data_path <- paste0(rdp_config$data_input_dir[[data_source]], "/data/")
  output_data_path <- rdp_config$data_output_dir[[data_source]]
  
  # code system map (count 2015 for both years)
  code_system <- data.table(year_id = c(1991:2015, 2015:2023),
                            code_system = c(rep("icd9", length(1991:2015)),
                                            rep("icd10", length(2015:2023))))
  
  # loop through tocs
  tocs <- config$METADATA$tocs[[data_source]]
  tocs <- intersect(tocs, tocs_included)
  
  for(toc in tocs){
    
    # get years
    data_files <- list.files(paste0(input_data_path, "/toc=", toc, "/"), pattern = "year", full.names = F)
    year_vec <- data_files %>% str_match_all("[0-9]{4}") %>% unlist %>% as.numeric
    data <- data.table(source_data_path = input_data_path,
                       source = data_source,
                       year_id = year_vec,
                       toc = toc)
    job_key_tmp <- merge(data, code_system, by = "year_id", all.x = T)
    job_key <- rbind(job_key, job_key_tmp)
    
  }

}

## Launch it!
job_key[, task_id := 1:.N]
task_key_path <- paste0(map_path, "task_id_key.csv")

# write task key path
write.csv(job_key, task_key_path, row.names = F)




#--------------------------------
# SUBMIT JOBS
#--------------------------------

tmp_log_dir <- paste0(log_dir, "/RDP_make/run_id", phase_run_id, "_", "rdp_mvid", rdp_map_version_id, "/")
if(dir.exists(tmp_log_dir)){
  system(paste0("rm -r ", tmp_log_dir))
}
dir.create(tmp_log_dir, recursive = T)

print(packages_path)
print(map_path)

map_jid <- SUBMIT_ARRAY_JOB(paste0("RDP_make_map"),
                            script = paste0(code_path, "/make_map/make_map.R"),
                            error_dir = tmp_log_dir,
                            output_dir = tmp_log_dir,
                            queue = "all.q",
                            memory = "100G",  
                            threads = 3, 
                            time = "05:00:00",
                            n_jobs = max(job_key$task_id),
                            archive = F,
                            user_email = paste0(Sys.info()['user'], "@uw.edu"),
                            args = c(task_key_path, packages_path, map_path)
)
print(map_jid)

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
                            args = c(map_path, tmp_log_dir, 0),  # 0 for make map as opposed to apply
                            hold = map_jid)

cat(paste0("RDP make maps (map_version ", rdp_map_version_id,") \n", '--rdp make maps: ', map_jid, "\n--rdp error checker: ", rdp_log_check))

# Mark map version as best
mark_best(table = "map_version", table_id = rdp_map_version_id)
