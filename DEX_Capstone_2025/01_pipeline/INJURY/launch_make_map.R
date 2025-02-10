#-------------------------------------------------
#
#  Launch the steps to generate injury maps
#   
#  Author: Haley Lescinsky
#
#---------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, arrow, data.table, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)
log_dir <- paste0("FILEPATH", Sys.info()['user'], "/")

#--------------------------------------------------------------
# Load in config and get versions
config <- get_config()
wipe_maps <- F

phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

# Make new injury version 
inj_map_version_id <- register_map_version(related_map = "INJURY",
                                           phase_run_id = phase_run_id,
                                           description = "DESCRIPTION")

map_metadata <- get_map_metadata(maps = list("CAUSEMAP"))
causemap_map_version_id <- map_metadata[status == "Best", map_version_id]

#------------------------------------------
#  Set up directories
#-----------------------------------------

inj_config <- parsed_config(config, "INJURY", run_id = as.character(phase_run_id), map_version_id = as.character(inj_map_version_id))
inj_map_path <- inj_config$map_output_dir

if(wipe_maps & dir.exists(inj_map_path)){
  confirm_delete_directory(inj_map_path)
}else if(dir.exists(inj_map_path)){
  print("Map dir already exists .... ")
}

dir.create(paste0(inj_map_path, "/inputs/dx_distribution"), recursive = T)
dir.create(paste0(inj_map_path, "/inputs/validation/prop_n_with_e"), recursive = T)
dir.create(paste0(inj_map_path, "/inputs/validation/count_inj_nec/"))
dir.create(paste0(inj_map_path, "/inputs/residual_distribution/"))
dir.create(paste0(inj_map_path, "/inputs/encounters_count/"))

#------------------------------------------------------------

sources_to_run <- inj_config$map_inputs  
tocs <- c("IP", "ED", "AM", "HH", "NF")

#--------------------------------
# Make job key across sources!
#--------------------------------

job_key <- data.table()
for(data_source in sources_to_run){ 
  
  toc <- config$METADATA$tocs[[data_source]]
  
  input_data_path <- paste0(inj_config$data_input_dir[[data_source]], "/data/")
  if(!dir.exists(input_data_path)){stop("input_data_dir doesn't exist!")}

  # demographic grid
  age_start <- seq(0, 80, by = 20)
  age_end <- c(seq(20, 80, by = 20), 100) 
  
  if(data_source %in% c("NEDS", "MDCD", "MDCR", "KYTHERA")){
    # For big datasets chunk into smaller bits
    age_start <- c(0, 1, seq(5, 95, by = 5))
    age_end <- c(1, seq(5, 95, by = 5), 100) 
  }
  
  demographic_grid <- data.table("age_start" = age_start, "age_end" = age_end)
  
  # Code system map (count 2015 for both years)
  code_system <- data.table(year_id = c(1991:2015, 2015:2019),
                            code_system = c(rep("icd9", length(1991:2015)),
                                            rep("icd10", length(2015:2019))))
  
  # Get years of data
  data_files <- list.files(paste0(input_data_path, "/toc=", toc), pattern = "year", full.names = T)
  year_vec <- data_files %>% str_match_all("[0-9]{4}") %>% unlist %>% as.numeric
  toc_vec <- data_files %>% str_match_all(paste0(toc, collapse = "|")) %>% unlist
  data <- data.table(source_data_path = input_data_path,
                     source = data_source,
                     year_id = year_vec,
                     toc = toc_vec)
  data <- merge(data, code_system, by = "year_id", all.x = T)
  tmp_job_key <- as.data.table(tidyr::crossing(data, demographic_grid))
  
  job_key <- rbind(job_key, tmp_job_key)
  
}
  
# Job key for the pulling of data
job_key <- job_key[toc %in% tocs]

job_key[, task_id := 1:.N]
task_key_path <- paste0(inj_map_path,"/inputs/_pull_data_task_id_key.csv")
write.csv(job_key, task_key_path, row.names = F)

# Job key for the modeling of trends
model_job_key <- rbind(unique(job_key[,.(toc, code_system)][, sex_id := 1]),
                       unique(job_key[,.(toc, code_system)][, sex_id := 2]))
model_job_key[, task_id := 1:.N]
model_task_key_path <- paste0(inj_map_path,"/inputs/_model_data_task_id_key.csv")
write.csv(model_job_key, model_task_key_path, row.names = F)

#-----------------------------------------
#  SUBMIT JOBS (run all at once)
##----------------------------------------
  
tmp_log_dir <- paste0(log_dir, "/INJ_make/run_id", phase_run_id, "_", "INJ_mvid", inj_map_version_id,"/")
if(dir.exists(tmp_log_dir)){
  system(paste0("rm -r ", tmp_log_dir))
}
dir.create(tmp_log_dir, recursive = T)
  
# LAUNCH TRUNCATION MAP
trunc_map_jid <- SUBMIT_JOB(paste0("INJ_trunc_map"),
                              script = paste0(code_path, "/make_map/01_inj_trunc_map.R"),
                              error_dir = tmp_log_dir, 
                              output_dir = tmp_log_dir, 
                              queue = "long.q",
                              memory = "2G",
                              threads = 1,
                              time = "00:05:00",
                              archive = F,
                              args = c(inj_map_path, causemap_map_version_id))


# LAUNCH PULL DATA AS ARRAY
pull_data_jid <- SUBMIT_ARRAY_JOB(paste0("INJ_pull_data"),
                              script = paste0(code_path, "/make_map/02_pull_data.R"),
                              error_dir = tmp_log_dir,
                              output_dir = tmp_log_dir,
                              queue = "all.q",
                              memory = "25G",
                              threads = 2, 
                              time = "05:00:00",
                              n_jobs = max(job_key$task_id),
                              archive = F,
                              #test = T,
                              args = c(task_key_path, inj_map_path, causemap_map_version_id),
                              hold = trunc_map_jid)

# LAUNCH FORMAT DATA
format_data_jid <- SUBMIT_JOB(paste0("INJ_format_data"),
                              script = paste0(code_path, "/make_map/03_format_data.R"),
                              error_dir = tmp_log_dir, 
                              output_dir = tmp_log_dir, 
                              queue = "all.q",
                              memory = "80G",
                              threads = 5,
                              time = "00:60:00",
                              archive = F,
                              args = c(inj_map_path, causemap_map_version_id),
                              hold = pull_data_jid)


# LAUNCH MODEL DATA AS ARRAY
model_data_jid <- SUBMIT_ARRAY_JOB(paste0("INJ_model"),
                              script = paste0(code_path, "/make_map/04_model_data.R"),
                              error_dir = tmp_log_dir, 
                              output_dir = tmp_log_dir, 
                              queue = "all.q",
                              memory = "15G",
                              threads = 10,
                              time = "01:00:00",
                              n_jobs = max(model_job_key$task_id),
                              archive = F,
                              args = c(model_task_key_path, inj_map_path),
                              user_email = paste0(Sys.info()['user'], "@uw.edu"),
                              hold = format_data_jid)
# LAUNCH COMPILE
compile_jid <- SUBMIT_JOB(paste0("INJ_compile"),
                            script = paste0(code_path, "/make_map/05_compile_map.R"),
                            error_dir = tmp_log_dir, 
                            output_dir = tmp_log_dir, 
                            queue = "all.q",
                            memory = "4G",
                            threads = 3,
                            time = "00:10:00",
                            archive = F,
                            args = c(inj_map_path),
                            hold = model_data_jid)

# LAUNCH ERROR CATCHER
inj_log_check <- SUBMIT_JOB(paste0("INJ_log_check"),
                              script = paste0(code_path, "/make_map/06_check_logs.R"),
                              error_dir = tmp_log_dir, 
                              output_dir = tmp_log_dir, 
                              queue = "all.q",
                              memory = "4G",
                              threads = 3,
                              time = "00:10:00",
                              archive = F,
                              args = c(inj_map_path, tmp_log_dir, 0),
                              hold = compile_jid)

# Mark map version as best
mark_best(table = "map_version", table_id = inj_map_version_id)

# Save somewhere to keep track of job ids
cat(paste0('injury make map (map version ', inj_map_version_id, ')\n',
           '-trunc map: ', trunc_map_jid, '\n',
           '-pull data: ', pull_data_jid, '\n',
           '-format data: ', format_data_jid, '\n',
           '-model data: ', model_data_jid, '\n',
           '-compile map: ', compile_jid, '\n',
           '-error checker: ', inj_log_check))

