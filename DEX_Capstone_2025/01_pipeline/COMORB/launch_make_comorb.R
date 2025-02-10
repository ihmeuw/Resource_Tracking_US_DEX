#-------------------------------------------------
#
#  Launch the steps to generate comorb adjustment maps
#
#
#  Author: Haley Lescinsky
#---------------------------------------------------

rm(list = ls())
Sys.umask(mode = 002)
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")
states <- fread("/FILEPATH/states.csv")

#--------------------------------------------------------------
drop_RDP <- T # Do not have RDP adjusted data inform comorbidity regression

# Load in config and get versions
config <- get_config()

# determine phase_run_id
phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]


# make new comorb version
comorb_map_version_id <- register_map_version(related_map = "COMORB",
                                           phase_run_id = phase_run_id,
                                          description = "DESCRIPTION")


comorb_config <- parsed_config(config, "COMORB", run_id = as.character(phase_run_id), map_version_id = as.character(comorb_map_version_id))

map_path <- comorb_config$map_output_dir

dir.create(paste0(map_path, "/prepped_data/"), recursive = T)

#------------------------------------------------------------
sources_to_run <- comorb_config$map_inputs
print(sources_to_run)

#--------------------------------
# Make job key across sources!
#--------------------------------

job_key <- data.table()
for(data_source in sources_to_run){ 
  
  toc <- config$METADATA$tocs[[data_source]]
  
  input_data_path <- paste0(comorb_config$map_data_input_dir[[data_source]], "/data/")
  if(!dir.exists(input_data_path)){
    print(input_data_path)
    stop("input_data_dir doesn't exist!")}
  
    age_start <- seq(0, 90, by = 10)
    age_end <- c(seq(10, 90, by = 10), 100)
  
  demographic_grid <- data.table("age_start" = age_start, "age_end" = age_end)
  
  # get years
  data_files <- list.files(paste0(input_data_path, "/toc=", toc), pattern = "year", full.names = T)
  year_vec <- data_files %>% str_match_all("[0-9]{4}") %>% unlist %>% as.numeric
  toc_vec <- data_files %>% str_match_all(paste0(toc, collapse = "|")) %>% unlist
  data <- data.table(source_data_path = input_data_path,
                     source = data_source,
                     year_id = year_vec,
                     toc = toc_vec)
  tmp_job_key <- as.data.table(tidyr::crossing(data, demographic_grid))
  
  job_key <- rbind(job_key, tmp_job_key)
  
}


# Break into smaller buckets for a few jobs since the data is very big
expand_job_key <- job_key[toc == "AM" & source %in% c("MSCAN", "MDCR", "MDCD")]
job_key <- job_key[!(toc == "AM" & source %in% c("MSCAN", "MDCR", "MDCD"))]

# for AM, we don't use two years so as to have a manageable amount of data. 
expand_job_key <- expand_job_key[!year_id %in% c(2011, 2013)]

expand_job_key <- as.data.table(tidyr::crossing(expand_job_key, data.table("state_id"=c("-1", states$abbreviation))))

job_key <- rbind(job_key, expand_job_key, fill = T)

# Job key for the pulling of data
job_key[, task_id := 1:.N]

task_key_path <- paste0(map_path,"/_prep_data_task_id_key.csv")
write.csv(job_key, task_key_path, row.names = F)

# AF job key
# - Parallelize by cause; some causes don't get comorb
causelist <- fread(config$CAUSEMAP$causelist_path)
causelist <- causelist[!(acause == "_gc" | acause %like% "_NEC")]
causelist <- causelist[!(acause %like% "neo_" | acause %in% c("neonatal_preterm", "neonatal_enceph") | acause %like% "lri_corona" | acause %like% "exp_well_dental")]
calc_af_job_key <- tidyr::crossing("cause"=causelist$acause, "toc" =  unique(job_key$toc)) %>% as.data.table()

# separate into memory sets
big_am_causes <- c("diabetes_typ2", "exp_well_person", "msk_other", "msk_pain_lowback", "resp_other", "rf_hyperlipidemia", "rf_hypertension", "skin", "urinary", "cvd_ihd", "cvd_afib", "cvd_other", "endo", "sense_visionlos", "ckd", "_mental_agg", "exp_well_pregnancy", "uri")
calc_af_job_key[, mem_set := "100G"]
calc_af_job_key[toc == "AM" & !cause %in% big_am_causes, mem_set := "400G"]
calc_af_job_key[toc == "AM" & cause %in% big_am_causes, mem_set := "600G"]
calc_af_job_key[toc == "AM" & cause %in% c("msk_other", "exp_well_person"), mem_set := "700G"]
calc_af_job_key[toc == "HH" & cause %in% c("_mental_agg", "diabetes_typ2", "resp_copd", "rf_hypertension"), mem_set := "400G"]

calc_af_job_key[, task_id := 1:.N, by = "mem_set"]

af_task_key_path <- paste0(map_path,"/_af_task_id_key.csv")
write.csv(calc_af_job_key, af_task_key_path, row.names = F)

map_path_submitted_jobs <- paste0(map_path, "/submitted_tasks/")
dir.create(map_path_submitted_jobs)

#-----------------------------------------
#  SUBMIT JOBS
##----------------------------------------

tmp_log_dir <- paste0(log_dir, "/COMORB_make/run_id", phase_run_id, "_", "mvid", comorb_map_version_id,"")
if(dir.exists(tmp_log_dir)){
  system(paste0("rm -r ", tmp_log_dir))
}
dir.create(tmp_log_dir, recursive = T)

# LAUNCH PREP DATA AS ARRAY
prep_data_jid <- SUBMIT_ARRAY_JOB(paste0("comorb_prep_", comorb_map_version_id),
                                  script = paste0(code_path, "/make/prep_data.R"),
                                  error_dir = tmp_log_dir,
                                  output_dir = tmp_log_dir,
                                  queue = "all.q",
                                  memory = "170G", 
                                  throttle = 750,
                                  threads = 1,  
                                  time = "01:15:00",
                                  n_jobs = max(job_key$task_id),
                                  archive = F,
                                  args = c(task_key_path, map_path, drop_RDP), 
                                  user_email = paste0(Sys.info()['user'], "@uw.edu"))
write.csv(job_key, paste0(map_path_submitted_jobs, "/comorb_prep_", prep_data_jid, ".csv"))


# LAUNCH CALCULATE AFs 
calc_afs <- c()
for(mem in unique(calc_af_job_key$mem_set)){
  
  jid <- SUBMIT_ARRAY_JOB(paste0("comorb_af_", mem, "_", comorb_map_version_id),
                                script = paste0(code_path, "/make/calc_rr_afs.R"),
                                error_dir = tmp_log_dir,
                                output_dir = tmp_log_dir,
                                queue = "all.q",
                                memory = mem, 
                                threads = ifelse(mem == "80G" , 7, 16), 
                                time = ifelse(mem == "80G" , "01:00:00", "08:00:00"),
                                n_jobs = max(calc_af_job_key[mem_set == mem]$task_id),
                                archive = F,
                                hold = prep_data_jid,
                                user_email = paste0(Sys.info()['user'], "@uw.edu"),
                                args = c(af_task_key_path, paste0(map_path), mem))
  calc_afs <- c(calc_afs, jid)
  
  # save record of task ids for easy relaunch
  write.csv(calc_af_job_key[mem_set == mem], paste0(map_path_submitted_jobs, "/comorb_af_", jid, ".csv"), row.names = F)
  
}


diagnostic_jid <- SUBMIT_ARRAY_JOB(paste0("comorb_summary_", comorb_map_version_id),
                              script = paste0(code_path, "/make/viz/diagnostics_map.R"),
                              error_dir = tmp_log_dir,
                              output_dir = tmp_log_dir,
                              queue = "long.q",
                              memory = "30G",
                              threads = 1, 
                              time = "01:00:00",
                              n_jobs = 1,
                              archive = F,
                              hold = calc_afs,
                              args = c(map_path, tmp_log_dir),
                              user_email = paste0(Sys.info()['user'], "@uw.edu")
                        )

cat(paste0("Comorb maps - run ", phase_run_id, " & map version ",comorb_map_version_id,
           "\n---\nprep data: ", prep_data_jid,
           "\ncalc AFs: \n", paste0(calc_afs, collapse = '\n'),
           "\nplot maps: ", diagnostic_jid))


mark_best("map_version", comorb_map_version_id)
