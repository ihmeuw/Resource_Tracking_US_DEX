#-------------------------------------------------
#
#  Launch the steps to generate the HCCI injury + RDP maps
#
#  Author: Haley Lescinsky
#
#---------------------------------------------------

rm(list = ls())
Sys.umask(mode = 002)
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
Sys.setenv("RETICULATE_PYTHON" = '/FILEPATH/python')
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

dir.create(paste0(map_version, "/inputs/data/"), recursive = T)
dir.create(paste0(map_version, "/maps/"), recursive = T)

#------------------------------------------------------------
sources_to_run <- c("NIS", "NEDS", "MSCAN", "KYTHERA", "MDCR", "MDCD")
tocs_to_run <- c("IP", "ED", "AM", "HH") 

#--------------------------------
# Make job key across sources!
#--------------------------------

job_key <- data.table()
for(data_source in sources_to_run){ 
  
  toc <- config$METADATA$tocs[[data_source]]
  
  input_data_path <- paste0(parsed_config$data_output_dir[[data_source]], "/data/")
  if(!dir.exists(input_data_path)){stop("input_data_dir doesn't exist!")}
  
  # demographic grid
  demographic_grid <- data.table("age_start" = c(0, 1, seq(5, 95, by = 5)))
  
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

# Job key for the pulling of data
job_key <- job_key[toc %in% tocs_to_run]

# drop 2015 since we don't use it! 
job_key <- job_key[year_id!=2015]

job_key[, task_id := 1:.N]
task_key_path <- paste0(map_version,"/inputs/_pull_data_task_id_key.csv")
job_key[, source_data_path := gsub("/data/", "/sankey_diagram_metadata/", source_data_path)]


write.csv(job_key, task_key_path, row.names = F)

#-----------------------------------------
#  SUBMIT JOBS
##----------------------------------------

tmp_log_dir <- paste0(log_dir, "/HCCI_INJ_RDP_make/")
if(dir.exists(tmp_log_dir)){
  system(paste0("rm -r ", tmp_log_dir))
}
dir.create(tmp_log_dir, recursive = T)


# LAUNCH PULL DATA AS ARRAY
pull_data_jid <- SUBMIT_ARRAY_JOB(paste0("hcci_adj_pull_data"),
                                  script = paste0(code_path, "/make/01_pull_data.R"),
                                  error_dir = tmp_log_dir,
                                  output_dir = tmp_log_dir,
                                  queue = "all.q",
                                  memory = "25G",
                                  threads = 8, 
                                  time = "01:00:00",
                                  n_jobs = max(job_key$task_id),
                                  archive = F,
                                  args = c(task_key_path, map_version))

make_map_jid <- SUBMIT_JOB(paste0("hcci_adj_save_map"),
                           script = paste0(code_path, "/make/02_format_plot.R"),
                           error_dir = tmp_log_dir,
                           output_dir = tmp_log_dir,
                           queue = "all.q",
                           memory = "25G",
                           threads = 8, 
                           time = "01:00:00",
                           hold = pull_data_jid,
                           archive = F,
                           args = c(map_version))


cat(paste0("HCCI maps - run ", phase_run_id,
           "\n---\ndata pull: ", pull_data_jid,
           "\nplot & save maps: ", make_map_jid))



