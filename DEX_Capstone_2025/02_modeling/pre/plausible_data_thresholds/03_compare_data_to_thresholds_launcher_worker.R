# -------------------------------------------------
#    Assess how much data exceeds the default thresholds 
#             - Calculate 2.5, 50, and 97.5% percent of data that is above the threshold, by cause and other granularities of stratifications
#
#    Author: Haley Lescinsky
#   
#
#   This is both a launcher and a worker script in one. Either run interactively with a single data combination (launch = F) or launch a full set of jobs for a run_id!
# -------------------------------------------------

rm(list = ls())
Sys.umask(mode = 002)
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <-  dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")
set.seed(6667)
t_start <- Sys.time()
'%ni%' <- Negate('%in%')

## --------------------
## 2. Parameters
## --------------------

config <- get_config()

if(interactive()){
  
  run_id <- 'XX'
  
  toc <- "IP"
  metric <- "encounters_per_person"
  acause <- "cvd_ihd"
  geo <- "national"
  
  launch <- T
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  toc <- params$toc
  metric <- params$metric
  acause <- params$acause
  geo <- params$geo
  
  run_id <- args[2]
  
  launch <- F # DO NOT CHANGE EVER
}

param_template <- data.table(toc, metric, acause, geo) 

save_dir <- paste0("/FILEPATH/run_", run_id, "/compare_data_to_thresholds/data/")
if(!dir.exists(save_dir)){
  dir.create(save_dir, recursive = T)
}

if(launch){
  
  toc <- c("AM", "IP", "ED", "HH", "NF", "DV", "RX")
  metric <- c("spend_per_encounter", "encounters_per_person")
  acause <- unique(fread(paste0("/FILEPATH/run_", run_id, "/params_for_model.csv"))$acause)
  geo <- c("state", "national", "county")
  
  job_key <- tidyr::crossing(toc, metric, acause, geo) %>% as.data.table()
  job_key <- job_key[!(toc == 'DV' & acause %ni% c("_oral", "exp_well_dental")) ]
  
  job_key[, task_id := 1:.N]
  
  task_key_path <- paste0(save_dir, "/../task_key_map.csv")
  write.csv(job_key, task_key_path, row.names = F)
  
  extract_data <- SUBMIT_ARRAY_JOB(paste0("data_above_threshold_", run_id),
                                   script = paste0(code_path, "/03_compare_data_to_thresholds_launcher_worker.R"),
                                   queue = "all.q",
                                   memory = "50G",
                                   threads = 2, 
                                   time = "01:15:00",
                                   n_jobs = max(job_key$task_id),
                                   archive = F,
                                   args = c(task_key_path, run_id), 
                                   user_email = paste0(Sys.info()['user'], "@uw.edu"))
  
  print(extract_data)
  
  
}else{
  
  thresholds_dt <- fread(paste0(save_dir, '/FILEPATH/across_cause_thresholds.csv'))
  threshold <- thresholds_dt[metric== param_template$metric & toc == param_template$toc]$threshold
  
  root_indir <- paste0("/FILEPATH/run_", run_id, "/")                 
  indir <- paste0(root_indir, 'data/',
                  'metric=',param_template$metric,'/',
                  'geo=',param_template$geo,'/',
                  'toc=',param_template$toc,'/')
  
  if(dir.exists(indir)){
  
  raw_data <- arrow::open_dataset(indir)
  data <- raw_data %>% 
    filter(acause == param_template$acause) %>% 
    collect() %>% 
    data.table()
  if("__index_level_0__" %in% names(data)){
    data[,`__index_level_0__` := NULL ]
  }
  
  data[,`:=`(
    acause = param_template$acause,
    toc = param_template$toc,
    metric = param_template$metric,
    geo = param_template$geo
  )]
  
  data[, data_upper := raw_val + 1.96*se]

  data[, raw_val_above := ifelse(raw_val > threshold, 1, 0)]
  data[, data_upper_above := ifelse(data_upper > threshold, 1, 0)]
  
  summarize_by_cols <- function(data, by_cols){
    
    level1 <- data[ ,.(raw_val_above = sum(raw_val_above),
                       data_upper_above  = sum(data_upper_above),
                       total_rows = .N,
                       total_obs = sum(n_obs)), by = by_cols]

    level1 <- melt(level1, measure.vars = c("raw_val_above", "data_upper_above"), value.name = "count")
    level1 <- level1[count > 0]
    level1[, prop_over := count / total_rows]
    
    val_over <- data[raw_val_above == 1, .(median = median(raw_val), 
                                           lower_quant = quantile(raw_val, 0.025),
                                           upper_quant = quantile(raw_val, 0.975),
                                           variable = "raw_val_above"), by = by_cols]
    
    upper_over <- data[data_upper_above == 1, .(median = median(data_upper),
                               median_se = median(se),
                               lower_quant = quantile(data_upper, 0.025),
                               upper_quant = quantile(data_upper, 0.975),
                               variable = "data_upper_above"), by = by_cols]

    quant_details <- rbind(val_over, upper_over, fill = T)
    level1 <- merge(level1, quant_details, by = c(by_cols, 'variable'), all = T)

    level1[, grouping := paste0(by_cols, collapse = ",")]
    
    return(level1)
    
  }
  
  level0 <- summarize_by_cols(data, by_cols = c("toc", "acause", "metric", "geo"))
  level1 <- summarize_by_cols(data, by_cols = c("pri_payer", "payer","toc", "acause", "metric", "geo"))
  level2 <- summarize_by_cols(data, by_cols = c("dataset","toc", "acause", "metric", "geo"))
  level3 <- summarize_by_cols(data, by_cols = c("age_group_years_start","toc", "acause", "metric", "geo"))
  level4 <- summarize_by_cols(data, by_cols = c("age_group_years_start","toc", "acause", "metric", "geo", "dataset"))
  
  summarize <- rbind(level0, level1, level2, level3, level4, fill = T)
  setcolorder(summarize, c("grouping"))
  summarize[, threshold := threshold]
  
  write_dataset(summarize, path = save_dir, 
                partitioning = c("geo", "toc"),
                basename_template = paste0(metric, "_", acause, "_{i}.parquet"))


  }
}
