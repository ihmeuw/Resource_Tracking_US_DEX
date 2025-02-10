# --------------------------------
#   Pull RMSE and MAD (Median Absolute Deviation) for a metric/toc/geo combination between raw inputs and modeled outputs
#
#   Author: Haley Lescinsky
# 
# --------------------------------
Sys.umask(mode = 002)
t0 <- Sys.time()
library(
  lbd.loader, 
  lib.loc = sprintf(
    "/FILEPATH/lbd.loader-%s.%s", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr, ggforce)
options(arrow.skip_nul = TRUE)
counties <- fread('/FILEPATH/merged_counties.csv')


if(interactive()){
  include_race <- T
  run_id <- "XX"
  set <- "setXX"
  g <- 'state'
  m <- 'spend_per_encounter'
  t <- 'IP'
  
  model_dir <- paste0("/FILEPATH/model_version_", set, "/")
  shiny_dir <- paste0("/FILEPATH/run_", run_id, "/data/model=", set, "/")

}else{
  args <- commandArgs(trailingOnly = TRUE)
  message(args)
  param_path <- args[1]
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  param_template <- fread(param_path)[task_id]
  print(param_template)
  message(param_template)
  g <- param_template$geo
  t <- param_template$toc
  m <- param_template$metric
  model_dir <- param_template$model_dir
  shiny_dir <- param_template$shiny_dir
}

save_dir <- paste0(model_dir, "rmse/rmse_files/")
if(!dir.exists(save_dir)){
  dir.create(save_dir, recursive = T)
}

# Calculate for just a select set of locations
if (g == 'national'){
  loc_list <- c('USA')
}else if(g == 'state'){
  loc_list <- c("CA", "TX")
}else{
  loc_list <- c('195','594','2605')
}

# Load data from shiny directory where modeled estimates and raw data points are already side by side
pull_rmse <- function(g, m, t, loc_list){
  
  preds_with_data <- open_dataset(shiny_dir) %>% filter(geo == g & metric == m & toc == t & location %in% loc_list) %>% collect() %>% as.data.table()
  
  preds_with_data <- preds_with_data[ !is.na(raw_val) & outlier==0]
  
  message('Got data, nrow: ', nrow(preds_with_data))
  combos_in_data <- unique(preds_with_data[, .(geo, toc, metric, acause, pri_payer, payer, sex_id, model_version_id)])
  
  combos_in_data_ref_dataset <- rbindlist(lapply(unique(combos_in_data$model_version_id), function(i){

    message(i)
    this_model_dir <- paste0("/FILEPATH/model_version_", i, "/")
    conv <- open_dataset(paste0(this_model_dir, "/convergence/")) %>% filter(geo == g & metric == m & toc == t) %>% collect() %>% as.data.table()
    conv <- conv[convergence!=1] #subset out the ones that didn't converge
    conv[, model_version_id := as.character(i)]


    map_with_ref <- merge(combos_in_data, conv, by = c("geo", "toc", "metric", "acause", "pri_payer", "payer", "sex_id", "model_version_id"))
    return(map_with_ref)
  }), fill = T)
  
  preds_with_data <- merge(preds_with_data, combos_in_data_ref_dataset[, .(geo, toc, metric, acause, pri_payer, payer, sex_id, ref_dataset)], by = c('acause', 'toc', 'metric', 'pri_payer', 'payer', 'sex_id', 'geo'), all.x = T)
  
  # subset to just reference data
  preds_with_data <- preds_with_data[dataset == ref_dataset]
  

  # For RMSE
  preds_with_data[, error_squared_1 := (raw_val - mean)^2]
  preds_with_data[, error_squared_2 := (raw_val - median)^2]
  
  # for MAD (median absolute deviation)
  preds_with_data[, absolute_deviation_1 := abs(raw_val - mean)]
  preds_with_data[, absolute_deviation_2 := abs(raw_val - median)]
  
  rmse <- preds_with_data[, .(rmse_mean = sqrt(sum(error_squared_1)/.N), mad_mean = median(absolute_deviation_1),rmse_median = sqrt(sum(error_squared_2)/.N), mad_median = median(absolute_deviation_2) )]
  rmse_by_loc <- preds_with_data[, .(rmse_mean = sqrt(sum(error_squared_1)/.N), mad_mean = median(absolute_deviation_1),rmse_median = sqrt(sum(error_squared_2)/.N), mad_median = median(absolute_deviation_2) ), by = c("location")]
  rmse_by_acause <- preds_with_data[, .(rmse_mean = sqrt(sum(error_squared_1)/.N), mad_mean = median(absolute_deviation_1),rmse_median = sqrt(sum(error_squared_2)/.N), mad_median = median(absolute_deviation_2) ), by = c("acause")]
  rmse_by_pri_payer <- preds_with_data[, .(rmse_mean = sqrt(sum(error_squared_1)/.N), mad_mean = median(absolute_deviation_1),rmse_median = sqrt(sum(error_squared_2)/.N), mad_median = median(absolute_deviation_2) ), by = c("pri_payer")]
  rmse_by_datasource <- preds_with_data[, .(rmse_mean = sqrt(sum(error_squared_1)/.N), mad_mean = median(absolute_deviation_1),rmse_median = sqrt(sum(error_squared_2)/.N), mad_median = median(absolute_deviation_2) ), by = c("dataset")]
  rmse_by_datasource2 <- preds_with_data[, .(rmse_mean = sqrt(sum(error_squared_1)/.N), mad_mean = median(absolute_deviation_1),rmse_median = sqrt(sum(error_squared_2)/.N), mad_median = median(absolute_deviation_2) ), by = c("dataset", 'pri_payer', "payer")]
  
  all_rmse <- rbind(rmse, rmse_by_loc, rmse_by_datasource, rmse_by_pri_payer, rmse_by_datasource2, rmse_by_acause, fill = T)
  all_rmse[, `:=` (geo = g, 
                   metric = m, 
                   toc = t, 
                   locations = paste0(loc_list, collapse = ","))]
  
  return(all_rmse)
  
}

message('Pulling RMSE')
rmse_table <- pull_rmse(g, m, t, loc_list)

write.csv(rmse_table, paste0(save_dir, "/", t, "_", m, "_", g, "_rmse.csv"), row.names = F)

print('Done')
