##' ----------------------------------------------------------------
##' Title: launch_scaling.R
##' Purpose: 
##' - Launches the jobs to scale estimates to SHEA envelopes.
##' 1. 2_agg_to_shea.R: Aggregates state estimates to SHEA categories
##'     - Parallelized on toc (and draw if running at draw level)
##'     - Only runs for geo = state
##' 2. 3_make_ratios.R: Creates scalars for aggregated DEX state estimates to SHEA
##'     - If running at draw level, all draws are pulled in at once 
##'       and operations are by draw (NOT parallelized on draw)
##' 3. 4_apply_ratios.R: Applies the scalars to estimates.
##'     - Parallelized on geo/toc/year/sate
##'     - Results partitioned by geo/toc/state/payer/{year-<year_id>.parquet} 
##' Author: Azalea Thomson
##' ----------------------------------------------------------------


Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)
username <- Sys.getenv('USER')

## --------------------
## Setup
## --------------------

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)
source(paste0(here,'/function_get_params.R'))

## --------------------
## Set args
## --------------------
run_queue <- 'all.q'
model_set_id <- 'XX'
scaling_version <- 'XX'
min_model_year <- 2010
max_model_year <- 2019
years <- c(min_model_year:max_model_year)

run_only_national <- F
run_at_draw_level <- T # Set to T if you want draws, otherwise F returns just means. n_draws must be equal to the number of draws run in modeling.
if (run_at_draw_level == T){
  save_draws <- T
  n_draws <- 50
}else{
  save_draws <- F
  n_draws <- NA
}

raw_config <- get_config()

model_set_data <- get_model_set(model_set_id = list(model_set_id))
mset_mvids <- unique(model_set_data$model_version_id) %>% as.character
print(paste0('Starting scaling for model version(s) ',toString(mset_mvids)))

denoms_path <- raw_config$POST_MODEL$scaling$pop_denom_dir
shea_dir <- raw_config$POST_MODEL$scaling$shea_env_path
denoms_version <- which_version_best(denoms_path)
shea_env_version <- which_version_best(shea_dir)

scaling_config <- parsed_config(raw_config, key = "POST_MODEL",
                                scaled_version = scaling_version)

param_dir <- paste0("FILEPATH") 
dir.create(param_dir, recursive = T)
missings_dir <- paste0("FILEPATH")
shea_path <- paste0(shea_dir, 'shea_adjusted_envelopes_allpayer.csv')
compile_dir <- scaling_config$scaling$compile_dir
state_compiled_dir <- paste0(compile_dir,'/geo=state')
agg_dir <- scaling_config$scaling$aggregate_dir
ratio_path <- scaling_config$scaling$ratio_path
final_dir <- scaling_config$final$final_dir
final_collapse_dir <- paste0(final_dir,'/collapsed/')
toc_cause_restrictions_path <- parsed_config(raw_config, key = "METADATA")$toc_cause_restrictions_path
cl <- fread(toc_cause_restrictions_path)[include == 1 & gc_nec == 0]


## ------------------------------
## Delete old output/make dir
## ------------------------------
dirs <- c(agg_dir, ratio_path, final_dir, final_collapse_dir)

for (d in dirs){
  dir.create(d, recursive = T)
}

## ------------------------------
## Get job params
## ------------------------------

tocs <- c('AM','ED','IP','HH','NF','RX','DV')
if (run_only_national == T){
  geogs <- 'national'
}else{
  geogs <- c('county','state','national')  
}



## PLOT PARAMS
if (run_only_national == T){
  plot_g <- 'national'
}else{
  plot_g <- 'county'
}
for (g in plot_g){
  print(g)
  plot_params <- get_plot_params(geog=g, tocs, missings_dir, scaling_version)
  assign(paste0("plot_param_dt_", g), plot_params[1] %>% as.data.table())
  assign(paste0("plot_param_path_", g), plot_params[2] %>% as.character())
  fwrite(get(paste0("plot_param_dt_", g)), get(paste0("plot_param_path_", g)))
}

## AGG PARAMS
agg_params <- get_agg_params(tocs = tocs, draws = run_at_draw_level, n_draws = n_draws, run_only_national = run_only_national)
agg_param_dt <- agg_params[1]%>% as.data.table()
agg_param_path <- agg_params[2]%>% as.character()
fwrite(agg_param_dt, agg_param_path)

## MAKE RATIO PARAMS
ratio_params <- get_ratio_params()
ratio_param_dt <- ratio_params[1]%>% as.data.table()
ratio_param_path <- ratio_params[2]%>% as.character()
fwrite(ratio_param_dt, ratio_param_path)

## APPLY RATIO PARAMS
for (g in geogs){
  print(g)
  apply_params <- get_apply_params(geog = g, toc_cause_restrictions_path = toc_cause_restrictions_path, save_draws = save_draws, 
                                   run_at_draw_level=run_at_draw_level, tocs = tocs, years = years)
  assign(paste0("apply_param_dt_", g), apply_params[1] %>% as.data.table())
  assign(paste0("apply_param_path_", g), apply_params[2] %>% as.character())

  if (g == 'county'){
    ## These states take too long and require too much more mem to run all causes at once, 
    ## so made a separate script that is parallelized on cause for them -- could be that its just TX in the future
    apply_param_dt_county_xtra <- apply_param_dt_county[state %in% c('TX','GA','TN','VA')] 
    apply_param_dt_county_xtra <- merge(apply_param_dt_county_xtra, cl[,.(cause = acause,toc)], by = 'toc', all.x=T, allow.cartesian = T)
    apply_param_dt_county_xtra[,mem_set:=ifelse(state == 'TX', '10G', '5G')]
    apply_param_path_county_xtra <- gsub('county.csv', 'county_xtra.csv', apply_param_path_county)
    fwrite(apply_param_dt_county_xtra,apply_param_path_county_xtra)
    apply_param_dt_county <- apply_param_dt_county[!(state %in% c('TX','GA','TN','VA'))]
    
    profiled_params <- fread(paste0(param_dir, 'profiled_params_county.csv'))
    profiled_params[,payer:=NULL]
    apply_param_dt_county <- merge(apply_param_dt_county, profiled_params, )
  }
  fwrite(get(paste0("apply_param_dt_", g)), get(paste0("apply_param_path_", g)))
}

## ------------------------------------------------
## Clear error/output log dirs/make them
## ------------------------------------------------

error_dir <- "FILEPATH"
output_dir <- "FILEPATH"

## --------------------------------------------------
## 2. LAUNCH AGG TO SHEA (holds for state compile) 
## --------------------------------------------------
state_throttle <- toString(nrow(agg_param_dt))
if (run_at_draw_level == T){
  agg_mem <- '90G'
}else{
  agg_mem <- '15G'
}
agg_jid <- SUBMIT_ARRAY_JOB(
  name = 'agg_to_shea', 
  script = paste0(here,'/2_agg_to_shea.R'), 
  queue = run_queue, 
  memory = agg_mem, 
  threads = "2", 
  time = "3:00:00",
  throttle = state_throttle,
  error_dir = error_dir,  
  output_dir = output_dir, 
  archive = F,
  n_jobs = nrow(agg_param_dt), 
  args = agg_param_path) 

agg_jid


## --------------------------------------------------
## 2b. Unscaled diagnostic plots
## --------------------------------------------------

plot_unscaled_jid <- SUBMIT_JOB(
  name = 'plot_unscaled',
  script = paste0(here,'/2b_plot_unscaled.R'), 
  queue = run_queue,
  memory = "15G", 
  threads = "1", 
  time = "4:00:00", 
  error_dir = error_dir,  
  output_dir = output_dir, 
  archive = F,
  hold = agg_jid,
  args = c(scaling_version, agg_dir, shea_path)) 

plot_unscaled_jid


## --------------------------------------------------
## 3. LAUNCH MAKE RATIOS (holds for state compile) 
## --------------------------------------------------

ratios_jid <- SUBMIT_ARRAY_JOB(
  name = 'make_ratios',
  script = paste0(here,'/3_make_ratios.R'), 
  queue = run_queue, 
  memory = "15G", 
  threads = "1", 
  time = "4:00:00", 
  throttle = '7',
  error_dir = error_dir,  
  output_dir = output_dir, 
  archive = F,
  n_jobs = nrow(ratio_param_dt), 
  hold = agg_jid,
  args = c(ratio_param_path,max_model_year)) 

ratios_jid

## --------------------------------------------------
## 3b. Ratio diagnostic plots
## --------------------------------------------------

plot_ratios_jid <- SUBMIT_JOB(
  name = 'plot_ratios',
  script = paste0(here,'/3b_plot_ratios.R'), 
  queue = run_queue, 
  memory = "15G", 
  threads = "1", 
  time = "4:00:00", 
  error_dir = error_dir,  
  output_dir = output_dir, 
  archive = F,
  hold = c(agg_jid, ratios_jid),
  args = c(scaling_version, ratio_path)) 

plot_ratios_jid

## --------------------------------------------------
## 4. LAUNCH APPLY RATIOS, SAVE FINAL RESULTS 
## --------------------------------------------------

if (run_only_national == T){
  
  nat_apply_jid <- SUBMIT_ARRAY_JOB(
    name = 'apply_ratios_nat', 
    script = paste0(here,'/4_apply_ratios.R'), 
    queue = run_queue, 
    memory = "40G", 
    threads = "1", 
    time = "12:00:00",
    archive = T,
    throttle = '200',
    error_dir = error_dir,  
    output_dir = output_dir, 
    n_jobs = nrow(apply_param_dt_national), 
    hold =  c(agg_jid, ratios_jid),
    args = c(apply_param_path_national, run_at_draw_level)) 
  
  nat_apply_jid
  
  plot_scaled_hold_jid <- nat_apply_jid
  
}

if (run_only_national == F){
    
  st_apply_mem <- "5G"
  cty_apply_mem <- "200G" 
  xtra_cty_apply_mem <- "250G" 

  
  st_apply_jid <- SUBMIT_ARRAY_JOB(
    name = 'apply_ratios_st', 
    script = paste0(here,'/4_apply_ratios.R'), 
    queue = run_queue, 
    memory = st_apply_mem, 
    threads = "1",
    time = "12:00:00", 
    archive = T,
    throttle = '800',
    error_dir = error_dir,  
    output_dir = output_dir, 
    n_jobs = nrow(apply_param_dt_state), 
    hold =  c(agg_jid, ratios_jid),
    args = c(apply_param_path_state, run_at_draw_level)) 
  
  st_apply_jid
  plot_scaled_hold_jid <- st_apply_jid
  

  cty_apply_jids <- c()
  
  for(m in unique(apply_param_dt_county$mem_set)){
    
    cty_apply_jid <- SUBMIT_ARRAY_JOB(
      name = 'apply_ratios_cty', 
      script = paste0(here,'/4_apply_ratios.R'), 
      queue = run_queue, 
      memory = m, 
      threads = "2", 
      time = "1:30:00", 
      archive = T,
      throttle = '500',
      error_dir = error_dir,  
      output_dir = output_dir, 
      n_jobs = nrow(apply_param_dt_county[mem_set == m]), 
      hold =  c( agg_jid, ratios_jid),
      args = c(apply_param_path_county, run_at_draw_level, m))
    
    job_note <- paste0("mem_set: ", m," --> ", cty_apply_jid, " --> ", nrow(apply_param_dt_county[mem_set == m]), ' tasks')
    print(job_note)
    cty_apply_jids <- c(cty_apply_jids, cty_apply_jid)
    
  }
  
  ## For counties that require additional memory, parallelize on cause (sources a different script, see below)
  cty_apply_jids_xtra <- c()
  
  for(m in unique(apply_param_dt_county_xtra$mem_set)){
    
    cty_apply_jid_xtra <- SUBMIT_ARRAY_JOB(
      name = 'apply_ratios_cty_xtra', 
      script = paste0(here,'/4_apply_ratios_cause.R'), 
      queue = run_queue, 
      memory = m, 
      threads = "1", 
      time = "1:00:00", 
      archive = T,
      throttle = '1000',
      error_dir = error_dir,  
      output_dir = output_dir, 
      n_jobs = nrow(apply_param_dt_county_xtra[mem_set == m]), 
      hold =  c( agg_jid, ratios_jid),
      args = c(apply_param_path_county_xtra, run_at_draw_level, m))
    
    job_note <- paste0("mem_set: ", m," --> ", cty_apply_jid_xtra, " --> ", nrow(apply_param_dt_county_xtra[mem_set == m]), ' tasks')
    print(job_note)
    cty_apply_jids_xtra <- c(cty_apply_jids_xtra, cty_apply_jid_xtra)
    
  }
  
  ## --------------------------------------------------
  ## 5. AGGREGATE STATE SCALED TO GET NATIONAL
  ## n_draws * n_toc - number of tasks
  ## --------------------------------------------------
  
  nat_agg_jid <- SUBMIT_ARRAY_JOB(
    name = 'agg_national', 
    script = paste0(here,'/5_agg_to_national.R'), 
    queue = run_queue, 
    memory = "20G", 
    threads = "1", 
    time = "4:00:00",
    archive = F,
    throttle = '70',
    error_dir = error_dir,  
    output_dir = output_dir, 
    n_jobs = nrow(apply_param_dt_national), 
    user_email = paste0(Sys.info()['user'], "@uw.edu"),
    hold =  c(agg_jid, ratios_jid, st_apply_jid),
    args = c(apply_param_path_national, run_at_draw_level))
  
  nat_agg_jid
  
  plot_scaled_hold_jid <- c(plot_scaled_hold_jid,nat_agg_jid)
}
## --------------------------------------------------
## 7. Scaled diagnostic plots
## --------------------------------------------------

plot_scaled_jid <- SUBMIT_JOB(
  name = 'plot_scaled',
  script = paste0(here,'/5b_plot_scaled.R'), 
  queue = run_queue,
  memory = "15G",
  threads = "1",
  time = "4:00:00",
  error_dir = error_dir,  
  output_dir = output_dir, 
  archive = F,
  hold = plot_scaled_hold_jid,
  args = c(scaling_version, agg_dir, shea_path, final_dir, run_only_national)) 

plot_scaled_jid


