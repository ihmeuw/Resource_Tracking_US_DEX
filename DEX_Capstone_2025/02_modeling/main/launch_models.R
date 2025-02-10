##' ***************************************************************************
##' Title: launch_models.R 
##' Purpose: Array job launcher for R worker script (which references a TMB template) for different variations of the SAE model
##' Authors: Azalea Thomson & Haley Lescinsky
##' 
##' ***************************************************************************

## -------------------------------------
## SETUP
## -------------------------------------
username <- Sys.getenv('USER')
user <- username 
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile()) 
setwd(code_path)
Sys.setenv("RETICULATE_PYTHON" = '/FILEPATH/python')
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
raw_config <- get_config()
source(paste0(code_path, '/launcher_functions.R'))
source(paste0(code_path, '/fit_models/functions.R'))

## -------------------------------------
## GLOBAL OPTIONS
## -------------------------------------
run_q <- "long.q"

## Update these 
run_id <- 'XX'
TEST <- F
delete_outputs <- T
geo_list <- c("all") # options: 'national','state','county', 'all'
interaction <- c('intercept') # options: intercept, no_loc (national), complex (state,county), intermediate (state,county), simple (state,county)

if (interaction %in% c('no_loc','complex')){
  params_from_cascade <- F # Never T for most complex model (complex for state and county OR no_loc for national); If T, will populate the params with the combinations that did not converge from a previously run (usually more complex) model_version 
}else{
  params_from_cascade <- T # Never T for most complex model (complex for state and county OR no_loc for national); If T, will populate the params with the combinations that did not converge from a previously run (usually more complex) model_version 
}
if (params_from_cascade == T){
  readline(message("Have you updated the prev model version for which to pull the non converged params? \n type 'yes' to proceed"))
  if (interaction == 'intercept'){
    prev_mv_list <- c('XX','XX')
    prev_mv <- NA
  }else{
    prev_mv_list <- NA
    prev_mv <- 'XX' # Must supply previous model version
  }
}else{
  prev_mv <- NA
  prev_mv_list <- NA
}

days_metrics <- F # Set to T if you want to model days_per_encounter and spend_per_day in addition to encounters_per_person and spend_per_encounter
if (days_metrics == T){
  days_metric_tocs <- c('RX') # List the TOCs you want to model days_per_encounter and spend_per_day
}else{
  days_metrics_tocs <- c()
}
max_model_year <- 2019 #latest desired year of model prediction
extrapolate_ages <- T #if you want to include all ages present for both metrics in the predictions (just back and forward casts the value)
include_race <- F #if you want to include race/ethnicity in the model
if (include_race==T & extrapolate_ages == T){
  full_age_span <- T # this ensures that all ages will be modeled because race models will always be scaled to all population estimates
}else{
  full_age_span <- F
}

override_mvid <- F # Either F or <insert mv_id #>. Only set to mv_id # if you want to relaunch that model version. 
retry <- F # Boolean - if you are launching a subset of jobs that failed or were out of time/memory -- must supply previous job ids. override_mvid must also be specified.
if (retry == T){
  params_from_cascade <- F
  delete_outputs <- F
  relaunch_jids <- c('XX')
  description <- NULL
}else{
  relaunch_jids <- NULL
}


## Static arguments that will very rarely change
use_profiled_params <- F
sim_data <- F
num_draws <- 100
drop_outliers <- T
priors <- T
set_se_floor <- T
normalize_mad <- F ## this should not be changed to T! vetting shows normalizing hurts our models

metric_list <- c("encounters_per_person", "spend_per_encounter")
if (days_metrics == T){
  metric_list <- c("encounters_per_person", "spend_per_encounter", "spend_per_day", "days_per_encounter")
}
description <- paste0(geo_list, " geo,", interaction, " model") # for DB
print(description)

##-------------------------------------
## CHECKS
##-------------------------------------  

if (retry == T & delete_outputs == T){
  stop('You are attempting to launch a subset and delete outputs. Are you sure you want to do this?')
}
if (retry == T & override_mvid == F){
  stop('You are attempting to launch a subset and create a new mvid. Are you sure you want to do this?')
}
if (retry == T & length(relaunch_jids) == 0){
  stop('You are attempting to launch a subset without supplying previous job ids')
}

if (params_from_cascade == T & length(prev_mv) == 0 & interaction != 'intercept'){
  stop('You are attempting to launch the non-converged params from a previous model version without supplying the previous model version id')
}
if (params_from_cascade == T & length(prev_mv_list) == 0 & interaction == 'intercept'){
  stop('You are attempting to launch the non-converged params from a previous model version without supplying the previous model versions')
}
if (retry == T & params_from_cascade == T){
  stop('You are attempting to launch a subset AND the non-converged params, choose 1!')
}

if (params_from_cascade == T & interaction %in% c('complex', 'no_loc') ){
  check <- readline(message('Are you sure you want to pull params from prev model version ', prev_mv, " for this complex/national model? \n type 'yes' to proceed"))
  if (check != 'yes'){
    stop('set params_from_cascade to F')
  }
}

## -------------------------------------
## MODEL SELECTION
## -------------------------------------

dict <- get_match_script_dict(include_race = include_race)
if (interaction == 'intercept'){
  worker_version <- 'mod0'
}else{
  worker_version <- dict[mod_type == interaction & geo == geo_list]$match_script  
}

## choose granularity
GEO <- as.list(geo_list) ## list("all") OR 1+ of list("national", "state", "county")
METRIC <- as.list(metric_list) ## list("all") OR 1+ of list("days_per_encounter", "encounters_per_person", "spend_per_encounter", "spend_per_day")
TOC <- list('all')# list("AM", "DV", "ED", "HH", "IP", "NF")#list("all") ## list("all") OR 1+ of list("AM", "DV", "ED", "HH", "IP", "NF", "RX")
PRIMARY <- list("all") ## should be "all" if not a test run

# longer note for version readme
note <- c(description)

message(paste0(worker_version, "     ", GEO, "-", ifelse(include_race, "include_race", "no_race")))

## -------------------------------------
## REGISTER MODEL
## -------------------------------------

if(override_mvid!=F){
  model_version <- override_mvid
}else{
  
  check <- readline(message('Are you sure you want to make a new model version: \n', worker_version, "?  \n type 'yes' to proceed"))
  if (check == 'yes'){
    print("OK - making new version")
    
    # register model
    model_version <- register_model_version(
      phase_run_id = run_id,
      geographic_granularity = GEO,
      metric = METRIC,
      toc = TOC,
      primary_payer = PRIMARY,
      description = description,
      status = if(TEST) 'Test' else 'Active',
      model_name = list("STA"),
      worker_version = gsub("mod", "", worker_version),
      tmb_cont_template_v_num = gsub("mod", "", worker_version),
      tmb_enctr_v_num = gsub("mod", "", worker_version)
    )
    
  }else{
    stop("Use override_mvid to continue without registering a model version")
  }
  
}

message(paste0("Saving to model_version ", model_version))



## -------------------------------------
## GET CONFIG AND SET PATHS
## -------------------------------------

model_config <- parsed_config(raw_config, key = "MODEL", run_id = as.character(run_id), model_version = as.character(model_version))

root_indir <- model_config$input_dir
outdir <- model_config$output_dir

if (retry == F){
  job_param_path <- paste0(outdir, "job_params.csv")
}else{
  job_param_path <- paste0(outdir, "job_params_retry_",paste0(relaunch_jids,collapse = '_'),".csv")
}

launched_job_dir <- paste0(outdir, "launched_job_tasks/")

if (include_race == T){
  params_for_model_path <- paste0(root_indir,"/params_for_model_race.csv")
}else{
  params_for_model_path <- paste0(root_indir,"/params_for_model.csv")
}


params_with_mem_path <- paste0(root_indir,"/params_with_mem.csv")

params_profiled_path <- paste0(root_indir,"/params_profiled.csv")

## -------------------------------------
## MAKE DIRS 
## -------------------------------------

if(dir.exists(outdir) & delete_outputs){
  message("deleting outputs! you have 5 seconds to cancel ...")
  print(outdir)
  Sys.sleep(5)
  message("deleting outdir!")
  system(paste0("rm -r ", outdir))
}

if(T){
if(!dir.exists(outdir)){dir.create(outdir)}
if(!dir.exists(paste0(outdir,"/convergence/"))){dir.create(paste0(outdir, "/convergence/"))}
if(!dir.exists(paste0(outdir,"/diagnostics/"))){dir.create(paste0(outdir, "/diagnostics/"))}
if(!dir.exists(paste0(outdir,"/draws/"))){dir.create(paste0(outdir, "/draws/"))}
if(!dir.exists(paste0(outdir,"/launched_job_tasks/"))){dir.create(paste0(outdir, "/launched_job_tasks/"))}
}

if(!file.exists(params_with_mem_path)){stop('No reference mem_sets exist yet. Pull them in from previous run_v or create new ones!')}
## -------------------------------------
## SET PARAMS
## -------------------------------------

if(retry==FALSE){
  
  
  ## If using profiled params, bring those in, otherwise bring in non profiled params
  if (use_profiled_params == T){
    message('Using profiled params')
    param_map <- fread(params_profiled_path)
  }else{
    message('Not using profiled params')
    param_map <- fread(params_for_model_path)
  }
  
  if (params_from_cascade == T){
    
    if (interaction == 'intercept'){
      message('Using tasks that did not run or converge from previous model version: ', 
              paste0(prev_mv_list,collapse=','), ' as params -- this takes a minute')
      param_map <- pull_intercept_params(prev_mv_list)
    }else{
      message('Using tasks that did not run or converge from previous model version: ', prev_mv, ' as params')
      param_map <- pull_nonconv_params(prev_mv, user)
    }

  }
  
  ## Merge on mem set reference
  params_with_mem <- fread(params_with_mem_path)
  
  # Defaults if missing mem_set
  params_with_mem[mem_set == "", mem_set := NA]
  params_with_mem[is.na(mem_set) & geo == "national", mem_set := '5G']
  params_with_mem[is.na(mem_set) & geo == "state", mem_set := "7G"]
  params_with_mem[is.na(mem_set) & geo == "county", mem_set := "50G"]
  
  param_map <- merge(param_map, params_with_mem, by = c("geo", "acause", "toc", "metric", "pri_payer", "sex_id", "payer"), all.x = T)
  
  if (param_map[is.na(mem_set),.N]>0){
    message('Warning you have NA memset combinations -- check your save_run_memset script --setting NAs to 5G')
    stop()
    param_map[is.na(mem_set),mem_set:='5G']
  }
  if(worker_version == "mod0"){
    param_map <- param_map[mem_set ==  "7G",  mem_set := "5G"]
    param_map <- param_map[mem_set %in% c('180G', '90G'), mem_set := "50G"]
  }
  
}else{ ## If you're relaunching only a subset do not need to merge on mem
  
  
  param_map <- data.table()
  for (jid in relaunch_jids){
    relaunch_params <- pull_relaunch_params(jid, model_version, user) ##Note this should be your own username
    param_map <- rbind(param_map, relaunch_params)
  }
  update <- readline(message("Do you want to update your mem_sets or runtime?\n type 'yes' to proceed, otherwise will continue with no updates"))
  if (update == 'yes'){
    stop()
  }
  
  
}

## Subset param map to param_list
if (geo_list == 'all'){
  geo_list <- c('national','state','county')
}
if( 'all' %in% unlist(TOC)){
  toc_list <- c("AM", "ED", "HH", "IP", "NF", "RX", "DV")
}else{
  toc_list <- TOC
}
if (days_metrics==T){
  param_list <- param_map[(toc %in% days_metric_tocs & metric %in% c('days_per_encounter','spend_per_day')) | metric %in% c('spend_per_encounter','encounters_per_person')]
  param_list <- param_list[geo%in% geo_list & toc %in% toc_list]
}else{
  param_list <- param_map[metric %in% metric_list & geo %in% geo_list & toc %in% toc_list]
}


if(T){ #run code as a block
  # Set hyperparameters
  param_list$rho_t_alpha <- 80
  param_list$rho_t_beta <- 2
  param_list$rho_a_alpha<- 50 
  param_list$rho_a_beta<- 2
  param_list$rho_j_alpha<- 10
  param_list$rho_j_beta<- 2
  param_list$sigma_alpha <- 1
  param_list$sigma_beta <- 4
  param_list[metric %in% c('spend_per_encounter','spend_per_day'), mad_cutoff:= 4]
  param_list[metric %in% c('encounters_per_person','days_per_encounter'), mad_cutoff:= 5]
  param_list[,num_draws:=num_draws]
  param_list[,drop_outliers:= drop_outliers]
  param_list[,use_profiled_params:= use_profiled_params]
  param_list[,priors:=priors]
  param_list[,set_se_floor:=set_se_floor]
  param_list[,normalize_mad:=normalize_mad]
  param_list[,sim_data:= as.logical(sim_data)]
  param_list[,model_type := interaction]
  param_list[,include_race := include_race]
  param_list[,extrapolate_ages := extrapolate_ages]
  param_list[,full_age_span:=full_age_span]
  param_list[,max_model_year:=max_model_year]
  if (days_metrics == T){
    param_list[,days_metrics:=ifelse(toc %in% days_metric_tocs & metric %in% c('days_per_encounter','spend_per_day'), T, F)] 
  }else{
    param_list[,days_metrics:=F] 
  }

}


if(TEST == T){
  test_causes <- c("cvd_ihd","mental_pdd","neuro_dementia","exp_well_person","diabetes_typ2","msk_pain_lowback")
  print(paste0("TEST model run, just will launch models for these causes: ", paste0(test_causes, collapse = ",")))
  param_list <- param_list[acause %in% test_causes]
}

param_list[, task_id := 1:.N, by = mem_set]

fwrite(param_list, job_param_path)


## -------------------------------------
## LAUNCH THE ARRAY JOB 
## -------------------------------------

log_dir <- paste0("/FILEPATH/", Sys.getenv('USER'),"/model/",worker_version, "_", model_version,"/")

if (retry == T){
  log_dir <- paste0(gsub("/$", "", log_dir), "_retry_",paste0(relaunch_jids,collapse = '_'),"/")
}

if(dir.exists(log_dir)){
  system(paste0("rm -r ", log_dir))
}
dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)


job_notes <- c(paste0("MVID ", model_version, " (", GEO,"-", interaction,":", worker_version, "-", ifelse(include_race, "include_race", "no_race")))
print(job_notes)


fit_script <- 'mod_all'

#### SET UP JID, one script for each template - only need to run this if templates have been edited
if (include_race){
  templates <- list.files(paste0(code_path, "/fit_models/race_templates/"), pattern = '.cpp')
}else{
  templates <- list.files(paste0(code_path, "/fit_models/templates/"), pattern = '.cpp')
}
templates_to_compile <- data.table('template' = templates)

templates_to_compile[, task_id := 1:.N]
compile_param_path <- paste0(outdir, "compile_tmb_params.csv")
write.csv(templates_to_compile, compile_param_path, row.names = F)

setup_jid <- SUBMIT_ARRAY_JOB(paste0("setup", "_sae_", "_compile"),
                              script = paste0(code_path, "/fit_models/compile_templates.R"),
                              img = '/FILEPATH',
                              queue = "all.q",
                              output_dir = log_dir,
                              error_dir = log_dir,
                              memory = "4G",
                              threads = 1,
                              time = "00:20:00",
                              n_jobs = nrow(templates_to_compile), # one job for each template
                              throttle = nrow(templates_to_compile),
                              archive = F,
                              args = c(compile_param_path, include_race))
setup_jid


JIDS <- data.table()
for(M in unique(param_list$mem_set)){
  
  if ( as.numeric(gsub('G','', M)) <7){ #national
    throt <- 2000
    runtime <- "00:10:00"
  }else if (as.numeric(gsub('G','', M)) >=7 & as.numeric(gsub('G','', M)) <50){ #state
    throt <- 2000
    runtime <- "00:30:00"
  }else{
    throt <- 700
    runtime <- "4:00:00"
  }
  
  jid <- SUBMIT_ARRAY_JOB(paste0("mem", M, "_sae_", worker_version),
                          script = paste0(code_path, "/fit_models/fit_", fit_script, ".R"),
                          queue = run_q,
                          output_dir = log_dir,
                          error_dir = log_dir,
                          memory = M, 
                          threads = 1, 
                          time = runtime,
                          n_jobs = nrow(param_list[mem_set == M]),
                          throttle = throt,
                          archive = F,
                          hold = setup_jid,
                          user_email = paste0(Sys.info()['user'], "@uw.edu"),
                          args = c(outdir, job_param_path, run_id, M))
  
  job_tasks <- param_list[mem_set == M]
  
  fwrite(job_tasks, paste0(launched_job_dir, jid,".csv"))
  job_note <- paste0("mem_set: ", M, " --> ", jid, " --> ", nrow(param_list[mem_set == M]), ' tasks, mv ', model_version)
  job_notes <- c(job_notes, job_note)
  print(job_note)
  
  JIDS <- rbind(JIDS, data.table(mem_set = M, JID = jid))
}

fileConn<-file(paste0(outdir, "/job_ids.txt"))
writeLines(job_notes, fileConn)
close(fileConn)

