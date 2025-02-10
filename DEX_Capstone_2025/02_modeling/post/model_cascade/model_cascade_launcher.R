###########################################################
# ------------Launch Model Cascade------------------------#
# Authors: Meera Beauchamp, Azalea Thomson, Haley Lescinsky
#
# Purpose: Multiple models of different complexity may be run, (ie, simple, intermediate, complex)
# We want to take the output from the most preferred model that converged for each combination
# making a composite of results across different models, called a model set
# This script adds a column to the outputs to indicate which model its from and save all the preferred outputs to another folder.
#
# This script executes Step 1 and parallelizes the launch of step 2 by acause
#
# Step 1 - Create model set: Use job param files to identify which data to use from which model version
# Step 2 - Combine models: Read in the desired partitions of data from the desired model and move to a new
#          model set folder
# ----------------------------------------------------------------------#
###########################################################
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/cascade")
if(!exists(log_dir)){dir.create(log_dir)}
'%ni%' <- Negate('%in%')

#Get model output directory from the config
config <- get_config()
dir <- config$MODEL$output_dir
in_dir <- parsed_config(config, key = "MODEL", model_version = 'model_#')$output_dir

#UPDATE THIS LINE AS NEEDED: List models to use in cascade, can add any number of models
model_set<-c('1','2','3','3','5')  #NOTE: must be in order of most preferred model to least preferred

#UPDATE THIS LINE AS NEEDED: run id, this is used to find and move shiny data
run_id<-'XX'
include_race <- F
days_metrics <- F # Set to T if you want to model days_per_encounter and spend_per_day in addition to encounters_per_person and spend_per_encounter
if (days_metrics == T){
  days_metric_tocs <- c('RX') # List the TOCs you want to model days_per_encounter and spend_per_day
}

## If only making a mod set of certain geos or metrics
modset_metrics <- c("encounters_per_person", "spend_per_encounter")

modset_geos <- c('county','state','national')
if (include_race == T){
  modset_geos <- c('state','national')
}

# Note to add to model set description
note <- "DESCRIPTION"

##-------------------------------------------------------------------------------------------------
## STEP 1: Launch post_run_job_params for each mvid in the model set, necessary before you can run step 2
##-------------------------------------------------------------------------------------------------
for(mvid in model_set){
  
  jid <- SUBMIT_JOB(name=paste0('post_run_job_params_', mvid),
                   script = paste0(code_path, "/../post_run_job_params.R"), #define location of script that does the cascade
                   error_dir = log_dir, 
                   output_dir = log_dir, 
                   queue = "all.q",
                   memory = "35G",
                   threads = 4, 
                   time = "02:00:00",
                   archive = F,
                   args = c(mvid, include_race))
  
  print(jid)
 
}


message('Check to make sure your jobs completed -- takes ~ 10 min')

## -------------------------------------------------------------------------------------------------
## STEP 2: Create Model Set list
## -------------------------------------------------------------------------------------------------


###
#   Determine which model to use for the combinations that DID converge
###
'%ni%' <- Negate('%in%')
df_mod_set<-data.table()
for(i in 1:length(model_set)) {
  print(model_set[i])
  in_dir_params <- paste0(gsub('model_#', model_set[i], in_dir),'post_run_job_params.csv')
  job_params <- fread(in_dir_params)
  job_params<-setDT(job_params)[convergence == 'Yes' & output_exists]
  job_params$model_version_id<-model_set[i]
  job_params$rank<- i
  df_mod_set<-rbind(df_mod_set,job_params, fill = T)
}

##Select the outputs from the most desired models first
df_mod_set<- df_mod_set %>% 
  group_by(acause, toc, metric, pri_payer, payer, sex_id, geo) %>%
  filter(rank == min(rank)) %>%
  mutate(convergence = 'Yes') %>% as.data.table()

## Subset to just the metrics and geos you want in your modset

if (days_metrics == T){
  df_mod_set <- df_mod_set[geo %in% modset_geos]
  df_mod_set <- df_mod_set[(toc %in% days_metric_tocs & metric %in% c("spend_per_day", "days_per_encounter")) |
                             (metric %in% modset_metrics) ] # want to keep other metrics in addition to days metrics
}else{
  df_mod_set <- df_mod_set[metric %in% modset_metrics & geo %in% modset_geos]
}

##----------------------------------------------------------------------
## Make a summary of convergence notes for models that did NOT converge, to properly track convergence
## on the full model set this is a little weird because we only want one row per model combination, 
## and we use the convergence note from the simplest model result
##----------------------------------------------------------------------  
job_params_nonconv <-data.table()
for(i in 1:length(model_set)) {
  print(model_set[i])
  in_dir_params <- paste0(gsub('model_#', model_set[i], in_dir),'post_run_job_params.csv')
  job_params <- fread(in_dir_params)
  job_params<-setDT(job_params)[convergence=='No'][convergence_note != "no solution reached or not launched"]
  job_params$model_version_id<-model_set[i]
  job_params$rank<- i
  job_params_nonconv<-rbind(job_params_nonconv,job_params, fill = T)
}

## these are jobs that failed or weren't launched and in order to get all of them its simplest to pull them in this way
if (include_race == T){
  total_possible_params <- fread(gsub("#",run_id, "/FILEPATH/run_#/params_for_model_race.csv"))
}else{
  total_possible_params <- fread(gsub("#",run_id, "/FILEPATH/run_#/params_for_model.csv"))
}

# subset for now with just these two metrics
if (days_metrics == T){
  total_possible_params <- total_possible_params[(toc %in% days_metric_tocs & metric %in% c("spend_per_day", "days_per_encounter")) |
                                                 (metric %in% modset_metrics)]
}else{
  total_possible_params <- total_possible_params[metric %in% modset_metrics]
}
total_possible_params[, `:=` (convergence = 'No', 
                              rank = 0, 
                              convergence_note = 'no solution reached or not launched',
                              output_exists = FALSE, 
                              model_version_id = NA)]

job_params_no_results <- rbind(job_params_nonconv, total_possible_params, fill = T)

## Subset to just the metrics and geos you want in your modset
if (days_metrics == T){
  job_params_no_results <- job_params_no_results[geo %in% modset_geos]
  job_params_no_results <- job_params_no_results[(toc %in% days_metric_tocs & metric %in% c("spend_per_day", "days_per_encounter")) |
                             (metric %in% modset_metrics) ]
}else{
  job_params_no_results <- job_params_no_results[metric %in% modset_metrics & geo %in% modset_geos]
}



##----------------------------------------------------------------------
## Select the outputs - 1 per model combo
##----------------------------------------------------------------------  
job_params_no_results <- job_params_no_results %>% 
  group_by(acause, toc, metric, pri_payer, payer, sex_id, geo) %>%
  filter(rank == max(rank)) %>%
  as.data.table()


id_cols <- c('acause','toc', 'metric', 'pri_payer', 'payer', 'sex_id', 'geo')
non_id_cols <- names(job_params_no_results[, !..id_cols])
new_cols <- paste0(non_id_cols,'_1')
setnames(job_params_no_results, non_id_cols, new_cols)

all_nc_cols <- c(id_cols, new_cols)
all_c_cols <- c(id_cols, non_id_cols)

job_params_merged <-merge(job_params_no_results, df_mod_set, all.x=TRUE)

converged <- job_params_merged[!is.na(convergence),..all_c_cols]
non_converged <- job_params_merged[is.na(convergence),..all_nc_cols]
setnames(non_converged, new_cols, non_id_cols)

job_params_final <- rbind(converged, non_converged)

if(nrow(job_params_final) != nrow(job_params_no_results)){
  stop('Warning, you are missing some combinations')
}

##----------------------------------------------------------------------
## Add in model_set, write output directory
##----------------------------------------------------------------------  

## Match geo to mvid using the database
model_set_geo_map <- unlist(lapply(model_set, function(mvid){
  return(get_model_version(model_version = list(paste0(mvid)))$geographic_granularity)
}))

param_grid_tmp <- data.table(geographic_granularity = model_set_geo_map, 
                             model_version_id = model_set)
param_grid <- tidyr::crossing(param_grid_tmp, metric = "all", toc = "all", primary_payer = "all") %>% as.data.table()

# Make a new model set in the database
model_set_id <- register_model_set(description = paste0('Combining models from cascade for run ',run_id, note), model_set_components = param_grid)

model_set_id<-paste0('set', model_set_id)
message(model_set_id)
outdir <- parsed_config(config, key = "MODEL", model_version = model_set_id)$output_dir

#Create the new folder
dir.create(outdir)

#Save out params for imputation and model set to run cascade on (will need to filter to non-NAs on read in)
model_set_dir<-paste0(outdir,'post_run_job_params.csv') 

write.csv(job_params_final, model_set_dir)

#Save out acause to parallelize on
path_cascade_tasks<-paste0(outdir,'model_cascade_tasks.csv')
causes<-subset(df_mod_set, select = acause) %>% unique()
causes$task_id<-1:nrow(causes)
write.csv(causes, path_cascade_tasks)


#-------------------------------------------------------------------------------------------------
#STEP 3: Launch array job to run cascade and create model set
#-------------------------------------------------------------------------------------------------

# LAUNCH APPLY DATA AS ARRAY - This will launch model_cascade_worker.R 
jid <- SUBMIT_ARRAY_JOB(name='model_cascade',
                 script = paste0(code_path, "/model_cascade_worker.R"), 
                 error_dir = log_dir, 
                 output_dir = log_dir, 
                 queue = "all.q",
                 memory = "35G",
                 threads = 4, 
                 time = "02:00:00",
                 n_jobs = length(causes$acause),
                 archive = F,
                 args = c(path_cascade_tasks, #dataframe of acauses to parallelize on
                          model_set_dir, #path to dataframe of model sets
                          outdir,#path to of where to save outputs
                          model_set_id, #model set id of the model cascade
                          run_id), 
                 user_email = paste0(Sys.info()['user'], "@uw.edu"))

message(jid)

#-------------------------------------------------------------------------------------------------
#STEP 4: Create descriptive summary of the model set
#-------------------------------------------------------------------------------------------------

print_convergence <- function(dt, note){
  
  sum <- dt[,.N, by = "convergence"]
  sum[, prop := N/sum(N)]
  
  print(paste0(round(sum[convergence == "Yes", prop]*100, digits = 2), "% convergence rate in ", note))
}

summarize_model_versions <- function(dt){
  
  mvids <- unique(dt[!is.na(model_version_id)]$model_version_id)
  
  model_descriptions <- unlist(lapply(mvids, function(mvid){
    return(get_model_version(model_version = list(paste0(mvid)))$description)
  }))
  
  
  sum <- dt[convergence == 'Yes',.N, by = c("model_version_id", "model_type", "geo")]
  sum[, prop := round((N/sum(N))*100, digits = 2), by = c("geo")]
  
  print(sum[order(geo, model_type)][,.(geo, model_type, model_version_id, prop)])
  
  message("now by metric too")
  
  sum2 <- dt[convergence == 'Yes',.N, by = c("model_version_id", "model_type", "geo", "metric")]
  sum2[, prop := round((N/sum(N))*100, digits = 2), by = c("geo", 'metric')]
  
  print(sum2[order(geo, metric, model_type)][,.(geo, metric ,model_type, model_version_id, prop)])
  
  return(list(sum[order(geo, model_type)][,.(geo, model_type, model_version_id, prop)],
              sum2[order(geo, metric, model_type)][,.(geo, metric ,model_type, model_version_id, prop)]))
  
}

drop_notes <- c("Insufficient data for modeling", "Payer source restriction drops all data")

print_convergence(job_params_final[geo == "national"], note = "all national combos")
print_convergence(job_params_final[geo == "national" & !(convergence_note %in% drop_notes)], note = "national combos with sufficient data")

print_convergence(job_params_final[geo == "state"], note = "all state combos")
print_convergence(job_params_final[geo == "state" & !(convergence_note %in% drop_notes)], note = "state combos with sufficient data")

print_convergence(job_params_final[geo == "county"], note = "all county combos")
print_convergence(job_params_final[geo == "county" & !(convergence_note %in% drop_notes)], note = "county combos with sufficient data")

d <- summarize_model_versions(job_params_final)



