# ----------------------------------------------------------------
#
#   Combine triple aim measures to calculate the (unadjusted) performance score
#       - launched by launch_calc_unadj_score.R to run main + sens at once
#
#   Author: Haley Lescinsky
# ----------------------------------------------------------------

Sys.umask(mode = 002)
pacman::p_load(data.table, tidyverse, dplyr, ggplot2, arrow, gridExtra)
library(tidyverse)
library(data.table)
library(arrow)
code_dir <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
draws <- T

# SET VERSION SETTINGS
if(interactive()){
  
  health_outcome_var <- 'le'
  performance_score_approach <- 'geomean'
  spend_var <- 'spend_var'
  
  # baseline subset
  patient_exp_pca_cols <- c("H_STAR_RATING", 
                            "annual_wellness_visit", 
                            "access_composite1" ,
                            "prevention_quality_indicator90", 
                            "safety_index_composite1")

  #---------- Set up paths --------------
  parent_dir <- "FILEPATH"
  
  set_version <- 'xxx'
  version <- 'baseline'
  
  work_dir <- paste0(parent_dir,"/results/",set_version,"/sens/", version)

  if(!dir.exists(work_dir)){dir.create(work_dir, recursive = T)}
  
  
}else{
  
  
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  parent_dir <- args[1]
  set_version <- args[2]
  param_path <- args[3]
  
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  
  tasks <- fread(param_path)[task_id,]
  for (i in names(tasks)){
    assign(i, as.character(tasks[, get(i)]))
    message(i,':', tasks[, get(i)])
  }
  
  version <- note
  
  if(version == 'baseline'){
    work_dir <- paste0(parent_dir,"/results/",set_version,"/", version)
  }else{
    work_dir <- paste0(parent_dir,"/results/",set_version,"/sens/", version)
  }
    
  
   if(!dir.exists(work_dir)){dir.create(work_dir, recursive = T)}
  
  # set PCA cols
  patient_col_key <- fread(paste0(parent_dir, "/results/", set_version, "/FILEPATH"))
  patient_exp_pca_cols <- patient_col_key[get(pca_col_subset) == 1, col]
  
  print(patient_exp_pca_cols)
  
}

# SET GLOBAL SETTINGS

#--------------------
# Load data
#-------------------
all_pat_exp <- fread(paste0(parent_dir, "/FILEPATH"))
spend_mort_data <- fread(paste0(parent_dir, "/FILEPATH"))
if(draws == T){
  spend_mort_data <- fread(paste0(parent_dir, "/FILEPATH"))
}

#---------------------
#  PATIENT EXPERIENCE PCA
#---------------------

# Variables where a low value indicates better patient experience; these will be flipped so all variables are
#    in the same direction
flipped <- c('ed_preventable_util_rate_agestndz',
             'ip_preventable_util_rate_agestndz',
             'mdcrhosp_readmissions_rate',
             'readmission_rate_all',
             'prevention_quality_indicator90',
             'psi_postoperative_sepsis',
             'psi_accidental_puncture_or_laceration',
             'mdcr_preventable_hosp_stay_rate',
             'uninsured_rate_stndz')

for(f in flipped){
  
  all_pat_exp[, paste0(f) := - get(f) ]
  
}

# subset to just the variables this version is doing the pca over
all_pat_exp <- all_pat_exp[,c('mcnty','year_id', patient_exp_pca_cols), with = F]

# Confirm no data is missing
if(nrow(all_pat_exp[!complete.cases(all_pat_exp)]) > 0){
  stop()
}

# standardize all patient experience vars to have mean of 0, SD of 1
all_pat_exp[, (patient_exp_pca_cols) := lapply(.SD, function(x) (x-mean(x))/sd(x)), .SDcols = patient_exp_pca_cols ]
all_pat_exp_stndz <- all_pat_exp[, patient_exp_pca_cols, with = F]

pca_mod <- prcomp(all_pat_exp_stndz, center = TRUE, scale. = TRUE)
print(summary(pca_mod))
print(summary(pca_mod)$importance[2,1])

pca_values <- data.table('mcnty' = all_pat_exp$mcnty, 'year_id'=all_pat_exp$year_id, pca_mod$x)
rotation <- data.table('variable' = rownames(pca_mod$rotation), pca_mod$rotation)
rotation[, flipped := ifelse(variable %in% flipped, 1, 0)]
var_exp <- round(summary(pca_mod)$importance[2,1]*100, digits = 0)

pca_direction <- ifelse(sum(rotation$PC1 < 1) > (length(patient_exp_pca_cols)/2), 'negative', 'positive')
if(pca_direction == 'positive'){
  print("PCA Component #1 has a positive contribution from each variable!")
}


write.csv(pca_values[,.(mcnty, patient_exp_pca = PC1)], paste0(work_dir, "/FILEPATH"), row.names = F)

#---------------------
#  COMBINE ACROSS 3 AIMS
#---------------------

triple_aim <- merge(pca_values[,.(mcnty, year_id, patient_exp_pca = PC1)],
                    spend_mort_data[,.(mcnty, year_id, draw, spend = get(spend_var), health_outcomes = get(health_outcome_var))], by = c('mcnty','year_id'))

# Unadjusted performance score
#---------------------

# Again, need to get all 3 variables in the right direction (high = good)

# For spend, high = bad, so reverse it
triple_aim[, spend_corrected := -spend]

# For PCA, usually direction is negative --> high = bad, so reverse it
if(pca_direction == 'negative'){
  triple_aim[, patient_exp_pca_corrected := -patient_exp_pca]
}else{
  triple_aim[, patient_exp_pca_corrected := patient_exp_pca]
}

# For LE, high = good, but for mortality rate high = bad
if(health_outcome_var != 'le'){
  outcome_direction <- 'negative'
  triple_aim[, health_outcomes_corrected := -health_outcomes]
}else{
  outcome_direction <- 'positive'
  triple_aim[, health_outcomes_corrected := health_outcomes]
}


# Performance score (unadjusted)
#-----------------

# Scale to 0-1
triple_aim[, patient_exp_pca_scaled := (patient_exp_pca_corrected - min(patient_exp_pca_corrected)) / (max(patient_exp_pca_corrected) - min(patient_exp_pca_corrected)), by = c('draw')]
triple_aim[, spend_scaled := (spend_corrected - min(spend_corrected)) / (max(spend_corrected) - min(spend_corrected)), by = c('draw')]
triple_aim[, health_outcomes_scaled := (health_outcomes_corrected - min(health_outcomes_corrected)) / (max(health_outcomes_corrected) - min(health_outcomes_corrected)), by = c('draw')]


if(performance_score_approach == 'mean'){
  triple_aim[, performance_score := (patient_exp_pca_scaled + spend_scaled + health_outcomes_scaled)/3]
  triple_aim[, double_performance_score := (spend_scaled + health_outcomes_scaled)/2]
  
}else if(performance_score_approach == 'geomean'){
  # cubic root - geometric mean
  triple_aim[, performance_score := (patient_exp_pca_scaled*spend_scaled*health_outcomes_scaled)^(1/3)]
  triple_aim[, double_performance_score := sqrt(spend_scaled*health_outcomes_scaled)]
  
}else{
  print(performance_score_approach)
  stop('performance_score_approach should be one of geomean or mean')
}

# Take means across draws
triple_aim_means <- triple_aim[, .(patient_exp_pca = mean(patient_exp_pca),
                                   spend = mean(spend),
                                   health_outcomes = mean(health_outcomes),
                                   performance_score = mean(performance_score)), by = c('year_id', 'mcnty')]

# Save
if(draws == T){
  triple_aim <- triple_aim[,.(year_id, mcnty, draw, spend, health_outcomes, patient_exp_pca, patient_exp_pca_scaled, spend_scaled, health_outcomes_scaled, performance_score, double_performance_score, pca_direction = pca_direction, outcome_direction = outcome_direction)]
  
}else{
  triple_aim <- triple_aim[,.(year_id, mcnty, spend, health_outcomes, patient_exp_pca, patient_exp_pca_scaled, spend_scaled, health_outcomes_scaled, performance_score, double_performance_score, pca_direction = pca_direction, outcome_direction = outcome_direction)]
  
}
write.csv(triple_aim, paste0(work_dir, "/FILEPATH"), row.names = F)

dev.off()

