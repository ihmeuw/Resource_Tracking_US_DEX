# ----------------------------------------------------------------
#
#   LAUNCH Combine triple aim measures to calculate the (unadjusted) performance score
#     - baseline and sensitivity tests
#
#   Author: Haley Lescinsky
# ----------------------------------------------------------------

Sys.umask(mode = 002)
pacman::p_load(data.table, tidyverse, dplyr, ggplot2, arrow, gridExtra)
library(tidyverse)
library(data.table)
library(arrow)
code_dir <- 'FILEPATH'
library(lbd.loader, lib.loc = sprintf("FILPEATH/lbd.loader-%s", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

# --------------------------------------
# Set filepaths and versions
# ----------------------
results_set_note <- ""
parent_dir <- "FILPEATH"
date <- paste0(format(Sys.time(), "%d%b%Y"), ifelse(results_set_note=="","", paste0('_', results_set_note)))
work_dir <- paste0(parent_dir,"/results/",date, "/")
if(!dir.exists(work_dir)){dir.create(work_dir, recursive = T)}

#--------------------
# Create metadata table with parameters for baseline and each sensitivity run
#--------------------

key <- c('note','health_outcome_var', 'spend_var', 'pca_col_subset', 'composite_score_approach')

baseline <- c('baseline','le', 'spend_pc_stndz_adj', 'subset1', 'geomean')

sens1 <- c('sens_w_subset4','le', 'spend_pc_stndz_adj', 'subset4',  'geomean')
sens2 <- c('sens_w_compositeMean','le', 'spend_pc_stndz_adj', 'subset1',  'mean')
sens3 <- c('sens_w_unadjspend','le', 'spend_pc_stndz_unadj', 'subset1',  'geomean')
sens4 <- c('sens_w_prematuremort','premature_mort_rate_stndz', 'spend_pc_stndz_adj', 'subset1',  'geomean')
sens5 <- c('sens_w_subset5','le', 'spend_pc_stndz_adj', 'subset5',  'geomean')
sens6 <- c('sens_w_subset3','le', 'spend_pc_stndz_adj', 'subset3',  'geomean')

versions_to_run <- as.data.table(rbind(baseline, sens1, sens2, sens3, sens4, sens5, sens6))
colnames(versions_to_run) <- key

param_path <- paste0(work_dir, "/FILEPATH")
write.csv(versions_to_run, param_path, row.names = F)

#--------------------
# Specify subsets of patient exp variables for the PCA
#--------------------

baseline_subset1 <- c("H_STAR_RATING", "annual_wellness_visit", "access_composite1" ,"prevention_quality_indicator90", "safety_index_composite1")

sensitivity_subset3 <- c("H_STAR_RATING", "annual_wellness_visit", "access_composite1" ,"mdcrhosp_readmissions_rate", "safety_index_composite1")
sensitivity_subset4 <- c("H_STAR_RATING", "annual_wellness_visit", "uninsured_rate_stndz" ,"prevention_quality_indicator90", "psi_postoperative_sepsis")
sensitivity_subset5 <- c("H_STAR_RATING")

# Read in prepped data to make list of possible covariates
all_pat_exp <- fread(paste0(parent_dir, "/FILEPATH"))
poss_pca_cols <- setdiff(colnames(all_pat_exp), c('mcnty', 'state', 'year_id'))

all_used_cols <- unique(c(baseline_subset1, sensitivity_subset3, sensitivity_subset4, sensitivity_subset5))
if(length(setdiff(all_used_cols, poss_pca_cols)) > 1){
  print(setdiff(all_used_cols, poss_pca_cols))
  stop("Trying to use a col for PCA that isn't in the prepped data!")
}

pca_cols_combo_map <- data.table('col' = poss_pca_cols)
pca_cols_combo_map[, subset1 := ifelse(col %in% baseline_subset1, 1, 0)]
pca_cols_combo_map[, subset3 := ifelse(col %in% sensitivity_subset3, 1, 0)]
pca_cols_combo_map[, subset4 := ifelse(col %in% sensitivity_subset4, 1, 0)]
pca_cols_combo_map[, subset5 := ifelse(col %in% sensitivity_subset5, 1, 0)]

write.csv(pca_cols_combo_map, paste0(work_dir, "/FILEPATH"), row.names = F)

#--------------------
# Launch
#--------------------

jid <- SUBMIT_ARRAY_JOB(name = paste0('triple_aim_',date),
                   script = paste0(code_dir,"/FILEPATH"),
                   img = 'FILEPATH',
                   queue = "long.q",
                   memory = "10G",
                   threads = 1,
                   time = "30:00",
                   archive = F,
                   n_jobs = nrow(versions_to_run),
                   args = c(parent_dir, date, param_path))
print(paste0(date, " triple aim: ", jid))
