##----------------------------------------------------------------
## Title: launch_calc_medians_array.R
## Purpose: Calculate the medians across different levels of combinations, coming out of modeling. Used as input for imputation.
##             lvl1: geo/cause/toc/pripay/pay/age/sex/year
##             lvl2: cause/toc/pripay/pay/age/sex/year
##             lvl3: toc/pripay/pay/age/sex/year
## Author: Azalea Thomson
##----------------------------------------------------------------


## --------------------
##  SUPPLY MODEL VERSION 
## --------------------


version <- 'setXX' 
all_draws <- T
if (all_draws == T){
  n_draws <- 100
}
tocs <- c('IP','AM','ED','HH','NF','DV','RX')
days_metrics <- F # If this is set to T, will also include the metrics for the types of care specified in days_metrics_tocs

if (days_metrics == T){
  days_metrics_tocs <- 'RX'
}

if (all_draws == F){
  draws <- 0
}else{
  draws <- c(0:n_draws)
}

## --------------------
## Setup
## --------------------
Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)
username <- Sys.getenv('USER')


Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(configr, lib.loc = "FILEPATH")
library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
'%ni%' <- Negate('%in%')

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

setwd(here)
config <- get_config()

valid_cause_toc_path <- parsed_config(config, key = "METADATA")$toc_cause_restrictions_path


root_dir <- "FILEPATH"
model_indir <- paste0(root_dir,"draws/")


med_outdir <- paste0(root_dir,"medians/")
outdir_1 <- paste0(med_outdir, "1_age_yr_loc/")
outdir_2 <- paste0(med_outdir, "2_geo_age_yr_loc/")
outdir_3 <- paste0(med_outdir, "3_cause_geo_age_yr_loc/")


print(med_outdir)

dir.create(paste0(outdir_1), recursive = T)
dir.create(paste0(outdir_2), recursive = T)
dir.create(paste0(outdir_3), recursive = T)

## --------------------
## Make params
## --------------------
params <- expand.grid(root_indir = c(model_indir),
                      toc = tocs,
                      draw = draws,
                      valid_cause_toc_path = valid_cause_toc_path,
                      med_outdir = med_outdir) %>% as.data.table()

if (days_metrics == T){
  params[, days_metrics:=ifelse(toc %in% days_metrics_tocs, T, F)]
}else{
  params$days_metrics <- F
}

params_path_2_3 <- paste0(med_outdir, '/params_2_3.csv')
fwrite(params,params_path_2_3)


params_1 <- crossing(geog = c('county','state'),
                        params) %>% as.data.table()

params_path_1 <- paste0(med_outdir, '/params_1.csv')

fwrite(params_1,params_path_1)

## ------------------------------------------------
## Clear error/output log dirs/make them
## ------------------------------------------------
error_dir <- 'FILEPATH'
output_dir <- 'FILEPATH'

## --------------------
##        01 
## --------------------

med1_jid <- SUBMIT_ARRAY_JOB(
  name = '1_calc_meds', 
  script = paste0(here,'/calc_medians.R'), 
  queue = "long.q", 
  memory = "20G", 
  threads = "4", 
  time = "8:00:00", 
  throttle = '1005',
  archive = F,
  n_jobs = nrow(params_1), 
  error_dir = error_dir,
  output_dir = output_dir,
  args = c(params_path_1, lvl = 1)) 

## --------------------
##         02 
## --------------------



med2_jid <- SUBMIT_ARRAY_JOB(
  name = '2_calc_meds',
  script = paste0(here,'/calc_medians.R'), 
  queue = "long.q", 
  memory = "60G",
  threads = "4", 
  time = "8:00:00",
  throttle = '1005',
  archive = F,
  n_jobs = nrow(params),
  error_dir = error_dir,
  output_dir = output_dir,
  args = c(params_path_2_3, lvl = 2) )


## --------------------
##        03
## --------------------

med3_jid <- SUBMIT_ARRAY_JOB(
  name = '3_calc_meds',
  script = paste0(here,'/calc_medians.R'), 
  queue = "long.q", 
  memory = "80G", 
  threads = "2", 
  time = "8:00:00", 
  throttle = '1005',
  archive = F,
  n_jobs = nrow(params),
  error_dir = error_dir,
  output_dir = output_dir,
  args = c(params_path_2_3, lvl = 3) )



