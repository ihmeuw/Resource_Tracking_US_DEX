##----------------------------------------------------------------
## Title: calc_medians.R
## Purpose: Calculate the medians of plausible values coming out of modeling for use in imputation. 
## If there are no plausible vals, or no data, just create a row of zeros
## Level 1 is at the geo/care/cause level.
## Level 2 is at the care/cause level.
## Level 3 is at the care level.
## Author: Azalea Thomson
##----------------------------------------------------------------

## --------------------
## 0. Setup 
## --------------------
Sys.umask(mode = 002)
t0 <- Sys.time()

'%ni%' <- Negate('%in%')

pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
library(lbd.loader,
        lib.loc = sprintf("FILEPATH",R.version$major))
suppressMessages(lbd.loader::load.containing.package())

options(arrow.skip_nul = TRUE)
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

source(paste0(here,'/function_pull_data.R'))


## --------------------
## 1. Parameters
## --------------------
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
lvl <- args[2]
geog <- tasks$geog
care <- tasks$toc
days_metrics <- tasks$days_metrics

med_outdir <- tasks$med_outdir
root_indir <- tasks$root_indir
draw_num <- tasks$draw
valid_cause_toc_path <- tasks$valid_cause_toc_path


if (draw_num == 0){
  draw_col <- paste0('mean') 
}else{
  draw_col <- paste0('draw_',draw_num) 
}

## --------------------
## 2. Constants
## --------------------
if (lvl ==1){
  outdir <- paste0(med_outdir, '1_age_yr_loc/modeled_',geog,'_',care,'_',draw_col, '.csv')
  message(paste0('Combo: geo_',geog,'_toc_',care,'_',draw_col))
}
if (lvl==2){
  outdir <- paste0(med_outdir, '2_geo_age_yr_loc/modeled_',care,'_',draw_col, '.csv')
  message(paste0('Combo: toc_',care,'_',draw_col))
}
if (lvl==3){
  outdir <- paste0(med_outdir, '3_cause_geo_age_yr_loc/modeled_',care, '_',draw_col, '.csv')
  message(paste0('Combo: toc_',care,'_',draw_col))
}



metrics <- c('spend_per_encounter','encounters_per_person')

if (days_metrics == T){
  metrics <- c(metrics, 'spend_per_day','days_per_encounter')
}

cl <- fread(valid_cause_toc_path)[include == 1 & gc_nec == 0 & toc == care,.(male,female,acause)] 
male_cl <- cl[male==1,.(male,acause)] 
female_cl <- cl[female==1,.(female,acause)]
setnames(male_cl, 'male','sex_id')
setnames(female_cl, 'female','sex_id')
female_cl[,sex_id:= 2]
expected_full <- rbind(male_cl,female_cl)



# get the modeled pri-payer/payer combos
all_params <- fread(paste0(root_indir, "/../post_run_job_params.csv"))

all_params <- unique(all_params[toc==care, .(pri_payer,payer,toc,metric)])

## ----------------------------------------------------
## Read in data and calculate fractions and medians
## ----------------------------------------------------

out_dt <- data.table()

for (met in metrics){
  print(met)
  
  valid_combos <- all_params[metric==met]
  
  for (i in 1:nrow(valid_combos)){
    pay_combo <- valid_combos[i]
    print(pay_combo)
    
    pp <- pay_combo$pri_payer
    p <- pay_combo$payer

    expected <- copy(expected_full)
    
    # Because of partioning, we can specify the lvl1 path but for lvl2 and lvl3 we have to just filter in the pull_data function
    if (lvl ==1){
      indir <- paste0(root_indir,'geo=',geog,'/toc=',care,'/metric=',met,'/pri_payer=',pp,'/')
    }else{
      indir <- root_indir
    }
          
    if(dir.exists(indir)){
      
      message(paste0('Data exists for ', indir))

      dt <- arrow::open_dataset(indir)
      
      summary_data <- pull_data(level=lvl, dt, care, met, pp, p, draw_col)

      
    }else{
      message(paste0('No modeled input for this geo, care, metric, pri_pay combo', indir))
      summary_data <- cbind(copy(expected_full),min=NA,max=NA,med=NA,total_n=NA,high=NA)
    }

    summary_data <- merge(summary_data,expected, all=T)
    
    if (lvl ==1){
      summary_data[,geo:=geog]
    }
    summary_stats <- summary_data[,`:=`(toc = care,
                                        metric = met,
                                        pri_payer = pp,
                                        payer = p,
                                        origin = 'modeled')]
    
  

    out_dt <- rbind(out_dt,summary_stats, fill = T)
    
  }
  
}

  
message('Got summaries')


if (lvl == 2 & nrow(out_dt[is.na(total_n)]) >0 ){
  out_dt <- out_dt[!(is.na(total_n))]
}
if (lvl == 3 & nrow(out_dt[is.na(total_n)]) >0 ){
  print(unique(out_dt[is.na(total_n), .(toc, metric, pri_payer, payer)]))
  stop('An entire toc combination is missing at all geos and causes-- that is not expected')
}
if (lvl == 1 & nrow(out_dt[is.na(total_n)]) >0 ){
  message('Dropping geo-toc-cause combination(s) that do not have modeled outputs') 
  out_dt <- out_dt[!(is.na(total_n))]
}

## Save out medians
fwrite(out_dt,outdir)


message('Done!')




