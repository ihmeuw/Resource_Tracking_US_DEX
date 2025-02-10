##'----------------------------------------------------------------
##' Title: 02_model_c2p.R
##' Purpose: Estimate a smooth time trend for charges/payments using sources that contain both types of information
##' Author: Azalea Thomson
##'----------------------------------------------------------------


Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr)

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(str_remove(here, 'create_map'),'/payer_dictionary.R'))
source(paste0(here, '/c2p_f2t_functions.R'))
library(ggforce)
library(ggplot2)

library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
for (i in names(tasks)){
  assign(i, as.character(tasks[, get(i)]))
  message(i,':', tasks[, get(i)])
}

test <- F
use_weights <- args[2]


plot_outdir <- 'FILEPATH'


if(!(dir.exists(save_dir))){
  dir.create(save_dir, recursive = TRUE, showWarnings = FALSE)
}
if(!(dir.exists(plot_outdir))){
  dir.create(plot_outdir, recursive = TRUE, showWarnings = FALSE)
}


## Get all the causes
level_causelist <- fread(cause_map_path)[,.(acause,family,cause_name)]
cl <- level_causelist[!(acause %like% '_NEC') & !(acause %like% '_gc')]
print(f)
causes <- unique(level_causelist[family == f]$acause)
print(causes)

## Get the valid payer and source combinations
valid_payers_path <- paste0(map_dir, 'payer_sources.csv')
payer_source_table <- fread(valid_payers_path)[toc == care]
payer_source_table <- melt(payer_source_table,id.vars = c('pri_payer','payer','toc','include'))
payer_source_table <-payer_source_table[!(value=='')]
setnames(payer_source_table, 'variable','source')
payer_source_table$value <- NULL

pri_payers <- unique(payer_source_table$pri_payer)

output <- data.table()
for (p in pri_payers){
  print('Starting pri payer:')
  print(p)
  
  sources <- unique(payer_source_table[pri_payer == p]$source)
  
  print(sources)
  
  
  # ------------------- BRING IN DATA ----------------------#
  orig_dt <- data.table()
  for (s in sources){
    
    indir <- unique(fread(paste0(map_dir,"metadata.csv"))[source == s]$indir)
    pp_num <- payer_nums[payer ==p]$payer_num
    
    data <- arrow::open_dataset(paste0(indir,'toc=',care,'/'))
    
    filtered <- pull_c2p_data(data, s, pp_num, test, causes)
    message(nrow(filtered))  
    unique(filtered$year_id)
    unique(filtered$acause)
    
    ## Drop anything where tot_chg_amt is missing
    filtered <- filtered[!(is.na(tot_chg_amt))]
    
    ## Ensure that summed pay columns is equal to tot_pay
    pay_cols <- filtered%>% dplyr::select(contains('_pay_') & -contains('_facility') & -contains('_fac')  & -contains('tot')) %>% names()
    filtered[,tot_pay_amt_summed:= rowSums(.SD, na.rm = T), .SDcols = pay_cols]
    filtered[tot_pay_amt_summed > tot_pay_amt, tot_pay_amt:= tot_pay_amt_summed]
    filtered$tot_pay_amt_summed <- NULL
    
    ## If the paid amount is greater than the charge amount, set charge amount equal to total paid amount, otherwise leave it alone
    filtered[,tot_chg_amt:= ifelse(tot_chg_amt < tot_pay_amt, tot_pay_amt, tot_chg_amt )] 
    
    filtered$source <- s
    
    orig_dt <- rbind(orig_dt,filtered,fill=T)   
  }
  
  orig_dt$family <- f
  print('Got data')
  print(Sys.time() - t0)
  
  payers <- unique(payer_source_table[pri_payer == p]$payer)
  print('Payers:')
  print(payers)
  pay_cols <- paste0(payers,'_pay_amt')
  keep_cols <- c(pay_cols, 'tot_chg_amt','acause','year_id','family','source')
  model_dt <- orig_dt[,..keep_cols]
  model_dt <- melt(model_dt,measure.vars = pay_cols)
  model_dt$payer <- str_extract(model_dt$variable, "[^_]+")
  model_dt$variable <- NULL
  setnames(model_dt,'value','pay_amt')
  # ------------------- Get rid of the rows from payer-sources we're not using ----------------------#
  model_dt <- merge(model_dt, payer_source_table[pri_payer == p,.(payer,source,include)], by = c('payer','source'), all = T)
  model_dt <- model_dt[!is.na(include)]
  # ------------------- MAKE C2P ----------------------#
  model_dt[,c2p_ratio:= pay_amt/tot_chg_amt]
  
  head(model_dt)
  nrow(model_dt)
  full <- expand.grid(year_id=c(2000:2023),
                      acause = causes,
                      source = sources,
                      family = f,
                      payer=payers)
  model_dt <- merge(model_dt, full, by=c("year_id","family","payer","acause", "source"),all.y=T)
  
  nrow(model_dt)
  save_dt <- copy(model_dt)
  if (p == 'mdcr'){
    model_dt <- model_dt[!(source == 'MDCD' & year_id == 2000)]
    if (f == 'fam_well' | f == 'fam_neonatal'){
      model_dt <- model_dt[!(source == 'MDCD' & year_id %in% c(2010,2016))]
    }
  }

  ## Set NaNs to NA
  model_dt[is.nan(c2p_ratio),c2p_ratio:=NA]
  
  ## Creates KYTHERA source dummy 
  non_na_sources <- unique(model_dt[!is.na(c2p_ratio)]$source)
  if ('KYTHERA' %in% non_na_sources){
    dummy_sources <- 'KYTHERA'
    for (s in dummy_sources){
      model_dt[, paste0(s,'_source') := list(ifelse(source==s,1,0))]
    }
  }
    

  
  for (pay in payers){
    model_dt[, paste0(pay,'_payer') := list(ifelse(payer==pay,1,0))]
    model_dt[, paste0(pay,'_year') := list(get(paste0(pay,'_payer')) * year_id)]
  }
  

  # ------------------- RUN MODEL ----------------------#
  threshold <- 200
  c2p_thresh <- 200
  
  print(payers)
  
  payer_obs <- data.table(payer = unique(payers))
  
  regression_terms <- c("tot_chg_amt","c2p_ratio")
  if ('KYTHERA' %in% non_na_sources){
    regression_terms <- c(regression_terms,'KYTHERA_source')
  }
  
  
  for (pay in payers) {
    
    print(pay)
    payer_count <- nrow(model_dt[!(is.na(c2p_ratio)) & payer==pay])
    print (paste(pay, "-", payer_count))
    
    payer_obs <- payer_obs[payer == pay, n_obs :=payer_count]
    
    if (payer_count >= threshold){
      regression_terms <- c(regression_terms, paste0(pay,"_payer"), paste0(pay,"_year"))
    }
    if (payer_count <threshold & payer_count>1){
      # When the number of obs for the payer is < 200, the corresponding payer-year interaction term is dropped
      regression_terms <- c(regression_terms, paste0(pay,"_payer"))
    }
    
    if (payer_count == 1){ ## Payer is not a predictor if only 1 obs
      regression_terms <- regression_terms
    }
    
  }
  non_zero_payers <- unique(payer_obs[n_obs>0]$payer)  
  zero_obs_payers <- unique(payer_obs[n_obs==0]$payer)  
  
  
  if (length(non_zero_payers)>1){
    
    print("Running a linear regression")
    if (use_weights == T){
      lm.fit <- lm(c2p_ratio ~ . - tot_chg_amt-1,
                   weights = tot_chg_amt,
                   data = model_dt[,..regression_terms])
    }else{
      lm.fit <- lm(c2p_ratio ~ . - tot_chg_amt-1,
                   data = model_dt[,..regression_terms])
    }
    
    
  }else if (length(non_zero_payers)==1 & nrow(model_dt[!(is.na(c2p_ratio))]) >=threshold ){
    
    print("Running a linear regression")
    if ('KYTHERA_source' %in% regression_terms){ ##Just for MEPS!
      if (use_weights == T){
        lm.fit <- lm(c2p_ratio ~ year_id + KYTHERA_source,
                     weights = tot_chg_amt,
                     data = model_dt)
      }else{
        lm.fit <- lm(c2p_ratio ~ year_id + KYTHERA_source,
                     data = model_dt)
      }
    }else{
      
      if (use_weights == T){
        lm.fit <- lm(c2p_ratio ~ year_id,
                     weights = tot_chg_amt,
                     data = model_dt)
      }else{
        lm.fit <- lm(c2p_ratio ~ year_id,
                     data = model_dt)
      }
      
    }
    
  }else if (length(non_zero_payers)==1 & nrow(model_dt[!(is.na(c2p_ratio))]) <threshold ){
    print('Insufficient observations, taking the median')
  }else{
    print("No payer data")
  }
  
  
  
  
  if (length(non_zero_payers)>1){
    
    pred_data <- model_dt[payer %in% non_zero_payers][,KYTHERA_source := 0]
    
    c2p_fit <- predict(lm.fit,newdata=pred_data[,..regression_terms])
    
    preds <- cbind(pred_data,c2p_fit)
  }  
  
  if (length(non_zero_payers)==1){
    pred_data <- model_dt[payer %in% non_zero_payers][,KYTHERA_source := 0]
    
    if(nrow(model_dt[!(is.na(c2p_ratio))]) <threshold) {
      preds <- pred_data[, c2p_fit := median(c2p_ratio,na.rm = T)]
    }else{
      c2p_fit <- predict(lm.fit,newdata=pred_data)
      preds <- cbind(pred_data,c2p_fit)
    }
    
  } 
  
  
  if (length(zero_obs_payers)>0){
    ## If there are zero obs for a given payer, set the ratio to 0
    zero_obs_preds <- cbind(model_dt[payer %in% zero_obs_payers],data.table(c2p_fit = 0))
    if (length(non_zero_payers)>=1){
      full <- rbind(preds, zero_obs_preds, fill = T)
    }else{
      full <- as.data.table(zero_obs_preds)
    }
  }else{
    full <- preds
  }
  
  full$pri_payer <- p
  full <- merge(full,payer_obs, by = 'payer')
  output <- rbind(output,full, fill = T)
  
}

output$toc <- care
output <- output[c2p_fit<0, c2p_fit :=0]



ratios_long <- unique(output[,.(payer,year_id,family,c2p_fit,pri_payer,toc)])

## Check
ratios_long[,n:= .N, by = c('family','pri_payer','year_id','payer','toc')]

if (nrow(ratios_long[n>1])>0){
  print(head(ratios_long[n>1]))
  stop('You have greater than 1 c2p ratio per fam/pripayer/year/payer. Stopping')
}


payer_dict <- copy(payer_nums)
setnames(payer_dict,'payer','pri_payer')
ratios_long <- merge(ratios_long,payer_dict, by = 'pri_payer')
setnames(ratios_long,c('pri_payer','payer_num'),c('pri_payer_name','pri_payer'))


ratios <- data.table()
for (i in unique(ratios_long$pri_payer)){
  print(i)
  i_ratios <- dcast(ratios_long[pri_payer == i],...~payer, value.var = 'c2p_fit')
  ratios <- rbind(ratios, i_ratios, fill=T)
}

ratios[,total:=rowSums(.SD,na.rm = T), .SDcols=c('mdcr','mdcd','priv','oop')]

if (nrow(ratios[total>1]) ){
  n <- nrow(ratios[total>1])
  message('Warning, you have ',n,' total payments equal or exceeding 100% of charge')
  message('Rescaling to 100% of tot charge')
  c2p_cols <- c('mdcr','mdcd','priv','oop')
  ratios[total>1, (c2p_cols) := lapply(.SD, function(x) x / total ),.SDcols = c2p_cols]
  ratios[,total:=rowSums(.SD,na.rm = T), .SDcols=c('mdcr','mdcd','priv','oop')]
}


if(!interactive()){
  arrow::write_dataset(ratios[order(year_id)], path =save_dir,
                       basename_template = paste0("family_",f,"-{i}.parquet"),
                       partitioning = c( "toc","pri_payer"))
}


rescaled_output <- melt(ratios, measure.vars = c('mdcd','mdcr','oop','priv'), value.name = 'c2p_fit', variable.name = 'payer')
rescaled_output$pri_payer <- NULL
setnames(rescaled_output, 'pri_payer_name', 'pri_payer')
rescaled_output <- merge(rescaled_output, output[,.(pri_payer,year_id,family,toc,payer,tot_chg_amt,c2p_ratio,n_obs,source)],all.y=T, by = c('pri_payer','year_id','family','toc','payer'))
## --------------------
## Plot output
## --------------------
library(gridExtra)

if (use_weights == T){
  
  
  stats <-  rescaled_output[, as.list(c(c2p_fit_mean=mean(c2p_fit,na.rm=T),
                               c2p_ratio_orig=weighted.mean(c2p_ratio,tot_chg_amt,na.rm=T))), ## plot weighted means if using weights
                   by = list(year_id,family,payer,source,pri_payer,n_obs)]
  
  
}else{
  

  stats <-  rescaled_output[, as.list(c(c2p_fit_mean=mean(c2p_fit,na.rm=T),
                               c2p_ratio_orig=mean(c2p_ratio,na.rm=T))),
                   by = list(year_id,family,payer,source,pri_payer,n_obs)]
  
  
}
stats$toc <- care
arrow::write_dataset(stats, path =plot_outdir,
                     basename_template = paste0("family_",f,"-{i}.parquet"),
                     partitioning = c( "toc","pri_payer"))




print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------




