##' ***************************************************************************
##' Title: 01_model_f2t.R
##' Purpose: Estimate a smoothed time trend for total charges/facility charges 
##' Note that we use total paid and facility paid (not charge) in MSCAN because MSCAN doesn't have charge amounts.
##' Author: Azalea Thomson
##' ***************************************************************************
Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr)

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(str_remove(here, 'create_map'),'/payer_dictionary.R'))
source(paste0(here, '/c2p_f2t_functions.R'))
library(ggforce)
library(ggplot2)
library(MASS)

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


plot_data_outdir <- paste0(map_dir,'/diagnostics/weights_',use_weights,'/modeled_f2t/data/')
plot_outdir <- paste0(map_dir,'/diagnostics/weights_',use_weights,'/modeled_f2t/plots/')

if(!(dir.exists(plot_outdir))){
  dir.create(plot_outdir, recursive = TRUE, showWarnings = FALSE)
  dir.create(plot_data_outdir, recursive = TRUE, showWarnings = FALSE)
}
bad_data_outdir <- paste0(map_dir,'/diagnostics/bad_data/')
if(!(dir.exists(bad_data_outdir))){
  dir.create(bad_data_outdir, recursive = TRUE, showWarnings = FALSE)
}

## Get all the causes
level_causelist <- fread(cause_map_path)[,.(acause,family,cause_name)]
fam_names <- unique(fread(cause_map_path)[,.(family,family_name)])
cl <- level_causelist[!(acause %like% '_NEC') & !(acause %like% '_gc')]
print(f)
causes <- unique(level_causelist[family == f]$acause)
print(causes)


if ( (f == 'fam_neonatal' | f == 'fam_gyne' | f == 'fam_maternal' |f == 'fam_std') & care == 'NF'){
  use_weights <- T
}
if ( f == 'fam_std' & care == 'HH'){
  use_weights <- T
}

## Get the valid payer and source combinations
valid_payers_path <- paste0(map_dir, 'payer_sources.csv')
payer_source_table <- fread(valid_payers_path)[toc == care]
payer_source_table <- melt(payer_source_table,id.vars = c('pri_payer','payer','toc','include'))
payer_source_table <-payer_source_table[!(value=='')]
setnames(payer_source_table, 'variable','source')
payer_source_table$value <- NULL

pri_payers <- unique(payer_source_table$pri_payer)


full_model_dt<- data.table()

sources <- unique(payer_source_table$source)
print(sources)


# ------------------- BRING IN DATA ----------------------#
orig_dt <- data.table()
for (s in sources){
  source_pps <- unique(payer_source_table[source == s]$pri_payer)
  print(source_pps)
  indir <- unique(fread(paste0(map_dir,"metadata.csv"))[source == s]$indir)
  # pp_num <- payer_nums[payer ==p]$payer_num
  pp_num <- payer_nums[payer %in% source_pps]$payer_num
  
  if (s == 'MEPS' & care == 'AM'){
    schema <- update_nulls_schema(paste0(indir,'toc=',care,'/'))
    schema[["tot_fac_pay_amt"]] <- arrow::float()
    schema[["tot_fac_chg_amt"]] <- arrow::float()
    schema[["mdcr_fac_pay_amt"]] <- arrow::float()
    schema[["mdcd_fac_pay_amt"]] <- arrow::float()
    schema[["priv_fac_pay_amt"]] <- arrow::float()
    schema[["oop_fac_pay_amt"]] <- arrow::float()

    data <- arrow::open_dataset(paste0(indir,'toc=',care,'/'), schema = schema)
  }else{
    data <- arrow::open_dataset(paste0(indir,'toc=',care,'/'))
  }
  
  filtered <- pull_f2t_data(data, s, pp_num, test, causes)

  ## MSCAN DOESN't HAVE CHARGE BUT SITLL WANT TO USE THE FAC/TOT INFO
  if (s=='MSCAN'){
    filtered[,`:=`(tot_chg_amt=NULL, tot_chg_amt_facility=NULL)]
    pay_cols <- filtered%>% dplyr::select(contains('_pay_') & -contains('_facility')  & -contains('tot')) %>% names()
    fac_cols <- filtered%>% dplyr::select(contains('_pay_') & contains('_facility')  & -contains('tot')) %>% names()
    filtered[,tot_pay_amt_summed:= rowSums(.SD, na.rm = T), .SDcols = pay_cols]
    filtered[tot_pay_amt_summed > tot_pay_amt, tot_pay_amt:= tot_pay_amt_summed]
    filtered$tot_pay_amt_summed <- NULL
    filtered[is.na(tot_pay_amt_facility),tot_pay_amt_facility:= rowSums(.SD, na.rm = T), .SDcols = fac_cols]
    setnames(filtered,names(filtered),gsub('pay','chg',names(filtered)))
    
  }else if (s=='MEPS'){ 
    setnames(filtered,'tot_fac_chg_amt','tot_chg_amt_facility')
  }else{
    ## If the paid amount is greater than the charge amount, set charge amount equal to total paid amount, otherwise leave it alone
    filtered[,tot_chg_amt:= ifelse(tot_chg_amt < tot_pay_amt, tot_pay_amt, tot_chg_amt )]
    filtered[is.na(tot_chg_amt_facility),tot_chg_amt_facility:= 0]
  }
  
  ## Drop anything where tot_chg_amt is missing
  filtered <- filtered[!(is.na(tot_chg_amt))]
  
  
  filtered$source <- s
  
  orig_dt <- rbind(orig_dt,filtered,fill=T)   
}

print('Got data')
print(Sys.time() - t0)



orig_dt$family <- f


# ------------------- MAKE F2T ----------------------#
orig_dt[,f2t_ratio:= tot_chg_amt/tot_chg_amt_facility]
orig_dt <- orig_dt[year_id >= 2000]
if (nrow(orig_dt[round(f2t_ratio,digits=0)<1])>0){
  n <- nrow(orig_dt[f2t_ratio<1])
  bad_sources <- unique(orig_dt[f2t_ratio<1]$source)
  message('Dropping where fac amt is higher than tot amount')
  fwrite(orig_dt[f2t_ratio<1], paste0('FILEPATH'))
}
orig_dt <- orig_dt[!(f2t_ratio <1)] #Not possible, drop them
orig_dt <- orig_dt[!(is.nan(f2t_ratio))] #Not possible, drop them

## Note that this is fine bc we are only ever applying F2T to places where we only have facility spend. 
# Ie. we're not trying to capture a proportion where facility would ever be zero.
orig_dt <-  orig_dt[!(f2t_ratio == Inf | f2t_ratio>100)] 


  
keep_cols <- c('tot_chg_amt','tot_chg_amt_facility','acause','year_id','family','source','f2t_ratio')
model_dt <- orig_dt[,..keep_cols]


full <- expand.grid(year_id=c(2000:2023),
                    acause = causes,
                    source = sources,
                    family = f)
model_dt <- merge(model_dt, full, by=c("year_id","family","acause", "source"),all=T)

dummy_sources <- 'MEPS'
for (s in dummy_sources){
  model_dt[, paste0(s,'_source') := list(ifelse(source==s,1,0))]
}

nrow(model_dt)

  
full_model_dt <- rbind(full_model_dt,model_dt, fill = T)




# ------------------- RUN MODEL ----------------------#
f2t_thresh <- 90

# get the number of non-missing data

num_non_nas <- nrow(full_model_dt[!is.na(f2t_ratio)])
print(paste("Number of non missing values:", num_non_nas))


# If there are enough, run a regression or take an average.
if (num_non_nas > f2t_thresh) {
  print("Running a linear regression")
  
  if ('MEPS' %in% unique(full_model_dt$source)){
    
    if (use_weights == T){
      lm.fit <- lm(f2t_ratio ~ year_id + MEPS_source,
                 weights = tot_chg_amt,
                 data = full_model_dt)
    }else{
      lm.fit <- lm(f2t_ratio ~ year_id + MEPS_source,
                    data = full_model_dt)
    }
    
    
  }else{
    
    if (use_weights == T){
      lm.fit <- lm(f2t_ratio ~ year_id,
                   weights = tot_chg_amt,
                   data = full_model_dt)
    }else{
      lm.fit <- lm(f2t_ratio ~ year_id,
                   data = full_model_dt)
    }

  }
  
  
  f2t_fit <- predict(lm.fit,newdata=full_model_dt[,MEPS_source:=0])
  full <- cbind(full_model_dt,f2t_fit)
  
}else if (num_non_nas >0 & num_non_nas < f2t_thresh) {
  
  print("Taking the median")
  full <- full_model_dt[, f2t_fit := median(f2t_ratio,na.rm = T)]
  
}else{
  # if no data at all, we set f2t fit = 1
  full <-full_model_dt[, f2t_fit := 1]
  
}

full$n_obs <- num_non_nas



full[f2t_fit<1, f2t_fit:=1]  



full$toc <- care

final_save_dt <- unique(full[,.(year_id,family,f2t_fit,toc)])
## Check
final_save_dt[,n:= .N, by = c('family','year_id','toc')]

if (nrow(final_save_dt[n>1])>0){
  print(head(final_save_dt[n>1]))
  stop('You have greater than 1 f2t ratio per fam/year. Stopping')
}

if(!interactive()){
  message('Saving maps to,',save_dir)
  arrow::write_dataset(final_save_dt[order(year_id)], path =save_dir,
                       basename_template = paste0("family_",f,"-{i}.parquet"),
                       partitioning = c( "toc"))
}


## --------------------
## Save data for plotting 
## --------------------


if (use_weights == T){
  stats <-  full[, as.list(c(f2t_fit_mean=mean(f2t_fit,na.rm=T),
                             f2t_ratio_orig=weighted.mean(f2t_ratio,tot_chg_amt,na.rm=T))), ## plot weighted means if using weights
                 by = list(year_id,family,source,n_obs)]
}else{
  stats <-  full[, as.list(c(f2t_fit_mean=mean(f2t_fit,na.rm=T),
                             f2t_ratio_orig=mean(f2t_ratio,na.rm=T))), ## plot weighted means if using weights
                 by = list(year_id,family,source,n_obs)]
}


stats[year_id == 2022, label := paste0('n_obs:',n_obs)]
stats[, label := replace(label, duplicated(label), NA)]

stats <- merge(stats, fam_names, by = 'family')
stats$toc <- care
arrow::write_dataset(stats, path =plot_data_outdir,
                     basename_template = paste0("family_",f,"-{i}.parquet"),
                     partitioning = c( "toc"))


print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------




