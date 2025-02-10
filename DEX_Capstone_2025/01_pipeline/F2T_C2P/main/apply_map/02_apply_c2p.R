##----------------------------------------------------------------
## Title: 02_apply_c2p.R
## Purpose: Use ratios calculated in 02_model_c2p.R to calculate pay, when we only have charge.
## Author: Azalea Thomson
##----------------------------------------------------------------

## --------------------
## 0. Setup 
## --------------------
Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr)
username <- Sys.getenv('USER')
'%ni%' <- Negate('%in%')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(str_remove(here, 'apply_map'),'/payer_dictionary.R'))

library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

# Arguments
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
for (i in names(tasks)){
  assign(i, tasks[, get(i)])
  message(i,':', tasks[, get(i)])
}
use_weights <- args[2]


c2p_map_path <- paste0(map_version,'maps/weights_',use_weights,'/')

##' --------------------
##' Pull in the map
##' payment = charge * adj_factor
##' --------------------

if (yr >2019){
  pull_yr <- 2019
}else{
  pull_yr <- yr
}
ratios <- arrow::open_dataset(c2p_map_path) %>% 
  filter(toc == care & 
           year_id == pull_yr) %>%
  select(year_id,family,pri_payer,mdcd,mdcr,priv,oop) %>%
  collect() %>% data.table() %>% unique() 

if (yr >2019){
  ratios[,year_id:=yr]
}

print('Got ratios')

payer_dict <- copy(payer_nums)
setnames(payer_dict,'payer','pri_payer')



##' --------------------
##' Read in the data to adjust
##' --------------------
schema <- update_nulls_schema(indir)

target_data <- arrow::open_dataset(indir, schema = schema) %>% 
  filter(toc == care &
           year_id == yr) %>% 
  collect() %>% data.table() %>% unique() 


print('Got data to adjust')

if ('family' %ni% names(target_data)){
  ## Merge on cause family
  level_causelist <- fread(cause_map_path)[,.(acause,family)]
  target_data <- merge(target_data, level_causelist, by = 'acause', all.x=T)
}



## --------------------
## Apply map
## --------------------
## Merge map on target data
adj_data <- merge(target_data, ratios, by = c('year_id','family','pri_payer'), all.x = T) #, all.x = T)

print('Merged on map')

adj_data[,mdcr_pay_amt:=tot_chg_amt*mdcr]
adj_data[,mdcd_pay_amt:=tot_chg_amt*mdcd]
adj_data[,priv_pay_amt:=tot_chg_amt*priv]
adj_data[,oop_pay_amt:=tot_chg_amt*oop]

adj_data[, `:=`(mdcr=NULL,mdcd=NULL,priv=NULL,oop=NULL, total=NULL,pri_payer_name=NULL,family=NULL)]
adj_data[, tot_pay_amt:=rowSums(.SD,na.rm = T), .SDcols=c('mdcr_pay_amt','mdcd_pay_amt','priv_pay_amt','oop_pay_amt')]

## CHECK
if (nrow(adj_data[tot_pay_amt>tot_chg_amt])){
  n <- nrow(adj_data[tot_pay_amt>tot_chg_amt])
  message('Warning, you have',n,' rows where tot pay is > tot charge')
}
## --------------------
## Save out data
## --------------------
print('Saving out')
arrow::write_dataset(adj_data[order(year_id,age_group_years_start)], 
                     path =outdir,
                     partitioning = c( "toc","year_id","code_system","age_group_years_start"))



print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------








