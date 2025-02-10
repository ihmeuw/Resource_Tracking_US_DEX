##----------------------------------------------------------------
## Title: 5_agg_to_national.R
## Purpose: Aggregate state level results to get national results
## Authors: Azalea Thomson, Haley Lescinsky
##----------------------------------------------------------------


t0 <- Sys.time()

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(configr, lib.loc = "FILEPATH")
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))


## ARGS
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
care <- tasks$toc
yr <- as.numeric(tasks$year_id)
final_dir <- tasks$final_dir
final_collapse_dir <- tasks$final_collapse_dir
run_at_draw_level <- args[2]


if (run_at_draw_level == T){
  indir <- paste0(final_dir, 'data/geo=state/toc=',care,'/')
  
  dt <- arrow::open_dataset(indir)
  
  ## Sum across states
  national <- dt %>%
    filter(year_id == yr) %>%
    group_by(age_group_years_start, year_id,sex_id,acause, pri_payer, payer, draw) %>%
    summarise(vol= sum(vol),
              spend = sum(spend)) %>%
    as.data.table()
  oth_payer <- national[payer == 'oth'][,draw:=NULL]
  setnames(oth_payer, c('spend','vol'),c('mean_spend','mean_vol'))
  oth_payer[,`:=` (lower_spend = mean_spend,upper_spend = mean_spend,lower_vol = mean_vol,upper_vol = mean_vol)]
  
  national[,`:=`(state = 'USA',location = 'USA',geo = 'national', toc = care)]
  
    
  nat_collapsed <- national %>%
    filter(payer != 'oth') %>%
    group_by(age_group_years_start, year_id,sex_id,acause, pri_payer, payer) %>%
    summarise(mean_spend = mean(spend),
              lower_spend = quantile(spend, 0.025),
              upper_spend = quantile(spend, 0.975),
              mean_vol = mean(vol),
              lower_vol = quantile(vol, 0.025),
              upper_vol = quantile(vol, 0.975)) %>% as.data.table() 
  
  ## Append in payer = 'oth'
  nat_collapsed <- rbind(nat_collapsed, oth_payer)

}else{
  indir <- paste0(final_collapse_dir, 'data/geo=state/toc=',care,'/')
  
  dt <- arrow::open_dataset(indir)
  
  nat_collapsed <- dt %>%
    filter(payer!='oth') %>%
    # filter(!is.na(acause))%>%
    group_by(age_group_years_start, year_id,sex_id,acause, pri_payer, payer) %>%
    summarise(mean_vol= sum(mean_vol),
              mean_spend = sum(mean_spend),
              lower_vol = sum(lower_vol),
              upper_vol = sum(upper_vol),
              lower_spend = sum(lower_spend),
              upper_spend = sum(upper_spend)) %>%
    as.data.table()
  
  oth_payer <- dt %>%
    filter(payer=='oth') %>% collect() %>% as.data.table()
  
  nat_collapsed <- rbind(nat_collapsed,oth_payer)
  
}

nat_collapsed[,`:=`(state = 'USA',location = 'USA',geo = 'national', toc = care)]



if (run_at_draw_level == T){
  arrow::write_dataset(national, path = paste0(final_dir, "/data/"),
                       basename_template = paste0("year_",yr,"-{i}.parquet"),
                       partitioning = c("geo","toc","state","payer"))
}


arrow::write_dataset(nat_collapsed, path = paste0(final_collapse_dir, "/data/"), 
                     basename_template = paste0("year_",yr,"-{i}.parquet"),
                     partitioning = c("geo","toc","state","payer"))



print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------