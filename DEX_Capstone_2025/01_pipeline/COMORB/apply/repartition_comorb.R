##--------------------------------------------------
#  Repartition comorb spending output in same format that modeling is expecting
# 
#
# Author: Haley Lescinsky
#           
##--------------------------------------------------
pacman::p_load(dplyr, openxlsx, RMySQL, ggplot2, data.table, ini, DBI, tidyr, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
suppressMessages(lbd.loader::load.containing.package())


if(interactive()){
  
  run_id <- 'XX'
  comorb_dir <- gsub("#", run_id, "/FILEPATH/COMORB/run_#/")
  final_dir <- gsub("#", run_id, "/FILEPATH/pipeline_output/run_#/")
  by_race<-'yes'
  
  params <- data.table(toc = "NF",
                       sex_id =2,
                       payer = "mdcd", 
                       geo = "national")
  
}else{
  
  args <- commandArgs(trailingOnly = TRUE)
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  comorb_dir <- args[2]
  final_dir <- args[3]
  by_race <- args[4]
  
  print(params)
  
}

# open up temp comorb data
tmp_path<-paste0(comorb_dir, "/tmp/")
out_dir<-paste0(final_dir, "/data/")
if (by_race == 'yes') {
  tmp_path<-paste0(comorb_dir, "/tmp_race/")
  out_dir<-paste0(final_dir, "/data_race/")
}
data <- open_dataset(tmp_path) %>% 
  filter(toc == params$toc & payer == params$payer & sex_id == params$sex_id & geo == params$geo) %>%
  collect() %>%
  as.data.table()

# drop comorb colums
data[, `:=` (orig_val_pre_comorb = NULL, 
             inflow = NULL,
             outflow = NULL,
             netflow = NULL)]

data <- data[order(dataset, year_id, age_group_years_start)]


# new data format, want acause in partition part name!
for(c in unique(data$acause)){
  print(c)
  write_dataset(data[acause == c],
                path = paste0(out_dir),
                partitioning = c("metric", "geo", "toc", "pri_payer", "payer"),
                basename_template = paste0("acause_", c, "_sex", as.integer(params$sex_id), "_part{i}.parquet"),
                existing_data_behavior = "overwrite")
}



print("saved!")
