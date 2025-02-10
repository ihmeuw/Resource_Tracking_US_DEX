## ==================================================
## Author(s): Haley Lescinsky, Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script that maps from zip code to county
## ==================================================

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, tidyverse, sparklyr, arrow)
library(lbd.loader, lib.loc = sprintf("FILEPATH"))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
set.seed(14523)

# Arguments
#-------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
print(args)

map_path <- args[1]
params <- fread(map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID")]
print(params)

save_data_folder <- args[2]
zip3path <- args[3]
zip5path <- args[4]
source <- args[5]

t0 <- Sys.time()

# Read in data
#-------------------------------------------
data_paths <- paste0(params$source_data_path, "/toc=", params$toc, "/year_id=",params$year_id, "/")

schema <- update_nulls_schema(data_paths)
all_data <- arrow::open_dataset(data_paths, schema = schema)

if(!is.na(params$state_id)){
  all_data <- all_data %>% filter(st_resi == params$state_id)
}

# LOOP THROUGH AGE/SEX COMBINATIONS
#-------------------------------------------

age_group_years_start <- c(0, 1, seq(5, 95, by = 5))
sex_id <- c(1, 2)
demographic_grid <- as.data.table(crossing(age_group_years_start, sex_id))

## determine map based on source/TOC
if(source == "KYTHERA" & params$toc == "RX"){
  ZIPMAP <- arrow::read_feather(zip3path)
  setnames(ZIPMAP, "zip3_resi", "zip_resi")
}else{
  ZIPMAP <- arrow::read_feather(zip5path)
  setnames(ZIPMAP, "zip5_resi", "zip_resi")
}

## filter to year; we only have the map for 2010-2021
if(params$year_id <= 2010){
  ZIPMAP <- ZIPMAP[year_id == 2010, -"year_id"]
}else if(params$year_id == 2022){
  ZIPMAP <- ZIPMAP[year_id == 2021, -"year_id"]
}else{
  ZIPMAP <- ZIPMAP[year_id == params$year_id, -"year_id"]
}

## loop over age/sex
for(i in 1:nrow(demographic_grid)){
  
  # subset data to this age/sex combo
  #-------------------------------------------
  print("--------------------------------------------------------")
  demo <- demographic_grid[i,]
  print(demo)
  
  data <- all_data %>% 
    filter(age_group_years_start == demo$age_group_years_start &
             sex_id == demo$sex_id) %>% collect() %>% as.data.table()
  
  # add on toc and year_id right away
  data[, toc := params$toc]
  data[, year_id := params$year_id]
  
  # standardize zip name to map
  if(source == "KYTHERA" & params$toc == "RX"){
    setnames(data, "zip_3_resi", "zip_resi")
  }else{
    setnames(data, "zip_5_resi", "zip_resi")
  }
  
  # if data is empty
  if(nrow(data) == 0){
    print("No data with these demographics! Stopping now")
    next
  }
  rows <- nrow(data)
  
  # STEP 1 - check zip5 exists everywhere
  #-------------------------------------------
  if(nrow(data[is.na(zip_resi)])>0){
    print("There are some missing zip codes in the data!")
  }
  
  # STEP 2 - a handful of zip codes have weight = 1, so those just need that mcnty
  #-------------------------------------------
  ZIPMAP_certain <- ZIPMAP[residence_wt == 1]
  data <- merge(data[, mcnty_resi := NULL], ZIPMAP_certain[, .(zip_resi, mcnty_resi)], by = c("zip_resi"), all.x = T)
  
  # STEP 3 - identify zip codes that aren't in the map
  #-------------------------------------------
  # the data we don't want to send through apply function
  data_a <- data[!zip_resi %in% ZIPMAP$zip_resi | !is.na(mcnty_resi)] 
  
  
  # STEP 4 - use apply function to probablistically map!
  #-------------------------------------------
  data_b <- data[zip_resi %in% ZIPMAP$zip_resi & is.na(mcnty_resi)] 
  
  # this function modifies in place, so just capturing the output to remove it!
  apply_map(
    my_data = data_b, 
    my_map = ZIPMAP, 
    from_col = "zip_resi", 
    to_col = "mcnty_resi", 
    prob_col = "residence_wt"
  )
  
  data <- rbind(data_a, data_b)
  if(nrow(data)!=rows){
    stop("data was dropped during apply!")
  }
  
  # STEP 5 - Save data!
  #-------------------------------------------
  print("saving data!")
  
  ## drop zip code info
  data[,names(data)[names(data) %like% "zip_"] := NULL]
  
  # Save parquet with correct partitioning
  parquet_filepath <- paste0(save_data_folder, "/data/")
  
  ## partition 
  data <- data[order(toc, year_id, st_resi, age_group_years_start, sex_id, acause)]
  arrow::write_dataset(data, path = parquet_filepath, partitioning = c("toc", "year_id", "st_resi", "age_group_years_start", "sex_id"))
  
}