##--------------------------------------------------
#  Use comorb AFs to adjust spending on collapsed data
# 
#
# Author: Haley Lescinsky
#           
##--------------------------------------------------
rm(list = ls())

source('/FILEPATH/get_age_metadata.R')
pacman::p_load(arrow, dplyr, openxlsx, RMySQL, ggplot2, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
set.seed(14523)
t0 <- Sys.time()
sum_af_threshold <- 0.8 # At most we will redistribute 80% away from a condition. Necessary since the AFs are made independently and a bunch of comorbidities could theoretically have AFs that sum to over 1 for the same primary cause

if(interactive()){
  
  map_version <- 'XX'
  run_id <- 'XX' 
  
  map_version <- paste0("/FILEPATH/map_version_", map_version, "/")
  collapse_dir <- paste0("/FILEPATH/COLLAPSE/run_", run_id, "/")
  save_data_folder <- paste0("/FILEPATH/COMORB/run_", run_id, "_interactive/")
  by_race<-'yes'
  
  #-----------------------
  params <- data.table(toc = "AM",
                       age_group_min = 60,
                       age_group_max = 70,
                       year = 2010, 
                       sex_id = 1)
  #-----------------------
  
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)

  map_version <- args[2]
  collapse_dir <- args[3]
  save_data_folder <- args[4]
  by_race <- args[5]
}

#---------------------------------------------------------------------
#  Read in collapsed data
#---------------------------------------------------------------------
if (by_race =='no'){
  col_path<-paste0(collapse_dir, "/data/")
  tmp_save_data_folder <- paste0(save_data_folder, "/tmp/")
  sf_save_data_folder <- paste0(save_data_folder, "/sankey_flows/")
  ss_save_data_folder <- paste0(save_data_folder, "/sankey_starting_$/")
  scar_save_data_folder <- paste0(save_data_folder, "/scalars/")
}else if (by_race =='yes'){
  col_path<-paste0(collapse_dir, "data_race/")
  tmp_save_data_folder <- paste0(save_data_folder, "/tmp_race/")
  sf_save_data_folder <- paste0(save_data_folder, "/sankey_flows_race/")
  ss_save_data_folder <- paste0(save_data_folder, "/sankey_starting_$_race/")
  scar_save_data_folder <- paste0(save_data_folder, "/scalars_race/")
}
collapse_data <- arrow::open_dataset(col_path)
collapse_data_filtered <- collapse_data %>% filter(
                                            toc == params$toc &
                                            age_group_years_start >= params$age_group_min &
                                            age_group_years_start < params$age_group_max &
                                            year_id == params$year &
                                            sex_id == params$sex_id)

data <- collapse_data_filtered %>% collect() %>% as.data.table()
starting_data <- nrow(data)

if(nrow(data) > 0 ){

#---------------------------------------------------------------------
#  Read in comorb maps
#---------------------------------------------------------------------

maps <- open_dataset(paste0(map_version, "/maps/")) %>% collect()
maps <- maps[age_group_min == params$age_group_min & toc == params$toc,]

# implement threshold and scale AFs underneath that threshold (so if a cause has sum AF > 1, it can't end up negative)
maps[, sum_af := sum(af), by = c("pri_cause", "toc", "age_group_min", "age_group_max")]
maps[ sum_af > sum_af_threshold, af := af * (sum_af_threshold/sum_af)]

# expand maps out to age group
d <- fread("/FILEPATH/age_metadata_with_age_groups.csv")
maps <- merge(maps, d[,.(age_group_years_start, age_group_min, age_group_max)], by = c("age_group_min", "age_group_max"), allow.cartesian = T)

# Control by columns for all the merges + summaries below
#---------------------------------------------------------------------
#  merge_by_cols = the columns the map can merge onto data by - currently just toc/age specific
#  sankey_by_cols = the columns to collapse flows on for plotting
#  collapse_by_cols = all relevant columns in collapse that rows could vary by (besides cause, raw_val, se)
merge_by_cols <- c("toc", "age_group_years_start")
sankey_by_cols <- c("age_group_years_start", "toc", "geo", "metric")
collapse_by_cols <- c("pri_payer", "payer", "geo", "location", "location_name", "dataset", "metric", "toc", "year_id", "age_group_years_start", "sex_id")
if (by_race == 'yes'){
  collapse_by_cols <- c(collapse_by_cols, 'race_cd')
}

orig_col_order <- colnames(data)

#---------------------------------------------------------------------
# Do comorb adjustment
#---------------------------------------------------------------------

print(Sys.time()-t0)
print("Data is read in, starting merges + comorb adjustment!")

# First, we basically do cause restriction - if we don't have collapsed data for BOTH the pri cause AND comorb we don't use that AF
#    A little messy because we merge the map on the data twice - once on pri cause and once on comorb to check they exist (by age/sex/etc)
setnames(maps, "pri_cause", "acause")
maps <- merge(maps, 
              data[,unique(c("acause", merge_by_cols, collapse_by_cols)), with = F], by = c("acause", merge_by_cols), allow.cartesian = T)
setnames(maps, "acause", "pri_cause")
setnames(maps, "comorb", "acause")
maps <- merge(maps, 
              data[,unique(c("acause", merge_by_cols, collapse_by_cols)), with = F], by = unique(c("acause", merge_by_cols, collapse_by_cols)))
setnames(maps, "acause", "comorb")
# next we calculate flow by merging data on map by primary cause
setnames(maps, "pri_cause", "acause")
map_flows <- merge(maps[,c("comorb", "acause", "af", collapse_by_cols), with = F],
                   data[,c("acause", collapse_by_cols, "raw_val"), with = F], by = c("acause", collapse_by_cols)) 
map_flows[, flow_ij := raw_val * af]

# save metadata here for sankey
sankey_map_inflows <- map_flows[,.(flow_ij = sum(flow_ij)), by = c("comorb", "acause", sankey_by_cols)]
sankey_map_starting <- data[, .(orig_val = sum(raw_val)), by = c("acause", sankey_by_cols)]

# calculate inflows at the comorb level
map_inflows <- map_flows[,.(inflow = sum(flow_ij)), by = c("comorb", collapse_by_cols)]
# calculate outflows at the pri cause level
map_outflows <- map_flows[,.(outflow = sum(flow_ij)), by = c("acause", collapse_by_cols)]

# merge inflows + outflows back onto data
setnames(map_inflows, "comorb", "acause")
data <- merge(data, map_inflows, by = c("acause", collapse_by_cols), all.x = T)
data <- merge(data, map_outflows, by = c("acause", collapse_by_cols), all.x = T)

# if NA, replace with 0
data[is.na(outflow), outflow := 0]
data[is.na(inflow), inflow:=0]

# calculate netflow
data[, netflow := inflow - outflow]

if(nrow(data)!=starting_data){
  stop("not ending with same rows at beginning - check inflow/outflow merges")
}

print(sum(data$outflow))
print(sum(data$inflow))

if(round(abs(sum(data$outflow)-sum(data$inflow)), digits = 2)!=0){
  stop("total outflow doesn't equal total inflow")
}

# adjust data
setnames(data, "raw_val", "orig_val_pre_comorb")
data[, raw_val := orig_val_pre_comorb + netflow]

# calculate scalar for comparability across causes
scalar_info <- data[,.(netflow = sum(netflow), orig_val_pre_comorb =  sum(orig_val_pre_comorb)), by = c("acause", sankey_by_cols)]
scalar_info[, current_scalar := (netflow/orig_val_pre_comorb) +1 ]

#---------------------------------------------------------------------
# Save!
#---------------------------------------------------------------------

# save new data
#----------------------------
setcolorder(data, orig_col_order)
arrow::write_dataset(data, 
                     path = tmp_save_data_folder, 
                     partitioning = c("toc", "year_id", "age_group_years_start", "sex_id"), 
                     existing_data_behavior = "overwrite")


# save helper data
#-----------------------------
save_sankey_flows <- sf_save_data_folder
save_sankey_starting <- ss_save_data_folder
save_scalars <- scar_save_data_folder

if(!dir.exists(save_sankey_flows)){dir.create(save_sankey_flows)}
if(!dir.exists(save_scalars)){dir.create(save_scalars)}

# sankey map inflows
sankey_map_inflows$year_id <- params$year
sankey_map_inflows$sex_id <- params$sex_id

# sankey starting $
sankey_map_starting$year_id <- params$year
sankey_map_starting$sex_id <- params$sex_id

# scalar info
scalar_info$year_id <- params$year
scalar_info$sex_id <- params$sex_id

path <- paste0(params, collapse = "_")
arrow::write_dataset(sankey_map_inflows, 
                     path = save_sankey_flows, 
                     basename_template = paste0(path, "_{i}.parquet"), 
                     existing_data_behavior = "overwrite")

arrow::write_dataset(scalar_info, 
                     path = save_scalars, 
                     basename_template = paste0(path, "_{i}.parquet"), 
                     existing_data_behavior = "overwrite")

}else{
  print("No collapsed data in this data split!")
}

print("Totally done")
print(Sys.time()-t0)