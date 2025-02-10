##---------------------------------------------------
#  Compile maps from parallelized model_data
#  Expand full distribution map to all residual dxs
#
# Author: Haley Lescinsky
#  
##---------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, ggplot2, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

#---------------------------------------
config <- get_config()

inj_causes <- c("inj_NEC", "_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", "inj_suicide")
by_cols <- c("age_group_years_start", "sex_id", "year_id", "code_system", "toc", "source")
#-----------------------------------------

# Arguments
if(interactive()){
  
  map_version <- "/FILEPATH/map_version_XX/"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  map_version <- args[1]
}

trunc_map <- fread(paste0(map_version, "/inj_trunc_map.csv"))
#---------------------------------------


#
#  Map for residual codes (codes with non-zero values less than threshold)
#

dxs_for_residual <- as.data.table(read.xlsx(paste0(map_version, "/intermediates/dxs_without_adjustment.xlsx")))
residual_props <- fread(paste0(map_version, "/intermediates/residual_for_package.csv"))[, tot_prop := NULL]
residual_props1 <- merge(unique(dxs_for_residual), residual_props, by = c("toc", "code_system"), all = T, allow.cartesian = T)

#
#  Map for codes with their own adjust proportions
#

modeled_adjusts <- list.files(paste0(map_version, "/intermediates"), pattern = ".feather", full.names = T)
adjust_props <- rbindlist(lapply(modeled_adjusts, function(r) 
  arrow::read_feather(r)))
setnames(adjust_props, c("age", "year"), c("age_group_years_start", "year_id"))


#
#  Now add on the residual map for codes with zero values (didn't even appear in the data!)
#
map <- rbind(adjust_props, residual_props1)

for(c in c("icd9", "icd10")){
  for(t in unique(adjust_props$toc)){
    
    add_residual_dxs <- setdiff(trunc_map[code_system==c]$trunc_code, map[toc == t & code_system==c]$dx)
    residual_props2 <- expand_grid(data.table("dx" = add_residual_dxs), residual_props[toc == t & code_system == c]) %>% as.data.table()
  
    # Add the toc-code system codes back on to the map
    map <- rbind(map, residual_props2)
    
  }
}


#
#  Perform validations
#


# Check there are no duplicate rows
if(nrow(unique(map))!=nrow(map)){
  stop("There are duplicate rows in final map!")
}

# Check that there are the same # of combinations of age/sex/year
test <- map[, .N, by = c("dx", "code_system", "toc")]
if(length(unique(test$N)) > 2){
  print(unique(test[,.(N, code_system, toc)]))
  stop("There are some dxs with missing rows!")
}

# Check that there are the same # of dxs for each age/sex/year
test <- map[, .N, by = c("age_group_years_start", "sex_id")]
if(length(unique(test$N)) > 1){
  print(unique(test[,.(N, age_group_years_start, sex_id)]))
  stop("There are some age/sex combinations with missing rows!")
}

# Check that there are the same # of dxs for each toc
test <- map[, .N, by = c("toc")]
if(length(unique(test$N)) > 1){
  print(unique(test[,.(N, toc)]))
  stop("There are some tocs with missing causes!")
}


# remove source since now it's all about TOC
map[, source := NULL]

# write maps as parquet and with partitioning - just pull in relevant age/sex/toc :) 
arrow::write_dataset(map[order(age_group_years_start)], path = paste0(map_version, "/maps/"), partitioning = c( "toc", "year_id", "code_system"))

