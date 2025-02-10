## ---------------------------
# Purpose: identify most truncated ICDs that map uniquely to causes, using RDP packages
#
# Author: Haley Lescinsky + Emily Johnson
## ---------------------------

rm(list = ls())

#--
# SETUP
#--
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)


## Parameters------------------------------------------------------

if(interactive()){
  
  # TEST PARAMS
  phase_run_id <- 'XX'
  icd_map_version_id <- 'XX'
  packages_map_version_id <- 'XX'
  
}else{
  
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  phase_run_id <- args[1]
  icd_map_version_id <- args[2]
  packages_map_version_id <- args[3]
  
}

config <- get_config()

# use versions and make paths
icd_map_path <- parsed_config(config, "CAUSEMAP", map_version_id = as.character(icd_map_version_id))$icd_map_path
map <- arrow::read_feather(icd_map_path)

package_folder <- parsed_config(config, "RDP", map_version_id = as.character(packages_map_version_id))$package_dir
out_path <- paste0(package_folder, "/trunc_map.csv")

#-----------------------------------------------------------------------------------------------

# Reference the packages to make map of GC code -> package
gc_package_list <- function(package_folder){
  
  package_list <- unlist(rjson::fromJSON(file = paste0(package_folder, "/_package_list.json")))
  package_code_list <- rbindlist(lapply(package_list, function(p) {
    
    metadata <- rjson::fromJSON(file = paste0(package_folder, p, "/metadata.json"))
    gcs <- unlist(metadata$codes)
    
    df_gc <- data.table("icd_code" = gcs, "package" = gsub("package_","", p))
    
    # remove the all gc package - 1401 or 1406
    df_gc <- df_gc[package!=1406 & package!=1401]
    
    return(df_gc)
    
  }))
  return(package_code_list)
}

icd10_package_code_list <- gc_package_list(paste0(package_folder, "/icd10/"))
icd10_package_code_list[, code_system := "icd10"]

icd9_package_code_list <- gc_package_list(paste0(package_folder, "/icd9/"))
icd9_package_code_list[, code_system := "icd9"]

package_code_list <- rbind(icd9_package_code_list, icd10_package_code_list)

# take only the first package if multiple packages for the same code (which now happens bc of nec packages)
#  the lists were made according to the package order, so the first one will be the package that actually gets used
package_code_list[, package_num := 1:.N, by = c('icd_code', 'code_system')]
package_code_list <- package_code_list[package_num == 1][, package_num := NULL]

# merge on package info to icd cause map
# since many codes are not gc and won't have packages
map <- merge(map, package_code_list, by = c("icd_code", "code_system"), all.x = T)

# Manually add codes so truncation up to them happens - for injury adjust
inj_causes <- c("_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", 
                "inj_suicide")
add_codes <- data.table(icd_code = paste0("adjusted_", inj_causes), code_system = c(rep("icd10", times = length(inj_causes)), 
                                                                                    rep("icd9", times = length(inj_causes))), icd_name = c("Adjusted during injury adjust"), acause = inj_causes, is_dex_cause = 1)
map <- rbind(map, add_codes, fill = T)
map[, package_acause := paste0(acause, "_", package)]

# Create columns for each truncated parent up to 3 digit ICDs
map[, `:=`(icd3 = str_sub(icd_code, start = 1L, end = 3L),
           icd4 = str_sub(icd_code, start = 1L, end = 4L),
           icd5 = str_sub(icd_code, start = 1L, end = 5L),
           icd6 = str_sub(icd_code, start = 1L, end = 6L))]

# This big merge is the fastest way I found to "lookup" the more aggregate causes
icd3_cause <- map[icd_code == icd3, .(code_system, icd3, icd3_cause = package_acause)]
icd4_cause <- map[icd_code == icd4, .(code_system, icd4, icd4_cause = package_acause)]
icd5_cause <- map[icd_code == icd5, .(code_system, icd5, icd5_cause = package_acause)]

map <- merge(map, icd3_cause, by = c("code_system", "icd3"), all.x = T)
map <- merge(map, icd4_cause, by = c("code_system", "icd4"), all.x = T)
map <- merge(map, icd5_cause, by = c("code_system", "icd5"), all.x = T)

# move longest to shortest codes
map[, trunc_code := icd_code]
map[package_acause == icd5_cause, trunc_code := icd5]
map[package_acause == icd4_cause, trunc_code := icd4]
map[package_acause == icd3_cause, trunc_code := icd3]

# exception for detailed codes with NA packages
#   if a 5 digit code has package=NA but 4 digit has non-NA package, we want to truncate! 
#   Don't want to truncate if there is a change of package though
map[grepl("NA", package_acause) & !grepl("NA", icd5_cause) & mapply(grepl, acause, icd5_cause), trunc_code := icd5]
map[grepl("NA", package_acause) & !grepl("NA", icd4_cause) & mapply(grepl, acause, icd4_cause), trunc_code := icd4]
# Only want to pull up to 3 digit if there isn't a difference between 3 and 4. Otherwise leave at 4
map[grepl("NA", package_acause) & !grepl("NA", icd3_cause) & mapply(grepl, acause, icd3_cause) & icd4_cause==icd3_cause, trunc_code := icd3]

# Create unique IDs for the truncated codes - more efficient than storing strings
icd_ids <- copy(map[,.(acause, code_system, trunc_code)]) %>% unique()
map <- map[icd_ids, on = c("acause","code_system","trunc_code")]

# Reduce output
map <- map[,.(code_system, icd_code, trunc_code, acause, package_acause)]
map <- unique(map)

## write out
write.csv(map, out_path, row.names = F)
