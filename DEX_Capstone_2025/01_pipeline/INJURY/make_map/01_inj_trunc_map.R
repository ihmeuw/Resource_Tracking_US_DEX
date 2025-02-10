## ---------------------------
## Identify most truncated ICDs that map uniquely to causes, then subset to only inj_NEC
##
##
## Author: Haley Lescinsky and Emily Johnson
## ---------------------------
rm(list = ls())
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
  cm_map_version_id <- 'X'
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  map_version <- args[1]
  cm_map_version_id <- args[2]
}


#---------------------------------------
causemap_config <- parsed_config(config, key = "CAUSEMAP", map_version_id = as.integer(cm_map_version_id ))

map <- arrow::read_feather(causemap_config$icd_map_path)
out_path <- paste0(map_version, "/inj_trunc_map.csv")

#-----------------------------------------------------------------------------------------------

# Manually add two codes so truncation up to them happens - for injury adjust
add_codes <- data.table(icd_code = c("S326", "S990"), 
                        code_system = "icd10", 
                        icd_name = c("Fracture of ischium", "Physeal fracture of calcaneus"), 
                        acause = "inj_NEC")
map <- rbind(map, add_codes, fill = T)

# Create columns for each truncated parent up to 3 digit ICDs
map[, `:=`(icd3 = str_sub(icd_code, start = 1L, end = 3L),
           icd4 = str_sub(icd_code, start = 1L, end = 4L),
           icd5 = str_sub(icd_code, start = 1L, end = 5L),
           icd6 = str_sub(icd_code, start = 1L, end = 6L))]

# This big merge is the fastest way I found to "lookup" the more aggregate causes
icd3_cause <- map[icd_code == icd3, .(code_system, icd3, icd3_cause = acause)]
icd4_cause <- map[icd_code == icd4, .(code_system, icd4, icd4_cause = acause)]
icd5_cause <- map[icd_code == icd5, .(code_system, icd5, icd5_cause = acause)]

map <- merge(map, icd3_cause, by = c("code_system", "icd3"), all.x = T)
map <- merge(map, icd4_cause, by = c("code_system", "icd4"), all.x = T)
map <- merge(map, icd5_cause, by = c("code_system", "icd5"), all.x = T)

# move longest to shortest codes
map[, trunc_code := icd_code]
map[acause == icd5_cause, trunc_code := icd5]
map[acause == icd4_cause, trunc_code := icd4]
map[acause == icd3_cause, trunc_code := icd3]

# Reduce output
map <- map[acause == "inj_NEC",.(code_system, icd_code, trunc_code, acause)]
map <- unique(map)

## write out
write.csv(map, out_path, row.names = F)
