#################
#
# 03_make_diabetes_package.R
#    - Use outputs from Marketscan and GBD comparison to save package showing proportional split between type 1 and type 2 by age. 
#
#
#  Author: Haley Lescinsky
# ################

#--
# Set up
#--

rm(list = ls())

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH/python')
library(configr, lib.loc = "FILEPATH/r_library/")
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, lme4, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
source("/FILEPATH/get_outputs.R")
config <- get_config()
obdc_path <- paste0(config$RDP$package_helper_dir,  "/map_versions/.odbc.ini")

#--
# Arguments
#--

overwrite <- F
cod_package_dir <-  config$RDP$cod_input_package_dir
work_dir <- paste0(config$RDP$package_helper_dir, "/dex_package_helpers/diabetes/")
new_package_dir <- paste0(config$RDP$package_helper_dir, "/dex_packages/")
dex_icd_map <- arrow::read_feather(gsub("#", 'XX', config$CAUSEMAP$icd_map_path))  

#
# make wgt + source from proportions
#

prop <- fread(paste0(work_dir, "/diabetes_package_proportions_age_sex_year.csv"))
prop[, age_start := as.integer(gsub(" to [0-9]*$", "", age_group_name))]
prop[, age_end := as.integer(gsub("^[0-9]* to ", "", age_group_name))]

prop <- prop[order(year_id, sex_id, age_start)]
prop[, shared_wgt_group_id := 1:.N]

# source
prop_demo <- prop[,.(year_id, sex_id, age_start, age_end, shared_wgt_group_id)]
prop_demo_long <- melt(prop_demo, id.vars = c("shared_wgt_group_id"))
prop_demo_long[variable=="year_id", operator:= "=="]
prop_demo_long[variable=="sex_id", operator:= "=="]
prop_demo_long[variable=="age_start", operator:= ">="]
prop_demo_long[variable=="age_end", operator:= "<="]
prop_demo_long[grepl("age", variable), variable := "age"]

source <- prop_demo_long[, .(shared_wgt_group_id, shared_wgt_group_logic_set_id=shared_wgt_group_id, variable, operator, value)]

# wgt group
prop_wgt <- prop[,.(shared_wgt_group_id, prop_type1, prop_type2)]
prop_wgt_long <- melt(prop_wgt, id.vars = c("shared_wgt_group_id"), value.name = "wgt")
prop_wgt_long[, shared_group_id := ifelse(variable == "prop_type1", 1, 2)][, variable := NULL]

wgt <- prop_wgt_long[, .(shared_wgt_group_id, wgt, shared_group_id)]

# target
target <- data.table(shared_group_id = c(1,2),
                     icd_code = c("rdptarget_diabetes_typ1",
                                 "rdptarget_diabetes_typ2"))


#--
# Load in CoD packages
#-- 

insulin_codes <- dex_icd_map[grepl("use of insulin", icd_name)]
new_icd9_code <- insulin_codes[code_system=="icd9", icd_code]    #V5867
new_icd10_code <- insulin_codes[code_system=="icd10", icd_code] # Z794


# establish database connection
odbc <- ini::read.ini(obdc_path)
con_def <- 'cod engine'
myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                            database = odbc[[con_def]]$DATABASE,
                            host = odbc[[con_def]]$SERVER,
                            username = odbc[[con_def]]$USER,
                            password = odbc[[con_def]]$PASSWORD)

icd_code_query <- sprintf("select c.code_id, c.value
  from engine_room.maps_codelistversion clv
  join engine_room.maps_codelisthistory clh using(code_list_version_id)
  join engine_room.maps_code c using(code_id)
  where clv.code_system_id = 6  
  and clv.gbd_round_id = 7
  and clv.code_list_version_status_id = 2")
icd_9_code_id_map <- as.data.table(dbGetQuery(myconn, icd_code_query))
setnames(icd_9_code_id_map, "value", "icd_code")
icd_9_code_id_map[, icd_code := gsub("\\.", "", icd_code)]


icd_code_query <- sprintf("select c.code_id, c.value
  from engine_room.maps_codelistversion clv
  join engine_room.maps_codelisthistory clh using(code_list_version_id)
  join engine_room.maps_code c using(code_id)
  where clv.code_system_id = 1  
  and clv.gbd_round_id = 7
  and clv.code_list_version_status_id = 2")
icd_10_code_id_map <- as.data.table(dbGetQuery(myconn, icd_code_query))
setnames(icd_10_code_id_map, "value", "icd_code")
icd_10_code_id_map[, icd_code := gsub("\\.", "", icd_code)]

#----
# icd9
#----
if(overwrite){
  
  package_folder = paste0(cod_package_dir, "6/")
  p <- 4035
  
  metadata <- rjson::fromJSON(file = paste0(package_folder, p, "/metadata.json"))
  
  metadata$codes <- c(icd_9_code_id_map[code_id %in% metadata$codes, icd_code], new_icd9_code)
  metadata$package_description <- "Diabetes unspecified type dex adj"
  metadata$package_id <- "4035_dex"
  
  
  metadata <- rjson::toJSON(metadata)
  write(metadata, paste0(new_package_dir, p, "/metadata.json"))
  arrow::write_parquet(source, paste0(new_package_dir, p, "/source.parquet"))
  arrow::write_parquet(target, paste0(new_package_dir, p, "/target.parquet"))
  arrow::write_parquet(wgt, paste0(new_package_dir, p, "/wgt.parquet"))
  
  
  # icd10
  package_folder = paste0(cod_package_dir, "1/")
  p <- 4032
  
  metadata <- rjson::fromJSON(file = paste0(package_folder, p, "/metadata.json"))
  
  
  metadata$codes <- c(icd_10_code_id_map[code_id %in% metadata$codes, icd_code], new_icd10_code)
  metadata$package_description <- "Diabetes unspecified type dex adj"
  metadata$package_id <- "4032_dex"
  
  
  metadata <- rjson::toJSON(metadata)
  write(metadata, paste0(new_package_dir, p, "/metadata.json"))
  arrow::write_parquet(source, paste0(new_package_dir, p, "/source.parquet"))
  arrow::write_parquet(target, paste0(new_package_dir, p, "/target.parquet"))
  arrow::write_parquet(wgt, paste0(new_package_dir, p, "/wgt.parquet"))

}else{
  print("Not actually saving new packages because overwrite is set to F - change to T and rerun!")
}


