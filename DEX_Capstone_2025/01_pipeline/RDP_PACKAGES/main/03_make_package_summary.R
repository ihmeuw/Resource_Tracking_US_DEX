##--------------------------------------------------
# After running knockout cod packages, this collects important metadata for future parts of RDP
# 
#
# Author: Haley Lescinsky
#           
##--------------------------------------------------


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
package_dir <- parsed_config(config, "RDP", map_version_id = as.character(packages_map_version_id))$package_dir

icd_map_config <- parsed_config(config, "CAUSEMAP", map_version_id = as.character(icd_map_version_id))
icd_code_map <- arrow::read_feather(icd_map_config$icd_map_path)
cause_restrictions <- fread(config$METADATA$toc_cause_restrictions_PRE_COLLAPSE_path)
dex_causelist <- fread(icd_map_config$causelist_path)

#--
# Save package metadata
#--

pull_package_metadata <- function(lab, package_dir, icd_code_map){
  
  package_dir_lab = paste0(package_dir, "/", lab, "/")
  package_list <- unlist(rjson::fromJSON(file = paste0(package_dir_lab, "/_package_list.json")))
  dex_icd_map <- icd_code_map[code_system==lab]
  
  package_metadata <- rbindlist(lapply(package_list, function(p) {
    
    # 
    metadata <- rjson::fromJSON(file = paste0(package_dir_lab, p, "/metadata.json"))
    targets <- as.data.table(arrow::read_parquet(paste0(package_dir_lab, p,"/target.parquet")))
    
    gcs <- unlist(metadata$codes)
    gc_names <- dex_icd_map[icd_code %in% gcs, .(icd_code, icd_name, acause)]
    
    tcs <- unique(targets$icd_code)
    tc_names <- dex_icd_map[icd_code %in% tcs, .(icd_code, icd_name, acause)]
    
    
    # map codes to code names and acauses
    if(nrow(gc_names) > 0 & p!=1406 & p!=1401 ){
      dex_gc_map <- paste0(lapply(1:nrow(gc_names), function(i) paste0(gc_names$icd_name[i], ":", gc_names$icd_code[i])), collapse = ";" )
      
      dex_gc_cause_map <- paste0(lapply(1:nrow(gc_names[acause!="_gc"]), function(i) paste0(gc_names[acause!="_gc"]$acause[i], ":", gc_names[acause!="_gc"]$icd_code[i])), collapse = ";" )
    }else{ 
      dex_gc_map <- ""
      dex_gc_cause_map <- ""}
    
    if(nrow(tc_names) > 0 & p!=1406 & p!=1401){
      dex_tc_map <- paste0(lapply(1:nrow(tc_names), function(i) paste0(tc_names$icd_name[i], ":", tc_names$icd_code[i])), collapse = ";" )
      dex_tc_cause_map <- paste0(lapply(1:nrow(tc_names), function(i) paste0(tc_names$acause[i], ":", tc_names$icd_code[i])), collapse = ";" )
    }else{ 
      dex_tc_map <- ""
      dex_tc_cause_map <- ""}
    
    row <- data.table("icd_code_system" = lab,
                      "package_id" = p,
                      "package_name" = metadata$package_name,
                      "package_description" = metadata$package_description,
                      "package_create_targets" = metadata$create_targets, 
                      
                      #  GC
                      "input_codes_num" = length(gc_names$icd_code),
                      "input_codes" = paste0(gc_names$icd_code, collapse = ","),
                      "input_codes_names" = dex_gc_map,
                      "input_codes_acause" = dex_gc_cause_map,
                      
                      
                      # TC
                      "target_codes_num" = length(unique(tcs)),
                      "target_codes_groups_num" = length(unique(targets$shared_group_id)),
                      "target_codes" = paste0(unique(tcs), collapse = ","),
                      "target_codes_names" = dex_tc_map,
                      "target_codes_acause" = dex_tc_cause_map
                      
    )
    return(row)
    
  }), fill = T)
  
  return(package_metadata)
  
}


icd9_package_metadata <- pull_package_metadata("icd9", package_dir, icd_code_map)
icd9_package_metadata[, package_list_order := 1:.N]

icd10_package_metadata <- pull_package_metadata("icd10", package_dir, icd_code_map)
icd10_package_metadata[, package_list_order := 1:.N]

package_metadata <- rbind(icd9_package_metadata, icd10_package_metadata)

# make a shorter name column for ease of plotting + vetting
package_metadata[, package_short_name := package_description]

package_metadata[, package_short_name := gsub("[u,U]nspecified", "Unsp", package_short_name)]
package_metadata[, package_short_name := gsub("[u,U]ndetermined", "Und", package_short_name)]
package_metadata[, package_short_name := gsub("sign and symptom", "symp", package_short_name)]
package_metadata[, package_short_name := gsub("correction", "correct", package_short_name)]
package_metadata[, package_short_name := gsub("  ", " ", package_short_name)]
package_metadata[, package_short_name := gsub("deficiency", "def", package_short_name)]
package_metadata[, package_short_name := gsub("shooting", "shoot", package_short_name)]
package_metadata[, package_short_name := gsub("[p,P]oisoning", "poison", package_short_name)]
package_metadata[, package_short_name := gsub(" and", " &", package_short_name)]
package_metadata[, package_short_name := gsub("Injuries", "Inj", package_short_name)]
package_metadata[, package_short_name := gsub("[r,R]espiratory", "resp", package_short_name)]

package_metadata[package_short_name == "Sepsis (Non- maternal & neonatal sepsis)", package_short_name := "Sepsis (Non-maternal & neonatal)"]
package_metadata[package_short_name == "All, Ill Defined code for causes of death", package_short_name := "All, Ill Defined code"]
package_metadata[package_short_name == "Fluid, Electrolyte, Acid Base Disorders", package_short_name := "Fluid, Electrolyte, AB Disorders"]
package_metadata[package_short_name == "Primary or secondary Liver Cancer Unsp", package_short_name := "Pri or sec Liver Cancer Unsp"]
package_metadata[package_short_name == "Und intent poison by multiple or Unsp drug", package_short_name := "Und intent poison by Unsp drug"]
package_metadata[package_short_name == "Und intent shoot by rifle & larger firearm", package_short_name := "Und intent shoot by larger firearm"]

# look at the short names to confirm none are "too long"
unique(package_metadata[order(nchar(package_short_name)),]$package_short_name)

write.xlsx(package_metadata, paste0(package_dir, "package_metadata.xlsx"))
message("1/3 package metadata file saved!")

#--
# Map RDP target codes
#--

# pull in the rdp target codes from all packages
extract_rdptarget_codes <- function(code_system, package_dir){
  
  package_list <- unlist(rjson::fromJSON(file = paste0(package_dir,code_system, "/_package_list.json")))
  
  metadata <- rbindlist(lapply(package_list, function(p){
    
    target <- as.data.table(arrow::read_parquet(paste0(package_dir, code_system,"/", p,"/target.parquet")))
    target <- target[grepl('rdptarget', icd_code) | grepl("acause", icd_code)]
    target[, shared_group_id:=NULL]
    target[, package_id := p]
    
    
    return(target)
    
  }))
  
  metadata[, code_system := code_system]
  return(metadata)
}

icd9 <- extract_rdptarget_codes("icd9", package_dir)
icd10 <- extract_rdptarget_codes("icd10", package_dir)

both_package <- rbind(icd9, icd10)
both <- unique(both_package[,package_id := NULL])[order(icd_code)]

#--- read in Dex cause list and see how many map
both[, acause := gsub("rdptarget_", "", gsub("acause_", "", icd_code))]

both[grepl("neo_liver_", icd_code), `:=` (acause = "neo_liver")]
both[grepl("ckd_diabetes", icd_code), `:=` (acause = "ckd")]
both[grepl("cvd_cmp", icd_code), `:=` (acause = "cvd_cmp")]
both[grepl("rdptarget_diabetes$", icd_code), `:=` (acause = "diabetes_typ1")]  # in ICD9 it's clear that the COD team uses rdptarget_diabetes_typ2 and rdptarget_diabetes indicating that the "diabetes" is type1
#icd9[grepl("rdptarget_diabetes", icd_code) & package_id %in% c(158, 245, 1406)]
both[grepl("gyne*", icd_code), `:=` (acause = "gyne")]  
both[grepl("hepatitis_e", icd_code), `:=` (acause = "_infect_agg")]  
both[grepl("inj_trans_road", icd_code), `:=` (acause = "inj_trans")]  
both[grepl("hepatitis_e", icd_code), `:=` (acause = "_infect_agg")]  
both[grepl("neo_leukemia_ll_chronic", icd_code), `:=` (acause = "neo_leukemia")]  
both[grepl("neo_nmsc", icd_code), `:=` (acause = "neo_nmsc")]  
both[grepl("neo_gallbladder", icd_code), `:=` (acause = "neo_gallbladder")]  
both[grepl("neo_ben", icd_code), `:=` (acause = "neo_other_benign")]  
both[grepl("diarrhea", icd_code), `:=` (acause = "_enteric_all")]  
both[grepl("intest_typhpartyph", icd_code), `:=` (acause = "_enteric_all")]  
both[grepl("ntd", icd_code), `:=` (acause = "_ntd")]  
both[grepl("malaria", icd_code), `:=` (acause = "_ntd")]  
both[grepl("std_", icd_code), `:=` (acause = "std")]  
both[grepl("urinary_", icd_code), `:=` (acause = "urinary")]  
both[grepl("unintent", icd_code), `:=` (acause = "_unintent_agg")]  
both[grepl("inj_poisoning", icd_code), `:=` (acause = "_unintent_agg")]  


both <- merge(both, dex_causelist[ ,.(acause, cause_name, family, is_dex_cause = 1)], by = "acause", all.x = T)
#both[is.na(is_dex_cause)]

both <- both[!is.na(is_dex_cause)]

both <- both[, .(code_system, icd_code, icd_name = icd_code, acause, is_dex_cause)]

# add on injury placeholders!
inj_causes <- c("_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", 
                "inj_suicide")
add_codes <- data.table(icd_code = paste0("adjusted_", inj_causes), code_system = c(rep("icd10", times = length(inj_causes)), 
                                                                                    rep("icd9", times = length(inj_causes))), icd_name = c("Adjusted during injury adjust"), acause = inj_causes, is_dex_cause = 1)

both <- rbind(both, add_codes)

write.csv(both, paste0(package_dir, "/all_rdptarget_codes.csv"), row.names = F)
message("2/3 rdptarget file saved!")

#--
# Save cause restrictions!
#--

# combine icd code map with the additional rdp target codes
full_rdp_code_map <- rbind(icd_code_map, both, fill = T)


##  Make icd level age/sex restrictions based on cause map and age/sex restrictions
icd_restrictions <- merge(full_rdp_code_map, cause_restrictions, by = c("acause"), allow.cartesian = T)

# format this way to make evaluating restrictions easier
icd_restrictions[male == 1 & female ==0, sex_restrict := "(sex==1.0)"]
icd_restrictions[male == 0 & female ==1, sex_restrict := "(sex==2.0)"]
icd_restrictions[, age_start_restrict := paste0("(age>=", age_start)]
icd_restrictions[age_end < 95, age_end_restrict := paste0("age<=", age_end,")")]

icd_restrictions[is.na(sex_restrict) & is.na(age_end_restrict), restrictions := paste0(age_start_restrict, ")")]
icd_restrictions[is.na(sex_restrict) & !is.na(age_end_restrict), restrictions := paste0(age_start_restrict, " and ", age_end_restrict)]
icd_restrictions[!is.na(sex_restrict) & is.na(age_end_restrict), restrictions := paste0(age_start_restrict, ") and ", sex_restrict)]
icd_restrictions[!is.na(sex_restrict) & !is.na(age_end_restrict), restrictions := paste0(age_start_restrict, " and ", age_end_restrict, " and ", sex_restrict)]


icd_restrictions <- icd_restrictions[acause!="_gc", .(toc, icd_code, include, acause, restrictions, code_system)]

write.csv(icd_restrictions, paste0(package_dir, "/icd_cause_map.csv"), row.names = F)
message("3/3 restrictions file saved!")
