##--------------------------------------------------
#  Update/'knockout' GBD CoD packages to align with DEX ICD Code map
# 
#  Author: Haley Lescinsky
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

residual_code <- "asr" # placeholder only
gc_only <- F # we want to have nec AND gc codes as inputs; keep as F
nec_packages <- fread("/FILEPATH/nec_packages.csv")

dex_custom_packages <- c(4032, 4035, nec_packages$package_id)

## Parameters  ------------------------------------------------------

if(interactive()){
  
  # USE DB
  phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

  # TEST PARAMS
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
cod_package_folder <- config$RDP$cod_input_package_dir
dex_custom_package_folder <- paste0(config$RDP$package_helper_dir, "/dex_packages/")

dex_icd_map <- parsed_config(config, "CAUSEMAP", map_version_id = as.character(icd_map_version_id))$icd_map_path
dex_restrictions <- config$CAUSEMAP$restrictions_path

save_dir_version <- parsed_config(config, "RDP", map_version_id = as.character(packages_map_version_id))$package_dir
obdc_path <- paste0(config$RDP$package_helper_dir,  "/map_versions/.odbc.ini")

# Define function that knocks out packages
#----------------------------------------------------------------------------------------------
knockout_packages_for_dex <- function(lab, save_version, package_folder){
    
  ## 1) Prepare maps and packages
  dex_icd_map <- as.data.table(arrow::read_feather(dex_icd_map))
  dex_icd_map <- dex_icd_map[code_system==lab]
  package_list <- unlist(rjson::fromJSON(file = paste0(package_folder, "/package_list.json")))
  
  # reorder package list. Move ill-defined (1 or 158) to just before the final package (1401 or 1406)
  #                       And add the NEC packages just before the ill defined ones
  pos <- which(package_list %in% c(1, 158))
  allill <- package_list[pos]
  package_list <- package_list[- pos]
  package_list <- append(package_list, allill, after = length(package_list) - 1)
  
  # add NEC packages
  nec_package_lab <- nec_packages[code_system == lab, package_id]
  package_list <- append(package_list, nec_package_lab, after = length(package_list) - 2)
  
  gc_codes <- dex_icd_map[acause=="_gc", icd_code]
  # want to redistribute NEC codes too, but not inj_NEC since we have the injury adjustment
  nec_codes <- dex_icd_map[acause %like% "NEC" & acause!="inj_NEC", icd_code]
  other_causes <- c('cvd_other', 'digest_other', 'maternal_other', 'neo_other_cancer','neonatal_other', 'neuro_other', 'resp_other', 'sense_other', 'mental_other') # everything but msk_other; what about mental_agg ? 
  other_codes <- dex_icd_map[acause %in% other_causes, icd_code]
  possible_tc_codes <- dex_icd_map[!icd_code %in% c(gc_codes, nec_codes, other_codes) & !acause=="inj_NEC", icd_code]
  
  ## 2) pull in CoD maps
  
  if(lab=="icd9"){
    icd_code_query <- sprintf("select c.code_id, c.value
  from engine_room.maps_codelistversion clv
  join engine_room.maps_codelisthistory clh using(code_list_version_id)
  join engine_room.maps_code c using(code_id)
  where clv.code_system_id = 6  
  and clv.gbd_round_id = 7
  and clv.code_list_version_status_id = 2")
    
  }else{
    
    icd_code_query <- sprintf("select c.code_id, c.value
  from engine_room.maps_codelistversion clv
  join engine_room.maps_codelisthistory clh using(code_list_version_id)
  join engine_room.maps_code c using(code_id)
  where clv.code_system_id = 1  
  and clv.gbd_round_id = 7
  and clv.code_list_version_status_id = 2")
  }
  
  odbc <- ini::read.ini(obdc_path)
  con_def <- 'cod engine'
  myconn <- RMySQL::dbConnect(RMySQL::MySQL(),
                              database = odbc[[con_def]]$DATABASE,
                              host = odbc[[con_def]]$SERVER,
                              username = odbc[[con_def]]$USER,
                              password = odbc[[con_def]]$PASSWORD)
  
  
  code_id_map <- as.data.table(dbGetQuery(myconn, icd_code_query))
  setnames(code_id_map, "value", "icd_code")
  
  # Ensure consistent formatting for ICD codes
  code_id_map[, icd_code := gsub("\\.", "", icd_code)]
  
  ## package-code-list
  icd_code_package_df <- rbindlist(lapply(setdiff(package_list, c(1401, 1406, nec_packages$package_id)), function(p){
    metadata <- rjson::fromJSON(file = paste0(package_folder, p, "/metadata.json"))
    tmp_codes <- data.table(icd_code = code_id_map[code_id %in% metadata$codes, icd_code],
                            package_id = p)
    return(tmp_codes)
  }))
  icd_code_package_df <- merge(icd_code_package_df, dex_icd_map[acause == "_gc" | acause %like% "_NEC", .(icd_code, acause)], by = "icd_code", all.y = T)
  
  ## 3) Knockout each package and save
  asr_target_groups <- data.table()
  fix_package <- function(p){
    
    if(p %in% dex_custom_packages){
      
      # read in 4 files of package
      metadata <- rjson::fromJSON(file = paste0(dex_custom_package_folder, p, "/metadata.json"))
      source <- as.data.table(arrow::read_parquet(paste0(dex_custom_package_folder, p,"/source.parquet")))
      target <- as.data.table(arrow::read_parquet(paste0(dex_custom_package_folder, p,"/target.parquet")))
      wgt <- as.data.table(arrow::read_parquet(paste0(dex_custom_package_folder, p,"/wgt.parquet")))
      
      gcs <- unlist(metadata$codes)
      
    }else{
      
      # read in 4 files of package
      metadata <- rjson::fromJSON(file = paste0(package_folder, p, "/metadata.json"))
      source <- as.data.table(arrow::read_parquet(paste0(package_folder, p,"/source.parquet")))
      target <- as.data.table(arrow::read_parquet(paste0(package_folder, p,"/target.parquet")))
      wgt <- as.data.table(arrow::read_parquet(paste0(package_folder, p,"/wgt.parquet")))
      
      # Determine if the input codes to the package are codes DEX needs to redistribute
      gc_ids <- unlist(metadata$codes)
      gcs <- code_id_map[code_id %in% gc_ids, icd_code]
      
    }
    
  
    if(gc_only){
      input_codes <- intersect(gcs, c(gc_codes))
    }else{
      input_codes <- intersect(gcs, c(gc_codes, nec_codes))
    }
    
    #----------------------------------------------------------------------------------------------
    # SPECIAL TREATMENT FOR SPECIFIC PACKAGES
    #---------------------------------------------------------------------------------------------
    
    # for the all ill defined packages, remove the diabetes insulin use code and CVD NEC
    if(p == 158){
      input_codes <- setdiff(input_codes, c("V5867", dex_icd_map[acause=="cvd_NEC", icd_code]))
    }else if(p==1){
      input_codes <- setdiff(input_codes, c("Z794", dex_icd_map[acause=="cvd_NEC", icd_code]))
    }
    
    # for cvd unspecified package, add any CVD_NEC code that is not going to a proper cvd package
    if(p %in% c(213, 45)){
      # Add on unsp symp heart disease (114, 245) or all-ill-defined (158, 1) or residual (is.na)
      input_codes <- unique(c(input_codes, icd_code_package_df[acause=="cvd_NEC" & (is.na(package_id) | package_id %in% c(114, 245, 158, 1)), icd_code]))
    }
    
    # for unsp symp heart disease package, remove CVD NEC codes from it
    if(p %in% c(114, 245)){
      input_codes <- setdiff(input_codes,dex_icd_map[acause=="cvd_NEC", icd_code])
    }
    
    # for Shock, Cardiac Arrest, Coma packages (icd9 + icd10), remove convulsion codes
    if(p %in% c(4319, 4317)){
      input_codes <- setdiff(input_codes, dex_icd_map[icd_code %in% input_codes][acause == 'neuro_NEC' & icd_name %like% "[c,C]onvulsions"]$icd_code)
    }
    
    #----------------------------------------------------------------------------------------------
    # END SPECIAL TREATMENT FOR SPECIFIC PACKAGES
    #---------------------------------------------------------------------------------------------
    
    # If none of the input codes should be redistributed, ignore package
    if(length(input_codes)==0){
      return()
    }
    print(p)
    
    # Reduce input code set to just those that intersect with DEX cause map
    #    Also use true codes and not code_ids
    metadata$codes = input_codes
    
    # only keep weight group ids that are for all countries or just USA to retain smaller detail
    drop_ids1 <- source[variable=="country_id" & operator=="==" & value!="102", shared_wgt_group_id]
    drop_ids2 <- source[variable=="region_id" & operator=="==" & value!="100", shared_wgt_group_id]
    drop_ids3 <- source[variable=="super_region_id" & operator=="==" & value!="64", shared_wgt_group_id]
    
    source <- source[!(shared_wgt_group_id %in% c(drop_ids1, drop_ids2, drop_ids3))]
    
    wgt <- wgt[shared_wgt_group_id %in% source$shared_wgt_group_id]
    
    # merge target codes onto target code_ids if we need to 
    if(!"icd_code" %in% colnames(target)){
      target <- merge(target, code_id_map[,.(code_id, icd_code)], by = "code_id", all.x = T)
    }
     if(nrow(target[is.na(icd_code)]) > 0){
      print(target[is.na(icd_code)])
      stop("code_id is missing from the database code_id map!")
    }
    
    # only keep targets that intersect with DEX cause map and are *not* GC or NEC
    #  also keep rdptarget codes that map to a DEX cause. The 6 here are known to not map, so we drop now
    orig_target_groups <- unique(target$shared_group_id)

    target <- target[icd_code %in% possible_tc_codes | (icd_code %like% "rdp" & !(icd_code %in% c("rdptarget__inj", 
                                                                                                 "rdptarget_digest",
                                                                                                 "rdptarget_hepatitis",
                                                                                                 "rdptarget_maternal",
                                                                                                 "rdptarget_neonatal", 
                                                                                                 "rdptarget_resp")))]
    target[, code_id := NULL]
    
    # confirm there are at least some non-empty target groups
    if(length(target$shared_group_id)==0){
      stop(paste0(p, " has no remaining target groups after merging with DEX hierarchy"))
    }
    
    # if empty target group, use residual code as placeholder
    #     the map will only redistribute to residual code if across a full proportion group there are no target codes
    empty_target_group <- setdiff(orig_target_groups, target$shared_group_id)
    
    if(length(empty_target_group) > 0 ){
      
      message("missing target group!")
      target <- rbind(target, data.table(shared_group_id = empty_target_group, 
                                         icd_code = residual_code))
      
      target_tmp <- as.data.table(arrow::read_parquet(paste0(package_folder, p,"/target.parquet")))
      target_tmp <- merge(target_tmp, code_id_map[,.(code_id, icd_code)], by = "code_id", all.x = T)
      asr_target_groups <<- rbind(asr_target_groups, 
                                  target_tmp[shared_group_id %in% empty_target_group, .(package_id = p, shared_group_id, icd_code)])
      
    }
    
    wgt[, sum(wgt), by = "shared_wgt_group_id"]
    
    # Write the package to the save_dir
    dir.create(paste0(save_version, p), recursive = T)
    
    metadata <- rjson::toJSON(metadata)
    write(metadata, paste0(save_version, p, "/metadata.json"))
    arrow::write_parquet(source, paste0(save_version, p, "/source.parquet"))
    arrow::write_parquet(target, paste0(save_version, p, "/target.parquet"))
    arrow::write_parquet(wgt, paste0(save_version, p, "/wgt.parquet"))
  
    return(p)  
  }
  
  new_package_list <- lapply(package_list, fix_package)
  
  ## 3) Ensure that the final package has all codes as inputs
  new_packages <- list.dirs(save_version, full.names = F)
  final_package <- grep("140[1,6]", new_packages, value = T)
  metadata <- rjson::fromJSON(file = paste0(package_folder, final_package, "/metadata.json"))
  
  metadata$codes <-c(gc_codes, nec_codes)
  metadata <- rjson::toJSON(metadata)
  write(metadata, paste0(save_version, final_package, "/metadata.json"))
  
  ## 4) Save new package list
  write(toJSON(unlist(new_package_list)), paste0(save_version, "/_package_list.json"))
  
  ## 4.5) Save list of empty target groups
  asr_target_groups <- merge(asr_target_groups, dex_icd_map, by = "icd_code", all.x =T)
  write.csv(asr_target_groups, paste0(save_version, "/empty_target_groups.csv"), row.names = F)
  
}


# Call function to knock out packages for both icd 9 and icd 10
#----------------------------------------------------------------------------------------------

#icd 9
knockout_packages_for_dex(lab = "icd9",
                          save_version = paste0(save_dir_version, "/icd9/"),
                          package_folder = paste0(cod_package_folder, "6/"))

# icd10
knockout_packages_for_dex(lab = "icd10",
                          save_version = paste0(save_dir_version, "/icd10/"),
                          package_folder = paste0(cod_package_folder, "1/"))



