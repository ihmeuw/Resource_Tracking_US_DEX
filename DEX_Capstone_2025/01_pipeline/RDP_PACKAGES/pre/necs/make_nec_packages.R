##--------------------------------------------------
# Make NEC packages, along family lines according to the DEX ICD Cause map
# 
#
#  Author: Haley Lescinsky
#           
##--------------------------------------------------


rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

overwrite_packages <- T

if(interactive()){

  cm_version <- get_map_metadata(maps = list("CAUSEMAP"))[status == "Best", map_version_id]
 
}else{
  
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  cm_version <- args[1]
  
}

# locate config
config <- get_config()
cod_package_dir <-  config$RDP$cod_input_package_dir
work_dir <- paste0(config$RDP$package_helper_dir, "/dex_package_helpers/nec/")
new_package_dir <- paste0(config$RDP$package_helper_dir, "/dex_packages/")


dex_icd_map <- arrow::read_feather(gsub("#", cm_version, config$CAUSEMAP$icd_map_path))  # only needed to identify insulin codes, so version doesn't really matter
causelist <- fread(config$CAUSEMAP$causelist_path)

dex_icd_map <- merge(dex_icd_map, causelist[, .(acause, family)], by = 'acause')

nec_map <- unique(dex_icd_map[acause  %like% "_NEC" & acause!='inj_NEC',.(acause, code_system)])
nec_map <- nec_map[order(code_system)]
# Give numeric name to match other packages, but use high starting value so these are clearly distinct from CoD packages
nec_map[, package_id := 6000:(6000 + .N - 1)]  

write.csv(nec_map[,.(code_system, package_id)], paste0(new_package_dir, "/nec_packages.csv"), row.names = F)

for(i in 1:nrow(nec_map)){
  
  nec <- nec_map[i,]
  icd_code_system_cod <- ifelse(nec$code_system == 'icd10',1,6) # 1 = icd10, 6 = icd9
  
  p <- nec$package_id
  package_folder <- paste0(new_package_dir, "/",p,"/")
  print(p)
  
  if(dir.exists(package_folder)){
    
    if(overwrite_packages==T){
      system(paste0('rm -r ', package_folder))
    }else{
      stop('overwrite set to F and this package already exists!')
    }
    
  }
  
  dir.create(paste0(new_package_dir, "/",p,"/"))
  
  
  input_codes <- dex_icd_map[acause == nec$acause & code_system == nec$code_system, icd_code]

  target_codes <- dex_icd_map[family ==  unique(dex_icd_map[acause == nec$acause]$family) & code_system == nec$code_system & acause != nec$acause, icd_code]

  # make metadata
  metadata <- list()
  metadata$codes <- c(input_codes)
  metadata$code_system_id <- ifelse(nec$code_system == 'icd10',1,6)
  metadata$package_id <- paste0(nec$package_id, "_dex")
  metadata$package_description <- paste0(nec$acause, " DEX package ", nec$code_system)
  metadata$create_targets <- T
  metadata <- rjson::toJSON(metadata)


  #
  #  source, target, and wgt use 'shared_wgt_group_id' and 'shared_group_id' that are a CoD thing. Not really needed in DEX RDP except to
  #            match the source - wgt- targets when applying RDP.
  #
  #  For these NEC packages, we don't need anything fancy here. We just want to take a list of codes and use the data frequencies.
  #        thus we are using 1 as the group_id everywhere. (wgt = 1 is important)


  # make source
  source <- data.table(shared_wgt_group_id = 1, shared_wgt_group_logic_set_id = 1, variable = 'age', operator = '>=', value = 0)

  # make wgt
  wgt <- data.table(shared_wgt_group_id = 1, wgt = 1, shared_group_id = 1)

  # make target
  target <- data.table(icd_code = target_codes, shared_group_id = 1)

  write(metadata, paste0(new_package_dir, p, "/metadata.json"))
  arrow::write_parquet(source, paste0(new_package_dir, p, "/source.parquet"))
  arrow::write_parquet(target, paste0(new_package_dir, p, "/target.parquet"))
  arrow::write_parquet(wgt, paste0(new_package_dir, p, "/wgt.parquet"))

}
