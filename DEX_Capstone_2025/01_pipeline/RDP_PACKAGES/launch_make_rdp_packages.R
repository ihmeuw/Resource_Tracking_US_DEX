##--------------------------------------------------
# Launch RDP PACKAGES
# 
#
# Author: Haley Lescinsky
#           
##--------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)

log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")

#-----------------------------------------------------------------------------
# Get and set up versions and arguments
#-----------------------------------------------------------------------------
override_package_version <- NA # NA or package version
overwrite_packages <- T

# locate config
config <- get_config()

# get best phase run
phase_run_id <- get_phase_run_id()[status == "Best", phase_run_id]

# get best icd map version
icd_map_version_id <- get_map_metadata(maps = list("CAUSEMAP"))[status == "Best", map_version_id]

# register new package version!
if(is.na(override_package_version)){
  packages_map_version_id <- register_map_version(related_map = "RDP_PACKAGES",
                                                  phase_run_id = phase_run_id,
                                                  description = "Run XX")
}else{
  packages_map_version_id <- override_package_version
}


#-----------------------------------------------------------------------------
# track log dir and output dir
#-----------------------------------------------------------------------------

package_path <- parsed_config(config, "RDP", map_version_id = as.character(packages_map_version_id))$package_dir


if(overwrite_packages & dir.exists(package_path)){
  
  confirm_delete_directory(package_path)
  
}else if(dir.exists(package_path)){
  rm(list = c(SUBMIT_JOB, package_path, log_dir))
  stop("package dir exists but overwrite set to F, please investigate!")
}
dir.create(package_path)

tmp_log_dir <- paste0(log_dir, "/RDP_PACKAGES/rdp_packages_mvid", packages_map_version_id, "")
if(dir.exists(tmp_log_dir)){
  confirm_delete_directory(tmp_log_dir)
}
dir.create(tmp_log_dir, recursive = T)


#-----------------------------------------------------------------------------
# SUBMIT JOBS
#-----------------------------------------------------------------------------

# make sure to run step0 if ICD map changed!

#----
step0_jid <- SUBMIT_JOB(paste0("00_custom_NEC_packages"),
                        script = paste0(code_path, "/pre/necs/make_nec_packages.R"),
                        error_dir = tmp_log_dir,
                        output_dir = tmp_log_dir,
                        queue = "all.q",
                        memory = "3G",  
                        threads = 1, 
                        time = "00:15:00",
                        archive = F,
                        args = c(icd_map_version_id)
)
print(step0_jid)

#----
step1_jid <- SUBMIT_JOB(paste0("01_RDP_packages"),
                            script = paste0(code_path, "/main/01_knockout_cod_packages.R"),
                            error_dir = tmp_log_dir,
                            output_dir = tmp_log_dir,
                            queue = "all.q",
                            memory = "3G",  
                            threads = 1, 
                            time = "00:15:00",
                            archive = F,
                            args = c(phase_run_id, icd_map_version_id, packages_map_version_id)
)
print(step1_jid)

#-----
step2_jid <- SUBMIT_JOB(paste0("02_RDP_packages"),
                        script = paste0(code_path, "/main/02_create_trunc_map.R"),
                        error_dir = tmp_log_dir,
                        output_dir = tmp_log_dir,
                        queue = "all.q",
                        memory = "3G",  
                        threads = 1, 
                        time = "00:15:00",
                        archive = F,
                        args = c(phase_run_id, icd_map_version_id, packages_map_version_id),
                        hold = step1_jid
)
print(step2_jid)

#-----
step3_jid <- SUBMIT_JOB(paste0("03_RDP_packages"),
                        script = paste0(code_path, "/main/03_make_package_summary.R"),
                        error_dir = tmp_log_dir,
                        output_dir = tmp_log_dir,
                        queue = "all.q",
                        memory = "3G",  
                        threads = 1, 
                        time = "00:15:00",
                        archive = F,
                        args = c(phase_run_id, icd_map_version_id, packages_map_version_id),
                        hold = step2_jid
)
print(step3_jid)


# Make the version as best in the db so future versions can pull it
mark_best(table = "map_version", table_id = packages_map_version_id)


cat(paste0('rdp make packages (map version ', packages_map_version_id, ')\n',
           '-00 NEC packages:      ', step0_jid, '\n',
           '-01 knockout packages: ', step1_jid, '\n',
           '-02 trunc map:         ', step2_jid, '\n',
           '-03 package summary:   ', step3_jid))
