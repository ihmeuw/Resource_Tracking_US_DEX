##---------------------------------------------------
#  Check output dir for error logs
#
#  Author: Haley Lescinsky
#  
##---------------------------------------------------

pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)


# Arguments
#-----------------------------------------------------------------------------------
if(interactive()){
  
  
  log_dir <- "/FILEPATH/"
  map_version <- "/FILEPATH/map_version_XX/"
  apply <- 1
  run_id <- 'XX'
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  map_version <- args[1]
  log_dir <- args[2]
  apply <- args[3]
  run_id <- args[4]
}

map_part <- ifelse(apply == 0, "make", "apply")

# Look at error logs
#----------------------------------------------------------------------------------
error_logs <- list.files(log_dir, recursive = T, pattern = "\\.e")

errors <- rbindlist(lapply(error_logs, function(e){
  
  txt <- readLines(paste0(log_dir, e))
  txt <- txt[txt %like% "[e,E]rror"]
  if(length(txt) > 0){
    return(data.table(file = e, error = txt))
  }
  
}))

print(paste0(nrow(errors), " jobs had an error (", round((nrow(errors)/length(error_logs))*100, digits = 0), "% of all jobs)"))

unique(errors$error)

if(nrow(errors) > 0 ){
  dir.create(paste0(map_version, "/errors/"))
  
  if(map_part == 'apply'){
    write.csv(errors, paste0(map_version, "/errors/", map_part, "_errors_run_", run_id, ".csv"), row.names = F)
  }
  
  
}
