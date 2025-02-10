#----------------------------------------
#
#  Mark population denominator version best
#
#----------------------------------------


pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = 'FILEPATH')
suppressMessages(lbd.loader::load.containing.package())

# Set parameters
working_dir <- "/FILEPATH/"
working_dir_folders <- c("inputs", "intermediates", "denoms_true", "denoms_for_compile")
mark_best <- T

# make new function
new_version <- versioned_folder(dir = working_dir, make_folder = T, string = "")
print(paste0("Moving all current outputs into versioned folder: ", new_version))

# move folders + files into new version
folders <- list.files(working_dir, pattern = paste0(working_dir_folders, collapse = "|"), full.names = T)
for(i in folders){
  system(paste0("mv ", i, " ", working_dir, new_version, "/"))
}

# mark best
if(mark_best){
  
  mark_version_best(working_dir, new_version, label = "best")
  
}

