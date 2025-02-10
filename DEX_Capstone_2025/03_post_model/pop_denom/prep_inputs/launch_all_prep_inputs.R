# launch all prep scripts
# takes approx 15 minutes for all to run (acs data prep is longest  - 10ish min)

rm(list = ls())
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)


# make folders
working_dir <- "FILEPATH"

dir.create(paste0(working_dir, "FILEPATH"))
dir.create(paste0(working_dir, "FILEPATH"), recursive = T)
dir.create(paste0(working_dir, "FILEPATH"), recursive = T)

#
script_list <- c("acs_data_prep.R", "acs_state_envelope_rates.R", "cms_rif_prep.R", "sahie_insurance_rates.R")


jid <- "XX" # placeholder for first one

for(s in script_list){
  
  j_name <- paste0(which(script_list == s), "_pd_inputs_", gsub(".R","", s))
  
  jid <<- SUBMIT_JOB(name = j_name,
                     script = paste0(code_path,"/", s),
                     queue = "long.q",
                     memory = "50G",
                     threads = 3, 
                     archive = T,
                     args = c(working_dir),
                     hold = jid
  )
  print(paste0(s, " -> ", jid))
  
}

