#----------------------------------------
# launch all pop denom scripts

# Takes several hours (especially mdcr_pop_denoms). The first 3 parts by far the longest (and longer with plot = T)
#----------------------------------------


rm(list = ls())
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(rstudioapi::getSourceEditorContext()$path)
setwd(code_path)

# make folders
working_dir <- "/FILEPATH/"
plot <- T


script_list <- c("01a_population_denom_regressions.R", 
                 "02a_time_trend_regression.R",
                 "0102b_mdcr_pop_denom.R", 
                 "03_combine_scale.R", 
                 "04_save_denoms.R")


jid <- "XX" # placeholder for first one

for(s in script_list){
  
  j_name <- gsub(".R","", s)
  
  use_code_path <- code_path
  
  jid <<- SUBMIT_JOB(name = j_name,
                     script = paste0(use_code_path,"/", s),
                     queue = "long.q",
                     memory = "100G",
                     threads = 1,
                     time = "10:00:00",
                     archive = T,
                     args = c(working_dir, plot),
                     hold = jid)
  print(paste0(s, " -> ", jid))
  
}

