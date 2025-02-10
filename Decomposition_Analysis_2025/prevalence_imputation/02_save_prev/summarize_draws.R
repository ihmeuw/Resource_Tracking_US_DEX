##############################################
#  Summarize prevalence draws and save out mean,lower,uppers
#
#  Authors: Drew DeJarnatt and Haley Lescinsky
#
##############################################

# HEADER
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

code_dir <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_dir)


library(data.table)
library(arrow)
library(dplyr)
'%nin%' <- Negate('%in%')

save_dir <- "/FILEPATH/"

if(interactive()){
  # ARGUMENTS FOR RUNNING INTERACTIVELY
  version <- 'XX' 

}else{
  # ARGUMENTS PULLED THROUGH FROM LAUNCHER
  args <- commandArgs(trailingOnly = TRUE)
  message(args)
  
  version <- args[1]
}

draw_path <- paste0(save_dir,version, "/prev_agesex_draws")
means_path <- paste0(save_dir,version, "/prev_agesex")

cause_path <- list.files(paste0(save_dir, version), pattern = 'included_causes_n', full.names = T)
causes <- fread(cause_path)

if(!dir.exists(draw_path)){
  stop("This version doesn't have draws saved!")
}

for(y in c(2010:2019)){
  
  
  print(y)
  cause_summary <- data.table()
  
  t0 <- Sys.time()
  for(c in causes$acause){

  message(c)
  

  draws <- open_dataset(paste0(draw_path, "/year_id=",y)) %>% filter(draw > 0 & acause == c) %>% 
    group_by(acause, ushd_acause, sex_id, age_group_years_start, state_name, state, location, pop, case_def) %>%
    summarize(prev_upper = quantile(prev, 0.975),
              prev_lower = quantile(prev, 0.025),
              prev = mean(prev)) %>% collect() %>% as.data.table()

  draws[, prev_rate := prev / pop]
  draws[, prev_rate_lower := prev_lower / pop]
  draws[, prev_rate_upper := prev_upper / pop]
  
  cause_summary <- rbind(draws, cause_summary)
  
  }
  
  cause_summary[, year_id := y]
  
  setcolorder(cause_summary, c('acause','ushd_acause','sex_id','age_group_years_start','state_name','state','location', 'prev','prev_rate','pop','case_def'))
  
  write_dataset(cause_summary, paste0(means_path), partitioning = 'year_id')
  print(Sys.time() - t0)
  
}

