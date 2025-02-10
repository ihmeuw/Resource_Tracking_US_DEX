library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

# add argument to run for chia cms data
is_chia <- 1

username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
hop_ed_script <- paste0(here, '/hop_ed.R')

if(is_chia == 1){
  years <- c(2022)
  
  hop_params <- data.table(year = years,
                           part_c = 0)
  
  hop_param_path <- paste0("FILEPATH/hop_params.csv")
  fwrite(hop_params,hop_param_path)
  mem = "5G"
  t = "00:30:00"
} else {
  
  hop_params <- rbind(data.table(year = c(2000,2008:2017,2019),
                                 part_c = 0), 
                      data.table(year=c(2016,2019), part_c=1))
  
  hop_param_path <- paste0("FILEPATH/hop_params.csv")
  fwrite(hop_params,hop_param_path)
  mem = "100G"
  t = "6:00:00"
}




SUBMIT_ARRAY_JOB(
  name = 'hop_ed',
  script = hop_ed_script,
  queue = "all.q", 
  memory = mem,
  threads = "1",
  time = t, 
  throttle = 14,
  n_jobs = nrow(hop_params),
  args = c(hop_param_path, is_chia)) 


