library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
toc_carrier_script <- paste0(here,'/carrier_toc.R')


is_chia <- 1

if(is_chia != 1){
carrier_toc_params <- rbind(data.table(year = c(2000,2010,2014:2016,2019),
                               part_c = 0), data.table(year=c(2016,2019), part_c=1))

carrier_toc_param_path <- paste0("FILEPATH/carrier_toc_params.csv")
fwrite(carrier_toc_params,carrier_toc_param_path)
mem <- "120G"
thrd <- "5"
tm <- "6:00:00"

} else {
carrier_toc_params <- data.table(year = c(2022), part_c = 0)
carrier_toc_param_path <- paste0("FILEPATH/carrier_toc_params.csv")
fwrite(carrier_toc_params,carrier_toc_param_path)
mem <- "40G"
thrd <- "3"
tm <- "1:30:00"
}




SUBMIT_ARRAY_JOB(
  name = 'carrier_toc_map',
  script = toc_carrier_script,
  queue = "all.q", 
  memory = mem,
  threads = thrd,
  time = tm, 
  throttle = 14,
  archive = T,
  n_jobs = nrow(carrier_toc_params),
  args = c(carrier_toc_param_path, is_chia)) 


