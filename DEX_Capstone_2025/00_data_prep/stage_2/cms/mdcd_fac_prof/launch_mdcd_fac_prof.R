## ------------------------------------------------
## 1. Set environment
## ------------------------------------------------
username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

source(paste0('FILEPATH/R/cluster_utils.R'))



Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

'%ni%' <- Negate('%in%')

states <- fread('FILEPATH')
states_reg <- states[abbreviation %ni% c('CA','NY','PA','TX','FL')]$abbreviation
states_extra_mem <- states[abbreviation %in% c('CA','NY','PA','TX','FL')]$abbreviation

outdir <- 'FILEPATH'

if (!(dir.exists(outdir))){
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
}
parts1 <- str_remove(list.files('FILEPATH'), 'partition=')
parts2 <- str_remove(list.files('FILEPATH'), 'partition=')
parts <- c(parts1, parts2) %>% unique()
parts<-parts[!(parts %in% c('_SUCCESS'))]

params <- expand.grid(state = states_reg,
                      part = parts,
                      year = c(2016,2019),
                      outdir = outdir ) %>% as.data.table()



extra_mem_params <- expand.grid(state = states_extra_mem,
                                part = parts,
                                year = c(2016),
                                outdir = outdir ) %>% as.data.table()


param_path <- paste0("FILEPATH")
fwrite(params,param_path)
param_path_extra_mem <- paste0("FILEPATH")
fwrite(extra_mem_params,param_path_extra_mem)

script_path <- paste0(here, '/mdcd_fac_prof.R')
SUBMIT_ARRAY_JOB(
  name = 'mdcd_fac_prof',
  script = script_path,
  queue = "all.q",
  memory = "50G",
  threads = "4",
  time = "6:00:00", 
  throttle = 350,
  n_jobs = nrow(params),
  args = param_path) 

SUBMIT_ARRAY_JOB( 
  name = 'mdcd_fac_prof_big',
  script = script_path,
  queue = "all.q", 
  memory = "250G",
  threads = "6",
  time = "6:00:00", 
  throttle = 100,
  n_jobs = nrow(extra_mem_params),
  args = param_path_extra_mem) 


##CHECK 
dir <-  paste0('FILEPATH/',timestamp,'/') # from summary file saved
files <- list.files(dir)
all <- data.table()
for (i in files){
  dt <- fread(paste0(dir, i))[1]
  dt$part <- i
  all <- rbind(all,dt)
}

all[,prof_sum:= sum(prof),by = .(st,yr)]
all[,fac_sum:= sum(fac),by = .(st,yr)]
all[,tot_sum:= sum(total),by = .(st,yr)]

check <- unique(all[,.(tot_sum,prof_sum,fac_sum, yr,st)])
check[,percent_p:=prof_sum/tot_sum]
check[,percent_f:=fac_sum/tot_sum]
check[,check:= percent_p+percent_f]