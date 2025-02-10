#--------------------------------------------------------------------#
# - Launch step 1 of MDCD sample denom  -------------#
# Run time: Launches jobs quickly but some states can take 1-2 hrs to run
# Purpose: Parallelizes the sample denom processing by state and year
# AUTHOR(S): Meera Beauchamp, Sawyer Crosby, Drew DeJarnatt
#--------------------------------------------------------------------#
rm(list = ls())
library(data.table)
library(tidyverse)
suppressMessages(lbd.loader::load.containing.package())
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
user<-Sys.info()[["user"]]
'%ni%' <- Negate('%in%')

timestamp <- Sys.Date()
log_dir <- paste0("FILEPATH",user,"FILEPATH", timestamp, "/")

## delete log
unlink(log_dir, recursive = TRUE)

states=c('AL','AK','AZ','AR','CA','CO','CT','DC','DE','FL',
         'GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
         'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH',
         'NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
         'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','NA')
#  Parallelize by year and state, TAF script
m<-'50G'
TAF_grid <- tidyr::crossing(state = states, year = c(2016, 2019)) %>% 
 as.data.table()
TAF_grid[, mem := m] 
TAF_grid[, timestamp := timestamp]
TAF_grid[, task_id := 1:.N, by = c('mem')]
TAF_job_path <- paste0("FILEPATH", timestamp, '.csv')
write.csv(TAF_grid, TAF_job_path, row.names = F)

jid <- SUBMIT_ARRAY_JOB(
  paste0("TAF"),
  script = paste0(here, "/MDCD_TAF.R"),
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "long.q",
  n_jobs = nrow(TAF_grid),
  memory = m,
  threads = 10,
  time = "08:00:00",
  user_email = paste0(Sys.info()['user'], "@uw.edu"),
  archive = F,
  args = c(TAF_job_path)
)
print(paste0("TAF_", m, ": ",nrow(TAF_grid),  ' tasks', " ---> ", jid))

#  Parallelize by year and state
MAX_grid <- rbind(data.table(state = states, year_id = 2000),
                  data.table(state = setdiff(states, c("KS", "ME")), year_id = 2010),
                  data.table(state = c('CA','GA','IA','ID','LA','MI','MN','MO','MS','NJ',
                                       'PA','SD','TN','UT','VT','WV','WY','NA'
                  ), year_id = 2014))
MAX_grid[, timestamp := timestamp]
MAX_grid[, task_id := 1:.N]
MAX_grid[, mem := "85G"]
m <- '20G'

MAX_job_path <- paste0("FILEPATH", timestamp, '.csv')
write.csv(MAX_grid, MAX_job_path, row.names = F)
jid <- SUBMIT_ARRAY_JOB(
  paste0("MAX"),
  script = paste0(here, "/MDCD.R"),
  error_dir = log_dir,
  output_dir = log_dir,
  queue = "all.q",
  n_jobs = nrow(MAX_grid),
  memory = m,
  threads = 2,
  time = "04:00:00",
  user_email = paste0(Sys.info()['user'], "@uw.edu"),
  archive = F,
  args = c(MAX_job_path)
)

print(paste0("MAX", ": ",nrow(MAX_grid),  ' tasks', " ---> ", jid, ' mem: ', m))
