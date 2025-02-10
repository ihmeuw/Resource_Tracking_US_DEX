# --------------------------------
#   Save convergence stats and evaluate model performance by launching jobs to
#                     calculate RMSE and MAD between raw inputs and modeled outputs
#
#   Author: Haley Lescinsky
# 
# --------------------------------

library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

code_dir <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_dir)


library(data.table)
library(arrow)
library(dplyr)

# -------------------------
run_id <- "XX"
set <- "setXX"
include_race <- F 
# -------------------------

# Set paths
model_dir <- paste0("/FILEPATH/model_version_", set, "/")
shiny_dir <- paste0("/FILEPATH/run_", run_id, "/data/model=", set, "/")

save_dir <- paste0(model_dir, "/rmse/")
if(!dir.exists(save_dir)){
  dir.create(save_dir)
}

# 1 -  convergence
conv <- fread(paste0(model_dir, "/post_run_job_params.csv"))

sum <- conv[geo != "national" & 
              convergence_note!="Payer source restriction drops all data" & 
              convergence_note!="Restriction: insufficient_data",  .N, by = c("convergence",  "metric", "geo")]
sum[, tot := sum(N), by = c("geo", "metric")]
sum <- sum[convergence == 'Yes', .(geo, metric, N/tot)]
write.csv(sum, paste0(save_dir, "convergence_summary.csv"), row.names = F)

print(sum)

# 2 - model performance (RMSE + MAD)

toc <- c("IP", "AM", "ED", 'RX', 'HH', 'NF', 'DV')
if (include_race == T){
  geo <- c("state", "national")
}else{
  geo <- c("state", "county")
}

metric <- c("spend_per_encounter", "encounters_per_person")

combos <- tidyr::crossing(toc, geo, metric) %>% as.data.table()
combos[, `:=` (model_dir = model_dir, 
               shiny_dir = shiny_dir)]


param_path <- paste0(save_dir ,"/param_path.csv")
write.csv(combos, param_path, row.names = F)


SUBMIT_ARRAY_JOB(
  name = 'pull_rmse', 
  script = paste0(code_dir, '/rmse_run.R'), 
  queue = "all.q", # string "all.q" or "long.q"
  memory = "10G", # string "#G"
  threads = "1", # string "#"
  time = "00:20:00", # string "##:##:##"
  archive = F,
  throttle = '21',
  n_jobs = nrow(combos),
  # test = T,
  hold = NULL,
  args = c(param_path))
