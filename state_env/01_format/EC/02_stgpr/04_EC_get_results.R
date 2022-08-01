################################################
#' @description Save results for Economics census data
################################################

rm(list = ls())
pacman::p_load(data.table, dplyr, matrixStats)

# if we modeled in per capita space
per_capita <- T


## *************************************************
## pick types to update (which do we want to save the latest results for?)

types_to_update <- c(
  # "envelope",
  # "dental",
  # "dme_optometry",
  # "home",
  # "hospital",
  "nursing"
  # "other_prof",
  # "other",
  # "phys_clin_service"
)

## *************************************************

## --------------------------
## setup
## --------------------------
source('FILEPATH/utility.r')
source("FILEPATH/get_population.R")

## adding dex nid
dex_nid <- fread("FILEPATH/economic_census.csv")
dex_nid <- unique(dex_nid$dex_nid)

## set working directory 
setwd("FILEPATH")

## get runinfo
RUNINFO <- fread("run_info.csv")

## get state location IDs
states <- fread("FILEPATH/states.csv")
states <- states[,.(location_id, state_name)]
loc_ids <- states$location_id

## get years
year_ids <- 1990:2019

pop <- get_population(
  gbd_round_id =7, 
  decomp_step = "iterative",
  location_id = loc_ids, 
  age_group_id = 22,
  sex_id = 3, 
  year_id = year_ids
)
pop <- pop[,.(location_id, year_id, population)]


## --------------------------
## for the given type, run stgpr, save runinfo
## --------------------------

for(i in types_to_update){
  
  ## filter runinfo
  runinfo <- RUNINFO[type == i]
  
  ## find which run_id you want for each type (default is latest)
  ## *******************************************
  run_id <- runinfo[model_index_id == max(model_index_id), run_id]
  date_run <- runinfo[model_index_id == max(model_index_id), date_run]
  date_run <- str_replace_all(date_run, "\\/", "-")
  ## *******************************************

  ## list all files in results directory
  results_dir <- paste0("FILEPATH/", run_id, "/draws_temp_0/")
  files <- list.files(results_dir)
  
  ## filter to state level
  files <- files[files %chin% paste0(loc_ids, ".csv")]
  
  ## function to read, melt, and select
  read_stgpr_draws <- function(x){
    draw <- fread(x)
    draw <- melt(draw, id.vars = c("location_id", "year_id", "age_group_id", "sex_id"))
    draw <- draw[, .(location_id, year_id, variable, value)]
    return(draw)
  }
  
  ## read, melt, select from, and bind all results
  results <- rbindlist(lapply(paste0(results_dir, files), read_stgpr_draws))
  
  ## get input data, linear fit, and spacetime fit
  input <- fread(paste0(getwd(), "/01_input/", i, ".csv"))
  input <- input[,.(location_id, year_id, raw = val, is_outlier, ds_flag)]
  stage1 <- data.table(model_load(run_id, "stage1"))[,.(location_id, year_id, stage1)]
  stage2 <- data.table(model_load(run_id, "st"))[,.(location_id, year_id, stage2 = st)]
  stage1 <- stage1[location_id %in% loc_ids]
  stage2 <- stage2[location_id %in% loc_ids]

  if(per_capita == T){
    results <- merge(results, pop, all.x = T)
    stage1 <- merge(stage1, pop, all.x = T)
    stage2 <- merge(stage2, pop, all.x = T)
    input <- merge(input, pop, all.x = T)
    results[,value := (value*population)/10^6][,population := NULL]
    stage1[,stage1 := (stage1*population)/10^6][,population := NULL]
    stage2[,stage2 := (stage2*population)/10^6][,population := NULL]
    input[,raw := (raw*population)/10^6][,population := NULL]
    results[,nid := dex_nid]
  } else {
    results <- results[,.(nid = dex_nid, location_id, year_id, variable, value)]
  }
  
  ## make wide again (save storage space)
  results <- dcast(results, nid + location_id + year_id ~ variable, value.var = "value")
  
  ## merge in input data
  results <- merge(results, input, by = c("location_id", "year_id"), all.x = T)
  
  ## merge in stage 1
  results <- merge(results, stage1, by = c("location_id", "year_id"), all.x = T)
  
  ## merge in stage 2
  results <- merge(results, stage2, by = c("location_id", "year_id"), all.x = T)
  
  ## add means and uis
  draws <- paste0("draw_", 0:999)
  results[,c("mean", "lower", "upper") := .(
    rowMeans(.SD),
    rowQuantiles(as.matrix(.SD), probs = .025), 
    rowQuantiles(as.matrix(.SD), probs = .975)
  ), .SDcols = draws]
  
  ## merge in state name
  results <- merge(results, states, by = "location_id", all.x = T)
  
  ## write outputs and save it in archive folder by date
  fwrite(results, paste0(getwd(), "/03_output/", i, "/", date_run, "_", run_id, ".csv"))
  
  ## print
  print(paste0("Saved run for ", i))
}
