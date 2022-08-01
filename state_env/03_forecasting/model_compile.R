# Compile the results of a set of models to compare OOS RMSE.
# Requires input argument of which models to compile - aggregate, payer, toc
# Uses most recent date for any model set
# USERNAME, Nov 2021

pacman::p_load(data.table, tidyverse, parallel)

args <- commandArgs(trailingOnly = TRUE)[1]
print(args)
model_type <- args[[1]][1] %>% print()
dir <- "FILEPATH"

check_covs_aggregate <- function(model_obj){
  print(model_obj)
  load(model_obj)
  coefs <- summary(m)$coefficients %>% as.data.table(keep.rownames = TRUE)
  model_id <- model_obj %>%
    str_extract("_\\d{1,9}") %>%
    str_remove("_")
  
  # check if the year predictor is significant, if it's in the model
  if(!("year_id" %in% coefs$rn)){
    significant_year <- NA
  }else if(coefs[rn == "year_id"]$`Pr(>|t|)` < 0.05){
    significant_year <- TRUE
  }else{
    significant_year <- FALSE
  }
  
  # check if all of the shea covariates are positive
  shea_covs <- coefs$rn[!str_detect(coefs$rn, "pop_fr|year_id|population|location|Intercept|us_spending")]
  # print(shea_covs)
  bad_coefs <- coefs[rn %in% shea_covs & Estimate < 0]$rn
  if(length(bad_coefs) > 0){
    bad_shea_coefs <- paste(bad_coefs, collapse = ", ")
  }else{
    bad_shea_coefs <- NA
  }
  return(data.table(model = as.integer(model_id), 
                    significant_year = significant_year, 
                    bad_shea_coefs = bad_shea_coefs, 
                    oos_rmse = rmse))
}

if(model_type == "aggregate"){
  
  exists <- Sys.glob(paste0(dir,"model_*.RData"))
  table <- fread(paste0(dir,"/state_dep_var_table.csv"))
  info <- rbindlist(mclapply(exists, check_covs_aggregate, mc.cores = 12))
  info$model <- as.numeric(info$model)
  table <- merge(table, info, by="model", all.x = TRUE)
  
  fwrite(table, paste0(dir,"/aggregate_model_rmse.csv"))

}else if(model_type == "payer"){
  
  exists <- Sys.glob(paste0(dir,"model_set_id*.RData"))
  table <- fread("FILEPATH/state_dep_var_table_payer.csv")
  rmse <- rbindlist(lapply(exists, function(mod_objs){
    print(mod_objs)
    load(mod_objs)
    setDT(oos_rmse)
    return(oos_rmse)
  }))
  rmse[, model := as.integer(model)]
  
  table <- merge(table, rmse, by="model", all.x = TRUE)
  fwrite(table, paste0(dir,"/payer_model_rmse.csv"))
  
}else if(model_type == "toc"){
  
  exists <- Sys.glob(paste0(dir,"model_set_id*.RData"))
  table <- fread("FILEPATH/state_dep_var_table_toc.csv")
  rmse <- rbindlist(lapply(exists, function(mod_objs){
    print(mod_objs)
    load(mod_objs)
    setDT(oos_rmse)
    return(oos_rmse)
  }))
  rmse[, model := as.integer(model)]
  
  table <- merge(table, rmse, by="model", all.x = TRUE)
  fwrite(table, paste0(dir,"/toc_model_rmse.csv"))
  
}

