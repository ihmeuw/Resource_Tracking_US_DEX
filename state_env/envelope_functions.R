library(data.table)
library(tidyverse)
library(boot)
library(filesstrings)


source("FILEPATH/currency_conversion.R")

# Aggregate functions -------

get_train_data <- function(){
  df <- fread("FILEPATH/shea_covariates.csv")
  covs <- fread("FILEPATH/state_covariate_data.csv")
  covs <- dplyr::select(covs, c("location_id","year_id","pop65"))
  insurance <- fread("FILEPATH/cdhi_rates.csv")
  insurance <- insurance[,.(location_id, year_id, ins_mdcd, ins_mdcr, ins_private, uninsured)]
  df <- merge(df, covs, by=c("year_id","location_id"))
  df <- merge(df, insurance, by=c("year_id","location_id"), all.x = TRUE)

  df[,`:=`(tot_spending_log = log(tot_spending),
           pc_spending_log = log(pc_spending),
           fr_spending_logit = logit(fr_spending),
           fpc_spending_log = log(fpc_spending))]

  df[, index := .I]
  df[, pop_us := sum(population), by="year_id"]
  
  df[, mdcd_pc := mdcd_tot/population][, mdcr_pc := mdcr_tot/population][, meps_pc := meps_tot/population]
  df[, nes_pc := nes_tot/population][, ec_pc := ec_tot/population][, crt_pc := crt_tot/population]
  df[, aha_pc := aha_tot/population][, feds_pc := feds_tot/population][, bea_pc := bea_tot/population]
 
  df[, mdcd_us := sum(mdcd_tot), by="year_id"][, mdcd_fr := mdcd_tot/mdcd_us]
  df[, mdcr_us := sum(mdcr_tot), by="year_id"][, mdcr_fr := mdcr_tot/mdcr_us]
  df[, nes_us := sum(nes_tot), by="year_id"][, nes_fr := nes_tot/nes_us]
  df[, wages_us := sum(wages_tot), by="year_id"][, wages_fr := wages_tot/wages_us]
  df[, meps_us := sum(meps_tot, na.rm = TRUE), by="year_id"][, meps_fr := meps_tot/meps_us]
  df[, ec_us := sum(ec_tot), by="year_id"][, ec_fr := ec_tot/ec_us]
  df[, crt_us := sum(crt_tot), by="year_id"][, crt_fr := crt_tot/crt_us]
  df[, aha_us := sum(aha_tot), by="year_id"][, aha_fr := aha_tot/aha_us]
  df[, oop_us := sum(oop_tot), by="year_id"][, oop_fr := oop_tot/oop_us]
  df[, feds_us := sum(feds_tot), by="year_id"][, feds_fr := feds_tot/feds_us]
  df[, bea_us := sum(bea_tot), by="year_id"][, bea_fr := bea_tot/bea_us]
  df[, pop_fr := population/pop_us]
  
  df[, mdcd_pc_us := mdcd_us/pop_us, by="year_id"][, mdcd_fpc := mdcd_pc/mdcd_pc_us]
  df[, mdcr_pc_us := mdcr_us/pop_us, by="year_id"][, mdcr_fpc := mdcr_pc/mdcr_pc_us]
  df[, nes_pc_us := nes_us/pop_us, by="year_id"][, nes_fpc := nes_pc/nes_pc_us]
  df[, wages_pc_us := wages_us/pop_us, by="year_id"][, wages_fpc := wages_pc/wages_pc_us]
  df[, meps_pc_us := meps_us/pop_us, by="year_id"][, meps_fpc := meps_pc/meps_pc_us]
  df[, ec_pc_us := ec_us/pop_us, by="year_id"][, ec_fpc := ec_pc/ec_pc_us]
  df[, crt_pc_us := crt_us/pop_us, by="year_id"][, crt_fpc := crt_pc/crt_pc_us]
  df[, aha_pc_us := aha_us/pop_us, by="year_id"][, aha_fpc := aha_pc/aha_pc_us]
  df[, oop_pc_us := oop_us/pop_us, by="year_id"][, oop_fpc := oop_pc/oop_pc_us]
  df[, feds_pc_us := feds_us/pop_us, by="year_id"][, feds_fpc := feds_pc/feds_pc_us]
  df[, bea_pc_us := bea_us/pop_us, by="year_id"][, bea_fpc := bea_pc/bea_pc_us]
  
  df$location_id <- as.factor(df$location_id)
  df[,c("mdcd_us", "mdcr_us", "nes_us", "wages_us", "meps_us", "ec_us", "crt_us", "aha_us", "oop_us", "feds_us", "bea_us",
        "mdcd_pc_us", "mdcr_pc_us", "nes_pc_us", "wages_pc_us", "meps_pc_us", "ec_pc_us", "crt_pc_us", "aha_pc_us", 
        "oop_pc_us", "feds_pc_us", "bea_pc_us") := NULL]
  return(df)
}

run_model <- function(x, df){
  space <- str_split(x$dep_var,"_")[[1]][1]
  
  shea_covs <- c(str_split(x$shea_covs,",")[[1]])
  other_covs <- str_split(x$other_covs,",")[[1]]
  
  if(all(shea_covs == "")) shea_covs <- NULL
  if(all(other_covs == "")) other_covs <- NULL
  
  if(!is.null(shea_covs)){
    shea_covs <- paste0(shea_covs,"_",space)
  }
  
  if(!is.null(other_covs)){
    covs <- c(other_covs, shea_covs) 
  }else{
    covs <- shea_covs
  }
  if(x$time_trend) covs <- c(covs, "year_id")
  if(x$state_effect) covs <- c(covs, "as.factor(location_id)")
  covs <- paste(c(1, covs), collapse = " + ")
  eqn <- paste0(x$dep_var," ~ ",covs)
  mod <- lm(eqn, data = df, na.action = na.exclude)
  mod$eqn <- eqn
  
  return(mod)
}

pred_df <- function(model, pred_years, df = NA){
  dep_var <- model$terms[[2]]
  if(is.na(df)) df <- get_train_data()
  df <- df[year_id %in% pred_years]
  pred <- predict(model, newdata = df)
  df$pred <- pred
  df <- as.data.table(df)
  
  if(str_detect(dep_var, "_log$")){
    df$pred <- exp(df$pred)
  }else if(str_detect(dep_var, "_logit$")){
    df$pred <- inv.logit(df$pred)
  }
  
  # put prediction in per capita space
  if(str_detect(dep_var, "^tot_")){
    df$pred <- df$pred/df$population
  }else if(str_detect(dep_var, "^fr_")){
    df$pred <- df$pred*df$us_spending/df$population
  }else if(str_detect(dep_var, "^fpc_")){
    df$pred <- df$pred*df$us_spending_pc
  }
  
  return(df)
}

pred_df_diff <- function(model, anchor_year, pred_years, direction, df, train_orig){
  dep_var <- model$terms[[2]]
  df <- df[year_id %in% pred_years]
  pred <- predict(model, newdata = df)
  df$pred_delta <- pred
  df <- as.data.table(df)
  
  df <- merge(df[,.(location_id, year_id, pred_delta)], 
              select(train_orig, year_id, location_id, population, pop65, pop_fr, us_spending, pc_spending, us_spending_pc, dep = all_of(dep_var)),
              by=c("location_id","year_id"),
              all = TRUE)
  df <- df[year_id %in% c(anchor_year, pred_years)]
  df[year_id == anchor_year, pred := dep]
  df <- split(df, by="location_id")
  if(direction == "forwards"){
    df <- rbindlist(lapply(df, rowwise_delta_forwards))
  }else{
    df <- rbindlist(lapply(df, rowwise_delta_backwards))
  }

    if(str_detect(dep_var, "_log$")){
      df$pred <- exp(df$pred)
    }else if(str_detect(dep_var, "_logit$")){
      df$pred <- inv.logit(df$pred)
    }
    
    # put prediction in per capita space
    if(str_detect(dep_var, "^tot_")){
      df$pred <- df$pred/df$population
    }else if(str_detect(dep_var, "^fr_")){
      df$pred <- df$pred*df$us_spending/df$population
    }else if(str_detect(dep_var, "^fpc_")){
      df$pred <- df$pred*df$us_spending_pc
    }

  df[year_id != anchor_year]
  return(df)
}

# Payer functions ------
get_train_data_payer <- function(){
  df <- get_best_agg_model(bad_coefs = TRUE)$pred %>% unique()
  train <- get_train_data()
  df <- left_join(df, train[,.(year_id, location_id, ins_mdcd, ins_mdcr, ins_private, 
                        uninsured, ma_pct, mdcd_tot, mdcr_tot, meps_tot, oop_tot, feds_pc)])
  
  df <- df[,.(year_id, location_id = as.character(location_id), population, pop_fr, pop65, ins_mdcr, ins_mdcd, ins_private, 
              uninsured, ma_pct, feds_pc, mdcd_tot, mdcr_tot, meps_tot, oop_tot, tot_spending_pred = pred*population)]
  df <- melt(df, id.vars = c("location_id","year_id","population","pop_fr","pop65","ins_mdcr","ins_mdcd","ins_private",
                             "uninsured","ma_pct","feds_pc","tot_spending_pred"),
             measure.vars = c("mdcr_tot","mdcd_tot","meps_tot","oop_tot"), variable.name = "payer", value.name = "cov_tot")
  df[, payer := str_remove(payer,"_tot")]
  df[, cov_pr := cov_tot/tot_spending_pred][, cov_pr_logit := logit(cov_pr)]
  df <- unique(df)
  df <- dcast(df, location_id + year_id + population + pop_fr + pop65 + ins_mdcr + ins_mdcd + ins_private + uninsured + 
                ma_pct + feds_pc + tot_spending_pred ~ payer, value.var = c("cov_pr","cov_pr_logit"))
  setnames(df,c("cov_pr_mdcr","cov_pr_mdcd","cov_pr_meps","cov_pr_oop","cov_pr_logit_mdcr","cov_pr_logit_mdcd","cov_pr_logit_meps","cov_pr_logit_oop"),
           c("mdcr_pr","mdcd_pr","meps_pr","oop_pr","mdcr_pr_logit","mdcd_pr_logit","meps_pr_logit","oop_pr_logit"))
  
  # Get SHEA payer data
  shea <- fread("FILEPATH/SHEA_by_payer.csv")
  shea[, location_id := as.character(location_id)]
  shea <- merge(shea, unique(df[,.(location_id, year_id, population)]), by=c("location_id","year_id"))
  shea <- melt(shea, id.vars = c("location_id","year_id","tot_spending","population"), variable.name = "payer", value.name = "tot",
               measure.vars = c("tot_spending_mdcr","tot_spending_mdcd","tot_spending_priv","tot_spending_oop"))
  shea[, payer := str_remove(payer,"tot_spending_")]
  shea[, pr := tot/tot_spending][, pr_logit := logit(pr)] # Get fractional variables for models
  shea[, pc := tot/population]
  shea <- dcast(shea, location_id + year_id + tot_spending ~ payer, value.var = c("pr","pr_logit","pc"))
  setnames(shea,c("pr_mdcd","pr_mdcr","pr_oop","pr_priv",
                  "pr_logit_mdcd","pr_logit_mdcr","pr_logit_oop","pr_logit_priv",
                  "pc_mdcd","pc_mdcr","pc_oop","pc_priv"),
           c("pr_spending_mdcd","pr_spending_mdcr","pr_spending_oop","pr_spending_priv",
             "pr_spending_mdcd_logit","pr_spending_mdcr_logit","pr_spending_oop_logit","pr_spending_priv_logit",
             "pc_spending_mdcd","pc_spending_mdcr","pc_spending_oop","pc_spending_priv"))
  
  load("FILEPATH/states.RData")
  
  df <- merge(df, states[,.(location_id = as.character(location_id), abbreviation)], by="location_id", allow.cartesian = TRUE)
  df <- merge(df, shea, by=c("location_id","year_id"), all = TRUE)
  df <- df[year_id >= 2003]
  return(df)
}


# pred_df: takes a set of SUR models and creates a data table of their prections, given data to predict on
# used in model_processing
pred_df_payer <- function(m, p_df, model_type){
  if(!str_detect(model_type,"no oop")){
    pred_columns <- c("mdcr_pred","mdcd_pred","priv_pred","oop_pred")
    
    dt <- list(predict(m$eq[[1]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[2]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[3]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[4]], newdata=p_df, na.action = "na.exclude"))
    dt <- data.table(dt[1][[1]],dt[2][[1]],dt[3][[1]],dt[4][[1]])
  }else{
    pred_columns <- c("mdcr_pred","mdcd_pred","priv_pred")
    
    dt <- list(predict(m$eq[[1]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[2]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[3]], newdata=p_df, na.action = "na.exclude"))
    dt <- data.table(dt[1][[1]],dt[2][[1]],dt[3][[1]])
  }

  colnames(dt) <- pred_columns
  
  # Take logit models out of logit space
  if(str_detect(model_type, "logit")){
    dt <- mutate_all(dt, inv.logit)
  }
  
  # Inverse CLO for CLO models, raking for others
  if(str_detect(model_type, "CLO")){
    dt <- inv_clr(dt)
    p_df <- inv_clr(p_df, cols = c("pr_spending_mdcr","pr_spending_mdcd","pr_spending_priv","pr_spending_oop"))
  }else if(str_detect(model_type, "raking")){
    dt <- dt/rowSums(dt)
  }else if(str_detect(model_type,"no oop")){
    dt[, oop_pred := 1 - (mdcr_pred + mdcd_pred + priv_pred)]
  }
  
  dt <- cbind(dt, p_df)
  return(dt)
}

pred_df_diff_payer <- function(m, p_df, model_type, anchor_year, pred_years, direction){
  if(!str_detect(model_type,"no oop")){
    pred_columns <- c("mdcr_pred","mdcd_pred","priv_pred","oop_pred")
    
    dt <- list(predict(m$eq[[1]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[2]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[3]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[4]], newdata=p_df, na.action = "na.exclude"))
    dt <- data.table(dt[1][[1]],dt[2][[1]],dt[3][[1]],dt[4][[1]])
  }else{
    pred_columns <- c("mdcr_pred","mdcd_pred","priv_pred")
    
    dt <- list(predict(m$eq[[1]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[2]], newdata=p_df, na.action = "na.exclude"),
               predict(m$eq[[3]], newdata=p_df, na.action = "na.exclude"))
    dt <- data.table(dt[1][[1]],dt[2][[1]],dt[3][[1]])
  }
  colnames(dt) <- paste(pred_columns,"_delta", sep = "")
  
  dt <- cbind(dt, p_df[,.(location_id, year_id)])
  
  dt <- melt(dt, id.vars = c("location_id", "year_id"), variable.name = "payer", value.name = "pred_delta")
  dt[, payer := str_remove_all(payer,"_pred_delta")]
  dt <- unique(dt)
  
  df_orig <- get_train_data_payer()
  # CLO models - must apply CLO transform first
  if(model_type == "CLO"){
    df_orig <- clr(df_orig, cols = c("pr_spending_mdcr","pr_spending_mdcd","pr_spending_priv","pr_spending_oop"))
  }
  dt2 <- df_orig[year_id == anchor_year,.(location_id, year_id, pr_spending_mdcd, pr_spending_mdcr, 
                                                pr_spending_oop, pr_spending_priv)]
  dt2 <- melt(dt2, id.vars = c("location_id", "year_id"), variable.name = "payer", value.name = "pred")
  dt2[, payer := str_remove(payer, "pr_spending_")]
  dt <- merge(dt, dt2, by=c("location_id", "year_id", "payer"), all.x = TRUE)
  
  dt <- dt[year_id %in% c(anchor_year, pred_years)]
  
  dt <- split(dt, by=c("location_id","payer"))
  if(direction == "forwards"){
    dt <- rbindlist(lapply(dt, rowwise_delta_forwards))
  }else{
    dt <- rbindlist(lapply(dt, rowwise_delta_backwards))
  }
  dt <- dcast(dt[, payer := paste0(payer,"_pred")], location_id + year_id ~ payer, value.var = c("pred"))
  
  # Take logit models out of logit space
  if(str_detect(model_type, "logit")){
    dt[, (pred_columns) := lapply(.SD, inv.logit), .SDcols = pred_columns]
  }
  
  # Inverse CLO for CLO models, raking for others
  if(str_detect(model_type, "CLO")){
    dt <- inv_clr(dt, cols = pred_columns)
    p_df <- inv_clr(p_df, cols = c("pr_spending_mdcr","pr_spending_mdcd","pr_spending_priv","pr_spending_oop"))
  }else if(str_detect(model_type, "raking")){
    dt[, tot := sum(.SD), .SDcols = pred_columns, by=c("location_id","year_id")]
    dt[, (pred_columns) := lapply(.SD,"/",tot), .SDcols = pred_columns, by=c("location_id","year_id")]
    dt[, tot := NULL]
  }else if(str_detect(model_type,"no oop")){
    dt[, oop_pred := 1 - (mdcr_pred + mdcd_pred + priv_pred)]
  }
  
  dt <- merge(dt, df_orig, by=c("location_id","year_id"))
  return(dt)
}

# get_resid: transforms prediction into per capita, calculates residual and returns RMSE
# used in model_processing - expecting data that has already been put in linear space 
# and raked/CLO inverse-transformed as appropriate
get_resid_payer <- function(pred_df){
  pred <- copy(pred_df)
  # move to PC space
  pred_columns <- c("mdcr_pred","mdcd_pred","priv_pred","oop_pred")
  pred[, (pred_columns) := .SD * tot_spending/population, .SDcols = (pred_columns)]
  # calculate residual
  pred[, mdcr_resid := pc_spending_mdcr - mdcr_pred][, mdcd_resid := pc_spending_mdcd - mdcd_pred]
  pred[, priv_resid := pc_spending_priv - priv_pred][, oop_resid := pc_spending_oop - oop_pred]
  # calculate rmse
  rmse <- select(pred,ends_with("resid"))
  rmse$overall_resid <- rowSums(abs(rmse))
  rmse <- sqrt(summarize_all(rmse^2, mean))
  rmse <- setDT(rmse)
  names(rmse) <- str_replace(names(rmse),"resid","rmse")
  return(rmse)
}

# TOC functions ----
get_train_data_toc <- function(){
  
  dep_vars <- fread("FILEPATH/SHEA_by_toc.csv")
  dep_vars$location_id <- as.factor(dep_vars$location_id)
  dep_vars <- dep_vars[year_id >= 1999]
  
  agg_model <- get_best_agg_model(bad_coefs = TRUE)$pred
  train <- get_train_data()
  agg_model <- left_join(agg_model, dplyr::select(train, year_id, location_id, tot_spending, aha_tot, starts_with("ec_"), starts_with("crt_")))
  
  agg_model[, tot_spending_pred := pred*population]
  agg_model <- select(agg_model, location_id, year_id, tot_spending, tot_spending_pred, population, pop_fr, aha_tot,
                      starts_with("ec_"), starts_with("crt_")) %>% unique()
  agg_model <- melt(agg_model, id.vars = c("year_id","location_id","population","pop_fr","tot_spending","tot_spending_pred"), 
                    value.name = "total")
  agg_model <- agg_model[!(variable %in% c("ec_pc","ec_pu","ec_us","ec_fr","ec_pc_us","ec_fpc","crt_pc","crt_pu",
                                           "crt_us","crt_fr","crt_pc_us","crt_fpc","ec_hosp"))]
  agg_model[, cov_pr := total/tot_spending_pred][, cov_pr_log := log(cov_pr)]
  agg_model <- agg_model[cov_pr >= 0]
  agg_model <- dcast(agg_model, year_id + location_id + population + pop_fr + tot_spending + tot_spending_pred ~ variable,
                     value.var = c("cov_pr","cov_pr_log"))

  dep_vars <- merge(dep_vars, agg_model[,.(location_id, year_id, population)], by=c("location_id","year_id"), allow.cartesian = TRUE, all = TRUE)
  dep_vars[, pc_spending := tot_spending/population]
  dep_vars[, pr_spending := tot_spending/sum(tot_spending), by=c("state_name","year_id")]
  dep_vars[, pr_spending_log := log(pr_spending)]
  dep_vars <- dep_vars[!is.na(toc)]
  dep_vars <- dcast(dep_vars, state_name + year_id + location_id + state + abbreviation ~ toc, 
                    value.var = c("tot_spending","pc_spending","pr_spending","pr_spending_log"))
  dep_vars[, tot_spending_other := tot_spending_other + tot_spending_dme][, tot_spending_dme := NULL]
  dep_vars[, pc_spending_other := pc_spending_other + pc_spending_dme][, pc_spending_dme := NULL]
  dep_vars[, pr_spending_other := pr_spending_other + pr_spending_dme][, pr_spending_dme := NULL]
  
  df <- merge(dep_vars, agg_model, by=c("location_id","year_id"), all = TRUE) %>% unique()
  df <- df[year_id > 2003]
  return(df)
}
# pred_df: takes a set of SUR models and creates a data table of their prections, given data to predict on
# used in model_processing
pred_df_toc <- function(m, p_df, model_type){
  pred_columns <- c("dent_pred","hh_pred","hosp_pred","nf_pred",
                    "oprof_pred","other_pred","pharma_pred","phys_pred")
  
  dt <- list(predict(m$eq[[1]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[2]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[3]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[4]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[5]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[6]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[7]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[8]], newdata=p_df, na.action = "na.exclude"))
  dt <- data.table(dt[1][[1]],dt[2][[1]],dt[3][[1]],dt[4][[1]],dt[5][[1]],dt[6][[1]],
                   dt[7][[1]],dt[8][[1]])
  colnames(dt) <- pred_columns
  
  # Take logit models out of logit space
  if(str_detect(model_type, "log")){
    dt <- mutate_all(dt, exp)
  }
  
  # Inverse CLO for CLO models, raking for others
  if(str_detect(model_type, "CLO")){
    dt <- inv_clr(dt)
    p_df <- inv_clr(p_df, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                                          "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
  }else if(str_detect(model_type, "raking")){
    dt <- dt/rowSums(dt)
    # dt <- dt
  }
  
  dt <- cbind(dt, p_df)
  return(dt)
}

pred_df_diff_toc <- function(m, p_df, model_type, anchor_year, pred_years, direction){
  df_orig <- get_train_data_toc()
  if(model_type == "CLO"){
    df_orig <- clr(df_orig, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                                     "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
  }
  
  pred_columns <- c("dent_pred","hh_pred","hosp_pred","nf_pred",
                    "oprof_pred","other_pred","pharma_pred","phys_pred")
  p_df <- unique(p_df[!is.na(abbreviation)])
  
  dt <- list(predict(m$eq[[1]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[2]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[3]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[4]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[5]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[6]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[7]], newdata=p_df, na.action = "na.exclude"),
             predict(m$eq[[8]], newdata=p_df, na.action = "na.exclude"))
  dt <- data.table(dt[1][[1]],dt[2][[1]],dt[3][[1]],dt[4][[1]],dt[5][[1]],dt[6][[1]],
                   dt[7][[1]],dt[8][[1]])
  colnames(dt) <- paste(pred_columns,"_delta", sep = "")
  
  dt <- cbind(dt, p_df[,.(location_id, year_id)])
  
  dt <- melt(dt, id.vars = c("location_id", "year_id"), variable.name = "toc", value.name = "pred_delta")
  dt[, toc := str_remove_all(toc,"_pred_delta")]
  
  dt2 <- df_orig[year_id == anchor_year,.(location_id, year_id, pr_spending_dent,pr_spending_hh,pr_spending_hosp,pr_spending_nf,
                                                pr_spending_oprof,pr_spending_other,pr_spending_pharma,pr_spending_phys)]
  dt2 <- melt(dt2, id.vars = c("location_id", "year_id"), variable.name = "toc", value.name = "pred")
  dt2[, toc := str_remove(toc, "pr_spending_")]
  dt <- merge(dt, dt2, by=c("location_id", "year_id", "toc"), all.x = TRUE)
  
  dt <- dt[year_id %in% c(anchor_year, pred_years)]
  dt <- split(dt, by=c("location_id","toc"))
  if(direction == "forwards"){
    dt <- rbindlist(lapply(dt, rowwise_delta_forwards))
  }else{
    dt <- rbindlist(lapply(dt, rowwise_delta_backwards))
  }
  dt <- unique(dt)
  dt <- dcast(dt[, toc := paste0(toc,"_pred")], location_id + year_id ~ toc, value.var = c("pred"))
  
  # Take logit models out of logit space
  if(str_detect(model_type, "log")){
    dt[, (pred_columns) := lapply(.SD, exp), .SDcols = pred_columns]
  }
  
  # Inverse CLO for CLO models, raking for others
  if(str_detect(model_type, "CLO")){
    dt <- inv_clr(dt, cols = pred_columns)
    p_df <- inv_clr(p_df, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                                   "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
  }else if(str_detect(model_type, "raking")){
    dt[, tot := sum(.SD), .SDcols = pred_columns, by=c("location_id","year_id")]
    dt[, (pred_columns) := lapply(.SD,"/",tot), .SDcols = pred_columns, by=c("location_id","year_id")]
    dt[, tot := NULL]
  }
  
  dt <- merge(dt, df_orig, by=c("location_id","year_id"))
  return(dt)
}

# get_resid: transforms prediction into per capita, calculates residual and returns RMSE
# used in model_processing - expecting data that has already been put in linear space 
# and raked/CLO inverse-transformed as appropriate
get_resid_toc <- function(pred_df){
  pred <- copy(pred_df)
  # move to PC space
  pred_columns <- c("dent_pred","hh_pred","hosp_pred","nf_pred",
                    "oprof_pred","other_pred","pharma_pred","phys_pred")
  pred[, (pred_columns) := .SD * tot_spending/population, .SDcols = (pred_columns)]
  # calculate residual
  pred[, dent_resid := pc_spending_dent - dent_pred]
  pred[, hh_resid := pc_spending_hh - hh_pred][, hosp_resid := pc_spending_hosp - hosp_pred]
  pred[, nf_resid := pc_spending_nf - nf_pred][, oprof_resid := pc_spending_oprof - oprof_pred]
  pred[, other_resid := pc_spending_other - other_pred][, pharma_resid := pc_spending_pharma - pharma_pred]
  pred[, phys_resid := pc_spending_phys - phys_pred]
  
  # calculate rmse
  rmse <- select(pred,ends_with("resid"))
  rmse$overall_resid <- rowSums(abs(rmse))
  rmse <- sqrt(summarize_all(rmse^2, mean))
  rmse <- setDT(rmse)
  names(rmse) <- str_replace(names(rmse),"resid","rmse")
  return(rmse)
}

# Misc ----
create_diff_df <- function(df){
  df <- rbindlist(lapply(split(df, by=c("location_id")), function(dt){
    diffs <- base::diff(as.matrix(select(dt, -location_id, -year_id))) 
    diffs <- setDT(as.data.frame(diffs))[, index := .I + 1]
    
    state_years <- dt[,.(location_id, year_id, index = .I)]
    diffs <- merge(state_years, diffs, by="index")[, index := NULL]
    return(diffs)
  }))
  return(df)
}

rowwise_delta_forwards <- function(df){
  years <- unique(df[is.na(pred)]$year_id)
  setorder(df, year_id)
  for(y in years){
    df[year_id == y]$pred <- df[year_id == y-1]$pred + df[year_id == y]$pred_delta
  }
  return(df)
}

rowwise_delta_backwards <- function(df){
  years <- unique(df[is.na(pred)]$year_id)
  setorder(df, -year_id)
  for(y in rev(years)){
    df[year_id == y]$pred <- df[year_id == y+1]$pred - df[year_id == y+1]$pred_delta
  }
  return(df)
}

# Payer and TOC formula modification
# String parse equation lists to create linear space copies of each list
logit_to_linear <- function(eq_list){
  eq_list <- lapply(eq_list, function(eq){
    eq <- str_remove_all(deparse(eq),"_logit")
    eq <- as.formula(eq)
    return(eq)
  })
  return(eq_list)
}

# String parse equation lists to create linear space copies of each list
log_to_linear <- function(eq_list){ 
  eq_list <- lapply(eq_list, function(eq){
    eq <- str_remove_all(deparse(eq),"_log")
    eq <- as.formula(eq)
    return(eq)
  })
  return(eq_list)
}


# Payer and TOC formula modification
# String parse equation lists to create models with a fixed effect on state
state_effect <- function(eq_list){
  eq_list <- lapply(eq_list, function(eq){
    eq <- str_c(deparse(eq)," + abbreviation")
    eq <- as.formula(eq)
    return(eq)
  })
  return(eq_list)
}
get_best_agg_model <- function(diff = FALSE, date = NULL, bad_coefs = FALSE){
  dir <- "FILEPATH"

  if(is.null(date)){
    m_dir <- Sys.glob(paste0(dir,"*/aggregate_model_rmse.csv"))
    m_dir <- str_remove(m_dir,"aggregate_model_rmse.csv")
    m_dir <- m_dir[basename(m_dir) == max(basename(m_dir))]
  }else{
    m_dir <- paste0(dir,date,"/")
  }
  table <- fread(paste0(m_dir,"aggregate_model_rmse.csv"))
  table$oos_rmse <- as.numeric(table$oos_rmse)
  
  if(bad_coefs){
    if(!(table[oos_rmse == min(table[!is.na(oos_rmse)]$oos_rmse)]$bad_shea_coefs%in% c("ma_pct",""))) print("Dropping lowest RMSE model bc of negative coeffs")
    table <- table[bad_shea_coefs %in% c("ma_pct","")]
  }
  
  best_m <- table[oos_rmse == min(table[!is.na(oos_rmse)]$oos_rmse)]$model
  
  print("Best model(s):")
  print(best_m)
  if(length(best_m) > 1) best_m <- best_m[1]
  
  load(paste0(m_dir,"model_",best_m,".RData"))
  return(list(metadata = table[model == best_m], pred = pred, model = m, m_dir = m_dir))
}

get_best_disagg_model <- function(disagg, diff = FALSE, date = NULL){
  if(diff){
    dir <- paste0("FILEPATH",disagg,"_diff_models/")
  }else{
    dir <- paste0("FILEPATH",disagg,"_models/")
  }
  
  if(is.null(date)){
    m_dir <- Sys.glob(paste0(dir,"/20*/"))
    m_dir <- m_dir[basename(m_dir) == max(basename(m_dir))]
  }else{
    m_dir <- paste0(dir,date,"/")
  }
  table <- fread(paste0(m_dir,disagg,"_model_rmse.csv"))
  table$overall_rmse_oos <- as.numeric(table$overall_rmse_oos)
  table <- table[!is.na(overall_rmse_oos)]
  best_m <- table[overall_rmse_oos == min(table$overall_rmse_oos)]$model
  
  print("Best model(s):")
  print(best_m)
  if(length(best_m) > 1) best_m <- best_m[1]
  
  load(paste0(m_dir,"model_set_id_",best_m,".RData"))
  return(list(metadata = table[model == best_m], pred = pred_is, model = m_list, m_dir = m_dir))
}

# # Validation - check that you don't have NAs in required cols
count_nas_worker <- function(x){
  x[!is.na(x) & !is.nan(x)] <- 0
  x[is.na(x)] <- 1
  x[is.nan(x)] <- 1
  x <- as.integer(x)
  x <- sum(x)
  return(x)
}

count_nas <- function(df, by_cols = "year_id", cols_of_interest = colnames(df)){
  na_df <- copy(df)
  na_df <- na_df[,lapply(.SD, count_nas_worker), by=by_cols, .SDcols = cols_of_interest]
  return(na_df)
}

