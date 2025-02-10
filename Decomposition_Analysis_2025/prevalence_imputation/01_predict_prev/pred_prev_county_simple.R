##############################################
#  Model prevalence by fitting a regression at the state level and predicting at county
#     formula: prevalence rate ~ mort rate + covs (by sex)
# 
#  Inputs:
#     - county covariates
#     - state prevalence and mortality estimates (GBD)
#     - county mortality estimates (USHD)
#     - population estimates
#     - causelist 
#
#
#  Authors: Drew DeJarnatt, Haley Lescinsky, and Lauren Wilner
#
##############################################


pacman::p_load(tidyverse, ggpubr, arrow, data.table, splines, DBI, glmnet)
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
set.seed(125)
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))


source("/FILEPATH/get_location_metadata.R")
source('/FILEPATH/get_age_metadata.R')

Sys.umask(mode = 002)
'%nin%' <- Negate('%in%')


note <- ""
min_lambda <- F # 1se if F
t0 <- Sys.time()



# LOAD ARUGMENTS
if(interactive()){
  # ARGUMENTS FOR RUNNING INTERACTIVELY
  
  save_dir <- "/FILEPATH/"
  
  # Set version (see launcher for how to make a new version)
  version_id <- 'XX'
  draws <- F # T or F
  model_type <- "lasso"
  save_dir <- paste0(save_dir, "/", version_id, '/')
  if(!dir.exists(save_dir)){dir.create(save_dir, recursive = T)}
  
  cause_subset <- c('all') # 'all' or specific cause names for interactive testing
  
  
}else{
  # ARGUMENTS PULLED THROUGH FROM LAUNCHER
  
  args <- commandArgs(trailingOnly = TRUE)
  message(args)
  save_dir <- args[1]
  draws_key <- args[2]
  cause_subset <- args[3]
  model_type <- args[4]

  if(draws_key == 'draws'){
    draws <- T
  }else{
    draws <- F
  }
  
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  if(!is.na(task_id)){
    
    shared_causelist <- fread("/FILEPATH/ushd_dex_causelist.csv")
    dex_causes <- shared_causelist[shared == 1]$dex_acause
    
    cause_subset <- dex_causes[task_id]
    
  }
  
  
  
  print(paste0(save_dir, ", draws", draws, "-", cause_subset))
}


#-------
# Load causelist, location lists
#-------

# Prep location information
states <- get_location_metadata(location_set_id = 128, location_set_version_id = 1133, gbd_round_id = 8, release_id = 15) %>% 
  filter(location_type_id == 3) %>% 
  select(c("location_id", "location_name")) 
mcntys <- fread('/FILEPATH/merged_counties.csv') %>% 
  select(c("mcnty", "state_name")) %>% 
  rename(location_name = state_name) %>% 
  merge(states, by = "location_name") %>%
  unique() %>% as.data.table()

#------
# dex causelist (only pull where we have dex outputs) - 'THE MERGE'
#------
shared_causelist <- fread("/FILEPATH/ushd_dex_causelist.csv")
if(cause_subset[1] == "all"){
  causes_to_model <- unique(shared_causelist[shared == 1 , dex_acause])
} else {
  causes_to_model <- shared_causelist[shared == 1 & dex_acause %in% cause_subset, dex_acause]
}


#-------
# Load mort + prevalence/incidence data at the state; GBD output
#-------

rho <- open_dataset("/FILEPATH/mx_prev_inc_yll_yld") %>% collect() %>% as.data.table()
rho[, rho := mx / state_rate]

# drop incidence and prevalence columns since we don't need them anymore
rho[,`:=` (inc = NULL, prev = NULL)]
setnames(rho, "state_rate", 'prev')


#-------
# Pull county covariates
#-------
ushd_covariates <- c("income_median", 
                     "mds_pc", 
                     "pop_density", 
                     "unemployed", 
                     "edu_ba", "edu_hs", "ethn_hisp",
                     "poverty", 
                     "race_nh_black", "race_nh_aian", "race_nh_api", 
                     "race_nh_white", "race_other", "reservations_all")

# read in population data and aggregate to county 
county_pop <- fread("/FILEPATH/pop_age_sex.csv")
county_pop <- county_pop[geo == 'county' & year_id %in% c(2000:2019)][,.(pop = sum(pop)), by = c('year_id', 'location')]
setnames(county_pop, "location", 'mcnty')

# read in county covariates
county_covs <- fread("/FILEPATH/covs_county.csv") %>%
  filter(year_id <= 2019 & year_id >= 2000, covariate %in% ushd_covariates)

# add state location_name and location_id
county_join <- merge(county_covs, mcntys, by = "mcnty", all.x = TRUE)
county_join[, mcnty := as.character(mcnty)]

# add in county populations
county_w_pop <- merge(county_join, county_pop, by = c("mcnty", "year_id"), all.x = TRUE)

# aggregate covariate estimates to state level with pop weighted mean
covs <- county_w_pop %>%  
  group_by(year_id, location_name, covariate) %>% 
  summarize(mean_cov = mean(cov_value),
            weighted_cov = weighted.mean(cov_value, pop))

# make data wide on covariate 
covs <- reshape2::dcast(covs, formula = location_name + year_id ~ covariate, value.var = "weighted_cov") %>% as.data.table()

# identify means + sds of state covariates so we can scale county with the same variables
means <- sapply(covs[, c(ushd_covariates), with = F], mean)
sds <- sapply(covs[, c(ushd_covariates), with = F], sd)
standization_factors <- data.table('mean' = means, 'sd' = sds, 'covariate' = names(means))

# rescale numeric values 
covs <- covs %>%
  mutate(across(all_of(ushd_covariates), scale))

county_covs <- merge(county_covs, standization_factors, by = 'covariate')
county_covs[, cov_value := (cov_value - mean)/sd]


#-------
# Make model_df (state) & pred_df (county)
#-------

# necessary formatting
rho[, location_name := as.character(location_name)]
covs[, location_name := as.character(location_name)]

model_df <- merge(rho, covs, by = c("location_name", "year_id")) %>% data.table()

# remove NAs that may have been missed above
model_df <- na.omit(model_df)
model_df[mx == 0, mx := 1e-8]

#data to predict on - county level covariates 2000-2019 - wide on covariate
pred_df <- dcast(county_covs, formula = year_id + mcnty ~ covariate, value.var = "cov_value")

#merge pred_df to state names
pred_df <- merge(pred_df,mcntys, by = 'mcnty', all.x = TRUE)
pred_df[,`:=` (location_id = NULL)]

# add on mort (mx) for prediction
if(draws == F){
  county_mx <- open_dataset("/FILEPATH/") %>% collect() %>% as.data.table()
  county_mx <- county_mx[,.(mcnty, sex_id, acause, age_group_years_start, mx = mortality_rate, year_id)]
}

covariates <- c(ushd_covariates, 'ln_mx')


#-------
# Regression function
#-------
fit_predict_lasso_reg <- function(cause, sex,
                                  model_df, 
                                  shared_causelist, 
                                  covariates,
                                  plot = T){
  
  print(cause)
  print(sex)
  df <- model_df[acause == cause & sex_id == sex]
  
  # log and scale mx since it's pulled in separately above
  df[, ln_mx := log(mx)]
  
  mean_mort <- mean(df$ln_mx)
  sd_mort <- sd(df$ln_mx)
  df[, ln_mx_prescale := ln_mx]
  df[, ln_mx := (ln_mx - mean_mort)/sd_mort]
  
  #
  # FIT MODEL
  # 
  
  if(nrow(df) ==0){return()}
  
  if(model_type ==  "lasso"){
    initial_covariates <- covariates
    repeat{
      current_covariates <- initial_covariates
      spline_names <- 'as.factor(age_group_years_start)'
      
      # FE by state
      loc_names <- 'as.factor(location_name)'
      
      # Set of all predictors
      var_names <- c(spline_names, current_covariates) # removing state FEs
      
      # Set formula for regression (no interactions for now)
      formula <- as.formula(paste0("~ (", paste(var_names, collapse = " + "), ") - 1"))
      
      # Set dep and indep vars
      y <- log(df$prev)
      
      x <- model.matrix(formula, data = df)
      
      # Tune lasso regression (lasso is when alpha = 1) to determine best value of lambda (lambda min)
      cv_model <- glmnet::cv.glmnet(x, y, alpha = 1)
      if(min_lambda == T){
        best_lambda <- cv_model$lambda.min
      }else{
        best_lambda <- cv_model$lambda.1se
      }
      
      # Fit lasso regression with optimal lambda value
      model <- glmnet::glmnet(x, y, alpha = 1, lambda = best_lambda)
      coef(model)
      
      # Make table of all selected (non-zero) coefficients
      table <- data.table(est_coeff_rounded = matrix(round(coef(model), 4)),
                          est_coeff = matrix(coef(model)),
                          cov = c("intercept", colnames(x)))
      table[, acause := cause]
      table[, r2 := model$dev.ratio]
      table <- table[est_coeff.V1!=0]
      
      ## Some covariates end up with coefficients with opposite directions than we expect (e.g. negative coefficient for poverty)
      ## Check if the sign of the coefficient matches our prior, and if not, drop covariate and rerun
      cov_pos_prior <- c("poverty", "unemployed", "ln_mx")
      cov_neg_prior <- c("income_pc", "edu_ba", "homeownership","edu_hs")
      
      # check the covariates that should have positive coefficients
      pos_table <- table[cov %in% cov_pos_prior]
      bad <- pos_table[est_coeff.V1 < 0]$cov
      print(paste0("removing:",bad))
      initial_covariates <- setdiff(initial_covariates, bad)
      
      # check the covaraites that should have negative coefficients
      neg_table <- table[cov %in% cov_neg_prior]
      bad <- neg_table[est_coeff.V1 > 0]$cov
      print(paste0("removing:",bad))
      initial_covariates <- setdiff(initial_covariates, bad)
      
      # break the loop if nothing gets dropped
      # otherwise, restart at the top and fit model with new covariate list
      if(length(initial_covariates) == length(current_covariates)){
        break
      }
    }
  } else if(model_type ==  "linear"){
    initial_covariates <- covariates
    repeat{
      current_covariates <- initial_covariates
      spline_names <- 'as.factor(age_group_years_start)'
      
      # FE by state
      loc_names <- 'as.factor(location_name) + sex_id'
      
      # Set of all predictors
      var_names <- c(spline_names, current_covariates, 'sex_id') # removing state FEs
      
      # Set formula for regression (no interactions for now)
      model_formula <- as.formula(paste0("log(prev) ~ (", paste(var_names, collapse = " + "), ") - 1"))
      formula <- as.formula(paste0("~ (", paste(var_names, collapse = " + "), ") - 1"))
      
      x <- model.matrix(formula, data = df)
      
      # Fit liner regression with optimal lambda value
      model <- lm(model_formula, data = df)
      model_summary <- summary(model)
      coef(model)
      
      # Make table of all selected (non-zero) coefficients
      table <- data.table(est_coeff_rounded = matrix(round(coef(model), 4)),
                          est_coeff = matrix(coef(model)),
                          cov = c("intercept", colnames(x)))
      table[, acause := cause]
      
      table[, r2 :=summary(model)$r.squared]
      table <- table[est_coeff.V1!=0]
      
      ## Some covariates end up with coefficients with opposite directions than we expect (e.g. negative coefficient for poverty)
      ## Check if the sign of the coefficient matches our prior, and if not, drop covariate and rerun
      cov_pos_prior <- c("poverty", "unemployed", "ln_mx")
      cov_neg_prior <- c("income_pc", "edu_ba", "homeownership","edu_hs")
      
      # check the covariates that should have positive coefficients
      pos_table <- table[cov %in% cov_pos_prior]
      bad <- pos_table[est_coeff.V1 < 0]$cov
      print(paste0("removing:",bad))
      initial_covariates <- setdiff(initial_covariates, bad)
      
      # check the covaraites that should have negative coefficients
      neg_table <- table[cov %in% cov_neg_prior]
      bad <- neg_table[est_coeff.V1 > 0]$cov
      print(paste0("removing:",bad))
      initial_covariates <- setdiff(initial_covariates, bad)
      
      # break the loop if nothing gets dropped
      # otherwise, restart at the top and fit model with new covariate list
      if(length(initial_covariates) == length(current_covariates)){
        break
      }
    }
  }
  #
  # Calculate/predict prevalence
  #
  
  ages <- unique(df$age_group_years_start)
  
  pred_df2 <- crossing(pred_df, age_group_years_start = ages, sex_id = c(1,2))
  pred_df2 <- as.data.table(pred_df2)
  
  # add on mx
  if(draws == T){
    
    # add draw 0 as mean here and then plot just draw 0 
    print("draws is T, so pulling draws of county mort!")
    keep_cols <- c('mcnty','sex_id', 'year_id','age_group_years_start','acause', 'mortality_rate_mean','pop', paste0("mx_draw_",1:50))
    
    county_mx_draws <- open_dataset("/FILEPATH/mort_draws_pop_agesex/") %>% 
      select(all_of(keep_cols)) %>% filter(acause == cause) %>%  collect() %>% as.data.table()
    
    county_mx_draws[, mcnty := as.numeric(mcnty)]
    
    
    if(nrow(county_mx_draws)==0){stop("no county mx draws!")}
    print(nrow(county_mx_draws))
    setnames(county_mx_draws,  "mortality_rate_mean", "mx_draw_0")
    county_mx_draws <- melt(county_mx_draws, id.vars = c('mcnty','sex_id', 'acause', 'age_group_years_start', 'year_id'), 
                            measure.vars = colnames(county_mx_draws)[colnames(county_mx_draws) %like% "mx_draw"],
                            value.name = 'mx',
                            variable.name = 'draw')
    county_mx_draws <- county_mx_draws[,.(mcnty, sex_id, acause, age_group_years_start, draw = as.numeric(gsub("mx_draw_","", draw)), mx, year_id)]
    print(nrow(county_mx_draws))
    
    # merge predictions on to the draws of mortality 
    pred_df2 <- merge(pred_df2[year_id %in% unique(county_mx_draws$year_id)], county_mx_draws[acause == cause], by = c('year_id', 'mcnty', 'age_group_years_start', 'sex_id'),all.x = T)

    
    if(nrow(pred_df2[is.na(draw)]) > 0 ){
      message("Looks like there are some demographics we don't have mortality for, assuming sex-restricted cause and dropping those")
      print(pred_df2[is.na(draw), .N, by = c('age_group_years_start','sex_id')])
      pred_df2 <- pred_df2[!is.na(draw)]
      
    }
    
  }else{
    # otherwise county_mx loaded up top
    pred_df2 <- merge(pred_df2[year_id %in% unique(county_mx$year_id)], county_mx[acause == cause], by = c('year_id', 'mcnty', 'age_group_years_start', 'sex_id'),all.x = T)
    
  }
  
  pred_df2[is.na(mx), mx := 1e-8]
  pred_df2[mx == 0, mx := 1e-8] # necessary offset
  pred_df2[, ln_mx := log(mx)]
  pred_df2[, ln_mx_prescale := ln_mx]
  pred_df2[, ln_mx := (ln_mx - mean_mort)/sd_mort]
  pred_df2[, acause := NULL]
  
  print("predicting!")
  
  # predict prevalence
  if(model_type == "lasso"){
    pred_df3 <- model.matrix(formula, data = pred_df2)
    pred_df2$prev_pred_log <- predict(model, pred_df3)
  }
  if(model_type == "linear"){
    pred_df3 <- copy(pred_df2)
    pred_df2$prev_pred_log <- predict(model, pred_df3)
  }
  
  pred_df2[, prev_rate := exp(prev_pred_log)]
  
  summary(pred_df2$prev_rate)
  if(draws == T){
    pred <- copy(pred_df2)[,.(year_id, mcnty, age_group_years_start, sex_id, acause = cause, prev_rate, draw)]
  }else{
    pred <- copy(pred_df2)[,.(year_id, mcnty, age_group_years_start, sex_id, acause = cause, prev_rate)]
  }
  rm(pred_df2)
  
  print("prediction is done")
  
  #
  # PREDICT STATE for RMSE
  #
  
  pred_state <- predict(model, newx = x)
  pred_state <- exp(pred_state)
  pred_data <- cbind(df[,.(location_name, year_id, age_group_years_start, sex_id, obs_prev = prev)], data.table('pred_prev' = pred_state))
  setnames(pred_data, "pred_prev.s0", "pred_prev", skip_absent=TRUE)
  
  rmse <- sqrt(mean(pred_data[, (obs_prev - pred_prev)^2]))
  
  table[, state_rmse := rmse]
  
  
  # Use observed for DC instead of predicted (since one county state)
  
  print("DC step")
  
  
  pred <- pred[mcnty!=304]
  dc_obs <- pred_data[location_name == 'District of Columbia']
  dc_obs <- dc_obs[, .(prev_rate = obs_prev, mcnty = 304, year_id, age_group_years_start, sex_id, acause = cause)]
  if(draws == T){
    
    # duplicate out 
    dc_obs <- tidyr::crossing(dc_obs, 'draw'=0:max(pred$draw)) %>% as.data.table()
    
  }
  
  pred <- rbind(pred, dc_obs) 
  
  
  
  #
  # PLOT AND SAVE
  #
  print("plotting!")
  
  
  pred <- merge(pred, mcntys[,.(location_name, mcnty)], by = 'mcnty')
  
  states1 <- unique(mcntys$location_name)[1:26]
  for(s in c(1,2)){
    
    if(nrow(pred[year_id == 2015 & location_name %in% states1 & sex_id == s]) ==0){
      next
    }
    
    plot_pred <- pred[year_id == 2015 & sex_id == s]
    
    if(draws == T){
      plot_pred <- plot_pred[draw == 0]
    }
    
    p1 <- ggplot(plot_pred[location_name %in% states1], aes(x = age_group_years_start, y = prev_rate))+
      geom_line(color = 'grey', alpha = 0.6, aes(group = mcnty, color = 'county predicted'))+
      geom_line(data = df[year_id == 2015 & location_name %in% states1 & sex_id == s], aes(y = prev, color = 'state observed'), color = 'red')+
      facet_wrap(~location_name, scales = 'free')+theme_bw()+
      labs(title = paste0(cause," sex",s, " county predictions vs state observed (2015)"), subtitle = 'one line per county (red is state obs)', y = 'predicted prevalence rate', x = 'age')
    p2 <- ggplot(plot_pred[location_name %nin% states1], aes(x = age_group_years_start, y = prev_rate))+
      geom_line(color = 'grey', alpha = 0.6, aes(group = mcnty, color = 'county predicted'))+
      geom_line(data = df[year_id == 2015 & location_name %nin% states1 & sex_id == s], aes(y = prev, color = 'state observed'), color = 'red')+
      facet_wrap(~location_name, scales = 'free')+theme_bw()+
      labs(title = paste0(cause," sex",s, " county predictions vs state observed (2015)"), subtitle = 'one line per county (red is state obs)', y = 'predicted prevalence rate', x = 'age')
    
    if(plot){
      print(p1)
      print(p2)
    }
  }
  
  
  
  
  #
  if(draws == T){
    
    print("Saving results by year")
    print(Sys.time() - t0)
    
    for(d in unique(pred$draw)){
      
      print(d)
      
      write_dataset(pred[draw  == d], path = paste0(save_dir, "/data/"), partitioning = c('draw'), basename_template = paste0("cause_", cause, "_",sex,"-{i}.parquet"))
      
      
    }
    
    
    
  }else{
    print("Saving results!")
    print(Sys.time() - t0)
    
    write_dataset(pred, path = paste0(save_dir, "/data/"), basename_template = paste0("cause_", cause, "_",sex,"-{i}.parquet"))
  }
  #Add sex_id to table
  table[, sex_id := sex]
  # Return table of LASSO coefficients
  return(table)
  
}


#-------
# Fit lasso regression, by cause
#-------
note <- paste0(note, "reg_prev_by_mort_predict")

if(draws == T){
  pdf(paste0(save_dir, "/viz_by_cause/viz_regression_", note, cause_subset, ".pdf"), width = 11, height = 7)
}else{
  pdf(paste0(save_dir, "/viz_regression_", note, ".pdf"), width = 11, height = 7)
  
}


lasso_table <- rbindlist(lapply(causes_to_model, function(cause) {
  lasso_results_for_cause <- rbindlist(lapply(c(1,2), function(sex) {
    return(fit_predict_lasso_reg(cause = cause,
                                 sex = sex, 
                                 model_df = model_df, 
                                 shared_causelist = shared_causelist,
                                 covariates = covariates))
  }))
  return(lasso_results_for_cause)
}))


dev.off()

if(draws == T){
  lasso_table_dir <- paste0(save_dir, "/viz_by_cause/lasso_table", note,cause_subset, ".csv")
  
}else{
  lasso_table_dir <- paste0(save_dir, "lasso_table", note, ".csv")
  
}



fwrite(lasso_table, lasso_table_dir)
