# Control model for state policy regressions
# Control for price/income, density and health indicators
# USERNAME, Nov 2021

# Setup ---------------------------------------------------------------
rm(list = ls())
pacman::p_load(data.table, tidyverse, parallel, plm, lmtest)
source("FILEPATH/envelope_functions.R")

set.seed(234)

args <- commandArgs(trailingOnly = TRUE)
print(args)
model <- args[1] %>% print()

state_demean <- FALSE

dir <- "FILEPATH"

spending <- fread(paste0("FILEPATH/",model,"_standardized.csv"))
covariates <- fread("FILEPATH/all_covs.csv")

controls <- c("density_g.1000","ldi_pc","PA_mets","cig_pc_15")

covariates <- select(covariates, location_id, year_id, all_of(controls))

df <- merge(spending, covariates, by=c("location_id","year_id"))
df[, log_st_spend := log(spending_pc_standardized)]
df <- na.omit(df)

# Modeling ------------------------------------------------------------------------------------
demean <- function(x){
  x <- x - mean(x)
  return(x)
}

predict_manually <- function(df, covs, betas){
  ## Melt df
  df[, `as.factor(year_id)2019` := 1][, `(Intercept)` := 1]
  df <- melt(df, id.vars = c("location_id","year_id","draw"), measure.vars = covs, variable.name = "covariate")
  
  ## Manually predict on simulated betas
  betas <- betas[covariate %in% covs]
  df <- merge(betas, df, by=c("covariate","draw"))
  df[, components := simbeta*value]
  return(df)
}

run_model_draws <- function(df, controls, state_demean = FALSE){
  if(state_demean){
    varbs <- c("log_st_spend",controls)
    df[, state_mean := mean(log_st_spend), by="location_id"]
    df <- df[,(varbs) := lapply(.SD, demean), by="location_id", .SDcols = varbs]
  }
  eqn <- paste0("log_st_spend ~ ",paste(controls, collapse = " + ")," + as.factor(year_id)")
  draws <- lapply(1:1000, function(i){
    mod <- lm(eqn, data = df[draw == i])
    
    ## Extract coefficients from this run
    pe <- coef(mod)
    ## Choose a variance-covariance matrix
    vc <- vcovHC(mod, 
                 method = "white1")
    ## Draw simulated betas from this run
    set.seed(234)
    simbetas <- data.table(covariate = names(pe), 
                           simbeta = MASS::mvrnorm(n = 1, mu = pe, Sigma = vc),
                           draw = i)

    ## Manually predict on simulated betas
    covs <- c("(Intercept)","as.factor(year_id)2019",controls)
    avg_df <- copy(df[draw == i & year_id == 2019])
    avg_df <- avg_df[, lapply(.SD, mean), by=c("year_id","draw")][, location_id := 102]
    avg_pred_coms <- predict_manually(avg_df, covs, simbetas)
    avg_pred <- avg_pred_coms[,.(estimate = sum(components)), by=c("location_id","year_id","draw")]
    
    state_year_preds_coms <- predict_manually(df[draw == i], covs, simbetas)
    state_year_preds <- state_year_preds_coms[,.(estimate = sum(components)), by=c("location_id","year_id","draw")]
    state_year_preds <- merge(state_year_preds, df[draw == i], by=c("location_id","year_id","draw"))
    state_year_preds[, residual := log_st_spend - estimate]
    
    if(state_demean){
      predictions <- state_year_preds[,.(location_id, year_id, draw, pred = residual + avg_pred$estimate + state_mean)]
    }else{
      predictions <- state_year_preds[,.(location_id, year_id, draw, pred = residual + avg_pred$estimate)]
    }
    predictions[, pred := exp(pred)]
    setnames(predictions, "pred", "spending_pc_controlled")
    return(list(simbetas = simbetas, predictions = predictions, avg_pred_coms = avg_pred_coms, state_year_preds_coms = state_year_preds_coms))
  })
  
  # Compile components for decomp table
  avg_pred_coms <- rbindlist(lapply(draws, function(d) return(d$avg_pred_coms)))
  setnames(avg_pred_coms, c("value", "components"), c("us_mean_value", "us_mean_components"))
  state_year_preds_coms <- rbindlist(lapply(draws, function(d) return(d$state_year_preds_coms)))
  state_components <- merge(avg_pred_coms[,.(covariate, draw, year_id, us_mean_value, us_mean_components)], state_year_preds_coms, by = c("covariate", "draw", "year_id"))
  
  # Compile draws
  betas <- rbindlist(lapply(draws, function(d) return(d$simbetas)))
  betas <- betas[,.(beta = mean(simbeta), 
                          std_err = sd(simbeta), 
                          beta_lower = quantile(simbeta, 0.025), 
                          beta_upper = quantile(simbeta, 0.975),
                          p_val = 2*pnorm(abs(mean(simbeta))/sd(simbeta), lower.tail = FALSE)),
                       by="covariate"]
  
  pred <- rbindlist(lapply(draws, function(d) return(d$predictions)))
  
  return(list(betas = betas, pred = pred, state_components = state_components))
}

# Version 1 - no state effects 
control_model <- run_model_draws(df, controls, state_demean)
fwrite(control_model$pred, paste0(dir,model,"_controlled.csv"))

year_patterns <- control_model$betas[covariate %in% str_subset(control_model$betas$covariate,"year_id")]
ggplot(year_patterns) + geom_col(aes(x = covariate, y = beta))

bfile <- paste0(dir,"betas_",model,".csv")
fwrite(control_model$betas, bfile)
