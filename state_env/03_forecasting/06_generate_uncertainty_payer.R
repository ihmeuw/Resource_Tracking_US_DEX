# Bootstrapping to create uncertainty by payer
# Scales uncertainty to include 2010-2014 shea
# USERNAME, Sept 2021

# Setup ------
rm(list = ls())
library(tidyverse)
library(arrow)
library(systemfit) # SUR modeling
library(boot)
library(multiwayvcov)
library(parallel)
repo_path <- dirname(dirname(dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())))
source("FILEPATH/clr.R")
source('FILEPATH/envelope_functions.R')

set.seed(23423)
load("FILEPATH/states.RData")
states$location_id <- as.character(states$location_id)

model <- get_best_disagg_model("payer")

df <- get_train_data_payer()[year_id %in% 2001:2019]
# CLO models - must apply CLO transform first
if(model$metadata$model_type == "CLO"){
  df <- clr(df, cols = c("pr_spending_mdcr","pr_spending_mdcd","pr_spending_priv","pr_spending_oop"))
}
df_diff <- create_diff_df(select(df, -abbreviation))
df_diff <- merge(df_diff, states[,.(location_id, abbreviation)], by="location_id", allow.cartesian = TRUE)

# Generate agg draws -----
agg_model <- get_best_agg_model(bad_coefs = TRUE)
aggregate <- fread(paste0(agg_model$m_dir,"/model_",agg_model$metadata$model,"_uncertainty.csv"))

aggregate <- aggregate[,.(location_id = as.character(location_id), year_id, population, pred = mean, se = (upper - mean)/1.96)]
agg_draws <- lapply(c(1:1000), function(i){
  agg_draws <- copy(aggregate)[, tot_spending_agg := population*rnorm(1, pred, se), by=c("location_id","year_id")]
  return(agg_draws)
})

# Model run function -----
run_model_and_make_prediction <- function(samp, model){
  # Create in-sample model
  m_is <- systemfit(model$model, method = "SUR", data = samp)

  # Predict on full df for in sample model
  if(model$metadata$difference_model){
    p1 <- pred_df_diff_payer(m_is, df_diff, model$metadata$model_type, 2014, 2015:2019, "forwards")
    p2 <- pred_df_diff_payer(m_is, df_diff, model$metadata$model_type, 2014, 2002:2014, "backwards") %>%
      filter(year_id != 2014)
    pred_is <- rbind(p1, p2)
  }else{
    pred_is <- pred_df_payer(m_is, df, model$metadata$model_type) # function handles raking/inv CLO
  }
  pred_is <- pred_is[,.(location_id, abbreviation, year_id, mdcr_pred, mdcd_pred, priv_pred, oop_pred)]
  return(pred_is)
}

# Bootstrapping ------
pred_dfl <- mclapply(c(1:1000), function(i){
  # print(i)
  samp <- copy(slice_sample(states, n = 50, replace = TRUE)) # Sample states with replacement
  samp[, boot_abbrev := paste0(abbreviation,.I), by="abbreviation"] # Generate unique state label
  if(model$metadata$difference_model){
    samp <- merge(samp[,.(location_id, abbreviation, boot_abbrev)], df_diff, by=c("location_id","abbreviation"), allow.cartesian = TRUE) # Get full dataset
  }else{
    samp <- merge(samp[,.(location_id, abbreviation, boot_abbrev)], df, by=c("location_id","abbreviation"), allow.cartesian = TRUE) # Get full dataset
  }
  samp[, abbreviation := NULL]
  samp[, abbreviation := boot_abbrev]

  pred <- run_model_and_make_prediction(samp, model = model) # Run model
  pred[, draw := i]
  
  pred <- merge(pred, agg_draws[i][[1]], by=c("location_id","year_id"))
  return(pred)
}, mc.cores = 30)
pred_df_draws <- rbindlist(pred_dfl)

# Aggregation -----
# Melt predictions over payers to calculate intervals
pred_df_draws <- melt(pred_df_draws, id.vars = c("location_id","year_id"), measure.vars = patterns("_pred$"), variable.name = "payer")
pred_df_draws[,`:=`(mean = mean(value), median = quantile(value, 0.5), lower = quantile(value, 0.025), upper = quantile(value, 0.975)),
                   by=c("location_id","year_id","payer")]
pred_df_draws[, payer := str_remove(payer,"_pred")]

# Melt observations over payers
if(model$metadata$model_type == "CLO"){
  df <- inv_clr(df, cols = c("pr_spending_mdcr","pr_spending_mdcd","pr_spending_priv","pr_spending_oop"))
}
df_long <- melt(df, id.vars = c("location_id","abbreviation","year_id"), measure.vars = patterns("pr_spending_.{3,4}$"), 
                variable.name = "payer", value.name = "data")
df_long[, payer := str_remove(payer,"pr_spending_")]

# Merge observation and prediction data
pred_df_draws <- merge(pred_df_draws, df_long, by=c("location_id","year_id","payer"))
arrow::write_feather(pred_df_draws, paste0(model$m_dir,"model_",model$metadata$model,"_draws.feather"))
pred_df <- unique(pred_df_draws[, value := NULL])

# Bounds scaling -------
# Check how much data is in bounds
pred_df[, in_ui := 0]
pred_df[data <= upper & data >= lower, in_ui := 1]

pred_df <- merge(pred_df, states, by=c("location_id","abbreviation"))
pred_df[, within_ui := sum(in_ui, na.rm = TRUE)/(.N-5), by=c("abbreviation","payer")]
unique(pred_df[,.(abbreviation, within_ui)])

# If necessary, scale bounds marginally until 95% of data is in bounds
# Need 20/21 data points to fall into UI for 95% coverage
pred_df <- rbindlist(lapply(split(pred_df, by=c("abbreviation","payer")), function(dt){
  dt[, ui_dist := upper - mean][, true_dist := abs(data - mean)] # calculate distance of PI from mean, distance of point from mean
  dt[, ratio := true_dist/ui_dist]
  scale <- ceiling(max(dt[year_id %in% 2009:2013]$ratio, na.rm = TRUE)*100)/100
  
  dt[, upper := mean + (scale*ui_dist)][, lower := mean - (scale*ui_dist)] # scale PI
  dt[, ui_dist := NULL][, true_dist := NULL]
  
  # validate
  stopifnot(nrow(dt[year_id %in% 2009:2013 & (data > upper | data < lower)]) == 0)
  return(dt)
}))

# final validations
count(pred_df, year_id)
count(pred_df, location_id)
count_nas(pred_df, "year_id", c("mean","upper","lower"))

pred_df[upper > 1, upper := 1][mean > 1, mean := 1]
pred_df[lower < 0, lower := 0][mean < 0, mean := 0]

fwrite(pred_df, paste0(model$m_dir,"model_",model$metadata$model,"_uncertainty.csv"))
