# Bootstrapping to create uncertainty by toc
# Scales uncertainty to include 2010-2014 shea
# USERNAME, Sept 2021
# Setup -----
rm(list = ls())
library(tidyverse)
library(arrow)
library(systemfit) # SUR modeling
library(boot)
library(multiwayvcov)
library(parallel)
source("FILEPATH/clr.R")
source("FILEPATH/envelope_functions.R")

set.seed(23423)
load("FILEPATH/states.RData")
states$location_id <- as.character(states$location_id)

model <- get_best_disagg_model("toc")

df <- get_train_data_toc()[year_id %in% 1999:2019]
df <- select(df, -ends_with("NA"), -state, -state_name)
# CLO models - must apply CLO transform first
if(model$metadata$model_type == "CLO"){
  df <- clr(df, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                         "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
}
df_diff <- create_diff_df(select(df, -abbreviation))
df_diff <- merge(df_diff, states[,.(location_id, abbreviation)], by="location_id", allow.cartesian = TRUE)

# Get aggregate to rake to
agg_model <- get_best_agg_model(bad_coefs = TRUE)
aggregate <- fread(paste0(agg_model$m_dir,"/model_",agg_model$metadata$model,"_uncertainty.csv"))
aggregate <- aggregate[,.(location_id = as.character(location_id), year_id, population, pred = mean, se = (upper - mean)/1.96)]
agg_draws <- lapply(c(1:1000), function(i){
  agg_draws <- copy(aggregate)[, tot_spending_agg := population*rnorm(1, pred, se), by=c("location_id","year_id")]
  return(agg_draws)
})

run_model_and_make_prediction <- function(samp, model){
  # Predict on full df for in sample model
  m_is <- systemfit(model$model, method = "SUR", data = samp)
  
  if(model$metadata$difference_model){
    p1 <- pred_df_diff_toc(m_is, df_diff, model$metadata$model_type, 2014, 2015:2019, "forwards")
    p2 <- pred_df_diff_toc(m_is, df_diff, model$metadata$model_type, 2014, 2003:2014, "backwards") %>%
      filter(year_id != 2014)
    pred_is <- rbind(p1, p2)
  }else{
    # Create in-sample model
    pred_is <- pred_df_toc(m_is, df, model$metadata$model_type) # function handles raking/inv CLO
  }
  
  # Predict on full df for in sample model
  pred_is <- pred_is[,.(location_id, abbreviation, year_id, dent_pred, hh_pred, hosp_pred, nf_pred, oprof_pred, 
                        other_pred, pharma_pred, phys_pred)]
  return(pred_is)
}

# Where the bootstrapping happens
pred_dfl <- mclapply(c(1:1000), function(i){
  print(i)
  samp <- copy(slice_sample(states, n = 50, replace = TRUE)) # Sample states with replacement
  samp[, boot_abbrev := paste0(abbreviation,.I), by="abbreviation"] # Generate unique state label
  if(model$metadata$difference_model){
    samp <- merge(samp[,.(location_id, abbreviation, boot_abbrev)], df_diff, by=c("location_id","abbreviation"), allow.cartesian = TRUE) # Get full dataset
  }else{
    samp <- merge(samp[,.(location_id, abbreviation, boot_abbrev)], df, by=c("location_id","abbreviation"), allow.cartesian = TRUE) # Get full dataset
  }
  samp[, abbreviation := boot_abbrev][, boot_abbrev := NULL]
  
  pred <- run_model_and_make_prediction(samp, model) # Run model
  pred[, draw := i]
  
  pred <- merge(pred, agg_draws[i][[1]], by=c("location_id","year_id"))
  return(pred)
}, mc.cores = 40)
pred_df_draws <- rbindlist(pred_dfl)

# Melt predictions over toc to calculate intervals
pred_df_draws <- melt(pred_df_draws, id.vars = c("location_id","year_id"), measure.vars = patterns("_pred$"), variable.name = "toc")
pred_df_draws[,`:=`(mean = mean(value), median = quantile(value, 0.5), lower = quantile(value, 0.025), upper = quantile(value, 0.975)),
                   by=c("location_id","year_id","toc")]
pred_df_draws[, toc := str_remove(toc,"_pred")]

# Melt observations over toc
if(model$metadata$model_type == "CLO"){
  df <- inv_clr(df, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                         "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
}
df_long <- melt(df, id.vars = c("location_id","abbreviation","year_id","population","tot_spending"), measure.vars = patterns("pr_spending_"), 
                variable.name = "toc", value.name = "data")
df_long <- df_long[!str_detect(toc,"log")]
df_long[, toc := str_remove(toc,"pr_spending_")]

pred_df_draws <- merge(pred_df_draws, df_long, by=c("location_id","year_id","toc"))
arrow::write_feather(pred_df_draws, paste0(model$m_dir,"model_",model$metadata$model,"_draws.feather"))
pred_df <- unique(pred_df_draws[,value := NULL])

# Check how much data is in bounds
pred_df[, in_ui := 0][, abbreviation := NULL]
pred_df[data <= upper & data >= lower, in_ui := 1]

pred_df <- merge(pred_df, states, by=c("location_id"))
pred_df[, within_ui := sum(in_ui, na.rm = TRUE)/(.N-5), by=c("abbreviation","toc")]

# If necessary, scale bounds marginally until 95% of data is in bounds
# Need 20/21 data points to fall into UI for 95% coverage
pred_df <- rbindlist(lapply(split(pred_df, by=c("abbreviation","toc")), function(dt){
  dt[, ui_dist := upper - mean][, true_dist := abs(data - mean)] # calculate distance of PI from mean, distance of point from mean
  dt[, ratio := true_dist/ui_dist]
  scale <- ceiling(max(dt[year_id %in% 2009:2013]$ratio, na.rm = TRUE)*1000)/1000
  
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

fwrite(pred_df, paste0(model$m_dir,"model_",model$metadata$model,"_uncertainty.csv"))

