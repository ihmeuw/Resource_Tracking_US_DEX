# Generate bootstrapped uncertainty for aggregate state models
# Runs model 1000x for draws, calculate upper/lower and scale to include all 2010-2014 data
# USERNAME, Sept 2021

rm(list = ls())
library(tidyverse)
library(data.table)
library(arrow)
library(parallel)
# repo_path <- dirname(dirname(dirname(if (interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())))
source("FILEPATH/envelope_functions.R")

load("FILEPATH/states.RData")
states$location_id <- as.character(states$location_id)
set.seed(12342)

# Get aggregate model
agg <- get_best_agg_model(bad_coefs = TRUE)

train <- get_train_data()
train <- train[year_id > 1999]
train[, location_id := as.character(location_id)]
train_diff <- create_diff_df(train)

# Run prediction
make_prediction <- function(samp, model){
  model$metadata[transformation != "linear", dep_var := paste0(spending, "_", transformation)]
  model$metadata[transformation == "linear", dep_var := spending] 
  
  if(model$metadata$difference_model){
    samp_diff <- create_diff_df(samp)
    m <- run_model(model$metadata, samp_diff)
    p1 <- pred_df_diff(m, 2014, 2015:2019, "forwards", train_diff, train)
    p2 <- pred_df_diff(m, 2014, 1999:2014, "backwards", train_diff, train) %>%
      filter(year_id != 2014)
    pred <- rbind(p1,p2)
  }else{
    m <- run_model(model$metadata, samp)
    pred <- pred_df(m, 1999:2019, train)
  }
  pred <- pred[,.(location_id, year_id, pred)]
  return(pred)
}

# Bootstrapping
pred_dfl <- mclapply(c(1:1000), function(i){
  print(i)
  samp <- copy(slice_sample(states, n = 50, replace = TRUE)) # Sample states with replacement
  samp[, boot_loc := paste0(location_id, "_", .I), by="location_id"] # Generate unique state label
  samp <- merge(samp[,.(location_id, boot_loc)], train, by="location_id") # Get full dataset
  
  samp[, location_id := boot_loc][, boot_loc := NULL]

  pred <- make_prediction(samp, model = agg) # Run model
  pred[, draw := i]
  return(pred)
}, mc.cores = 30)
pred_df_draws <- rbindlist(pred_dfl)

# Aggregate and calculate intervals
pred_df_draws[, loc := str_sub(location_id, 1, 3)]
pred_df_draws <- pred_df_draws[!is.na(pred)]
pred_df_draws[,`:=`(mean = mean(pred), median = quantile(pred, 0.5), lower = quantile(pred, 0.025), upper = quantile(pred, 0.975)),
        by=c("loc","year_id")]
pred_df_draws[, location_id := loc][, loc := NULL]
pred_df_draws <- merge(pred_df_draws, train, by=c("location_id","year_id"), allow.cartesian = TRUE)
arrow::write_feather(pred_df_draws, paste0(agg$m_dir,"/model_",agg$metadata$model,"_draws.feather"))

pred_df <- unique(pred_df_draws[, draw := NULL][, pred := NULL])
pred_df[, in_ui := 0]
pred_df[pc_spending <= upper & pc_spending >= lower, in_ui := 1]

pred_df <- merge(pred_df, states, by="location_id")
pred_df[, within_ui := sum(in_ui, na.rm = TRUE)/(.N-5), by="abbreviation"]
unique(pred_df[,.(abbreviation, within_ui)])

# If necessary, scale bounds marginally until 95% of data is in bounds
# Need 20/21 data points to fall into UI for 95% coverage
df_scaled <- rbindlist(lapply(split(pred_df, by="abbreviation"), function(dt){
  # See how many points we need to be incuded
  i <- ceiling(0.95*nrow(dt[!is.na(pc_spending)])) + nrow(dt[is.na(pc_spending)])
  
  dt[, ui_dist := upper - mean][, true_dist := abs(pc_spending - mean)] # calculate distance of PI from mean, distance of point from mean
  scale <- ceiling(max(dt[year_id %in% 2009:2013]$true_dist, na.rm = TRUE)*1000)/1000
  
  dt[, upper := mean + scale][, lower := mean - scale]
  dt[, ui_dist := NULL][, true_dist := NULL]
  
  # validate
  # stopifnot(nrow(dt[!is.na(pc_spending) & pc_spending <= upper & pc_spending >= lower])/nrow(dt[!is.na(pc_spending)]) >= 0.95)
  return(dt)
}))

pred_df <- rbind(pred_df[within_ui >= 0.95], df_scaled)

# final validations
count(pred_df, year_id)
count(pred_df, location_id)
count_nas(pred_df, "year_id", c("mean","upper","lower"))

fwrite(pred_df, paste0(agg$m_dir,"/model_",agg$metadata$model,"_uncertainty.csv"))
