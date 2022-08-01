# Generate toc-specific total state spending models -------
# Tests a set of seemingly unrelated regression (SUR) models using center log offset (CLO) vs. raking to the total model
##
# Transformations depend on model parameters:
## CLO: 
### 1. Put dependent and independent variables in fraction of total state spending space
### 2. Run center log offset transform (from shared tooling)
### 3. Generate model and create predictions
### 4. Run inverse center log offset transform
### 5. Transform data and prediction to per capita space, calculate RMSE
##
## Linear space model w/ raking: 
### 1. Put dependent and independent variables in fraction of total state spending space
### 2. Generate model and create predictions
### 3. Rake predictions to total state spending
### 4. Transform data and prediction to per capita space, calculate RMSE
##
## Logit-space model w/ raking:
### 1. Put dependent and independent variables in fraction of total state spending space
### 2. Perform logit transformation on dependent and independent variables
### 3. Generate model and create predictions
### 4. Perform inverse logit transformation
### 5. Rake predictions to total state spending
### 6. Transform data to per capita space, calculate RMSE

# Setup and data prep -----------------------------------------------------
rm(list = ls())
suppressMessages(library(data.table))
suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
suppressMessages(library(systemfit)) # SUR modeling
suppressMessages(library(boot))
source("FILEPATH/clr.R")
source("FILEPATH/envelope_functions.R")
load("FILEPATH/states.RData")
states$location_id <- as.factor(states$location_id)

# args <- commandArgs(trailingOnly = TRUE)[1]
# # args <- "1"
# print(args)
# mod <- args[[1]][1] %>% print()

mod <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")) %>% print()

# Load model specific metadata
table <- fread("FILEPATH/state_dep_var_table_toc.csv")
x <- table[model == mod]

load("FILEPATH/toc_model_set.RData")
m_list <- m[x$state_effect][[1]]
m_list <- m_list[x$model_types][[1]]
m_list <- m_list[x$addl_covs][[1]]

print(x)
print(m_list)

# Load data
if(!x$difference_model){
  dt <- get_train_data_toc()
  # CLO models - must apply CLO transform first
  if(x$model_type == "CLO"){
    dt <- clr(dt, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                           "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
  }
}else{
  df_orig <- get_train_data_toc()
  # CLO models - must apply CLO transform first
  if(x$model_type == "CLO"){
    df_orig <- clr(df_orig, cols = c("pr_spending_dent","pr_spending_hh","pr_spending_hosp","pr_spending_nf",
                           "pr_spending_oprof","pr_spending_other","pr_spending_pharma","pr_spending_phys"))
  }
  dt <- create_diff_df(select(df_orig, -abbreviation, -state_name, -ends_with("NA")))
  dt <- merge(dt, unique(df_orig[,.(location_id, abbreviation)]), by="location_id", allow.cartesian = TRUE)
}

dt <- dt[, c("abbreviation","state_name","state") := NULL]
dt <- merge(dt, states, by="location_id")

dir <- paste0("FILEPATHtoc_models/",Sys.Date(),"/")
dir.create(dir, showWarnings = FALSE)

# TOC model functions ---------------------------------------------------
# model_processing: takes a set of SUR models and runs in-sample and out-of-sample fitting and prediction.
# output is a one-row df containing model metadata, in-sample RMSE by toc and overall, and oos RMSE by toc and overall


# Create in-sample & out-of-sample models
m_is <- systemfit(m_list, method = "SUR", data = dt[year_id %in%  1999:2019])
m_oos_a <- systemfit(m_list, method = "SUR", data = dt[year_id < 2011])
m_oos_b <- systemfit(m_list, method = "SUR", data = dt[year_id >= 2003 & year_id < 2015])

if(!x$difference_model){
  # Predict on full df for is, opposite dfs for oos using pred_df function
  pred_is <- pred_df_toc(m_is, dt[year_id %in%  1999:2019], x$model_type)
  pred_oos_a <- pred_df_toc(m_oos_a, dt[year_id >= 2011 & year_id < 2015], x$model_type)
  pred_oos_b <- pred_df_toc(m_oos_b, dt[year_id < 2003], x$model_type)
}else{
  # Predict on full df for is, opposite dfs for oos using pred_df function
  pred_is <- pred_df_diff_toc(m_is, dt, x$model_type, 2014, 2015:2019, "forwards") %>%
    rbind(pred_df_diff_toc(m_is, dt, x$model_type, 2014, 2003:2014, "backwards")) %>%
    unique()
  pred_oos_a <- pred_df_diff_toc(m_oos_a, dt, x$model_type, 2009, 2010:2014, "forwards")
  pred_oos_b <- pred_df_diff_toc(m_oos_b, dt, x$model_type, 2007, 2003:2007, "backwards")
}

# Get residuals using get_resid function, relabel for in/out of sample
oos_rmse <- get_resid_toc(rbind(pred_oos_a, pred_oos_b))
names(oos_rmse) <- paste0(names(oos_rmse),"_oos")
oos_rmse$model <- mod

save(m_list, m_is, m_oos_a, m_oos_b, pred_is, pred_oos_a, pred_oos_b, oos_rmse,
     file = paste0(dir,"model_set_id_",mod,".RData"))
  