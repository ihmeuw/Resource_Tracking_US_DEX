# Worker script for the aggregate models
# Runs one model with dependent and independent variable params
# Writes predictions, model fits, and rmse
# USERNAME, Sept 2021

rm(list = ls())
library(kableExtra)
library(parallel)
source("FILEPATH/envelope_functions.R")

args <- commandArgs(trailingOnly = TRUE)[1]
# args <- "10000"
# print(args)
# array_chunk <- args[[1]][1] %>% as.integer() %>% print()

dir <- paste0("FILEPATH/aggregate_models/",Sys.Date(),"/")
dir.create(dir, showWarnings = FALSE)
existing_mods <- Sys.glob(paste0(dir,"*.RData"))
existing_mods <- str_extract(existing_mods,"model_\\d+") %>% str_remove("model_") %>% as.integer()

table <- fread(paste0(dir,"state_dep_var_table.csv"))

array_chunk <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")) %>% print()
table <- table[chunk == array_chunk]
table <- table[!(model %in% existing_mods)]
table[transformation != "linear", dep_var := paste0(spending, "_", transformation)]
table[transformation == "linear", dep_var := spending]

lapply(split(table, by="model"), function(x){
  print(x)
  
  if(x$difference_model){
    train_orig <- get_train_data()
    train <- create_diff_df(train_orig)
    train <- train[year_id > 1999]
  }else{
    train <- get_train_data()
  }
  
  # Modeling
  m <- run_model(x, train)
  m_oos_a <- run_model(x, train[year_id %in% c(2000:2009)])
  m_oos_b <- run_model(x, train[year_id >= 2004 & year_id < 2015])
  
  # Prediction
  if(x$difference_model){
    pred <- pred_df_diff(m, 2014, 2015:2019, "forwards", train, train_orig) %>%
      rbind(pred_df_diff(m, 2014, 1999:2014, "backwards", train, train_orig)) %>%
      unique()
    pred_oos_a <- pred_df_diff(m_oos_a, 2009, c(2010:2014), "forwards", train, train_orig)
    pred_oos_b <- pred_df_diff(m_oos_b, 2004, c(2000:2004), "backwards", train, train_orig)
  }else{
    pred <- pred_df(m, 1999:2019)
    pred_oos_a <- pred_df(m_oos_a, c(2010:2014))
    pred_oos_b <- pred_df(m_oos_b, c(2000:2004))
  }
  
  # RMSE
  resid <- na.omit(rbind(pred_oos_a,pred_oos_b)$pc_spending - rbind(pred_oos_a,pred_oos_b)$pred)
  rmse <- sqrt(mean(resid^2))
  
  save(m, m_oos_a, m_oos_b, pred, pred_oos_a, pred_oos_b, rmse, file = paste0(dir,"/model_", x$model, ".RData"))
})



