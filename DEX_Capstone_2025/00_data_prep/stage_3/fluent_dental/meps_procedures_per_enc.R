#
# Pulling procedures per dental encounter from MEPS to apply to Fluent dental to convert # of treatments to # of encounters
#
# Drew DeJarnatt

# load packages
library(tidyverse)
library(data.table)
library(arrow)
'%nin%' <- Negate('%in%')

# stage 2 MEPS file paths
in_dir <- "FILEPATH"
files <- list.files(path = in_dir, pattern = ".parquet", full.names = TRUE)[5:24]

# dental column names used each year
dental_cb <- fread("FILEPATH", na.strings = "")

# Read MEPS file paths and keep only relevant dental procedure columns
meps_dv <- lapply(files, function(x) {
  
  year <- str_extract(x, pattern = "\\d{4}")
  print(year)
  
  df <- data.table(read_parquet(x))
  
  # post 2017 there are fewer dental procedures reported 
  if(year < 2017){
    # pull dental columns names used in specific year    
    d_col_names <- toupper(as.character(dental_cb[year_id == year, ])[-1])
    print(d_col_names)
    # the column names of dental_cb are what we want to replace the MEPS column names with    
    clean_d_names <- names(dental_cb)[-1]
    print(clean_d_names)
    setnames(df, d_col_names, clean_d_names)
  } else {
    
    d_col_names <- toupper(as.character(dental_cb[year_id == year, ])[-1])
    # columns not present in 2017-2019 have NA value in dental_cb
    # drop the NAs because they were causing errors in setnames()
    valid_d_col_names <- d_col_names[!is.na(d_col_names)]
    clean_d_names <- names(dental_cb)[-1]
    clean_d_names <- clean_d_names[!is.na(d_col_names)]
    setnames(df, valid_d_col_names, clean_d_names)
  }
  
  df2 <- select(df, YEAR_ID, age_group_years_start, sex_id, DUPERSID, EVNTIDX, all_of(clean_d_names))
  
  return(df2)
}) %>% bind_rows()

# calculate procedures per encounter by year and age (shit and cause?)
dental_columns <- names(meps_dv)[6:ncol(meps_dv)]

# make sure they are appropriately numeric
meps_dv[, (dental_columns) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = dental_columns]
# convert 2s to 0s
meps_dv[, (dental_columns) := lapply(.SD, function(x) replace(x, x ==2, 0)), .SDcols = dental_columns]

# count procedures in each encounter
meps_dv[, procedures := rowSums(.SD, na.rm = TRUE), .SDcols = dental_columns]

# Routine cleanings set as exp_well_dental, all other procedures are _oral
meps_dv[, acause := ifelse(clenteth == 1, "exp_well_dental", "_oral")]

# aggregate to year, age, sex with number of encounters and number of procedures                                     
proc_per_enc <- meps_dv[procedures > 0, .(encounters = .N, procedures = sum(procedures)), by = .(year_id = YEAR_ID, age_group_years_start, acause)]
proc_per_enc[, proc_per_enc := procedures/encounters]


# Loess smoothing
loess_data <- proc_per_enc[!is.na(age_group_years_start) & age_group_years_start %nin% c(0, 90), .(year_id, proc_per_enc, acause, age_group_years_start)]

# For exp_well_dental predictions
exp_data <- loess_data[acause == "exp_well_dental"]
exp_well_model <- loess(data = exp_data, proc_per_enc ~ year_id + age_group_years_start, span = 0.95)
exp_well_pred_values <- predict(exp_well_model, exp_data)
exp_well_pred_df <- data.table(prediction = exp_well_pred_values, exp_data[, .(year_id, age_group_years_start)])
exp_well_pred_df[, acause := "exp_well_dental"]

# For _oral predictions
oral_data <- loess_data[acause == "_oral"]
oral_model <- loess(data = oral_data, proc_per_enc ~ year_id + age_group_years_start, span = 0.95)
oral_pred_values <- predict(oral_model, oral_data)
oral_pred_df <- data.table(prediction = oral_pred_values, oral_data[, .(year_id, age_group_years_start)])
oral_pred_df[, acause := "_oral"]

pred_df <- rbind(exp_well_pred_df, oral_pred_df)
setnames(pred_df, "prediction", "proc_per_enc")
fwrite(pred_df, "FILEPATH")
