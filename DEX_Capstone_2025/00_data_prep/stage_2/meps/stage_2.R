# Running MEPS stage 2 processing
# - data formatting
#
# Author: Drew DeJarnatt

library(data.table)
library(arrow)
library(tidyverse)

# Source stage 2 functions
source("FILEPATH/stage_2_helpers.R")

stage_2 <- function(dataset){
  
  print(dataset)
  
  # file paths of stage 1 and stage 2 data
  in_dir <- "FILEPATH"
  out_dir <- paste0("FILEPATH", dataset, "/")
  
  # Gets full file names of parquet files in TOC specific folder
  files = list.files(path = paste0(in_dir, dataset), pattern = ".parquet", full.names = TRUE)
  
  for(file in files){
    
    year <- as.numeric(str_extract(file, pattern = "\\d{4}"))
    print(year)
    df <- as.data.table(read_parquet(file))
    
    # Adds service date to IP and OP 
    if(dataset %in% c("IP", "OP")){
      df <- get_serv_date(dataset, df, year)
    }
    
    # Cast some columns as numeric
    cast_num(dataset, df)
    
    # Replace values with NA
    df <- na_replace(df, na_values)
    
    # Rename columns to match DEX formatting
    df <- col_rename(dataset, df, year)
    
    # Add date columns
    get_year_cols(dataset, df)
    
    # Recode TOC to match DEX standard
    if(dataset %in% c("OP", "OB")){
      df[, TOC := "AM"]
    } else if(dataset == "ER"){
      df[, TOC := "ED"]
    } 
  
    # Recode same day check out -  LOS from 0 to 1 and add ER-IP transfer flag
    if(dataset == "IP"){
      df[LOS == 0, LOS := 1]
      df[, IP_TRANS_DCHG := ifelse(is.na(ERHEVIDX), 0, 1)]
    }
    
    # adding 'CODE_SYSTEM' column to HH files where missed the first time
    if(dataset == "HH" & year <= 2015){
      df[, CODE_SYSTEM := "icd9"]
    }
    
    # Format Race
    format_race(df, year)
    
    # Create Payment Amount Columns
    df <- get_pay_amts(dataset, df)
    
    # Add Primary payer
    get_pri_payer(dataset, df)
    
    # Add facility info for relevant TOCs
    if(dataset %in% c("IP", "ER", "OP")){
      get_fac_amounts(dataset, df)
    }
    
    # Age bin
    df <- age_bin(df, age_column_name = "AGE")
    print(colnames(df))

    # Write df to stage 2 folders
    file_name <- paste0("USA_MEPS_", as.character(year), "_", dataset)
    write_parquet(df, paste0(out_dir, file_name, ".parquet"))
    
    print(paste0("Year ", as.character(year), " successfully converted for ", dataset))
    
  }
}

# Run function above looping over datasets
tocs <- c("OP", "OB", "IP", "ER", "HH", "RX", "DV")
lapply(tocs, stage_2)

























