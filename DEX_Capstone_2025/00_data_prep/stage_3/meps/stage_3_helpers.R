# Functions used in MEPS stage 3 processing
#
# Author: Drew DeJarnatt

library(data.table)
library(arrow)
library(tidyverse)
library(haven)

# Stage 2 files
HH_files <- list.files(path = "FILEPATH/HH", pattern = ".parquet", full.names = TRUE)
ER_files <- list.files(path = "FILEPATH/ER", pattern = ".parquet", full.names = TRUE)
IP_files <- list.files(path = "FILEPATH/IP", pattern = ".parquet", full.names = TRUE)
OB_files <- list.files(path = "FILEPATH/OB", pattern = ".parquet", full.names = TRUE)
OP_files <- list.files(path = "FILEPATH/OP", pattern = ".parquet", full.names = TRUE)
RX_files <- list.files(path = "FILEPATH/RX", pattern = ".parquet", full.names = TRUE)
DV_files <- list.files(path = "FILEPATH/DV", pattern = ".parquet", full.names = TRUE)

meps_input_dir <- "FILEPATH"

# CONDITIONS
conditions <- list()
for(year in 1996:2021){
  file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = ".*MEDICAL_CONDITIONS.*\\.DTA$", full.names = TRUE)
  conditions <- append(conditions, file)
}

#EVENTS
events <- list()
for(year in 1996:2021){
  if(year <= 2010 | year == 2012){
    file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = ".*CLINK.*\\.DTA$", full.names = TRUE)
    events <- append(events, file)
  } else if(year == 2011){
    file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = "APPENDIX.*_1_.*\\.DTA$", full.names = TRUE)
    events <- append(events, file)
  } else if(year %in% c(2013, 2014)){
    file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = "APPENDIX.*FILE_1.*\\.DTA$", full.names = TRUE)
    events <- append(events, file)
  } else if(year == 2016){
    file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = ".*APPENDIX.*\\.DTA$", full.names = TRUE)
    events <- append(events, file)
  } else {
    file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = "APPENDIX.*F1_.*\\.DTA$", full.names = TRUE)
    events <- append(events, file)
  }
}

event_type <- list(
  "OB" = 1,
  "OP" = 2,
  "ER" = 3,
  "IP" = 4,
  "HH" = 7,
  "RX" = 8
)

# Function to read in stage 2 data
get_s2_file <- function(files, year){
  year <- as.character(year)
  file <- files[str_detect(files, paste0("_", year, "_"))][[1]]
  df <- as.data.table(read_parquet(file))
  colnames(df) <- tolower(colnames(df))
  setDT(df)
  return(df)
}

# Function to read in raw stata file when needed
get_raw_file <- function(files, year){
  year <- as.character(year)
  file <- files[str_detect(files, paste0("/", year, "/"))][[1]]
  df <- as.data.table(read_stata(file))
  colnames(df) <- toupper(colnames(df))
  return(df)
}

# Wide to long on diagnosis code
w2l <- function(df){
  id_cols <- colnames(df)[!str_detect(colnames(df), "dx_")]
  meas_cols <- colnames(df)[str_detect(colnames(df), "dx_")]
  df <- melt(df,
             id.vars = id_cols,
             measure.vars = meas_cols ,
             variable.name = "dx_level",
             value.name = "dx")

  setorder(df, claim_id, dx_level)
  setDT(df)
  df[, dx_level := NULL]
  df <- distinct(df, claim_id, dx, .keep_all = TRUE)
  df <- df %>%
    group_by(bene_id, claim_id) %>%
    mutate(dx_level = paste0('dx_', row_number())) %>%
    as.data.table()
}

# Columns to read in
dex_cols1 <- list("IP" = c("bene_id", "claim_id", "dx_1", "dx_2", "dx_3", "dx_4", "pri_payer", "code_system", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr",
                           "tot_pay_amt", "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_chg_amt", "wc_pay_amt",
                           "tot_fac_pay_amt", "tot_fac_chg_amt", "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "oop_fac_pay_amt", "priv_fac_pay_amt", "oth_fac_pay_amt", 
                           "wc_fac_pay_amt","service_date", "los", "ip_trans_dchg", "toc", "nid","month"), 
                  "OP" = c("bene_id", "claim_id", "dx_1", "dx_2", "dx_3", "dx_4", "pri_payer", "code_system", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "tot_pay_amt", "mdcr_pay_amt", "mdcd_pay_amt", 
                           "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_chg_amt",
                           "tot_fac_pay_amt", "tot_fac_chg_amt", "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "oop_fac_pay_amt", "priv_fac_pay_amt", "oth_fac_pay_amt", 
                           "wc_fac_pay_amt","service_date", "toc", "nid", "month"),
                  "OB" = c("bene_id", "claim_id", "dx_1", "dx_2", "dx_3", "dx_4", "pri_payer", "code_system", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "tot_pay_amt", "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_chg_amt",
                           "toc", "nid", "month"),
                  "ER" = c("bene_id", "claim_id", "er_ip_transfer","dx_1", "dx_2", "dx_3", "pri_payer", "code_system", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr",
                           "tot_pay_amt", "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_chg_amt", "wc_pay_amt",
                           "tot_fac_pay_amt", "tot_fac_chg_amt", "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "oop_fac_pay_amt", "priv_fac_pay_amt", "oth_fac_pay_amt", "wc_fac_pay_amt",
                           "toc", "nid", "month"),
                  "HH" = c("bene_id", "claim_id", "pri_payer", "code_system", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "tot_pay_amt", "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_chg_amt",
                           "toc", "nid", "month"),
                  "RX" = c("dupersid", "rxrecidx", "rxname","bene_id", "claim_id", "dx_1", "dx_2", "dx_3", "pri_payer", "code_system", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "tot_pay_amt", "mdcr_pay_amt", "mdcd_pay_amt", 
                           "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "toc", "nid", "ndc","month"),
                  "DV" = c("bene_id", "claim_id", "pri_payer", "year_id", "year_adm", "year_dchg", "year_clm", 
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr","mdcr_pay_amt", "mdcd_pay_amt", 
                           "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt","toc", "nid", "month"))

# Columns to write out
dex_cols2 <- list("IP" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm", "service_date", "los", "ip_trans_dchg",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "code_system", "dx", "dx_level", 
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt",
                           "tot_fac_pay_amt", "tot_fac_chg_amt", "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "oop_fac_pay_amt", "priv_fac_pay_amt", "oth_fac_pay_amt",
                           "pri_payer", "toc", "nid", "month"),
                  
                  "OP" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm", "service_date",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "code_system", "dx", "dx_level", 
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt",
                           "tot_fac_pay_amt", "tot_fac_chg_amt", "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "oop_fac_pay_amt", "priv_fac_pay_amt", "oth_fac_pay_amt",
                           "pri_payer", "toc", "nid", "month"),
                  
                  "ER" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "code_system", "dx", "dx_level", 
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt",
                           "tot_fac_pay_amt", "tot_fac_chg_amt", "mdcr_fac_pay_amt", "mdcd_fac_pay_amt", "oop_fac_pay_amt", "priv_fac_pay_amt", "oth_fac_pay_amt",
                           "pri_payer", "toc", "nid", "month"),
                  
                  "OB" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "code_system", "dx", "dx_level", 
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt","pri_payer",
                           "toc", "nid", "month"),
                  
                  "HH" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "code_system", "dx", "dx_level", 
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt","pri_payer",
                           "toc", "nid", "month"),
                  
                  "RX" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "code_system", "dx", "dx_level", "ndc",
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt","pri_payer",
                           "toc", "nid", "month", "days_supply"),
                  
                  "DV" = c("claim_id","bene_id", "year_id", "year_adm", "year_dchg", "year_clm",
                           "age_group_years_start", "age_group_id", "sex_id", "race_cd", "survey_wt", "varpsu", "varstr", "acause", "primary_cause",
                           "mdcr_pay_amt", "mdcd_pay_amt", "priv_pay_amt", "oop_pay_amt", "oth_pay_amt", "tot_pay_amt", "tot_chg_amt","pri_payer",
                           "toc", "nid", "month"))


# Dental codebook
dental_cb <- fread("FILEPATH/dental_codebook.csv", na.strings = "")
dental_cb <- dental_cb[,1:20]

# Apply days_supply map
days_supply_map <- open_dataset("FILEPATH") %>% collect() %>% setDT()
days_supply_map[, days_supply := as.integer(round(days_supply))]
days_supply_map[is.na(age_cat), age_cat := "-1"]
ds_map_na <- days_supply_map[is.na(year_id)][,.(ndc, days_supply)]
setnames(ds_map_na, "days_supply", "days_supply_na")
ds_map <- days_supply_map[!is.na(year_id)]
setnames(ds_map, "days_supply", "days_supply_imp")


