# Functions used in MEPS stage 2 processing
#
# Author: Drew DeJarnatt

library(data.table)
library(arrow)
library(tidyverse)

# Cast some columns as numeric
cast_num <- function(dataset, df){
  
  num_cols <- colnames(df[, .SD, .SDcols = str_detect(colnames(df), "_AMT|FXP|DUPERSID|YR|YEAR|SEX|WT|VAR")])
  df[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]
  
  if(dataset == "IP"){
    df[, LOS := as.numeric(LOS)]
  }
}

# Replace following values values with NA
na_values <- c("-1", "-3", "-7", "-8", "-9", "999 TAKEN AS NEEDED", "-8 DO NOT KNOW", "-7 REFUSED")

na_replace<- function(df, na_values) {
  for (col in names(df)) {
    for (na_val in na_values) {
      df[get(col) == na_val, (col) := NA]
    }
  }
  return(df)
}



# Column Renaming to DEX format
col_rename <- function(dataset, df, year){
  df[, sex_id := SEX]
  df[, bene_id := DUPERSID]
  df[, survey_wt := WT]
  
  if(dataset == "RX"){
    df[, claim_id := RXRECIDX]
  } else {
    df[, claim_id := EVNTIDX]
  }
  
  # Before 2002 VAR columns had the short year at the end of the name
  if(year <= 2001){
    setnames(df, colnames(df)[str_detect(colnames(df), pattern = "^VARPSU")], "VARPSU")
    setnames(df, colnames(df)[str_detect(colnames(df), pattern = "^VARSTR")], "VARSTR")
  }
  
  return(df)
}

# Adding date columns
get_year_cols <- function(dataset, df){
  setDT(df)
  df[, year_clm := YEAR_ID]
  
  # IP only TOC that has the BEGYR and ENDYR columns
  if(dataset == "IP"){
    df[, year_adm := IPBEGYR]
    df[, year_dchg := IPENDYR]
  } else {
    df[, year_adm := YEAR_ID]
    df[, year_dchg := YEAR_ID]
  }
}

# Service Date
get_serv_date <- function(dataset, df, year){
  
  if(year >= 2013 & dataset == "IP"){
    df[, paste0(dataset, "BEGDD") := "1"]
  } else if(year >= 2013 & dataset == "OP"){
    df[, paste0(dataset, "DATEDD") := '1']
  }
  
  if(dataset == "IP"){
    df <- df %>%
      mutate(IPBEGYR = as.character(IPBEGYR),
             IPBEGDD = ifelse(is.na(IPBEGDD) | IPBEGDD %in% c("-1", "-8", "-9"), "1", IPBEGDD),
             IPBEGMM = ifelse(is.na(IPBEGMM) | IPBEGMM %in% c("-1", "-8", "-9"), "1", IPBEGMM)) %>%
      mutate(service_date = str_c(IPBEGDD, IPBEGMM, IPBEGYR, sep = "/")) %>%
      mutate(service_date = format(as.Date(service_date, format = "%d/%m/%Y"), format = "%d/%m/%Y")) #changes 1/1/2001 to 01/01/2001
  } else {
    date_renames <- c(day = paste0(dataset, "DATEDD"), month = paste0(dataset, "DATEMM"), year = paste0(dataset, "DATEYR"))
    
    df <- df %>%
      rename(all_of(date_renames)) %>%
      mutate(year = as.character(year),
             day = ifelse(is.na(day) | day %in% c("-1", "-8", "-9"), "1", day),
             month = ifelse(is.na(month) | month %in% c("-1", "-8", "-9"), "1", month)) %>%
      mutate(service_date = str_c(day, month, year, sep = "/")) %>%
      mutate(service_date = format(as.Date(service_date, format = "%d/%m/%Y"), format = "%d/%m/%Y"))
  }
  return(df)
}

# Formatting race to race_cd dex format
format_race <- function(df, year){
  if(year <= 2001){
    df[, RACE_CD := fcase(
      HISPANX %in% c(1, "1.0", "1 HISPANIC"), "HISP",
      RACEX %in% c(1, "1.0"), "AIAN",
      RACEX %in% c(2, "2.0"), "BLCK",
      RACEX %in% c(3, "3.0"), "API", 
      RACEX %in% c(4, "4.0"), "BLCK",
      RACEX %in% c(5, "5.0"), "WHT",
      RACEX %in% c(91, "91.0"), "UNK",
      default = "UNK"
    )]
  }else if(between(year, 2002, 2011)) {
    df[, RACE_CD := fcase(
      HISPANX %in% c(1, "1.0", "1 HISPANIC"), "HISP",
      RACEX %in% c(1, "1.0", "1 WHITE - NO OTHER RACE REPORTED"), "WHT",
      RACEX %in% c(2, "2.0", "2 BLACK - NO OTHER RACE REPORTED"), "BLCK",
      RACEX %in% c(3, "3.0", "3 AMER INDIAN/ALASKA NATIVE - NO OTH RAC"), "AIAN", 
      RACEX %in% c(4, "4.0", "4 ASIAN - NO OTHER RACE REPORTED"), "API",
      RACEX %in% c(5, "5.0", "5 NATIVE HAWAIIAN/PACIFIC ISLANDER-NO OTHR"), "API",
      RACEX %in% c(6, "6.0", "6 MULTIPLE RACES REPORTED"), "UNK",
      default = "UNK"
    )]
  } else {
    df[, RACE_CD := fcase(
      HISPANX %in% c(1, "1.0", "1 HISPANIC"), "HISP",
      RACEV1X %in% c(1, "1.0", "1 WHITE - NO OTHER RACE REPORTED"), "WHT",
      RACEV1X %in% c(2, "2.0", "2 BLACK - NO OTHER RACE REPORTED"), "BLCK",
      RACEV1X %in% c(3, "3.0", "3 AMER INDIAN/ALASKA NATIVE-NO OTHER RACE"), "AIAN", 
      RACEV1X %in% c(4, "4.0", "4 ASIAN/NATV HAWAIIAN/PACFC ISL-NO OTH"), "API",
      RACEV1X %in% c(5, "5.0", "5 NATIVE HAWAIIAN/PACIFIC ISLANDER-NO OTHR"), "API",
      RACEV1X %in% c(6, "6.0", "6 MULTIPLE RACES REPORTED"), "UNK",
      default = "UNK"
    )]
  }
}

# Age Binning
source("FILEPATH")

age_bin <- function(df, age_column_name = "age"){
  setnames(df, age_column_name, "age")
  dfcols <- colnames(df)
  
  if(class(df$age) != "numeric"){
    df[, age := as.numeric(age)]
  }
  
  ages <- get_age_metadata(age_group_set_id = 27, gbd_round_id = 7)
  ages <- select(ages, age_group_id, age_group_years_start)
  setorder(ages, age_group_years_start)
  
  df <- df %>%
    mutate(age_group_years_start = cut(age, breaks = c(ages$age_group_years_start, 125), labels = ages$age_group_years_start, right = FALSE)) %>%
    mutate(age_group_years_start = as.numeric(as.character(age_group_years_start)))
  df <- left_join(df, ages, by="age_group_years_start")
  
  select(df, c(all_of(dfcols), colnames(ages)))
  setnames(df, "age", age_column_name)
  return(df)
}

# Add payment amount columns 
get_pay_amts <- function(dataset, df){
  
  # these datasets are split by facility and professional payments
  if(dataset %in% c("ER", "IP", "OP")){
    
    # rename facility payments column if is wasn't converted properly in initial run
    if(any(str_detect(colnames(df), pattern = paste0("^", dataset, "FXP")))) {
      setnames(df, "TOT_F_PMT_AMT", "TOT_FAC_CHG_AMT")
      setnames(df, colnames(df)[str_detect(colnames(df), pattern = paste0("^", dataset, "FXP"))], "TOT_FAC_PAY_AMT") 
    } else {
      setnames(df, "TOT_F_PMT_AMT", "TOT_FAC_PAY_AMT")
    }
    
    # including worker's comp payment column here to determine pri_payer, but will drop later bc it's included in oth_pay_amt
    df[, ':='(oop_pay_amt = PMT_AMT_F_SF + PMT_AMT_D_SF,     #SF = Self or family (out of pocket)
              mdcd_pay_amt = PMT_AMT_F_MD + PMT_AMT_D_MD,    #MD = Medicaid
              mdcr_pay_amt = PMT_AMT_F_MR + PMT_AMT_D_MR,    #MR = Medicare
              wc_pay_amt = PMT_AMT_F_WC + PMT_AMT_D_WC)]     #WC = Worker's Comp
    
    # Sum payments from private insurance into priv_pay_amts (PMT_AMT_{F/D}_OR not in every year so use na.rm = TRUE to avoid NAs)
    df[, priv_pay_amt := rowSums(.SD, na.rm = TRUE), .SDcols = c("PMT_AMT_F_PV", "PMT_AMT_D_PV",  #PV = Private
                                                                 "PMT_AMT_F_OR", "PMT_AMT_D_OR")] #OR = Other Private
    
    # These are present every year except encounters that have entirely missing payment info, na.rm arg above sets all missing to 0 but we want NA in that case
    df[is.na(PMT_AMT_F_PV) & is.na(PMT_AMT_D_PV), priv_pay_amt := NA]
    
    
    # Sum across other payment categories
    df[, oth_pay_amt := rowSums(.SD, na.rm = TRUE), .SDcols = c("PMT_AMT_F_OF", "PMT_AMT_D_OF",  #OF = Other Federal
                                                                "PMT_AMT_F_OT", "PMT_AMT_D_OT",  #OT = Other Insurance
                                                                "PMT_AMT_F_OU", "PMT_AMT_D_OU",  #OU = Other Public
                                                                "PMT_AMT_F_SL", "PMT_AMT_D_SL",  #SL = State and Local Gov
                                                                "PMT_AMT_F_TR", "PMT_AMT_D_TR",  #TR = Tricare
                                                                "PMT_AMT_F_CH", "PMT_AMT_D_CH",  #CH = CHAMPUS/CHAMPVA
                                                                "PMT_AMT_F_VA", "PMT_AMT_D_VA",  #VA = VETERANS/CHAMPVA
                                                                "PMT_AMT_F_WC", "PMT_AMT_D_WC")] #WC = Worker's Comp
    
    # When these are missing, the encounter doesn't have any payment info
    df[is.na(PMT_AMT_F_OF) & is.na(PMT_AMT_F_OT), oth_pay_amt := NA]
    
  } else {
    
    # Rename out-of-pocket, medicaid, medicare, and worker's comp payment columns
    df[, ':='(oop_pay_amt = PMT_AMT_SF,
              mdcd_pay_amt = PMT_AMT_MD,
              mdcr_pay_amt = PMT_AMT_MR, 
              wc_pay_amt = PMT_AMT_WC)]
    
    # OR not present every year
    df[, priv_pay_amt := rowSums(.SD, na.rm = TRUE), .SDcols = c("PMT_AMT_PV", "PMT_AMT_OR")]
    
    # Above returns 0 when both are missing but we want NA
    df[is.na(PMT_AMT_PV) & is.na(PMT_AMT_OR), priv_pay_amt := NA]
    
    # Sum across other payment categories
    df[, oth_pay_amt := rowSums(.SD, na.rm = TRUE), .SDcols = c("PMT_AMT_OF", 
                                                                "PMT_AMT_OT", 
                                                                "PMT_AMT_OU", 
                                                                "PMT_AMT_SL", 
                                                                "PMT_AMT_TR", 
                                                                "PMT_AMT_CH", 
                                                                "PMT_AMT_VA",
                                                                "PMT_AMT_WC")] 
    
    # When these are missing, the encounter doesn't have any payment info
    df[is.na(PMT_AMT_OF) & is.na(PMT_AMT_OT), oth_pay_amt := NA]
  }
  
  # Rename total payment amount to match dex formatting
  setnames(df, "TOT_PMT_AMT", "tot_pay_amt")
  
  # RX does not have charge amounts
  if(dataset != "RX"){
    df <- setnames(df, "TOT_CHG_AMT", "tot_chg_amt")
  }
  
  # Set as data.table object of not already
  if(is.data.table(df) == FALSE){
    df <- as.data.table(df)
  }
  return(df)
}


# Determine primary payer with payer order rules - WC > MDCR > OTH > PRIV > MDCD > OOP
get_pri_payer <- function(dataset, df){
  
  # RX does not have charge info 
  if(dataset != "RX"){
    df[, pri_payer := fcase(
      is.na(tot_pay_amt) & is.na(tot_chg_amt), 21,
      tot_pay_amt == 0 & tot_chg_amt > 0, 21,
      tot_chg_amt == 0 , 20,
      wc_pay_amt > 0, 19, 
      mdcr_pay_amt > 0, 1,
      oth_pay_amt > 0 & mdcr_pay_amt == 0, 19,
      priv_pay_amt > 0 & mdcr_pay_amt == 0 & oth_pay_amt == 0, 2,
      mdcd_pay_amt > 0 & mdcr_pay_amt == 0 & oth_pay_amt == 0 & priv_pay_amt == 0, 3,
      oop_pay_amt > 0 & mdcr_pay_amt == 0 & oth_pay_amt == 0 & priv_pay_amt == 0 & mdcd_pay_amt == 0, 4)]
  } else {
    df[, pri_payer := fcase(
      is.na(tot_pay_amt) , 21,
      tot_pay_amt == 0 , 21,
      wc_pay_amt > 0, 19, 
      mdcr_pay_amt > 0, 1,
      oth_pay_amt > 0 & mdcr_pay_amt == 0, 19,
      priv_pay_amt > 0 & mdcr_pay_amt == 0 & oth_pay_amt == 0, 2,
      mdcd_pay_amt > 0 & mdcr_pay_amt == 0 & oth_pay_amt == 0 & priv_pay_amt == 0, 3,
      oop_pay_amt > 0 & mdcr_pay_amt == 0 & oth_pay_amt == 0 & priv_pay_amt == 0 & mdcd_pay_amt == 0, 4)]
  }
}

## Adding step to include the facility charges and payer specific facility payments to IP, ED, and OP
get_fac_amounts <- function(dataset, df) {
  
  # Sum payments from private insurance into priv_pay_amts (PMT_AMT_{F/D}_OR not in every year so use na.rm = TRUE to avoid NAs)
  df[, priv_fac_pay_amt := rowSums(.SD, na.rm = TRUE), .SDcols = c("PMT_AMT_F_PV",
                                                                   "PMT_AMT_F_OR")] 
  
  # These are present every year except encounters that have entirely missing payment info, 
  # na.rm arg above sets all missing to 0 but we want NA in that case
  df[is.na(PMT_AMT_F_PV) & is.na(PMT_AMT_D_PV), priv_fac_pay_amt := NA]
  
  # Sum across other payment categories
  df[, oth_fac_pay_amt := rowSums(.SD, na.rm = TRUE), .SDcols = c("PMT_AMT_F_OF",   
                                                                  "PMT_AMT_F_OT",   
                                                                  "PMT_AMT_F_OU",   
                                                                  "PMT_AMT_F_SL",   
                                                                  "PMT_AMT_F_TR", 
                                                                  "PMT_AMT_F_CH", 
                                                                  "PMT_AMT_F_VA", 
                                                                  "PMT_AMT_F_WC")] 
  
  # When these are missing, the encounter doesn't have any payment info
  df[is.na(PMT_AMT_F_OF) & is.na(PMT_AMT_F_OT), oth_fac_pay_amt := NA]
  
  # match names to dex formatting
  if(any(str_detect(colnames(df), "TOT_F_CHG_AMT"))){
    setnames(df, "TOT_F_CHG_AMT", "TOT_FAC_CHG_AMT")
  }
  
  setnames(df, "PMT_AMT_F_SF", "oop_fac_pay_amt")
  setnames(df, "PMT_AMT_F_MD", "mdcd_fac_pay_amt")
  setnames(df, "PMT_AMT_F_MR", "mdcr_fac_pay_amt")
  setnames(df, "PMT_AMT_F_WC", "wc_fac_pay_amt")
  
  return(df)
}

