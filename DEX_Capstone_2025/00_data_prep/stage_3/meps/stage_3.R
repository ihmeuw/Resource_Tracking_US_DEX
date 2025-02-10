# MEPS stage 3 processing
# - pulling ICD code from conditions/events file
# - going wide to long on diagnosis code
# - dental specific data prep
# 
# Author: Drew DeJarnatt

library(data.table)
library(arrow)
library(tidyverse)
library(haven)

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source("../stage_3_helpers.R")
source("FILEPATH/stage_2_helpers.R")

stage_3 <- function(dataset, test){
  in_dir <- "FILEPATH"
  # writing to temp folder before joining primary payer from sample denominators
  out_dir <- paste0("FILEPATH")
  
  files <- list.files(path = paste0(in_dir, dataset), pattern = ".parquet", full.names = TRUE)
  
  df_final <- data.table(matrix(nrow = 0, ncol = length(dex_cols2[[dataset]])))
  
  colnames(df_final) <- dex_cols2[[dataset]]
  
  for(file in files){
    
    year <- as.numeric(str_extract(file, pattern = "\\d{4}"))
    # Read in stage 2 expenditure file
    df <- get_s2_file(files, year)
    setDT(df)
    #rename month column for sample denominator prep
    month_cols <- c("ipbegmm", "dvdatemm", "obdatemm", "erdatemm", "hhdatemm", "rxbegmm", "month", "hhbegmm")
    if(any(colnames(df) %in% month_cols)){
      df <- df %>% rename(month = any_of(month_cols))
    }
    
    # Years <= 2015 had ICD codes 
    # HH didn't have any ICD or CCC in raw files so always have to join to events/conditions - DV assigns cause based on procedures
    if(year <= 2015 & dataset != "HH" & dataset != "DV"){
      
      # Selecting dex columns plus necessary columns for processing
      cols <- dex_cols1[[dataset]]
      if(dataset == "RX" & year >= 2010){
        cols <- c(cols, "rxdaysup")
      }
      df <- df[, ..cols]
      if(dataset == "RX" & year < 2010){
        df[, rxdaysup := NA]
      }
      
      # Drop ER-IP transfers from ER data - aggregated to IP instead
      if(dataset == "ER"){
        df <- df[is.na(er_ip_transfer)]
      }
      
      # Aggregate ER-IP transfers
      # er-ip-transfer column in ER is either NA or contains the claim_id of matching IP encounter
      if(dataset == "IP"){
        
        # Create temp ER data containing just linking IDs and payment info (defer to IP dx)
        er_temp <- get_s2_file(ER_files, year) %>%
          filter(!is.na(er_ip_transfer)) %>%
          # Assign ER the claim_id that matches IP encounter
          select(-claim_id) %>%
          mutate(claim_id = er_ip_transfer) %>% # er_ip_transfer in ER == claim_id of corresponding IP encounter
          select(bene_id, claim_id, contains("pay_amt"), contains("chg_amt"))
        
        # Join temp ER data to IP and sum payment columns 
        df <- left_join(df, er_temp, by = c("bene_id", "claim_id"))
        df <- df %>%
          rowwise() %>%
          mutate(mdcr_pay_amt = sum(mdcr_pay_amt.x, mdcr_pay_amt.y, na.rm = TRUE),
                 mdcd_pay_amt = sum(mdcd_pay_amt.x, mdcd_pay_amt.y, na.rm = TRUE),
                 oop_pay_amt = sum(oop_pay_amt.x, oop_pay_amt.y, na.rm = TRUE),
                 oth_pay_amt = sum(oth_pay_amt.x, oth_pay_amt.y, na.rm = TRUE),
                 priv_pay_amt = sum(priv_pay_amt.x, priv_pay_amt.y, na.rm = TRUE),
                 wc_pay_amt = sum(wc_pay_amt.x, wc_pay_amt.y, na.rm = TRUE),
                 tot_fac_pay_amt = sum(tot_fac_pay_amt.x, tot_fac_pay_amt.y, na.rm = TRUE),
                 tot_fac_chg_amt = sum(tot_fac_chg_amt.x, tot_fac_chg_amt.y, na.rm = TRUE),
                 tot_pay_amt = sum(tot_pay_amt.x, tot_pay_amt.y, na.rm = TRUE),
                 tot_chg_amt = sum(tot_chg_amt.x, tot_chg_amt.y, na.rm = TRUE),
                 #Adding facility payment columns
                 mdcr_fac_pay_amt = sum(mdcr_fac_pay_amt.x, mdcr_fac_pay_amt.y, na.rm = TRUE),
                 mdcd_fac_pay_amt = sum(mdcd_fac_pay_amt.x, mdcd_fac_pay_amt.y, na.rm = TRUE),
                 oop_fac_pay_amt = sum(oop_fac_pay_amt.x, oop_fac_pay_amt.y, na.rm = TRUE),
                 oth_fac_pay_amt = sum(oth_fac_pay_amt.x, oth_fac_pay_amt.y, na.rm = TRUE),
                 priv_fac_pay_amt = sum(priv_fac_pay_amt.x, priv_fac_pay_amt.y, na.rm = TRUE),
                 wc_fac_pay_amt = sum(wc_fac_pay_amt.x, wc_fac_pay_amt.y, na.rm = TRUE)
          ) %>%
          select(-contains("pay_amt."), -contains("chg_amt.")) %>%
          as.data.table() %>%
          # Reassign pri_payer with new payment info 
          get_pri_payer("IP", df = .) 
        
      }
      
      # limit prescriptions per person per drug to 12
      ## some people have ~100 prescriptions of the same drug 
      if(dataset == "RX"){
        # limit # of prescriptions per drug per year to 12
        df <- df %>%
          group_by(dupersid, ndc, tot_pay_amt) %>% #first make sure we're not removing unique payment info
          mutate(row_number = row_number()) %>%
          filter(row_number <= 12) %>%
          group_by(dupersid, ndc) %>%
          mutate(row_number = row_number()) %>%
          filter(row_number <= 12) %>%
          select(-row_number)
      }
      
      # Wide to long on DX
      df <- w2l(df)
      
      
    } else if(dataset != "DV"){
      
      # Event file is the bridge between expenditure file and conditions file
      event_file <- get_raw_file(events, year)
      
      # Conditions file contains ICD codes
      cond_file <- get_raw_file(conditions, year)
      
      if(year <= 2015){
        setnames(cond_file, "ICD9CODX", "dx")
      } else {
        setnames(cond_file, "ICD10CDX", "dx")
      }
      
      # Join events to conditions (event type = TOC)
      cond_ev <- left_join(cond_file, event_file, by = c("DUPERSID", "CONDIDX")) %>%
        select(DUPERSID, CONDIDX, EVNTIDX, EVENTYPE, dx) %>%
        filter(EVENTYPE == event_type[[dataset]]) %>%
        distinct(DUPERSID, EVNTIDX, dx) %>%
        group_by(DUPERSID, EVNTIDX) %>%
        # MEPS gives codes in the order they were reported by respondent, so just use first given as dx_1
        mutate(dx_level = paste0('dx_', row_number()),
               dupersid = as.numeric(DUPERSID),
               claim_id = EVNTIDX,
               dx = ifelse(dx %in% c("-1", "-8", "-9", "-15"), NA, dx)) %>%
        ungroup() %>%
        select(-DUPERSID, -EVNTIDX)
      
      # RX joins to conditions file on another ID
      if(dataset == "RX"){
        # limit # of prescriptions per drug per year to 12
        df <- df %>%
          group_by(dupersid, linkidx, tot_pay_amt) %>% #first make sure were not removing unique payment info
          mutate(row_number = row_number()) %>%
          filter(row_number <= 12) %>%
          group_by(dupersid, linkidx) %>%
          mutate(row_number = row_number()) %>%
          filter(row_number <= 12) %>%
          select(-row_number)
        
        df <- left_join(df, cond_ev, by = c("dupersid", "evntidx" = "claim_id"), relationship = 'many-to-many')
        # relationship = many to many bc the cond_ev file is DX per prescription (can be multiple)
        # and the RX expenditure file has a row per prescription filled (refills have the same event id (evntidx))
        # so if multiple refills in df - the dx's from cond ev will have multiple matches
        # and if multiplt dxs in cond_ev, the prescription event in df will have multiple matches
        
      } else{
        df <- left_join(df, cond_ev, by = c("dupersid","claim_id"))
      }
      
    } else if(dataset == "DV"){
      
      
      cols <- dex_cols1[[dataset]]
      d_col_names <- as.character(dental_cb[year_id == year, ])[-1]
      
      oral_cols <- d_col_names[1:14]
      well_dent_cols <- d_col_names[15:19]
      
      if(year >= 2017){
        d_col_names <- d_col_names[!is.na(d_col_names)]
        oral_cols <- d_col_names[1:8]
        well_dent_cols <- d_col_names[9:12]
      } else if(year == 1998){
        d_col_names <- d_col_names[!is.na(d_col_names)]
      }
      
      # Recoding 1 (yes), 2 (no), and NA
      df <- get_s2_file(DV_files, year)
      print("dental exp read in")
      setnames(df, "dvdatemm", "month")
      #if procedure indicator is NA, set cause to _gc
      df[, (d_col_names) := lapply(.SD, function(x) ifelse(is.na(x), "_gc", x)), .SDcols = d_col_names]
      # for procedures related to acause = _oral, leave the 1 code and set all else to "none"
      df[, (oral_cols) := lapply(.SD, function(x) ifelse(x == 1, "_oral", ifelse(x == 2, "none", x))), .SDcols = oral_cols]
      df[, (well_dent_cols) := lapply(.SD, function(x) ifelse(x == 1, "exp_well_dental", ifelse(x == 2, "none", x))), .SDcols = well_dent_cols]
      df <- select(df, all_of(cols), all_of(d_col_names))
      
      # W2L on procedures
      df <- melt(df, id.vars = cols, measure.vars = d_col_names, value.name = "acause", variable.name = "procedure")
      setorder(df, claim_id, acause)
      df[, primary_cause := ifelse(!duplicated(claim_id), 1, 0)]
      df[!is.na(age_group_id), prop_id := paste0(age_group_id, sex_id)]
      df[is.na(age_group_id), prop_id := 999] # placeholder for missing ages
      df[acause == "none", acause := "_gc"]
      df <- df[(primary_cause == 1) | (acause != "_gc")]
      
      ## Make proportions table
      # Proportion of primary cause
      df_prop <- df[primary_cause == 1 & acause != "_gc", 
                    .(prop_o = mean(acause == "_oral"), 
                      n = .N), 
                    by = prop_id]
      
      # Causes to sample from
      causes <- c("_oral", "exp_well_dental")
      
      ## Join on prop_id and sample using observed proportion for age,sex group
      s_mean <- mean(df[primary_cause == 1 & acause != "_gc", acause] == "_oral")
      
      df <- left_join(df, df_prop, by = "prop_id") %>%
        rowwise() %>%
        mutate(acause = ifelse(acause == "_gc", 
                               ifelse(prop_id == 999,
                                      sample(causes, 1, prob = c(s_mean, 1 - s_mean)),
                                      sample(causes, 1, prob = c(prop_o, 1-prop_o))), 
                               acause)) %>%
        as.data.table()
      
      stopifnot(all(df$acause %in% c("_oral", "exp_well_dental")))
      
      # Keep unique claim and cause 
      df <- distinct(df, claim_id, acause, .keep_all = TRUE)
    }
    
    setDT(df)
    
    # Add/rename RX days supply variable
    if(dataset == "RX" & year < 2010){
      df[, days_supply := NA]
    } else if(dataset == "RX" & year >= 2010){
      setnames(df, "rxdaysup", "days_supply")
      df[, days_supply := as.integer(days_supply)]
    }
    
    # Keep only dex columns
    cols <- dex_cols2[[dataset]]
    setDT(df)
    df <- df[, ..cols]
    df[, bene_id := as.character(bene_id)]
    
    # Small percentage of ids were used twice (never in the same year), so add year to beginning of id to make them all unique to encounter
    df[, claim_id := str_c(year_id, claim_id)]
    
    if(dataset != "DV"){ 
      # filter out missing dx except dx_1 
      df <- df[dx_level == "dx_1" | !is.na(dx)]
    }
    
    # Rbind year specific file to full TOC file
    df_final <- rbind(df_final, df)
    print(paste0(dataset, year, " completed"))
    
    
  }
  
  #Changing ER to ED for team consistency 
  if(dataset == "ER"){
    dataset <- "ED"
  }
  
  # Write as a parquet file to stage 3 folder
  file_name <- paste0("USA_MEPS_", dataset)
  write_parquet(df_final, paste0(out_dir, file_name, ".parquet"))
  print(paste0(dataset, " completed"))
  
}

# Run stage 3 function looping over datasets
for(dataset in c("DV", "ER", "HH", "IP", "OB", "OP", "RX")){ 
  stage_3(dataset)
}

