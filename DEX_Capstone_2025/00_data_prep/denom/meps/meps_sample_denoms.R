#------------------------------------------------------------------------------------
# MEPS sample denominators
# - used for sample denominators and for assigning primary payer in expenditure data
#
#
# 1. Pulls demographic info from the MEPS consolidated files
#     - these contain age, sex, race, and insurance coverage by month
# 2. Merge insurance coverage to expenditure data to determine primary payer
# 3. Aggregate to race/age/sex/year/primarypayer specific sample denominators
# 4. Aggregate to allrace/age/sex/year/primarypayer specific sample denominators
#------------------------------------------------------------------------------------
library(data.table)
library(tidyverse)
library(arrow)
library(haven)

'%nin%' <- Negate('%in%')

meps_input_dir <- "FILEPATH"

### Getting file paths of all necessary MEPS files
# grab list of MEPS "CONSOLIDATED" files (demographic variables - age, sex, race, insurance, etc)
consolidated <- list()
for(year in 1996:2021){
  file <- list.files(path = paste0(meps_input_dir, as.character(year)),  pattern = ".*CONSOLIDATED.*\\.DTA$", full.names = TRUE)
  consolidated <- append(consolidated, file)
}
# Removing duplicated files (2 of the same file for 2009,2019, and 2020)
consolidated <- consolidated[-28] 
consolidated <- consolidated[-26] 
consolidated <- consolidated[-15]


# Age bin function
source('FILEPATH')

age_bin <- function(data, age_column_name = "age"){
  dt <- copy(data)
  setnames(dt, age_column_name, "age")
  dtcols <- colnames(dt)
  
  ages <- get_age_metadata(age_group_set_id = 27, release_id = 9)
  ages <- select(ages, age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)
  setorder(ages, age_start)
  
  dt$age_start <- cut(dt$age, breaks = c(ages$age_start, 125), labels = ages$age_start, right = FALSE)
  dt$age_start <- as.integer(as.character(dt$age_start))
  dt <- left_join(dt, ages, by="age_start")
  
  select(dt, c(all_of(dtcols), colnames(ages)))
  setnames(dt, "age", age_column_name)
  return(dt)
}


# Determine Primary Payer by month (to join onto expenditure file) and pri_payer to use in sample denoms
payers <- c("MCR", "MCD", "PRI")
months <- c("JA", "FE", "MA", "AP", "MY", "JU", "JL", "AU", "SE", "OC", "NO", "DE")
months_map <- c("JA" = 1,
                "FE" = 2, 
                "MA" = 3, 
                "AP" = 4, 
                "MY" = 5, 
                "JU" = 6, 
                "JL" = 7, 
                "AU" = 8, 
                "SE" = 9, 
                "OC" = 10, 
                "NO" = 11, 
                "DE" = 12)

# Need insurance by month both for determining primary payer in the expenditure data
pri_payer_exp <- data.table() 
for(file in consolidated){
  
  # get full and abbreviated yr from file name
  year <- as.numeric(str_extract(file, pattern = "\\d{4}"))
  year_short <- as.character(str_sub(year, start = 3))
  
  print(paste0("Starting ", as.character(year)))
  
  # read in demographic data
  fyc <- as.data.table(haven::read_stata(file))
  colnames(fyc) <- toupper(colnames(fyc))
  
  # expand the data to have a row for each month for each person
  person_month_cross <- data.table(crossing(DUPERSID = fyc[, DUPERSID], month = c(1:12)))
  
  # there is an indicator column for every month and payer 
  #ex. PRIJA96 is the column that indicates if that person had private insurance in January in 1996
  # loop over every combination of payer and month to get the indicator columns, create month and payer columns, and reshape to be long on month 
  insurance_status <- fyc[, .(DUPERSID)]
  for(p in payers){
    for(m in months){
      if(p == "PRI"){
        col <- paste0(p, m, year_short)
      } else {
        col <- paste0(p, m, year_short, "X")
      }
      
      # select payer/month column from demographic data
      month_specific_insurance <- fyc[, .SD, .SDcols = c("DUPERSID", col)]
      insurance_status <- merge(insurance_status, month_specific_insurance, "DUPERSID")
      
    }
    # Create numeric month column and insured status indicator
    ins_status_long <- melt(insurance_status, id.vars = "DUPERSID", value.name = p, variable.name = "month")
    ins_status_long[, month:= as.character(month)]
    ins_status_long[, month := substr(month, start = 4, stop = 5)]
    ins_status_long[, month := months_map[month]]
    
    #Reset insurance_status
    insurance_status <- fyc[, .(DUPERSID)]
    
    # join new insurance indicator column to the expanded data (bene_id and long on month)
    person_month_cross <- left_join(person_month_cross, ins_status_long, by = c("DUPERSID", "month"))
    
  }
  
  #recoding FROM 1 = yes 2 = no -3 - NA TO 1 = yes 0 = no and NA = NA
  cols_to_change <- c("MCR", "MCD", "PRI")  # specify the column names
  person_month_cross[, (cols_to_change) := lapply(.SD, function(x) ifelse(x == 1, 1, ifelse(x == 2, 0, NA))), .SDcols = cols_to_change]
  
  # renaming payers to match DEX abbreviations
  setnames(person_month_cross, old = c("MCR", "MCD", "PRI"), new = c("mdcr", "mdcd", "priv"))
  
  #-----------------------------------------
  # Data to join to expenditure data
  #------------------------------------------
  cols_to_change2 <- c("mdcr", "mdcd", "priv")
  # assign primary payer based on insurance indicator column (separated by hyphen if multiple insurers)
  # ex. if mdcr == 1 and others 0 then pri_payer = 'mdcr', if mdcr and mdcd == 1 and pri = 0 then pri_payer = 'mdcr-mdcd'
  person_month_cross[, pri_payer := apply(person_month_cross[, cols_to_change2, with = FALSE], 
                                          1, #to apply over rows
                                          function(x) paste(names(x)[x == 1], collapse = "-"))]
  
  # if no insurance present then considered uninsured and pri_payer set to out-of-pocket
  person_month_cross[pri_payer == "", pri_payer := "oop"]
  
  # if insurance indicators are missing then pri_payer is set to unknown
  person_month_cross[pri_payer == "NA-NA-NA", pri_payer := "unk"]
  person_month_cross[, year_id := year]
  pri_payer_exp <- rbind(pri_payer_exp, person_month_cross)
  
}


# set bene id to character value
pri_payer_exp[, bene_id := as.character(as.numeric(DUPERSID))]

# For cases where we don't have month in the expenditure files (either missing or RX data), 
#   use logic based on the "ever covered by x in year" columns in the FYC data
pri_payer_year <- data.table()
for(file in consolidated){
  
  year <- as.numeric(str_extract(file, pattern = "\\d{4}"))
  year_short <- as.character(str_sub(year, start = 3))
  print(year)
  
  fyc <- as.data.table(haven::read_stata(file))
  colnames(fyc) <- toupper(colnames(fyc))
  
  # Standardizing uninsured indicator  
  if(year != "1996"){
    setnames(fyc, paste0("UNINS", year_short), "UNINSURD")
  }
  setnames(fyc, "UNINSURD", "oop")
  
  # Standardizing private insurance indicator 
  if(year != "1996"){
    setnames(fyc, paste0("PRVEV", year_short), "PRVEVER")
  }
  setnames(fyc, "PRVEVER", "priv")
  
  # Standardizing medicare indicator 
  if(year != "1996"){
    setnames(fyc, paste0("MCREV", year_short), "MCREVER")
  }
  setnames(fyc, "MCREVER", "mdcr")
  
  # Standardizing medicaid indicator  
  if(year != "1996"){
    setnames(fyc, paste0("MCDEV", year_short), "MCDEVER")
  }
  setnames(fyc, "MCDEVER", "mdcd")
  
  fyc <- select(fyc, DUPERSID, mdcr, mdcd, priv, oop, starts_with("HISP"), starts_with("RACE"))
  fyc[, (names(fyc)) := lapply(.SD, function(x) replace(x, x==2, 0))]
  fyc[, "pri_payer_year" := apply(fyc[, c("mdcr", "mdcd", "priv", "oop"), with = FALSE], 1, function(x) paste(names(x)[x == 1], collapse = "-"))]
  fyc[, year_id := year]
  
  fyc <- select(fyc, DUPERSID, pri_payer_year, year_id, starts_with("HISP"), starts_with("RACE"))
  labelled_vars <- colnames(fyc)[sapply(fyc, function(x) inherits(x, "haven_labelled"))]
  fyc <- fyc %>% mutate(across(all_of(labelled_vars), ~as.numeric(as.character(.))))
  
  pri_payer_year <- rbind(pri_payer_year, fyc, fill = TRUE)
  
  print("done")
}
pri_payer_year[, bene_id := as.character(as.numeric(DUPERSID))]



#-----------------------------------------
# Assign primary payer to stage 3 data
#------------------------------------------

# read in temporary stage 3 files
MEPS_IP <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]
MEPS_OP <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]
MEPS_OB <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]
MEPS_DV <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]
MEPS_HH <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]
MEPS_RX <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]
MEPS_ED <- data.table(read_parquet("FILEPATH"))[, month := as.numeric(month)]

# bind expenditure files together
MEPS_exp <- rbindlist(list(MEPS_DV, MEPS_ED, MEPS_HH, MEPS_IP, MEPS_OB, MEPS_OP, MEPS_RX),fill = TRUE)
setDT(MEPS_exp)

# aggregate to bene_id-month-year to get monthly payment amounts by payer
denom <- MEPS_exp[!is.na(month) & toc != "RX", 
                  .(mdcr_pay_amt = sum(mdcr_pay_amt), 
                    mdcd_pay_amt = sum(mdcd_pay_amt), 
                    priv_pay_amt = sum(priv_pay_amt), 
                    oop_pay_amt = sum(oop_pay_amt)), 
                  by = .(bene_id, year_id, month, age_group_years_start, race_cd)]
denom[, month := as.numeric(month)]

# where month is missing or RX (no usable month column) aggregate to bene_id-year
denomNA <- MEPS_exp[is.na(month) | toc == "RX",
                    .(mdcr_pay_amt = sum(mdcr_pay_amt), 
                      mdcd_pay_amt = sum(mdcd_pay_amt), 
                      priv_pay_amt = sum(priv_pay_amt), 
                      oop_pay_amt = sum(oop_pay_amt)), 
                    by = .(bene_id, year_id, age_group_years_start, race_cd)]

setDT(pri_payer_exp)

# create separate table for each year to speed up run time below
denom_list <- split(denom, denom$year_id)
denom_list_NAmonth <- split(denomNA, denomNA$year_id)

# create empty list to store resulting data in 
denom_logic_list <- vector('list', length(denom_list))
denom_logic_list_NAmonth <- vector('list', length(denom_list))

# loop over items in list
# apply payer logic
for(i in seq_along(denom_list)){
  
  year <- as.numeric(names(denom_list[i]))
  print(names(denom_list[i]))
  start <- Sys.time()
  
  df <- denom_list[[i]] 
  pp_df <- pri_payer_exp[year_id == year]
  
  print(paste("filter", round(Sys.time()-start, 2), "seconds"))
  
  # set columns to join expenditure data to reported monthly insurance coverage
  setkey(df, bene_id, year_id, month)
  setkey(pp_df, bene_id, year_id, month)
  df2 <- df[pp_df, nomatch = 0]
  
  # make a column that determines priamry payer based on which payers made payments that month
  df2[, pri_payer2 := fcase(
    mdcr_pay_amt > 0 & mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start > 65, "mdcr-mdcd",
    mdcr_pay_amt > 0 & mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start < 65, "mdcr-priv",
    mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start < 65, "priv",
    mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start > 65, "mdcd",
    mdcr_pay_amt > 0 & priv_pay_amt > 0,  "mdcr-priv",
    mdcr_pay_amt > 0 & mdcd_pay_amt > 0, "mdcr-mdcd",
    mdcr_pay_amt > 0, "mdcr",
    mdcd_pay_amt > 0, "mdcd",
    priv_pay_amt > 0, "priv",
    mdcr_pay_amt == 0 & mdcd_pay_amt == 0 & priv_pay_amt == 0, "oop",
    default = "unk"
  )]

  denom_logic_list[[i]] <- df2
  
  # Same as above for when month is missing
  df_NAmonth <- denom_list_NAmonth[[i]]
  pp_df_NAmonth <- pri_payer_year[year_id == year]
  setkey(df_NAmonth, bene_id, year_id)
  setkey(pp_df_NAmonth, bene_id, year_id)
  df2_NAmonth <- df_NAmonth[pp_dfNA, nomatch = 0]
  
  df2_NAmonth[, pri_payer_year_spend := fcase(
    mdcr_pay_amt > 0 & mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start > 65, "mdcr-mdcd",
    mdcr_pay_amt > 0 & mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start < 65, "mdcr-priv",
    mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start < 65, "priv",
    mdcd_pay_amt > 0 & priv_pay_amt > 0 & age_group_years_start > 65, "mdcd",
    mdcr_pay_amt > 0 & priv_pay_amt > 0,  "mdcr-priv",
    mdcr_pay_amt > 0 & mdcd_pay_amt > 0, "mdcr-mdcd",
    mdcr_pay_amt > 0, "mdcr",
    mdcd_pay_amt > 0, "mdcd",
    priv_pay_amt > 0, "priv",
    mdcr_pay_amt == 0 & mdcd_pay_amt == 0 & priv_pay_amt == 0, "oop",
    default = "unk"
  )]
  
  df2_NAmonth[, pri_payer_year := fifelse(pri_payer_year == "", "unk", pri_payer_year)]
  
  denom_logic_list_NAmonth[[i]] <- df2_NAmonth
  
}
denom_logic <- rbindlist(denom_logic_list)
denom_logic_NAmonth <- rbindlist(denom_logic_list_NAmonth)

# default to what they reported they were covered by (pri_payer), 
# but if it is an impossible combination use the payment method (pri_payer2) 
denom_logic[, pri_payer_true := fcase(
  is.na(pri_payer), pri_payer2,
  pri_payer %nin% c("mdcd-priv", "mdcr-mdcd-priv"), pri_payer,
  pri_payer %in% c("mdcd-priv", "mdcr-mdcd-priv"), pri_payer2
)]

denom_logic_NAmonth[, pri_payer_true := fcase(
  pri_payer_year %nin% c("mdcd-priv", "mdcr-mdcd-priv"), pri_payer_year,
  pri_payer_year %in% c("mdcd-priv", "mdcr-mdcd-priv"), pri_payer_year_spend
)]


#converting to pri_payer_number
pri_payer_ids <- c(
  "mdcr"               = 1,
  "priv"               = 2,
  "mdcd"               = 3,
  "oop"                = 4,
  "oth_not_priv"       = 5,
  "oth_not_mdcr"       = 6,
  "oth_not_mdcd"       = 7,
  "oth_not_oop"        = 8,
  "oth_not_mdcd_mdcr"  = 9,
  "oth_not_mdcd_oop"   = 10,
  "oth_not_mdcd_priv"  = 11,
  "oth_not_mdcr_oop"   = 12,
  "oth_not_mdcr_priv"  = 13,
  "oth_not_oop_priv"   = 14,
  "oth_not_mdcd_mdcr_oop" = 15,
  "oth_not_mdcd_mdcr_priv" = 16,
  "oth_not_mdcd_oop_priv" = 17,
  "oth_not_mdcr_oop_priv" = 18,
  "oth_not_mdcd_mdcr_oop_priv" = 19,
  "nc"                 = 20,
  "unk"                = 21,
  "mdcr-mdcd"          = 22,
  "mdcr-priv"          = 23,
  "mdcr-mc"            = 24,
  "mdcd-mc"            = 25,
  "mdcr-mc-mdcd"       = 26
)

denom_logic[, pri_payer := pri_payer_ids[pri_payer_true]]
denom_logic_NAmonth[, pri_payer := pri_payer_ids[pri_payer_true]]

#make tables to join to expenditure data
w_month <- unique(denom_logic[, .(bene_id, year_id, month, pri_payer)])
wo_month <- denom_logic_NAmonth[, .(bene_id, year_id, pri_payer)]

# For each dataset, join monthly pri_payer along with annual pri_payer
MEPS_DV <- left_join(MEPS_DV, w_month, by = c("bene_id", "year_id", "month"))
MEPS_DV <- left_join(MEPS_DV, wo_month, by = c("bene_id", "year_id"))
# if montly pri_payer is NA, use annual pri_payer
MEPS_DV[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_DV <- MEPS_DV[, !c("pri_payer.x", "pri_payer.y")]
MEPS_DV[, month := NULL]
MEPS_DV <- unique(MEPS_DV)
write_parquet(MEPS_DV, "FILEPATH")

MEPS_ED <- unique(left_join(MEPS_ED, w_month, by = c("bene_id", "year_id", "month")))
MEPS_ED <- left_join(MEPS_ED, wo_month, by = c("bene_id", "year_id"))
MEPS_ED[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_ED <- MEPS_ED[, !c("pri_payer.x", "pri_payer.y")]
MEPS_ED[, month := NULL]
MEPS_ED <- unique(MEPS_ED)
write_parquet(MEPS_ED, "FILEPATH")

MEPS_HH <- left_join(MEPS_HH, w_month, by = c("bene_id", "year_id", "month"))
MEPS_HH <- left_join(MEPS_HH, wo_month, by = c("bene_id", "year_id"))
MEPS_HH[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_HH <- MEPS_HH[, !c("pri_payer.x", "pri_payer.y")]
MEPS_HH[, month := NULL]
MEPS_HH <- unique(MEPS_HH)
write_parquet(MEPS_HH, "FILEPATH")

MEPS_IP <- left_join(MEPS_IP, w_month, by = c("bene_id", "year_id", "month"))
MEPS_IP <- left_join(MEPS_IP, wo_month, by = c("bene_id", "year_id"))
MEPS_IP[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_IP <- MEPS_IP[, !c("pri_payer.x", "pri_payer.y")]
MEPS_IP[, month := NULL]
MEPS_IP <- unique(MEPS_IP)
write_parquet(MEPS_IP, "FILEPATH")

MEPS_OB <- left_join(MEPS_OB, w_month, by = c("bene_id", "year_id", "month"))
MEPS_OB <- left_join(MEPS_OB, wo_month, by = c("bene_id", "year_id"))
MEPS_OB[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_OB <- MEPS_OB[, !c("pri_payer.x", "pri_payer.y")]
MEPS_OB[, month := NULL]
MEPS_OB <- unique(MEPS_OB)
write_parquet(MEPS_OB, "FILEPATH")

MEPS_OP <- left_join(MEPS_OP, w_month, by = c("bene_id", "year_id", "month"))
MEPS_OP <- left_join(MEPS_OP, wo_month, by = c("bene_id", "year_id"))
MEPS_OP[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_OP <- MEPS_OP[, !c("pri_payer.x", "pri_payer.y")]
MEPS_OP[, month := NULL]
MEPS_OP <- unique(MEPS_OP)
write_parquet(MEPS_OP, "FILEPATH")

MEPS_RX <- left_join(MEPS_RX, wo_month, by = c("bene_id", "year_id"))
MEPS_RX[, pri_payer := ifelse(is.na(pri_payer.x), pri_payer.y, pri_payer.x)]
MEPS_RX <- MEPS_RX[, !c("pri_payer.x", "pri_payer.y")]
MEPS_RX[, month := NULL]
MEPS_RX <- unique(MEPS_RX)
MEPS_RX[days_supply == 999, days_supply := NA]
write_parquet(MEPS_RX, "FILEPATH")


#-----------------------------------------
# Sample denominators
#------------------------------------------
# Save race specific sample denominators straight from demographic files
# Read in consolidated file, determine primary payer based on insurance coverage for the year
race_denoms <- data.table()
for(file in consolidated){
  
  year <- as.numeric(str_extract(file, pattern = "\\d{4}"))
  year_short <- as.character(str_sub(year, start = 3))
  
  print(year)
  
  fyc <- as.data.table(haven::read_stata(file))
  colnames(fyc) <- toupper(colnames(fyc))
  setnames(fyc, paste0("AGE", year_short, "X"), "AGE")
  
  # survey weights  
  if(year <= 1998){
    setnames(fyc, paste0("WTDPER", year_short), "WT")  
  } else {
    setnames(fyc, paste0("PERWT", year_short, "F"), "WT")
  }
  
  # Standardizing uninsured indicator  
  if(year != "1996"){
    setnames(fyc, paste0("UNINS", year_short), "UNINSURD")
  }
  
  setnames(fyc, "UNINSURD", "oop")
  
  
  # Standardizing private insurance indicator 
  if(year != "1996"){
    setnames(fyc, paste0("PRVEV", year_short), "PRVEVER")
  }
  
  setnames(fyc, "PRVEVER", "priv")
  
  # Standardizing medicare indicator 
  if(year != "1996"){
    setnames(fyc, paste0("MCREV", year_short), "MCREVER")
  }
  
  setnames(fyc, "MCREVER", "mdcr")
  
  # Standardizing medicaid indicator  
  if(year != "1996"){
    setnames(fyc, paste0("MCDEV", year_short), "MCDEVER")
  }
  
  setnames(fyc, "MCDEVER", "mdcd")
  
  
  fyc <- select(fyc, DUPERSID, AGE, SEX, mdcr, mdcd, priv, oop, starts_with("HISP"), starts_with("RACE"), WT)
  
  fyc[, (names(fyc)) := lapply(.SD, function(x) replace(x, x==2, 0))]
  
  fyc[, "pri_payer_year" := apply(fyc[, c("mdcr", "mdcd", "priv", "oop"), with = FALSE], 1, function(x) paste(names(x)[x == 1], collapse = "-"))]
  fyc[, year_id := year]
  
  fyc <- select(fyc, DUPERSID, AGE, SEX, pri_payer_year, year_id, starts_with("HISP"), starts_with("RACE"), WT)
  labelled_vars <- colnames(fyc)[sapply(fyc, function(x) inherits(x, "haven_labelled"))]
  fyc <- fyc %>% mutate(across(all_of(labelled_vars), ~as.numeric(as.character(.))))
  
  
  race_denoms <- rbind(race_denoms, fyc, fill = TRUE)
  
  print("done")
}
# rename columns to DEX standards
race_denoms[, bene_id := as.character(as.numeric(DUPERSID))]
race_denoms <- age_bin(race_denoms, "AGE")
setnames(race_denoms, "SEX", "sex_id")

# Race variables are different in some years 
# assign race_cd 
race_denoms[year_id <= 2001, race_cd := fcase(
  HISPANX %in% c(1, "1.0", "1 HISPANIC"), "HISP",
  RACEX %in% c(1, "1.0"), "AIAN",
  RACEX %in% c(2, "2.0"), "BLK",
  RACEX %in% c(3, "3.0"), "API", 
  RACEX %in% c(4, "4.0"), "BLCK",
  RACEX %in% c(5, "5.0"), "WHT",
  RACEX %in% c(91, "91.0"), "UNK",
  default = "UNK"
)]
race_denoms[year_id %in% c(2002:2011), race_cd := fcase(
  HISPANX %in% c(1, "1.0", "1 HISPANIC"), "HISP",
  RACEX %in% c(1, "1.0", "1 WHITE - NO OTHER RACE REPORTED"), "WHT",
  RACEX %in% c(2, "2.0", "2 BLACK - NO OTHER RACE REPORTED"), "BLCK",
  RACEX %in% c(3, "3.0", "3 AMER INDIAN/ALASKA NATIVE - NO OTH RAC"), "AIAN", 
  RACEX %in% c(4, "4.0", "4 ASIAN - NO OTHER RACE REPORTED"), "API",
  RACEX %in% c(5, "5.0", "5 NATIVE HAWAIIAN/PACIFIC ISLANDER-NO OTHR"), "API",
  RACEX %in% c(6, "6.0", "6 MULTIPLE RACES REPORTED"), "UNK",
  default = "UNK"
)]
race_denoms[year_id > 2011, race_cd:= fcase(
  HISPANX %in% c(1, "1.0", "1 HISPANIC"), "HISP",
  RACEV1X %in% c(3, "3.0", "3 AMER INDIAN/ALASKA NATIVE-NO OTHER RACE"), "AIAN",
  RACEV1X %in% c(1, "1.0", "1 WHITE - NO OTHER RACE REPORTED"), "WHT",
  RACEV1X %in% c(2, "2.0", "2 BLACK - NO OTHER RACE REPORTED"), "BLCK",
  RACEV1X %in% c(4, "4.0", "4 ASIAN/NATV HAWAIIAN/PACFC ISL-NO OTH"), "API",
  RACEV1X %in% c(5, "5.0", "5 NATIVE HAWAIIAN/PACIFIC ISLANDER-NO OTHR"), "API",
  RACEV1X %in% c(6, "6.0", "6 MULTIPLE RACES REPORTED"), "UNK",
  default = "UNK"
)]
race_denoms[race_cd == "UNK", race_cd := fcase(
  RACEBX == 1, "BLCK",
  RACEWX == 1, "WHT",
  default = "UNK"
)]


#converting to pri_payer_id
pri_payer_ids <- c(
  "mdcr"               = 1,
  "priv"               = 2,
  "mdcd"               = 3,
  "oop"                = 4,
  "oth_not_priv"       = 5,
  "oth_not_mdcr"       = 6,
  "oth_not_mdcd"       = 7,
  "oth_not_oop"        = 8,
  "oth_not_mdcd_mdcr"  = 9,
  "oth_not_mdcd_oop"   = 10,
  "oth_not_mdcd_priv"  = 11,
  "oth_not_mdcr_oop"   = 12,
  "oth_not_mdcr_priv"  = 13,
  "oth_not_oop_priv"   = 14,
  "oth_not_mdcd_mdcr_oop" = 15,
  "oth_not_mdcd_mdcr_priv" = 16,
  "oth_not_mdcd_oop_priv" = 17,
  "oth_not_mdcr_oop_priv" = 18,
  "oth_not_mdcd_mdcr_oop_priv" = 19,
  "nc"                 = 20,
  "unk"                = 21,
  "mdcr-mdcd"          = 22,
  "mdcr-priv"          = 23,
  "mdcr-mc"            = 24,
  "mdcd-mc"            = 25,
  "mdcr-mc-mdcd"       = 26,
  "mdcd-priv"          = 2,
  "mdcr-mdcd-priv"     = 1
)

# Map primary payer label to id
race_denoms[, pri_payer := pri_payer_ids[pri_payer_year]]
# if pri payer is missing, set to unknown
race_denoms[pri_payer_year == "", pri_payer := 21]

# convert sex_id = 0 to 2 and write out race specific denominators
race_denoms[sex_id == 0, sex_id := 2]
race_sample_denoms <- race_denoms[, .(n_obs = .N, pop = sum(WT)),
                                  by = .(year_id, age_group_years_start = age_start, sex_id, pri_payer, race_cd)]
write_parquet(race_sample_denoms, "FILEPATH/MEPS_race_sample_denoms.parquet")

# Aggregate over race for all race denominators
all_race_denom <- race_sample_denoms[, .(pop = sum(pop), n_obs = sum(n_obs)), by = .(year_id, pri_payer, age_group_years_start, sex_id)]
write_parquet(all_race_denom, "FILEPATH/MEPS_sample_denoms.parquet")
