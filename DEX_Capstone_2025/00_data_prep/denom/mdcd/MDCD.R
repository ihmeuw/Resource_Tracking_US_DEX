#----------------------------------------------------------------------#
# - Step 1 in creation of sample denominator for MDCD MAX -------------#
# Run time: Some states take 1-2 hours
# Purpose: This script creates 
# 1) the intermediate demographic files for MDCD MAX that is used in stage 2 processing
# 2) the first step in creating the sample denominator for MDCD MAX
# AUTHOR(S): Meera Beauchamp, Drew DeJarnatt, Emily Johnson
#----------------------------------------------------------------------#
rm(list = ls())
print('loading packages')
#install.packages('pacman')
pacman::p_load(arrow,data.table,tidyverse,parallel)
library(wrapr, lib.loc = "FILEPATH")
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(here, "/../cms_denom_functions.R"))

options(arrow.skip_nul = TRUE)

'%ni%' <- Negate('%in%')
print('getting maps')
# Get counties map
load("FILEPATH")
#cnty is 5 characters long, first two are state code, last three are county code
counties <- counties %>%
  select(mcnty, cnty)

#Get state map 
load("FILEPATH") 
states <- states %>% 
  select(abbreviation, state, state_name) %>%
  mutate(state := str_sub(state, -2, -1))

# Read data in arrow, select columns
print('getting inputs')
if(interactive()){
  d_year <- 2010
  state <- "MI"
  timestamp <-  Sys.Date()
  state_l<- tolower(state)
}else{
  
  args <- commandArgs(trailingOnly = TRUE)
  
  task_map_path <- args[1]
  # use Task id to identify params for this run!
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID")]
  
  d_year <- as.numeric(params$year) %>% print()
  state <- as.character(params$state) %>% print()
  timestamp <- as.character(params$timestamp) %>% print() 
  state_l<- tolower(state)
}
print(paste('year', d_year))
print(paste('state', state))
print(paste('timestamp', timestamp))

plot_save_dir <- paste0('FILEPATH',timestamp,'/')
if (!dir.exists(plot_save_dir)){dir.create(plot_save_dir, recursive = T)}

print('setting up templates')
demo_cols <- c("BENE_ID", "MSIS_ID", "STATE_CD", "EL_RSDNC_CNTY_CD_LTST", "MAX_YR_DT", "EL_DOB", "EL_SEX_CD", 
               "EL_RACE_ETHNCY_CD", "MSNG_ELG_DATA","EL_RSDNC_ZIP_CD_LTST")

## CODING - USING NAMED VECTORS
re_coding <- c('1' = 'WHT', '2' = 'BLCK', '3' = 'AIAN', '4' = 'API', '5' = 'HISP', '6' = 'API',
               '7' = 'HISP', '8' = 'UNK', '9' = 'UNK') 
usable_races_num<-c(1,2,3,4,5,6,7)
usable_races_abb<-c('WHT', 'BLCK', 'HISP', 'API', 'AIAN')
sex_coding <- qc(M = 1, `F` = 2, U = -1)
chip_coding <- qc(`0` = 0, `1` = 0, `2` = 1, `3` = 1, `9` = NA)

claim_info_null <- data.frame (BENE_ID= c(NA),
                               MSIS_ID= c(NA),
                               STATE_CD= c(NA),
                               dob_claim= c(NA),
                               race_claim= c(NA),
                               sex_claim= c(NA))

if(state != 'NA'){
  # Read in table created from claims which contains unique bene/msis/state/year/dob/sex/race
  claim_path<-paste0('FILEPATH',
                     d_year,
                     'FILEPATH',
                     state)
  if(file.exists(claim_path)){
    print('reading claim data')
    claim_info<- open_dataset(claim_path) %>% 
      select(BENE_ID,MSIS_ID,dob_claim, race_claim, sex_claim)%>%
      unique() %>%
      mutate(STATE_CD=state)%>%
      collect()
  }else{
    claim_info <- claim_info_null
  }
} else if(state == 'NA'){
  claim_info <- claim_info_null
} 

# Raw MDCD personal summary files are partitioned by state and year. This loop takes advantage 
# of the state-level stratification to do most column recoding and pivot long on month.
# The outputs of the loop are no longer person-level but are still state-year specific.
if (state != 'NA'){
  files <- Sys.glob(paste0("FILEPATH",d_year,"FILEPATH",state_l,"*/*.parquet"))
} else if (state == 'NA'){
  files <- Sys.glob(paste0("FILEPATH",d_year,"FILEPATH"))
}

ps <- data.table()
for(f in files){
  print(f)
  if (state != 'NA'){
    data <- open_dataset(f) %>%
      filter(STATE_CD == state)
  } else if (state == 'NA') {
    data <- open_dataset(f) %>%
      filter(is.na(STATE_CD))
  }
  print(" -> reading data")
  data <- data %>% 
    select(BENE_ID, MSIS_ID, STATE_CD, EL_RSDNC_CNTY_CD_LTST, MAX_YR_DT, EL_DOB, EL_SEX_CD, 
           EL_RACE_ETHNCY_CD, MDCR_RACE_ETHNCY_CD, MDCR_LANG_CD, MSNG_ELG_DATA, EL_RSDNC_ZIP_CD_LTST,
           starts_with("EL_MDCR_DUAL_MO_"), starts_with("MAX_ELG_CD_MO_"), starts_with("EL_DAYS_EL_CNT_"), 
           starts_with("MC_COMBO_MO_"), starts_with("EL_PVT_INS_CD_")) %>%
    collect() %>%
    setDT()
  
  if(nrow(data) > 0){
    #-------------------------------------------------------------------------------------
    print(" -> creating st_resi")
    #-------------
    data <- merge(data, states, by.x = "STATE_CD", by.y = "abbreviation", all.x = TRUE)
    #recode from fips to state abbreviations
    data$st_resi=data$STATE_CD
    
    #-------------------------------------------------
    # Make Race adjustments/imputation
    # 1) fill using other RE variables in data
    # 2) fill using other years (back forward fill w/ CI race tables)
    # 3) fill using claim info
    # 4) Language map
    # 5) apply race proportions by zip, later multiple imputation
    #-------------------------------------------------
    #1) Use MDCR_RACE_ETHNCY_CD to impute race when OTH or missing
    # First make race codes the same, UNKN = 0 in MDCR_RACE_ETHNCY_CD and 9 in EL_RACE_ETHNCY_CD, set to match EL_RACE_ETHNCY_CD
    data[, MDCR_RACE_ETHNCY_CD := ifelse(MDCR_RACE_ETHNCY_CD == 0, 9, MDCR_RACE_ETHNCY_CD)]
    #MULTI/OTH = 3 in MDCR_RACE_ETHNCY_CD and 8 in EL_RACE_ETHNCY_CD, set to match EL_RACE_ETHNCY_CD
    data[, MDCR_RACE_ETHNCY_CD := ifelse(MDCR_RACE_ETHNCY_CD == 3, 8, MDCR_RACE_ETHNCY_CD)]
    data[, EL_RACE_ETHNCY_CD := ifelse((EL_RACE_ETHNCY_CD %in% c(8, 9) | is.na(EL_RACE_ETHNCY_CD)), MDCR_RACE_ETHNCY_CD, EL_RACE_ETHNCY_CD)]
    #Create a column for original race code (before anything else)
    data[, orig_race_cd := EL_RACE_ETHNCY_CD]
    data[, orig_race_cd := ifelse(orig_race_cd %in% usable_races_num, orig_race_cd, 9)]
    
    #2) Fill in race where known for the same bene in a diff year
    #Get unique list of bene's that don't have a known race to filter race column on
    #https://resdac.org/sites/datadocumentation.resdac.org/files/CCW%20Codebook%20MAX%20Person%20Summary_Version%202014.pdf 8 = multi, 9=UNK
    bene_list <- data %>% filter(EL_RACE_ETHNCY_CD %in% c(8,9) | is.na(EL_RACE_ETHNCY_CD)) %>% select(BENE_ID) %>% unique() %>% collect() 
    bene_list <-unique(bene_list$BENE_ID) 
    
    #Pull in CI's race table
    print(" -> cross-fill race")
    re_table <- open_dataset(paste0("FILEPATH")) %>%
      filter(bene_id %in% bene_list) %>%
      filter(year_id == d_year,
             rti_race_cd!= 0, #0 = UNKN, 3 = OTH)
             rti_race_cd!= 3) %>%
      select(BENE_ID = bene_id, rti_race_cd) %>%
      mutate(BENE_ID = cast(BENE_ID, string())) %>%
      arrange(BENE_ID) %>%
      collect() %>%
      unique()
    
    initial_row_count <- nrow(data)
    data<- merge(data, re_table, by="BENE_ID", all.x = TRUE)
    final_row_count <- nrow(data)
    # check if row count is different and throw an error if it is
    if (initial_row_count != final_row_count) {
      stop("Error: Row count is different after merge to CI race table")
    }
    data[, EL_RACE_ETHNCY_CD := ifelse((EL_RACE_ETHNCY_CD %in% c(8, 9) | is.na(EL_RACE_ETHNCY_CD)), rti_race_cd, EL_RACE_ETHNCY_CD)]
    data[, rti_race_cd := NULL]
    #Create a column for race after filling, adding onto the original race_cd
    data[, fill_race_cd := ifelse(orig_race_cd %in% usable_races_num, orig_race_cd, EL_RACE_ETHNCY_CD)]
    data[, fill_only_race_cd := ifelse(orig_race_cd != fill_race_cd, fill_race_cd, NA)]
    
    #3) Use claim info
    #Join claim data onto ps data, where ps is NA, fill with claim info
    #Add a column to indicate imputed, then drop claim columns
    # store initial row count
    print(" -> merging in claim info")
    initial_row_count <- nrow(data)
    data<-merge(data, claim_info, by=c('BENE_ID','MSIS_ID','STATE_CD'), all.x=TRUE)
    # store row count after merge
    final_row_count <- nrow(data)
    
    # check if row count is different and throw an error if it is
    if (initial_row_count != final_row_count) {
      stop("Error: Row count is different after merge")
    }
    
    #Create imputed indicator
    print(" -> imputing stuff")
    data[, imputed_dob_ind := ifelse((is.na(EL_DOB) & !is.na(dob_claim)), 1, 0)]
    data[, imputed_race_ind := ifelse((is.na(EL_RACE_ETHNCY_CD) & !is.na(fill_race_cd)), 1, 0)]
    data[, imputed_sex_ind := ifelse((is.na(EL_SEX_CD) & !is.na(sex_claim)), 1, 0)]
    #impute the columns
    data[is.na(EL_DOB) & !is.na(dob_claim), EL_DOB := dob_claim]
    data[is.na(EL_SEX_CD) & !is.na(sex_claim), EL_SEX_CD := sex_claim]
    setnames(data, old = "race_claim", new = "claim_only_race_cd")
    #Create a column for race after filling
    data[, claim_only_race_cd:= ifelse(claim_only_race_cd %in% usable_races_num, claim_only_race_cd, 9)]
    data[, claim_race_cd := ifelse(fill_race_cd %in% usable_races_num, fill_race_cd, claim_only_race_cd)]
    
    #Drop claim columns
    data[, c('dob_claim','sex_claim'):=NULL]
    other_cols<-c('imputed_dob_ind','imputed_race_ind','imputed_sex_ind','state','state_name','st_resi')
    
    #4) Add in language map!
    print(" -> language mapping")
    #Change language codes to match those in the map
    data<- data %>% mutate(MDCR_LANG_CD =  case_when(MDCR_LANG_CD == 'C' ~ 'CHI',
                                                     MDCR_LANG_CD == 'D' ~ 'GER',
                                                     MDCR_LANG_CD == 'E' ~ 'ENG',
                                                     MDCR_LANG_CD == 'F' ~ 'FRE',
                                                     MDCR_LANG_CD == 'G' ~ 'GRE',
                                                     MDCR_LANG_CD == 'I' ~ 'ITA',#'PI', 
                                                     MDCR_LANG_CD == 'J' ~ 'HISP', 
                                                     MDCR_LANG_CD == 'N' ~ 'NOR', 
                                                     MDCR_LANG_CD == 'P' ~ 'POL', 
                                                     MDCR_LANG_CD == 'R' ~ 'RUS', 
                                                     MDCR_LANG_CD == 'S' ~ 'SPA',
                                                     MDCR_LANG_CD == 'V' ~ 'SWE',
                                                     MDCR_LANG_CD == 'W' ~ 'SRP',
                                                     TRUE ~ 'ENG'))
    #Read in language map
    lang_map<- fread(paste0('FILEPATH')) %>% 
      select(lang_cd, race_cd, pct) %>% rename(MDCR_LANG_CD = lang_cd) 
    #Set all race codes to their race abbreviations (WHT, BLCK etc - doing this here because the language map uses abbreviations)
    data[,orig_race_cd := plyr::revalue(as.character(orig_race_cd), re_coding)]
    data[,fill_race_cd := plyr::revalue(as.character(fill_race_cd), re_coding)]
    data[,fill_only_race_cd := plyr::revalue(as.character(fill_only_race_cd), re_coding)]
    data[,claim_race_cd := plyr::revalue(as.character(claim_race_cd), re_coding)]
    data[,claim_only_race_cd := plyr::revalue(as.character(claim_only_race_cd), re_coding)]
    #Create column to run language mapping
    setDT(data)
    
    #Subset data to just unknowns and multi
    initial_row_count <- nrow(data)
    data[, claim_race_cd := ifelse(claim_race_cd %ni% usable_races_abb, 'UNK', claim_race_cd)]
    unk_data <- data[claim_race_cd %in% c('UNK') & MDCR_LANG_CD != 'ENG'] #can't reassign those whose primary language is English#can't reassign those whose primary language is English
    #subset other df to everything not in unk_df
    data <- data[claim_race_cd %ni% c('UNK') | MDCR_LANG_CD == 'ENG']
    
    assign_race <- function(x){
      dt <- copy(unk_data[MDCR_LANG_CD==x])
      sample_pop <- copy(lang_map[MDCR_LANG_CD==x])
      dt[,lang_only_race_cd:= sample(x = sample_pop$race_cd, 
                                size = nrow(dt), 
                                replace = T,
                                prob = sample_pop$pct)]
      return(dt)
    }
    unk_data <- rbindlist(lapply(unique(unk_data$MDCR_LANG_CD), assign_race))
    data$lang_only_race_cd<-NA
    data<-rbind(data,unk_data)
    final_row_count <- nrow(data)
    #Make lange_race_cd
    data[, lang_race_cd := ifelse(claim_race_cd %in% usable_races_abb, claim_race_cd, lang_only_race_cd)]
    
    # check if row count is different and throw an error if it is
    if (initial_row_count != final_row_count) {
      stop("Error: Row count is different after lang mapping, something went wrong w/ imputing")
    }
    #Checks!! Check to make sure races are imputed and not dropped along the way
    if (sum(data$orig_race_cd %in% usable_races_abb) > sum(data$fill_race_cd %in% usable_races_abb)) {
      stop("Error: There are less known races in fill race than orig race, something went wrong w/ imputing")
    }
    if (sum(data$fill_race_cd %in% usable_races_abb) > sum(data$claim_race_cd %in% usable_races_abb)) {
      stop("Error: There are less known races in claim race than fill race, something went wrong w/ imputing")
    }
    if (sum(data$claim_race_cd %in% usable_races_abb) > sum(data$lang_race_cd %in% usable_races_abb)) {
      stop("Error: There are less known races in lang race than fill race, something went wrong w/ imputing")
    }
    
    
    #---------------misc data cleaning before race imputation  --------------------------------------#
    race_cols<-c(grep("race_cd", names(data), value = TRUE),'MDCR_LANG_CD')
    
    data[, sex_id := sex_coding[EL_SEX_CD]]
    data[, EL_DOB := as.Date(as.character(EL_DOB), format = "%Y%m%d")]
    #want to adjust age to reflect if their birth hasn't happened yet that year, but need to be long on month in order to do that,
    #so will adjust for that further in the script once long on month (don't want long on month now bc don't want diff bene's to have
    # diff races in diff months)
    data[, age := MAX_YR_DT - year(EL_DOB)][, bmonth := month(EL_DOB)]
    
    #zip code column, want to only be 5 digits long
    #year 2000 - the zips are zero filled on right side to length of 
    #year 2010 & 2014 no zero filling coming in, just 1-5 characters long
    if (state %ni% c('VT', 'CT', 'MA','NH','NJ','RI') & d_year == 2000){
      data[, EL_RSDNC_ZIP_CD_LTST := substr(EL_RSDNC_ZIP_CD_LTST, 1, 5)] #some states only want first 4 in 2000
    } else if (state %in% c('VT', 'CT', 'MA','NH','NJ','RI') & d_year == 2000){
      data[, EL_RSDNC_ZIP_CD_LTST := substr(EL_RSDNC_ZIP_CD_LTST, 1, 4)] #some states only want first 4 in 2000
    }
    data[, EL_RSDNC_ZIP_CD_LTST := str_pad(as.character(EL_RSDNC_ZIP_CD_LTST), width = 5, side = "left", pad = "0")]
    #cnty is 5 characters long, first two are state code, last three are county code, ensure MDCR county code is 3 characters long
    data[, EL_RSDNC_CNTY_CD_LTST := str_pad(as.character(EL_RSDNC_CNTY_CD_LTST), width = 3, side = "left", pad = "0")]
    data[, EL_RSDNC_CNTY_CD_LTST := paste0(state, EL_RSDNC_CNTY_CD_LTST)]
    data <- merge(data, counties, by.x = "EL_RSDNC_CNTY_CD_LTST", by.y = "cnty", all.x = TRUE)
    data[, year_id := MAX_YR_DT][, MAX_YR_DT := NULL]
    
    #5) Imputing race
    print(" -> imputing race")
    ## --------------------
    ## Get population
    ## --------------------
    message('Getting population')
    pop_dir <- 'FILEPATH'
    pop_indir <- 'FILEPATH'
    pop <- open_dataset(pop_indir) %>% collect()
    setnames(pop,'age_group_years_start','age_bin')
    setnames(pop,'zip','EL_RSDNC_ZIP_CD_LTST')
    pop[,sex_id:=as.character(sex_id)]
    
    ## --------------------
    ## Assign probabilistically
    ## --------------------
    # save out validation info
    zip_validation(df=data, pop=pop, source='MDCD_MAX', timestamp=timestamp, state=state)
    
    #split data into race known and not known
    initial_row_count <- nrow(data)
    data[, lang_race_cd := ifelse(lang_race_cd %in% usable_races_abb, lang_race_cd, 'UNK')]
    unk_data <- data[lang_race_cd == 'UNK']
    data <- data[lang_race_cd != 'UNK']
    
    unk_data[age<25,age_bin:='0-25']
    unk_data[age>=25 & age <65,age_bin:='25-65']
    unk_data[age>=65,age_bin:='65+']
    
    ## Can't assign the claims with UNK race where zip is UNK
    unk_data$EL_RSDNC_ZIP_CD_LTST[unk_data$EL_RSDNC_ZIP_CD_LTST == '00000'] <- NA
    data_to_assign <- copy(unk_data[!is.na(EL_RSDNC_ZIP_CD_LTST)])
    cant_assign <- copy(unk_data[is.na(EL_RSDNC_ZIP_CD_LTST)])
    
    if (data_to_assign[,.N]>0){
      
        message('Assigning race')
        data_to_assign[, group := .GRP, by = c('age_bin', 'sex_id', 'EL_RSDNC_ZIP_CD_LTST')]
        group_key <- unique(data_to_assign[,.(EL_RSDNC_ZIP_CD_LTST,age_bin,sex_id,group)])
        
        pop <- merge(group_key,pop, by = c('age_bin', 'sex_id', 'EL_RSDNC_ZIP_CD_LTST'), all.x=T)
        zero_pop_zips <- unique(pop[is.na(weight)]$group)
        
        ## Can't assign the age-sex-zips where pop is zero
        cant_assign <- rbind(data_to_assign[group %in% zero_pop_zips][,group:=NULL], cant_assign)
        cant_assign <- cant_assign[,age_bin:=NULL][,imp_only_race_cd:=NA]
        cant_assign$imputed_race<-0
        data_to_assign <- data_to_assign[!(group %in% zero_pop_zips)]
        
        assign_race <- function(x){
          dt <- copy(data_to_assign[group==x])
          sample_pop <- copy(pop[group==x])
          dt[,imp_only_race_cd:= sample(x = sample_pop$race, 
                                   size = nrow(dt), 
                                   replace = T,
                                   prob = sample_pop$weight)]
          return(dt)} 
        if (data_to_assign[,.N]>0){
          data_to_assign <- rbindlist(lapply(unique(data_to_assign$group), assign_race))
          data_to_assign[,`:=`(group=NULL, age_bin = NULL)]
          data_to_assign[,imputed_race:=1]
          data_to_assign <- rbind(data_to_assign,cant_assign,fill=T)
        } else {
          data_to_assign <- cant_assign
        }
    }else{
      cant_assign <- cant_assign[,age_bin:=NULL][,imp_only_race_cd:=NA]
      data_to_assign <- copy(cant_assign)
      data_to_assign$imputed_race <- 0
    }
    data$imp_only_race_cd <- NA
    data$imputed_race <- 0
    data<- rbind(data, data_to_assign)
    final_row_count <- nrow(data)
    # check if row count is different and throw an error if it is
    if (initial_row_count != final_row_count) {
      stop("Error: Row count is different after imputation")
    }
    
    #Make final race_cd column
    data[, race_cd := ifelse(lang_race_cd %in% usable_races_abb, lang_race_cd, imp_only_race_cd)]
    data[, race_cd := ifelse(race_cd %in% usable_races_abb, race_cd, 'UNK')]
    
    #Make sure there are more known races now than before
    if (sum(data$lang_race_cd %in% usable_races_abb) > sum(data$race_cd %in% usable_races_abb)) {
      stop("Error: There are less known races in race_cd than lang race, something went wrong w/ imputing")
    }
    #exclude these state years because there are not usable age and zip info when race is UNK to do zip imputation
    if (!((d_year == 2000 & state %in% c("AR", "ID", "ME", "ND", "NV", "OK")) |
          (d_year == 2010 & state %in% c("ID","CT", "WV")) |
          (d_year == 2014 & state %in% c("ID", "WV"))) &&
        sum(data$imp_only_race_cd %in% usable_races_abb) == 0) {
      stop("Error: Nothing got imputed with zip-race imputation - this should not happen unless there are no zips")
    }
    
    ## Confirm we only have the 5 main race codes in the final step post imputation and lang map apply
    unique_races <- unique(data$race_cd)
    expected_races <- c(usable_races_abb,'UNK') 
    if ( any(unique_races %ni% expected_races) ){
      message(unique_races[unique_races %ni% expected_races])
      stop('Uh oh, you have some race codes we do not expect')
    }

    # If we don't want to impute race w/ zip map, set race code raw col to the race column just imputed from the claims
    data[, race_cd_raw := claim_race_cd] 
    setnames(data, 'race_cd', 'race_cd_imp')
    
    #Confirm THERE IS ONLY ONE RACE FOR EACH BENE_MSIS ID
    one_race <- data[, .(race_count=.N), by=.(BENE_ID, MSIS_ID)]
    one_race_cnt <- unique(one_race$race_count)
    if (any(sapply(one_race_cnt, function(x) x > 1))){
      #if there is more than one race per bene, choose the one w/ valid info
      bene_multi_race <- one_race[race_count > 1, .(MSIS_ID)]
      bene_multi_race <- unique(bene_multi_race$MSIS_ID)
      data_one_re <- data[MSIS_ID %ni% bene_multi_race]
      #instances in which there are more than one race is often when there are 2 entries and one doesn't have race info
      data_multi_re <- data[MSIS_ID %in% bene_multi_race & MSNG_ELG_DATA != 1]
      data <-rbind(data_one_re, data_multi_re)
      one_race <- data[, .(race_count=.N), by=.(BENE_ID, MSIS_ID)]
      one_race_cnt <- unique(one_race$race_count)
    }
    if (any(sapply(one_race_cnt, function(x) x > 1))){
      stop('Uh oh, some bene-msis pairs have more than one race')
    }

    # Transform data wide to long on month ----------------------
    misc_cols <-c("sex_id","age","bmonth","mcnty","year_id",
                  "imp_only_race_cd","imputed_race", "race_cd_imp","race_cd_raw")
    #remove column that doesn't exist anymore
    demo_cols <- demo_cols[sapply(demo_cols, function(x) x != 'MAX_YR_DT')]
    data <- suppressWarnings(melt(data, id.vars = c(demo_cols,other_cols, race_cols, misc_cols), measure.vars = patterns("_\\d{1,2}$")))
    data[, mnth := str_extract(variable, "\\d{1,2}$")][, variable := str_remove(variable, "_\\d{1,2}$")]
    data <- dcast(data, ... ~ variable, value.var = "value")
    
    #Adjust age to reflecgt true age based on month of enrollment
    data[ bmonth > as.numeric(mnth), age := age - 1]
    data[MAX_ELG_CD_MO %in% c("3A","2A","ZZ"), MAX_ELG_CD_MO := 3]
    data[, mc_status := "partial_mc"]
    data[MC_COMBO_MO %in% c("16","99"), mc_status := "ffs_only"]
    data[MC_COMBO_MO %in% c("01","06","07","08","09","10","11","12","13"), mc_status := "mc_only"]
    data[MC_COMBO_MO == "00", mc_status := "no_coverage"]
    data[, EL_DAYS_EL_CNT := as.integer(EL_DAYS_EL_CNT)]

    #---------------End of race adjustments --------------------------------------#

    # WRITE INTERMEDIATES FOR CLAIMS
    print(" -> writing intermediary claims")
    intermediates <- data %>%
      select('BENE_ID', 'MSIS_ID', 'STATE_CD', 'age', 'year_id', 'mnth', 'sex_id', 'race_cd_raw', 'race_cd_imp', 'mcnty', 'state', 'st_resi',
             'EL_RSDNC_ZIP_CD_LTST', 'EL_RSDNC_CNTY_CD_LTST', 'EL_DOB', 'EL_MDCR_DUAL_MO', 'EL_PVT_INS_CD',
             'MAX_ELG_CD_MO', 'MC_COMBO_MO', 'mc_status', 'EL_DAYS_EL_CNT',
             'imputed_dob_ind','imputed_race_ind','imputed_sex_ind')
    write_dataset(intermediates,
                  path = paste0('FILEPATH'),
                  partitioning = c('year_id', 'STATE_CD'),
                  basename_template = paste0(state,"_part-{i}"),
                  existing_data_behavior = "overwrite")

    data <- data[,.(days = sum(EL_DAYS_EL_CNT)),
                 by=c("sex_id","age","state_name","mcnty","race_cd_raw", "race_cd_imp","mc_status","year_id")]

    print(" -> binding on outputs")
    ps <- rbind(ps, data)
  }
}


## sometimes this is empty here
if(nrow(ps) > 0){
  ps <- age_bin(ps, age_column_name = "age")
  ps <- ps[,.(enrollment = sum(days)/365),
           by=c("sex_id", "state_name", "mcnty", "mc_status", "year_id", "age_start","age_group_id",'race_cd_raw', 'race_cd_imp')]
  ps <- ps[!is.na(age_start) & !is.na(sex_id) & !is.na(age_group_id)& !is.na(mcnty)]

  print("saving out")

  write_dataset(ps,
              path = paste0('FILEPATH'),
              partitioning = c("year_id", "state_name"),
              basename_template = paste0(state,"_part-{i}."),
              existing_data_behavior = "overwrite")
}
print('done!')
