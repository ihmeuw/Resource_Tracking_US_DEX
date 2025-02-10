########################################################################
# PURPOSE: This is the first step in creating the sample denoms for MDCR from the mbsf files: https://resdac.org/cms-data/files/mbsf-base/data-documentation
# AUTHOR(S): Meera Beauchamp, Drew DeJarnatt, Emily Johnson
########################################################################
rm(list = ls())
library(data.table)
library(tidyverse)
library(arrow)
library(parallel)
#install.packages("wrapr")
library(wrapr, lib.loc = "FILEPATH")
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(here, "/../cms_denom_functions.R"))
options(arrow.skip_nul = TRUE)

if(interactive()){
  d_year <- 2022 %>% print() 
  state <- '22' %>% print()
  timestamp <- Sys.Date() %>% print()
  chia <- 1
}else{
  #Read data in arrow, select columns
  args <- commandArgs(trailingOnly = TRUE)
  task_map_path <- args[1]
  # use Task id to identify params for this run!
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID")]
  
  print('year')
  d_year <- as.numeric(params$year) %>% print()
  print('state')
  state <- as.character(params$state) %>% print()
  print('timestamp')
  timestamp <- args[2] %>% print()
  print('chia')
  chia <- args[3] %>% print()
}
print(timestamp)
print(d_year)
print(state)

'%ni%' <- Negate('%in%')
plot_save_dir <- paste0('FILEPATH')
if (!dir.exists(plot_save_dir)){dir.create(plot_save_dir, recursive = T)}

# Get counties map 
load('FILEPATH')
load('FILEPATH')

states<-states %>%
  select(state_name, abbreviation) %>%
  rename(st_resi = abbreviation,
         state_name1 = state_name)

counties <- counties %>%
  select(mcnty, cnty, state_name)

if(chia != 1){
  #Years of data: 2000, 2008:2017, 2019
  file <- Sys.glob(paste0('FILEPATH'))
  data_source <- 'MDCR'
} else if(chia == 1){
  # CHIA
  file <- Sys.glob(paste0('FILEPATH'))
  data_source <- 'CHIA_MDCR'
}

# Read in SSA -> FIPs map for year 2000
ssa <- fread("FILEPATH")
ssa <- ssa[,.(ssa, cnty = fips)]
ssa[, ssa := str_pad(as.character(ssa), width = 5, side = "left", pad = "0")]
ssa[, cnty := str_pad(as.character(cnty), width = 5, side = "left", pad = "0")]

non_standard_states<-c('67','68','69','70','71','72','73','74','80','99')
state_coding <- c('1'='AL','2'='AK','3'='AZ','4'='AR','5'='CA','6'='CO','7'='CT','8'='DE','9'='DC','10'='FL',
                   '11'='GA','12'='HI','13'='ID','14'='IL','15'='IN','16'='IA','17'='KS','18'='KY','19'='LA','20'='ME',
                   '21'='MD','22'='MA','23'='MI','24'='MN','25'='MS','26'='MO','27'='MT','28'='NE','29'='NV','30'='NH',
                   '31'='NJ','32'='NM','33'='NY','34'='NC','35'='ND','36'='OH','37'='OK','38'='OR','39'='PA','41'='RI',
                   '42'='SC','43'='SD','44'='TN','45'='TX','46'='UT','47'='VT','49'='VA','50'='WA','51'='WV','52'='WI',
                   '53'='WY','67'='TX','68'='FL','69'='FL','70'='KS','71'='LA','72'='OH','73'='PA','74'='TX','80'='MD')
re_coding <- c('0' = 'UNK','1' = 'WHT', '2' = 'BLCK', '3' = 'UNK', '4' = 'API', '5' = 'HISP', '6' = 'AIAN')
#3 is OTH - but we are recoding as UNK
usable_races_num<-c(1,2,4,5,6)
usable_races_abb<-c('WHT', 'BLCK', 'HISP', 'API', 'AIAN')

if ((!state %in% non_standard_states) & (chia == 0 | (chia ==1 & d_year==2018))){
  # Read in table created from claims which contains unique bene/msis/state/year/dob/sex/race
  claim_path<-paste0('FILEPATH')
  claim_info<- open_dataset(claim_path) %>%
    select(BENE_ID,dob_claim, race_claim, sex_claim)%>%
    unique() %>%
    mutate(STATE_CODE=as.integer(state))%>%
    collect()
} else {
  claim_info<- df <- data.frame (BENE_ID= c(NA),
                                 STATE_CODE= c(NA),
                                 dob_claim= c(NA),
                                 race_claim= c(NA),
                                 sex_claim= c(NA))
}

bene_remove <- function(x){
  return(str_remove(x, "BENE_"))
}
print('claim info read in')

demo_cols <- c("ID", "ENROLLMT_REF_YR", "ENHANCED_FIVE_PERCENT_FLAG", "BIRTH_DT", "SEX_IDENT_CD","RACE_CD",
               "STATE_CODE", "COUNTY_CD", "ZIP_CD")

print(d_year)

if (state != '99'){
    df <- open_dataset(file) %>% 
      filter(STATE_CODE == as.integer(state)) 
  } else if (state == '99'){
    df <- open_dataset(file, partitioning = schema(STATE_CODE = int32())) %>%
      filter(is.na(STATE_CODE))
  }
df<- df %>%
  rename_with(bene_remove) %>%
  select(all_of(demo_cols), "RTI_RACE_CD","STATE_CODE",
         starts_with("STATE_CNTY_FIPS"), starts_with("MDCR_ENTLMT_BUYIN"), starts_with("HMO_IND"),
         starts_with("PTD_CNTRCT_ID"), starts_with("DUAL_STUS_CD"), starts_with("PTC_PLAN_TYPE_CD")) %>%
  arrange(ID) %>%
  # formatting to the way we want to get pulled into the claims data when intermediates get written out
  rename(BENE_ID = ID, sex_id = SEX_IDENT_CD, race_cd = RTI_RACE_CD) %>%
  collect()
print('enrollment data read in')

#year 2000 - the zips are 9 digit, all other years are 5 digit
if (d_year == 2000){
  df$ZIP_CD <- str_pad(as.character(df$ZIP_CD), width = 9, side = "left", pad = "0")
  df$ZIP_CD <- substr(df$ZIP_CD, 1, 5)
}
if(!("ZIP_CD" %in% names(df))) df <- df %>% mutate(ZIP_CD = NA)
df<- df %>% mutate(ZIP_CD = str_pad(as.character(ZIP_CD), width = 5, side = "left", pad = "0"))

#-------------------------------------------------
# Make Race adjustments/imputation
# -fill in from other years
# - no language variable in mdcr so can't map
# - apply race proportions by zip, later multiple imputation
#-------------------------------------------------
#Create a column for original race code (before anything else)
df$race_cd <- ifelse((df$race_cd %in% c(0,3) | is.na(df$race_cd)), df$RACE_CD, df$race_cd)
df$orig_race_cd<-df$race_cd

#1) Fill in race where known for the same bene in a diff year
#Get unique list of bene's that don't have a known race to filter race column on
#https://resdac.org/cms-data/variables/research-triangle-institute-rti-race-code
if (chia != 1) { #can't do this for CHIA bc they have different bene_ids and we haven't created this table for it yet
  bene_list <- df %>% filter(race_cd %in% c(0,3) | is.na(race_cd)) %>% select(BENE_ID) %>% unique() %>% collect() 
  bene_list <-unique(bene_list$BENE_ID) #do this to make it a list

  if (d_year %in% c(2000, 2008:2016)){
    re_bene <- open_dataset(paste0('FILEPATH')) 
    if (d_year %in% c(2008,2009, 2011:2013)){ re_bene <- re_bene %>%
      rename(bene_id =BENE_ID, rti_race_cd = race_cd) %>% 
      mutate(rti_race_cd = case_when(rti_race_cd == 'UNK' ~ 0,
                                     rti_race_cd == 'WHT' ~ 1,
                                     rti_race_cd == 'BLCK' ~ 2,
                                     rti_race_cd == 'OTH' ~ 3,
                                     rti_race_cd == 'API' ~ 4,
                                     rti_race_cd == 'HISP' ~ 5,
                                     rti_race_cd == 'AIAN' ~ 6,
                                     TRUE ~ 0))
    } 
    re_bene<- re_bene%>%
      filter(bene_id %in% bene_list) %>%
      filter(rti_race_cd!= 0, #0 = UNKN, 3 = OTH)
             rti_race_cd!= 3)%>%
      select(BENE_ID = bene_id, rti_race_cd) %>%
      arrange(BENE_ID) %>%
      collect() %>%
      unique()
    
    df$BENE_ID<-as.character(df$BENE_ID)
    initial_row_count <- nrow(df)
    df<- merge(df, re_bene, by="BENE_ID", all.x = TRUE)
    final_row_count <- nrow(df)
    # check if row count is different and throw an error if it is
    if (initial_row_count != final_row_count) {
      stop("Error: Row count is different after merge to CI race table")
    }
    df$rti_race_cd<-ifelse(is.na(df$rti_race_cd),0, df$rti_race_cd)
    df$race_cd <- ifelse((df$race_cd %in% c(0,3) | is.na(df$race_cd)), df$rti_race_cd,df$race_cd)
    #Create a column for race after filling
    df$fill_race_cd<-df$race_cd
    df$fill_only_race_cd<-df$rti_race_cd
    df$rti_race_cd<-NULL
  }
} else {
  df$fill_race_cd<-df$race_cd
  df$fill_only_race_cd<-NA
}

if(!("fill_race_cd" %in% names(df))) df$fill_race_cd <-df$race_cd
if(!("fill_only_race_cd" %in% names(df))) df$fill_only_race_cd <- NA

#Join claim data onto ps data, where ps is NA, fill with claim info
#Add a column to indicate imputed, then drop claim columns
# store initial row count
initial_row_count <- nrow(df)
df<-merge(df, claim_info, by=c('BENE_ID','STATE_CODE'), all.x=TRUE)
# store row count after merge
final_row_count <- nrow(df)

# check if row count is different and throw an error if it is
if (initial_row_count != final_row_count) {
  stop("Error: Row count is different after merge")
}
setDT(df)
#Create imputed indicator
df[, imputed_dob_ind := ifelse((is.na(BIRTH_DT) & !is.na(dob_claim)), 1, 0)]
df[, imputed_race_ind := ifelse((is.na(race_cd) & !is.na(race_claim)), 1, 0)]
df[, imputed_sex_ind := ifelse((is.na(sex_id) & !is.na(sex_claim)), 1, 0)]
#impute the columns
df[is.na(BIRTH_DT) & !is.na(dob_claim), BIRTH_DT := dob_claim]
df[is.na(race_cd) & !is.na(race_claim), race_cd := race_claim]
df[is.na(sex_id) & !is.na(sex_claim), sex_id := sex_claim]
#Create a column for race after filling
df$claim_race_cd<-df$race_cd
df[, claim_only_race_cd := ifelse(race_cd != race_claim, race_claim, 'UNK')]
print('claim imputation done')

if(chia == 1){
  df <- df %>%
    mutate(birth_date = as.Date(as.character(BIRTH_DT), format = "%Y%m%d"))
} else {
  df <- df %>%
    mutate(birth_date = as.Date(BIRTH_DT, format = "%d%B%Y"))
}
setDT(df)
df[, AGE := ENROLLMT_REF_YR - year(birth_date)]
df[, year_id := ENROLLMT_REF_YR]

#---Imputing race-----------------------------
print(" -> imputing race")
## --------------------
## Get population
## --------------------
message('Getting population')
pop_dir <- 'FILEPATH'
df[,st_resi := plyr::revalue(as.character(STATE_CODE), state_coding)]
if (chia ==1){
  df[,st_resi := 'MA']
}
pop_indir <- paste0('FILEPATH') 
pop <- open_dataset(pop_indir) %>% collect()
setnames(pop,'age_group_years_start','age_bin')
setnames(pop,'zip','ZIP_CD')
pop[,sex_id:=as.numeric(sex_id)]
pop[,ZIP_CD:=as.character(ZIP_CD)]
#change all the race cols from codes to abv
setDT(df)
race_cols<-grep("race_cd", names(df), value = TRUE)
df[,race_cd := plyr::revalue(as.character(race_cd), re_coding)]
df[,orig_race_cd := plyr::revalue(as.character(orig_race_cd), re_coding)]
df[,fill_race_cd := plyr::revalue(as.character(fill_race_cd), re_coding)]
df[,fill_only_race_cd := plyr::revalue(as.character(fill_only_race_cd), re_coding)]
df[,claim_race_cd := plyr::revalue(as.character(claim_race_cd), re_coding)]
df[,claim_only_race_cd := plyr::revalue(as.character(claim_only_race_cd), re_coding)]
df[,RACE_CD := plyr::revalue(as.character(RACE_CD), re_coding)]
df$race_cd <-ifelse(df$race_cd %ni% usable_races_abb, 'UNK', df$race_cd)

## --------------------
## Assign probabilistically
## --------------------
#save out validation info
zip_validation(df=df, pop=pop, source=data_source, timestamp=timestamp, state=state)

#split data into race known and not known
initial_row_count <- nrow(df)
unk_data <- df[race_cd %ni% usable_races_abb]
data <- df[race_cd %in% usable_races_abb]

unk_data[AGE<25,age_bin:='0-25']
unk_data[AGE>=25 & AGE <65,age_bin:='25-65']
unk_data[AGE>=65,age_bin:='65+']

## Can't assign the claims with UNK race where zip is UNK
unk_data$ZIP_CD[unk_data$ZIP_CD == '00000'] <- NA
unk_data[,ZIP_CD:=as.character(ZIP_CD)]
data_to_assign <- copy(unk_data[!is.na(ZIP_CD)])
cant_assign <- copy(unk_data[is.na(ZIP_CD)]) 

if (data_to_assign[,.N]>0){
  
  message('Assigning race')
  setDT(data_to_assign)
  data_to_assign[, group := .GRP, by = c('age_bin', 'sex_id', 'ZIP_CD')]
  group_key <- unique(data_to_assign[,.(ZIP_CD,age_bin,sex_id,group)])
  
  pop <- merge(group_key,pop, by = c('age_bin', 'sex_id', 'ZIP_CD'), all.x=T)
  zero_pop_zips <- unique(pop[is.na(weight)]$group)
  
  ## Can't assign the age-sex-zips where pop is zero
  cant_assign <- rbind(data_to_assign[group %in% zero_pop_zips][,group:=NULL], cant_assign)
  cant_assign <- cant_assign[,age_bin:=NULL][,imp_only_race_cd:=NA]
  data_to_assign <- data_to_assign[!(group %in% zero_pop_zips)]
  
  assign_race <- function(x){
    dt <- copy(data_to_assign[group==x])
    sample_pop <- copy(pop[group==x])
    dt[,imp_only_race_cd:= sample(x = sample_pop$race, 
                             size = nrow(dt), 
                             replace = T,
                             prob = sample_pop$weight)]
    return(dt)} 
  data_to_assign <- rbindlist(lapply(unique(data_to_assign$group), assign_race))
  data_to_assign[,`:=`(group=NULL, age_bin = NULL)]
  data_to_assign <- rbind(data_to_assign,cant_assign,fill=T)
}else{
  cant_assign <- cant_assign[,age_bin:=NULL][,imp_only_race_cd:=NA]
  data_to_assign <- cant_assign
}
data$imp_only_race_cd <- NA
df<- rbind(data, data_to_assign)
final_row_count <- nrow(df)
# check if row count is different and throw an error if it is
if (initial_row_count != final_row_count) {
  stop("Error: Row count is different after imputation")
}

df$race_cd <-ifelse(df$race_cd %ni% usable_races_abb, df$imp_only_race_cd, df$race_cd)
df$race_cd <-ifelse(df$race_cd %ni% usable_races_abb, 'UNK', df$race_cd)

#----------CHECKS---------------------------------------------------------------------#
## Confirm we only have the 5 main race codes in the final step post imputation and lang map apply
unique_races <- unique(df$race_cd)
expected_races <- c(usable_races_abb,'UNK') 
if ( any(unique_races %ni% expected_races) ){
  message(unique_races[unique_races %ni% expected_races])
  stop('Error: Data contains unexpected races')
}
#ensure there are more known races after every step
#Make sure there are more known races now than before
if (sum(df$orig_race_cd %in% usable_races_abb) > sum(df$fill_race_cd %in% usable_races_abb)) {
  stop("Error: There are less known races in fill_race_cd than orig_race_cd, something went wrong w/ imputing")
} 
if (sum(df$fill_race_cd %in% usable_races_abb) > sum(df$claim_race_cd %in% usable_races_abb)) {
  stop("Error: There are less known races in claim_race_cd than fill_race_cd, something went wrong w/ imputing")
}
if (sum(df$claim_race_cd %in% usable_races_abb) > sum(df$race_cd %in% usable_races_abb)) {
  stop("Error: There are less known races in race_cd than claim_race_cd, something went wrong w/ imputing")
}
if (sum(df$imp_only_race_cd %in% usable_races_abb) == 0 & (state %ni% c('67','68','69','70','71','72','73','74','80'))) {
  #list above is outdated state codes with expected zip missingness
  stop("Error: Nothing got imputed with zip-race imputation - this should not happen unless there are no zips")
}

## ------------------------------------------------------------
## Save data for plotting, consistent w/ other data sources
## ------------------------------------------------------------
df[, race_cd_raw := claim_race_cd] 
setnames(df, 'race_cd', 'race_cd_imp')
print('race imputation done')
# At this point, data is wide by month so that each enrollment measure has 12 month-specific columns. Want to have the data
# long on month but wide on measure so that enrollment can be summed
# The size of the data makes pivoting super ugly/hard, so I'm selecting the columns for each month, cleaning them,
# aggregating, and then binding the data back together


months_df <- lapply(str_pad(as.character(1:12), 2, "left", pad = "0"), function(mnth){
  print(mnth)
  
  mdf <- df %>%
    select('BENE_ID','ENROLLMT_REF_YR', 'ENHANCED_FIVE_PERCENT_FLAG', 'BIRTH_DT', 'RACE_CD', 'COUNTY_CD', 'ZIP_CD',
           ends_with(mnth),"sex_id", "race_cd_raw","race_cd_imp",'STATE_CODE','birth_date',
           'imputed_dob_ind','imputed_race_ind','imputed_sex_ind','st_resi') %>%
    mutate(MONTH = mnth) %>%
    mutate(BMONTH = month(birth_date)) %>%
    rename_with(~ gsub(paste0("_",mnth),"", .x, fixed = TRUE)) 
  print(dim(mdf))
  #Create age
  mdf[, AGE := ENROLLMT_REF_YR - year(birth_date)][, bmonth := month(birth_date)]
  mdf[ as.numeric(bmonth) > as.numeric(mnth), AGE := AGE - 1]
  
  # Monthly location not available for the year 2000 - fill in w/ annual values
  if(("STATE_CNTY_FIPS_CD" %in% names(mdf))){ # years with monthly info
    mdf <- mdf %>%
      mutate(cnty = str_pad(as.character(STATE_CNTY_FIPS_CD), width = 5, side = "left", pad = "0"))
  }else{ # year 2000
    mdf <- mdf %>%
      mutate(ssa = paste0(str_pad(as.character(STATE_CODE), width = 2, side = "left", pad = "0"),
                          str_pad(as.character(COUNTY_CD), width = 3, side = "left", pad = "0"))) %>%
      left_join(ssa, by = "ssa") %>%
      filter(!is.na(STATE_CODE) & !is.na(COUNTY_CD))
  }
  
  # Part D didn't exist in year 2000, and dual status isn't available for that year - fill in
  if(!("PTD_CNTRCT_ID" %in% names(mdf))) mdf <- mdf %>% mutate(PTD_CNTRCT_ID = "0")
  if(!("DUAL_STUS_CD" %in% names(mdf))) mdf <- mdf %>% mutate(DUAL_STUS_CD = "99")
  if(!("PTC_PLAN_TYPE_CD" %in% names(mdf))) mdf <- mdf %>% mutate(PTC_PLAN_TYPE_CD = NA)
  mdf$DUAL_STUS_CD <- as.character(mdf$DUAL_STUS_CD)
  mdf$PTC_PLAN_TYPE_CD <- as.character(mdf$PTC_PLAN_TYPE_CD)
  
  #moving this up bc there are individuals in CHIA whose county is not in MA, but there state is
  mdf <- left_join(mdf, counties, by="cnty")
  mdf <- left_join(mdf, states, by="st_resi")
  #if not a matching county with state, use st_resi to fill in state_name
  mdf$state_name<- ifelse(is.na(mdf$state_name), mdf$state_name1, mdf$state_name)
  if (chia == '1'){
    mdf$COUNTY_CD <- ifelse(mdf$state_name != 'Massachusetts', NA, mdf$COUNTY_CD)
    mdf$cnty <- ifelse(mdf$state_name != 'Massachusetts', NA, mdf$cnty)
    mdf$ZIP_CD <- ifelse(mdf$state_name != 'Massachusetts', NA, mdf$ZIP_CD)
    mdf$state_name<- 'Massachusetts'
  }
  
  mdf[, AGE := ENROLLMT_REF_YR - year(birth_date)][, bmonth := month(birth_date)]
  mdf[ as.numeric(bmonth) > as.numeric(mnth), AGE := AGE - 1]

  intermed <- mdf %>%
    select(BENE_ID, ENROLLMT_REF_YR, MONTH, BIRTH_DT, BMONTH, STATE_CODE, RACE_CD, race_cd_raw, race_cd_imp, sex_id, cnty,
           ENHANCED_FIVE_PERCENT_FLAG, COUNTY_CD, ZIP_CD, HMO_IND, DUAL_STUS_CD, PTC_PLAN_TYPE_CD,
           imputed_dob_ind,imputed_race_ind,imputed_sex_ind,AGE) %>% copy()
  write_dataset(intermed,
                path = paste0('FILEPATH'),
                partitioning = c('ENROLLMT_REF_YR', 'STATE_CODE', 'MONTH'),
                format = "parquet",
                basename_template = paste0(state,"_part-{i}"),
                existing_data_behavior = "overwrite")
  print('intermed files saved out')

  # First level grouping
  mdf <- mdf %>%
    rename(year_id = ENROLLMT_REF_YR) %>%
    group_by(year_id, ENHANCED_FIVE_PERCENT_FLAG, sex_id, race_cd_raw, race_cd_imp, BMONTH,
             AGE, MONTH, cnty, mcnty, state_name, STATE_CODE, MDCR_ENTLMT_BUYIN_IND, HMO_IND, PTD_CNTRCT_ID, DUAL_STUS_CD) %>%
    summarize(n = n(), .groups = "keep") %>%
    ungroup() %>%
    setDT()

  # Clean up enrollment indicators
  mdf <- mdf %>%
    mutate(five_pct = case_when( # inclusion in the 5% sample
      ENHANCED_FIVE_PERCENT_FLAG == "Y" ~ 1,
      TRUE ~ 0
    )) %>%
    mutate(part_a = case_when( # part A coverage from buy-in indicator
      MDCR_ENTLMT_BUYIN_IND %in% c("1","3","A","C") ~ 1,
      MDCR_ENTLMT_BUYIN_IND == "0" ~ NA_real_,
      TRUE ~ 0
    )) %>%
    mutate(part_b = case_when( # part B coverage from buy-in indicator
      MDCR_ENTLMT_BUYIN_IND %in% c("2","3","B","C") ~ 1,
      MDCR_ENTLMT_BUYIN_IND == "0" ~ NA_real_,
      TRUE ~ 0
    )) %>%
    mutate(part_c = case_when( # part C coverage from HMO plan detail
      HMO_IND %in% c("2","B","C") ~ 1,
      TRUE ~ 0
    )) %>%
    mutate(part_d = case_when( # part D coverage from part D contract detail
      is.na(PTD_CNTRCT_ID) | PTD_CNTRCT_ID %in% c("N","0") ~ 0,
      TRUE ~ 1
    )) %>%
    mutate(dual_enrol = case_when( # dual mdcr-mdcd enrollment from dual indicator
      DUAL_STUS_CD %in% c("NA","09","9") ~ 0,
      DUAL_STUS_CD %in% c("99","00") ~ NA_real_,
      TRUE ~ 1
    ))
  # final enrollment formatting - if part c, don't count in part a or b
  # also calculate part a+b and any coverage
  mdf_pc <- mdf %>%
    filter(part_c == 1) %>%
    mutate(part_a = 0, part_b = 0)

  mdf <- mdf %>%
    filter(part_c != 1) %>%
    rbind(mdf_pc) %>%
    mutate(part_ab = case_when(
      part_a == 1 | part_b == 1 ~ 1,
      TRUE ~ 0
    )) %>%
    mutate(any_coverage = case_when(
      part_a == 1 | part_b == 1 | part_c == 1 ~ 1,
      TRUE ~ 0
    ))

  mdf <- mdf %>%
    select(year_id, sex_id, race_cd_raw, race_cd_imp, BMONTH, AGE, MONTH, cnty, mcnty, state_name, STATE_CODE,
           five_pct, part_a, part_b, part_c, part_d, part_ab, any_coverage, dual_enrol, n)
  return(mdf)
})

df <- rbindlist(months_df)
print('bene-level files done')

rm(months_df)

# Standardize to our counties/age bins
df <- age_bin(df, age_column_name = "AGE")

# Counting monthly enrollment by enrolment indicator, then dividing by 12 for annual
# each enrollment subgroup is binary as a 1 or a 0. Can multiply by the sample size on each row (n)
# to get the counts by each demographic
setDT(df)
df <- df[!is.na(age_start) & !is.na(sex_id) & !is.na(age_group_id)& !is.na(state_name)]# & !is.na(cnty) 
df[is.na(dual_enrol), dual_enrol := 0]
df<-df %>% filter(sex_id %in% c(1,2)) %>% copy()

df <- df[,.(n = sum(n, na.rm = TRUE)/12),
         by=c("age_start", "age_group_id", "sex_id", "year_id", "race_cd_raw", "race_cd_imp", "mcnty", "state_name", "five_pct",
              "part_a", "part_b", "part_c", "part_d", "part_ab", "any_coverage", "dual_enrol")]

# Re-create full sample set so that you don't need to sum over five_pct column to get the 100% sample
# since 5% sample indicator was binary, the five_pct == 0 category only has the remaining 95% enrollees
# now sample == "full" has everyone
df_full <- copy(df)[,.(n = sum(n, na.rm = TRUE)),
                    by=c("age_start","age_group_id","sex_id","year_id","race_cd_raw", "race_cd_imp","mcnty","state_name",
                         "part_a", "part_b", "part_c", "part_d", "part_ab", "any_coverage", "dual_enrol")]
df_full[, sample := "full"]
df <- df[five_pct == 1][, sample := "five_pct"][, five_pct := NULL]
df <- rbind(df, df_full)

# Remove NA state/ages/sexes
df <- df[!is.na(state_name) & !is.na(age_start) & !is.na(sex_id)]#

# final denoms now race-specific
df <- df[,.(n = sum(n, na.rm = TRUE)),
         by=c("age_start","age_group_id","sex_id","race_cd_raw", "race_cd_imp","year_id","mcnty","state_name","sample",
              "part_a", "part_b", "part_c", "part_d", "part_ab", "any_coverage", "dual_enrol")]

write_dataset(df,
              path = paste0('FILEPATH'),
              partitioning = c("year_id", "age_group_id", "sex_id"),
              basename_template = paste0(state,"_part-{i}"),
              existing_data_behavior = "overwrite")

print(dim(df))
print('MDCR disag DONE!')


