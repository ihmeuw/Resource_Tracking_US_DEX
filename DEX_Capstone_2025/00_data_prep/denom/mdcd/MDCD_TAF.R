#----------------------------------------------------------------------#
# - Step 1 in creation of sample denominator for MDCD TAF -------------#
# Run time: Some states take 1-2 hours
# Purpose: This script creates 
# 1) the intermediate demographic files for MDCD TAF that is used in stage 2 processing
# 2) the first step in creating the sample denominator for MDCD TAF
# AUTHOR(S): Meera Beauchamp, Sawyer Crosby, Drew DeJarnatt, Emily Johnson
#----------------------------------------------------------------------#
rm(list = ls()) 
#############################################################################

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
counties <- counties %>%
  select(mcnty, cnty) 

#Get state map 
load("FILEPATH") 
states <- states %>% 
  select(abbreviation, state, state_name) %>%
  mutate(state := str_sub(state, -2, -1))

# Read data in arrow, select columns
if(interactive()){
  d_year <- 2019
  state <- 'ID'
  timestamp <- Sys.Date()
}else{
  args <- commandArgs(trailingOnly = TRUE)

  task_map_path <- args[1]
  # use Task id to identify params for this run!
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID")]

  d_year <- as.numeric(params$year) %>% print()
  state <- as.character(params$state) %>% print()
  timestamp <- as.character(params$timestamp) %>% print()
}

plot_save_dir <- paste0('FILEPATH',timestamp,'/')
if (!dir.exists(plot_save_dir)){dir.create(plot_save_dir, recursive = T)}

print(paste('args: ', d_year, state, timestamp))
print('setting up templates')
demo_cols <- c("BENE_ID", "MSIS_ID", "BENE_STATE_CD", "STATE_CD","BENE_CNTY_CD", "RFRNC_YR", "BIRTH_DT", "SEX_CD", 
               "RACE_ETHNCTY_CD", "RACE_ETHNCTY_EXP_CD", "ETHNCTY_CD","PRMRY_LANG_CD","PRMRY_LANG_GRP_CD",
               "MISG_ELGBLTY_DATA_IND",  "BENE_ZIP_CD", "TPL_INSRNC_CVRG_IND", "SUBMTG_STATE_CD")

## CODING - USING NAMED VECTORS
re_coding <- c('1' = 'WHT', '2' = 'BLCK', '3' = 'API', '4' = 'AIAN', '5' = 'API', '6' = 'UNK', '7' = 'HISP', '8' = 'UNK')
usable_races_num<-c(1,2,3,4,5,7)
usable_races_abb<-c('WHT', 'BLCK', 'HISP', 'API', 'AIAN')
#6 is Multi but we recode to UNK, 8 is OTH but we recode to UNK
sex_coding <- c('M' = 1, 'F' = 2, 'U' = -1)
chip_coding <- qc(`0` = 0, `1` = 0, `2` = 1, `3` = 1, `9` = NA)
state_coding <- states$abbreviation

print(state)
names(state_coding) <- states$state

claim_info_null <-data.frame (BENE_ID= c(NA),
                              MSIS_ID= c(NA),
                              STATE_CD= c(NA),
                              dob_claim= c(NA))
# Read in table created from claims which contains unique bene/msis/state/year/dob/sex/race
# In TAF there is no sex and race info in claims so those are all NA
if(state != 'NA'){
  claim_path<-paste0('FILEPATH',
                     d_year,
                     'FILEPATH',
                     state)
  if(file.exists(claim_path)){ ## split into two to avoid memory issues
    print('reading claim data')
    claim_ds <- open_dataset(claim_path)
    L <- dim(claim_ds)[1]
    H <- round(L/2,0)
    t <- L-H
    claim_info1 <- claim_ds %>%
      select("BENE_ID", "MSIS_ID","dob_claim") %>%
      head(H) %>%
      unique() %>%
      mutate(STATE_CD = state)%>%
      collect()
    claim_info2 <- claim_ds %>%
      select("BENE_ID", "MSIS_ID","dob_claim") %>%
      tail(t) %>%
      unique() %>%
      mutate(STATE_CD = state)%>%
      collect()
    stopifnot(nrow(claim_info1) == nrow(claim_info1))
  }else{
    claim_info <- claim_info_null
    claim_info1 <- claim_info2 <- claim_info_null
  }
}
if(state == 'NA'){
  claim_info1 <- claim_info2 <- claim_info_null
} 

file <- c(Sys.glob(paste0("FILEPATH")))

print(" -> reading data")
df <- open_dataset(file) %>% select(all_of(demo_cols), matches('MDCD_ENRLMT_DAYS'), matches('MC_PLAN_TYPE_CD'), 
                                    matches('DUAL_ELGBL_CD'), matches('MASBOE_CD'))
if (state != 'NA'){
  df<-df %>% 
    filter(STATE_CD == state)
} else if (state =='NA'){
  df<-df %>%
    filter(is.na(STATE_CD))
}
df<-df  %>%
  mutate(BENE_STATE_CD = str_pad(as.character(BENE_STATE_CD), width = 2, side = "left", pad = "0")) %>%
  mutate(BENE_CNTY_CD = str_pad(as.character(BENE_CNTY_CD), width = 3, side = "left", pad = "0")) %>%
  rename(year_id = RFRNC_YR) %>%
  collect()%>%
  setDT()
df[df == ""] <- NA

#-------------------------------------------------------------------------------------
print(" -> Imputing state code/creating st_resi")
#Get st_resi info from a tiered imputation approach
#-------------
#turn the STATE_CD column into fips codes and save in column STATE_CD_fips
states1<- states %>% select('abbreviation', 'state') %>% rename('STATE_CD_fips' = 'state')
df <- merge(df, states1, by.x = "STATE_CD", by.y = "abbreviation", all.x = TRUE)
df$STATE_CD_fips = str_pad(as.character(df$STATE_CD_fips), width = 2, side = "left", pad = "0")
#Mark where bene_st_cd will get imputed from state_cd
df[is.na(BENE_STATE_CD) & !is.na(STATE_CD_fips), "imputed_st_from_serv_ind" := 1]
#Where the BENE_STATE_CD is NA, impute with STATE_CD_fips and save to column named BENE_STATE_CD
df[, BENE_STATE_CD := fifelse(is.na(BENE_STATE_CD), STATE_CD_fips, BENE_STATE_CD)]

#recode from fips to state abbreviations
df[, st_resi := state_coding[BENE_STATE_CD]]
#Get state_name
states1<- states %>% select('abbreviation', 'state_name')
df <- merge(df, states1, by.x = "st_resi", by.y = "abbreviation", all.x = TRUE)
#Where the STATE_CD is NA, impute with st_resi 
df[, STATE_CD := fifelse(is.na(STATE_CD), st_resi, STATE_CD)]
df$st_serv<-df$STATE_CD

#Join claim data onto ps data, where ps is NA, fill with claim info
#Add a column to indicate imputed, then drop claim columns
# store initial row count
print(" -> merging in claim info")
initial_row_count <- nrow(df)
df<-merge(df, claim_info1, by=c('BENE_ID','MSIS_ID','STATE_CD'), all.x=TRUE)
df<-merge(df, claim_info2, by=c('BENE_ID','MSIS_ID','STATE_CD'), all.x=TRUE)
# store row count after merge
final_row_count <- nrow(df)

# check if row count is different and throw an error if it is
if (initial_row_count != final_row_count) {
  stop("Error: Row count is different after merge")
}

#impute
print(" -> begin imputation")
df[,imputed_dob_ind := 0]
df[is.na(BIRTH_DT) & !is.na(dob_claim.x), c("BIRTH_DT", "imputed_dob_ind") := .(dob_claim.x, 1)]
df[is.na(BIRTH_DT) & !is.na(dob_claim.y), c("BIRTH_DT", "imputed_dob_ind") := .(dob_claim.y, 1)]
df[, c('dob_claim.x','dob_claim.y') := NULL]

df[, sex_id := sex_coding[SEX_CD]]
df[is.na(sex_id), sex_id := -1]
df[is.na(BENE_STATE_CD), BENE_STATE_CD := -1]
df<- df %>%
  mutate(BENE_CNTY_CD = paste0(BENE_STATE_CD, BENE_CNTY_CD))
df <- merge(df, counties, by.x = "BENE_CNTY_CD", by.y = "cnty", all.x = TRUE)

#-------------------------------------------------
# Make Race adjustments/imputation
#Don't have info to do 1 
# 1) fill using other years (back forward fill w/ race tables) 
# x) fill using claim info  - did this in MAX, but can't in TAF bc race info not in claims
# 2) Use other race columns to fill in race
# 3) language mapping
# 4) apply race proportions by zip, later multiple imputation
#-------------------------------------------------
df$RACE_ETHNCTY_CD<-as.character(df$RACE_ETHNCTY_CD)
df[, race_cd := re_coding[RACE_ETHNCTY_CD]]
df[, race_cd := ifelse(race_cd %ni% usable_races_abb, 'UNK', race_cd)] 
#2) use other race columns to fill in race -https://resdac.org/sites/datadocumentation.resdac.org/files/CCW%20Codebook%20TAF%20Demographic%20Eligibility_Version%20022023.pdf
#this column lists Hispanic origin
df[, ETHNCTY_CD := ifelse(ETHNCTY_CD %in% c(1,2,3,4,5), 'HISP', 'UNK') ]
df[, race_cd := ifelse(ETHNCTY_CD == 'HISP', ETHNCTY_CD, race_cd)]
#This column lists more granular race
df[, RACE_ETHNCTY_EXP_CD := ifelse(RACE_ETHNCTY_EXP_CD %in% c(3:18), 'API', RACE_ETHNCTY_EXP_CD)]
df[, RACE_ETHNCTY_EXP_CD := ifelse(RACE_ETHNCTY_EXP_CD ==1, 'WHT', RACE_ETHNCTY_EXP_CD)]
df[, RACE_ETHNCTY_EXP_CD := ifelse(RACE_ETHNCTY_EXP_CD ==2, 'BLCK', RACE_ETHNCTY_EXP_CD)]
df[, RACE_ETHNCTY_EXP_CD := ifelse(RACE_ETHNCTY_EXP_CD ==20, 'HISP', RACE_ETHNCTY_EXP_CD)]
df[, RACE_ETHNCTY_EXP_CD := ifelse(RACE_ETHNCTY_EXP_CD %ni% usable_races_abb, 'UNK', RACE_ETHNCTY_EXP_CD)] 
df[, race_cd := ifelse(race_cd == 'UNK', RACE_ETHNCTY_EXP_CD, race_cd)]
#Make copy for original race code column. Original as in before language or pct imputation
df[, orig_race_cd := race_cd]

#3) Add in language map!
print(" -> language mapping")
# use language group column to fill in where language map is missing or not helpful
#Change language codes to match those in the map
df$PRMRY_LANG_GRP_CD = case_when(df$PRMRY_LANG_GRP_CD == 'C' ~ 'CHI',
                                 df$PRMRY_LANG_GRP_CD == 'D' ~ 'GER',
                                 df$PRMRY_LANG_GRP_CD == 'E' ~ 'ENG',
                                 df$PRMRY_LANG_GRP_CD == 'F' ~ 'FRE',
                                 df$PRMRY_LANG_GRP_CD == 'G' ~ 'GRE',
                                 df$PRMRY_LANG_GRP_CD == 'I' ~ 'ITA', 
                                 df$PRMRY_LANG_GRP_CD == 'J' ~ 'HISP', 
                                 df$PRMRY_LANG_GRP_CD == 'N' ~ 'NOR', 
                                 df$PRMRY_LANG_GRP_CD == 'P' ~ 'POL', 
                                 df$PRMRY_LANG_GRP_CD == 'R' ~ 'RUS', 
                                 df$PRMRY_LANG_GRP_CD == 'S' ~ 'SPA',
                                 df$PRMRY_LANG_GRP_CD == 'V' ~ 'SWE',
                                 df$PRMRY_LANG_GRP_CD == 'W' ~ 'SRP',
                                 TRUE ~ 'UND')
setDT(df)
df$PRMRY_LANG_CD[is.na(df$PRMRY_LANG_CD)] <- 'UND'
df$PRMRY_LANG_CD<- ifelse(df$PRMRY_LANG_CD %in% c('UND', 'MIS', 'ZXX'), df$PRMRY_LANG_GRP_CD, df$PRMRY_LANG_CD)

#Read in language map
lang_map<- fread(paste0('FILEPATH')) %>% 
  select(lang_cd, race_cd, pct) %>% rename(PRMRY_LANG_CD = lang_cd)

#Subset df to just unknowns and multi
initial_row_count <- nrow(df)
unk_df <- df[race_cd %in% c('UNK', 'OTH', 'MULTI') &
               PRMRY_LANG_CD %ni% c('MIS', 'UND', 'ZXX', 'ENG')] #should all be 'UNK' 
df<- df[!(race_cd %in% c('UNK', 'OTH', 'MULTI')) |
          PRMRY_LANG_CD %in% c('MIS', 'UND', 'ZXX', 'ENG')]

assign_race <- function(x){
  print(x)
  dt <- copy(unk_df[PRMRY_LANG_CD==x])
  sample_pop <- copy(lang_map[PRMRY_LANG_CD==x])
  dt[,lang_only_race_cd:= sample(x = sample_pop$race_cd, 
                                 size = nrow(dt), 
                                 replace = T,
                                 prob = sample_pop$pct)]
  return(dt)
}
unk_df <- rbindlist(lapply(unique(unk_df$PRMRY_LANG_CD), assign_race))
df$lang_only_race_cd<-NA
df<-rbind(df,unk_df)
df$race_cd<-ifelse(df$race_cd %ni% usable_races_abb, df$lang_only_race_cd, df$race_cd)
df$lang_race_cd<-df$race_cd
final_row_count <- nrow(df)
# check if row count is different and throw an error if it is
if (initial_row_count != final_row_count) {
  stop("Error: Row count is different after lang mapping")
}

#4) Imputing race
print(" -> imputing race")
## --------------------
## Get population
## --------------------
message('Getting population')
pop_dir <- 'FILEPATH'
pop_indir <- paste0(pop_dir, 'year_id=',d_year)
pop <- open_dataset(pop_indir) %>% collect()
setnames(pop,'age_group_years_start','age_bin')
pop[,sex_id:=as.numeric(sex_id)]

## --------------------
## Assign probabilistically
## --------------------
#Create an age column just to use for binning (won't be exact that is created later once data is long on month)
setDT(df)
df[, BIRTH_DT := as.Date(as.character(BIRTH_DT), format = "%d%b%Y")]
df[, age := year_id - year(BIRTH_DT)]
df$zip<-substr(df$BENE_ZIP_CD, start = 1, stop = 5)

# save out validation info
zip_validation(df=df, pop=pop, source='MDCD_TAF', timestamp=timestamp, state=state)

#split df into race known and not known
initial_row_count <- nrow(df)
unk_df<-df[race_cd %ni% usable_races_abb]
df<-df[race_cd %in% usable_races_abb]

unk_df[age<25,age_bin:='0-25']
unk_df[age>=25 & age <65,age_bin:='25-65']
unk_df[age>=65,age_bin:='65+']

## Can't assign the claims with UNK race where zip is UNK
unk_df$zip[unk_df$zip == '00000'] <- NA
df_to_assign <- copy(unk_df[!is.na(zip)])
cant_assign <- copy(unk_df[is.na(zip)])

if (df_to_assign[,.N]>0){
  
  message('Assigning race')
  setDT(df_to_assign)
  df_to_assign[, group := .GRP, by = c('age_bin', 'sex_id', 'zip')]
  group_key <- unique(df_to_assign[,.(zip,age_bin,sex_id,group)])
  
  pop <- merge(group_key,pop, by = c('age_bin', 'sex_id', 'zip'), all.x=T)
  zero_pop_zips <- unique(pop[is.na(weight)]$group)
  
  ## Can't assign the age-sex-zips where pop is zero
  cant_assign <- rbind(df_to_assign[group %in% zero_pop_zips][,group:=NULL], cant_assign)
  cant_assign <- cant_assign[,age_bin:=NULL][,imp_only_race_cd:=NA]
  cant_assign$imp_only_race_cd <- as.character(cant_assign$imp_only_race_cd)
  df_to_assign <- df_to_assign[!(group %in% zero_pop_zips)]
  
  assign_race <- function(x){
    dt <- copy(df_to_assign[group==x])
    sample_pop <- copy(pop[group==x])
    dt[,imp_only_race_cd:= sample(x = sample_pop$race, 
                             size = nrow(dt), 
                             replace = T,
                             prob = sample_pop$weight)]
    return(dt)} 
  
  df_to_assign <- rbindlist(lapply(unique(df_to_assign$group), assign_race))
  df_to_assign[,`:=`(group=NULL, age_bin = NULL)]
  df_to_assign <- rbind(df_to_assign,cant_assign,fill=T)
}else{
  cant_assign <- cant_assign[,age_bin:=NULL][,imp_only_race_cd:=NA]
  cant_assign$imp_only_race_cd <- as.character(cant_assign$imp_only_race_cd)
  df_to_assign <- cant_assign
}
df$imp_only_race_cd <- NA
df<- rbind(df, df_to_assign)
final_row_count <- nrow(df)
# check if row count is different and throw an error if it is
if (initial_row_count != final_row_count) {
  stop("Error: Row count is different after imputation")
}

#Make final race_cd column
df[, race_cd := ifelse(lang_race_cd %in% usable_races_abb, lang_race_cd, imp_only_race_cd)]
df[, race_cd := ifelse(race_cd %in% usable_races_abb, race_cd, 'UNK')]

#-----------------CHECKS-----------------------------------------#
## Confirm we only have the 5 main race codes in the final step post imputation and lang map apply
unique_races <- unique(df$race_cd)
expected_races <- c(usable_races_abb,'UNK') 
if ( any(unique_races %ni% expected_races) ){
  message(unique_races[unique_races %ni% expected_races])
  stop('Uh oh, you have some race codes we do not expect')
}
#Make sure there are more known races after each imputation step than before each step
if (sum(df$orig_race_cd %in% usable_races_abb) > sum(df$lang_race_cd %in% usable_races_abb)) {
  stop("Error: There are less known races in race_cd than lang race, something went wrong w/ imputing")
}
if (sum(df$lang_race_cd %in% usable_races_abb) > sum(df$race_cd %in% usable_races_abb)) {
  stop("Error: There are less known races in race_cd than lang race, something went wrong w/ imputing")
}
if (sum(df$imp_only_race_cd %in% usable_races_abb) == 0 & (state %ni% c('NA', 'RI'))) { #RI 2019 has no zips
  stop("Error: Nothing got imputed with zip-race imputation - this should not happen unless there are no zips")
}

# If we don't want to impute race w/ zip map, set race code raw col to the race column just imputed from the claims
df[, race_cd_raw := orig_race_cd] 
setnames(df, 'race_cd', 'race_cd_imp')

# WRITE race cols
print(" -> saving out race columns for vetting/visuals w/ language")
race_data<- distinct(df, BENE_ID, MSIS_ID, .keep_all= TRUE) #Bc its long on mnth, we only want one entry per person for diagnostics
race_data_lang<- race_data %>%
  select(year_id, st_resi, age, sex_id, orig_race_cd, lang_race_cd, PRMRY_LANG_CD) %>%
  group_by(year_id, st_resi, age, sex_id, orig_race_cd, lang_race_cd, PRMRY_LANG_CD) %>%
  summarise(count=n())

#save out w/ language to vet map
write_dataset(race_data_lang,
              existing_data_behavior = c("overwrite"),
              path = paste0('FILEPATH',timestamp,'.parquet'),
              basename_template = paste0(state,"_part{i}.parquet"),
              partitioning = c("year_id"))

## ------------------------------------------------------------
## Save data for plotting, consistent w/ other data sources
## ------------------------------------------------------------
ps <- data.table()

for(mnth in str_pad(as.character(1:12), 2, "left", pad = "0")){
  print(mnth)
  mdf <- df %>%
    select("BENE_ID", "MSIS_ID", "BENE_STATE_CD", "STATE_CD","st_resi", "BENE_CNTY_CD", "BIRTH_DT",
           "race_cd_raw", "race_cd_imp", "year_id", "sex_id","MISG_ELGBLTY_DATA_IND",  "BENE_ZIP_CD", "TPL_INSRNC_CVRG_IND",
           "SUBMTG_STATE_CD", "imputed_st_from_serv_ind", "state_name", "st_serv", "imputed_dob_ind","mcnty",
           ends_with(mnth)
    ) %>%
    mutate(month = mnth) %>%
    rename_with(~ gsub(paste0("_",mnth),"", .x, fixed = TRUE)) %>%
    rename(mnth = month) %>%
    mutate(MDCD_ENRLMT_DAYS = as.integer(MDCD_ENRLMT_DAYS)) %>%
    setDT()

  mdf[, age := year_id - year(BIRTH_DT)][, bmonth := month(BIRTH_DT)]
  mdf[ bmonth > as.numeric(mnth), age := age - 1]

  mdf[, mc_status := "partial_mc"]
  mdf[MC_PLAN_TYPE_CD == "00", mc_status := "ffs_only"]
  mdf[MC_PLAN_TYPE_CD == "01", mc_status := "mc_only"][is.na(MC_PLAN_TYPE_CD), mc_status := "ffs_only"]

  # WRITE INTERMEDIATES FOR CLAIMS
  print(" -> writing intermediary claims")
  intermediates <- mdf %>%
    select(BENE_ID, MSIS_ID, STATE_CD, BENE_STATE_CD, age, year_id, mnth, sex_id, race_cd_raw, race_cd_imp, mcnty, st_resi,st_serv,
           BENE_ZIP_CD, BENE_CNTY_CD, BIRTH_DT, DUAL_ELGBL_CD, MISG_ELGBLTY_DATA_IND,TPL_INSRNC_CVRG_IND, MASBOE_CD,
           MC_PLAN_TYPE_CD, mc_status, MDCD_ENRLMT_DAYS, imputed_dob_ind) 
  write_dataset(intermediates,
                path = paste0("FILEPATH", timestamp),
                format = "parquet",
                partitioning = c('year_id', 'STATE_CD', 'mnth'),
                basename_template = paste0(state,"_part-{i}."),
                existing_data_behavior = "overwrite")

  mdf <- mdf[,.(days = sum(MDCD_ENRLMT_DAYS)),
             by=c("sex_id","age","state_name","mcnty","BENE_CNTY_CD","race_cd_raw","race_cd_imp","mc_status","year_id","mnth")]
  ps <- rbind(ps, mdf)
}

print("aggregation")
ps <- age_bin(ps, age_column_name = "age")
ps <- ps[,.(enrollment = sum(days)/365),
         by=c("sex_id", "state_name", "mcnty", "mc_status", "year_id", "age_start","age_group_id","race_cd_raw","race_cd_imp")]
ps <- ps[!is.na(age_start) & !is.na(sex_id) & !is.na(age_group_id)& !is.na(state_name)]
print("saving out")
print(nrow(ps))
write_dataset(ps,
                path = paste0("FILEPATH", timestamp),
                format = "parquet",
                basename_template = paste0(state,"_part-{i}."),
                partitioning = c("year_id", "state_name"),
                existing_data_behavior = "overwrite")
print(state)
print('done!')

