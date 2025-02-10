##----------------------------------------------------------------
## Title: 1_claims_to_encounter.R
## Purpose: Aggregate claims from service level to encounter level.
## Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')
username <- Sys.getenv('USER')
## --------------------
## Setup
## --------------------

library(uuid)
library(lubridate)


library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(here, '/functions_c2e.R'))


## Arguments
args <- commandArgs(trailingOnly = TRUE)
param_path <- args[1]
run_v <- args[2]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
metadata <- fread(param_path)
metadata <- metadata[task_id,]

source <- metadata$name
indir <- metadata$indir
carrier_indir <- metadata$carrier_indir
outdir <- metadata$outdir
care <- metadata$toc
yr <- metadata$year
sex <- metadata$sex
st <- metadata$state

if (care == 'AM'){
  age <- metadata$age
}
  


## --------------------------------------------------
## 1. Read in data 
## --------------------------------------------------

## -----------
## MDCR 
## -----------

if (source %like% 'MDCR'){
  
  dt <- pull_data(indir=indir, care, yr, st)

  if ( !is.null(dt) ){
    
    dt[, fac_prof_ind := 'F']
    dt[ENHANCED_FIVE_PERCENT_FLAG != 'Y',ENHANCED_FIVE_PERCENT_FLAG:="N"]

    if (care %in% c('NF','IP')){
      nf_mp <- dt[sub_dataset == 'nf_medpar']
      dt <- dt[!(sub_dataset == 'nf_medpar')]
    
      dt <- clean_data(dt)
      
      if (nrow(dt)>0){
        dt <- dedupe_admission(data = dt, buffer = T)
      }
      
      if (nrow(nf_mp)>0){
        dt <- rbind(dt,nf_mp,fill=T)
      }
      
    }else{
      dt <- clean_data(dt)
    }
  }
  
  ## Pull in physician claims for relevant years 
  if (source == 'MDCR'){
    if ( (care %ni% c('NF','ED') & yr %in% c(2000,2010,2014,2015,2016,2019)) | 
       (care == 'NF' & yr == 2019) | 
       (care == 'ED' & yr %in% c(2000,2010,2015,2016,2019)) ){
    
      prof_dt <- pull_data(indir=carrier_indir, care, yr, st)
      
      if (!(is.null(prof_dt))){
        prof_dt <- clean_data(prof_dt)
        prof_dt[,fac_prof_ind := 'P']
        prof_dt[ENHANCED_FIVE_PERCENT_FLAG != 'Y',ENHANCED_FIVE_PERCENT_FLAG:="N"]
      }
      
    }else{
      prof_dt <- NULL
    }
  }
    
  if (source == 'CHIA_MDCR'){
    prof_dt <- pull_data(indir=carrier_indir, care, yr, st)
    
    if (!(is.null(prof_dt))){
      prof_dt <- clean_data(prof_dt)
      prof_dt[,fac_prof_ind := 'P']
      prof_dt[ENHANCED_FIVE_PERCENT_FLAG != 'Y',ENHANCED_FIVE_PERCENT_FLAG:="N"]
    }
      
  }

  ## If both facility and professional data exist, combine them
  if ( !is.null(dt) & !is.null(prof_dt) ){
    p_benes <- unique(prof_dt[,.(bene_id)])
    f_benes <- unique(dt[,.(bene_id)])
    m <- merge(p_benes,f_benes)

    dt <- merge_prof_data(facility_data = dt, prof_data = prof_dt)
  }
  
  ## If only professional data exists
  if ( !is.null(prof_dt) & is.null(dt) ){
    dt <- copy(prof_dt)
    ## Deduplicate admission date for NF and IP
    if (care %in% c('NF','IP')){
      dt <- dedupe_admission(data = dt, buffer = T)
    }
  }
  
  ## If neither exists
  if ( is.null(prof_dt) & is.null(dt)  ){
    print('NO DATA')
  }else{
    
    ## Make DX1 based on minimum dx_level for a claim that is not a _gc
    dt <- make_dx1(data=dt)
    
    ## Create the "source_pay_amt" which is a temp column used to rank the spend. Varies across sources so need to standardize.
    dt[, source_pay_amt := mdcr_pay_amt]
    dt[is.na(ENHANCED_FIVE_PERCENT_FLAG), ENHANCED_FIVE_PERCENT_FLAG := "N"]
  }



}

## -----------
## MDCD
## -----------
if (source == 'MDCD'){
  
  dt <- pull_data(indir, care, yr, st)

  ## If data exists, make DX1 based on minimum dx_level for a claim that is not a _gc
  if ( !is.null(dt) ){
    
    dt <- clean_data(dt)
    #Create a bene_id if its missing
    dt[is.na(bene_id), bene_id := paste0(MSIS_ID, '_', st_resi)]

    prof_dt <- dt[fac_prof_ind == 'P']
    dt <- dt[fac_prof_ind == 'F']
    
    if (!is.null(prof_dt)){
      if (nrow(prof_dt) ==0){
        prof_dt <- NULL
      }
    }
    
    if (!is.null(dt)){
      if (nrow(dt) ==0){
        dt <- NULL
      }
    }

    ## If both facility and professional data exist, combine them
    if ( !is.null(dt) & !is.null(prof_dt) ){
      
      ## Deduplicate admission date for NF and IP
      if (care %in% c('NF','IP')){
        dt <- dedupe_admission(data = dt, buffer = T)
      }
      
      p_benes <- unique(prof_dt[,.(bene_id)])
      f_benes <- unique(dt[,.(bene_id)])
      m <- merge(p_benes,f_benes)
      dt <- merge_prof_data(facility_data = dt, prof_data = prof_dt)
      dt[,n_tocs:=length(unique(toc)), by = 'temp_id']
      dt[n_tocs >1, toc := 'AM'] # any P claims that get grouped with an F claim that is now marked as AM should be reassigned to AM too 
      dt$n_tocs <- NULL
    }
    
    ## If only professional data exists
    if ( !is.null(prof_dt) & is.null(dt) ){
      dt <- copy(prof_dt)
      
      ## Deduplicate admission date for NF and IP
      if (care %in% c('NF','IP')){
        dt <- dedupe_admission(data = dt, buffer = T)
      }
      
    }
    
    dt <- make_dx1(data=dt)
    ## Create the "source_pay_amt" which is a temp column used to rank the spend. Varies across sources so need to standardize.
    dt[, source_pay_amt := mdcd_pay_amt]
  }

}

## -----------
## MSCAN & KYTHERA
## -----------
if (source == 'MSCAN' | source == 'KYTHERA' ){
  
  ## If its inpatient, make encounters across ED and IP
  if (care == 'IP'){
    ## Pull in inpatient
    dt_ip <- pull_data(indir, care = 'IP', yr, st)
    if (!(is.null(dt_ip))){
      dt_ip$toc <- 'IP'
    }
    
    ## Pull in ED
    dt_ed <- pull_data(indir, care = 'ED', yr, st)
    if (!(is.null(dt_ed))){
      dt_ed$toc <- 'ED'
    }
    
    ## Bind together
    dt <- rbind(dt_ip,dt_ed) %>% as.data.table()
    
    if (nrow(dt)==0){
      dt <- NULL
    }
    
  }else{
    dt <- pull_data(indir, care, yr, st)

    if (!is.null(dt)){
      dt$toc <- care
    }
  }
  
  ## If data exists, make DX1 based on minimum dx_level for a claim that is not a _gc
  if (is.null(dt)){
    print('NO DATA')
  }else{

    dt[, n_toc:=length(unique(toc)), by = c('claim_id','bene_id')]
    if (dt[n_toc>1, .N] >0 ){
      stop('You have more than 1 toc per claim id indicating a processing issue')
    }
    dt$n_toc <- NULL
    
    dt <- clean_data(dt)
    
    prof_dt <- dt[fac_prof_ind == 'P']
    dt <- dt[fac_prof_ind == 'F']
    if (!is.null(prof_dt)){
      if (nrow(prof_dt) ==0){
        prof_dt <- NULL
      }
    }
    
    if (!is.null(dt)){
      if (nrow(dt) ==0){
        dt <- NULL
      }
    }
    
    ## Deduplicate admission date for NF and IP
    if (care %in% c('NF','IP') & !is.null(dt) ){
      dt <- dedupe_admission(data =dt, buffer=T)
    }
    ## If both facility and professional data exist, combine them
    if ( !is.null(dt) & !is.null(prof_dt) ){
      p_benes <- unique(prof_dt[,.(bene_id)])
      f_benes <- unique(dt[,.(bene_id)])
      m <- merge(p_benes,f_benes)
      dt <- merge_prof_data(facility_data = dt, prof_data = prof_dt)
    }
    
    ## If only professional data exists
    if ( !is.null(prof_dt) & is.null(dt) ){
      dt <- copy(prof_dt)
      ## Deduplicate admission date for NF and IP
      if (care %in% c('NF','IP')){
        dt <- dedupe_admission(data = dt, buffer = T)
      }
    }
    
    dt <- make_dx1(data=dt)
    
    ## Create the "source_pay_amt" which is a temp column used to rank the spend. Varies across sources so need to standardize.
    if (source == 'MSCAN'){
      dt$tot_chg_amt <- -1
      dt[, source_pay_amt := tot_pay_amt]
    }
    
    if (source == 'KYTHERA' ){
      dt[, source_pay_amt := tot_chg_amt]
    }
    dt[, claim_id:=paste0(claim_id,"_",toc,"_", bene_id)] #ensuring claim ids are unique across tocs, important for ED and IP
    
  }
  

  
}


## --------------------------------------------------
## 2. Do the remaining only if there's data
## --------------------------------------------------

if ( !(is.null(dt)) ){
  
  ## --------------------------------------------------
  ## 3. Make encounter_id 
  ## --------------------------------------------------
  
  group_cols <- c('bene_id','service_date')


  ## If group wasn't already assigned in professional data merge - then create groups
  if ( 'temp_id' %ni% names(dt) ){
    if ( source %like% 'MDCR' & care == 'NF'){
      # Grouping columns and assigning a temporary unique id
      dt[sub_dataset != 'nf_medpar', temp_id := .GRP, by = group_cols]
      if ( max(dt[!is.na(temp_id)]$temp_id) != -Inf ){
        max_grp <- max(dt[!is.na(temp_id)]$temp_id)
      }else{
        max_grp <- 0
      }
      dt[sub_dataset == 'nf_medpar', temp_id2 := .GRP, by = 'claim_id']
      dt[is.na(temp_id), temp_id:= temp_id2 + max_grp]
      dt$temp_id2 <- NULL
      dt[,nf_medpar:=ifelse(sub_dataset == 'nf_medpar', 1, 0)]
    }else{
      # Grouping columns and assigning a temporary unique id
      dt[, temp_id := .GRP, by = group_cols]
      dt$nf_medpar <- 0
    }
  }


  # Getting all temporary ids as list
  groups <- as.list(unique(dt$temp_id))
  
  # Creating a uuid for each unique id and mapping
  dt[, encounter_id := UUIDgenerate(use.time = NA, n = 1L, output = c("string")), by = 'temp_id']
  
  # Ensuring all encounter ids are actually unique
  if (length(unique(dt$encounter_id)) != length(unique(dt$temp_id))){
    message("Some created ids are not unique. Please re-run this step.")
    stop()
  }
  dt$temp_id <- NULL
  
  ##----------------------------------------------------------------------
  ## 4. If an encounter has only professional claims that originally were mapped to NF or IP toc, 
  ##    these now get reassigned back to AM since they do not have a facility claim
  ##----------------------------------------------------------------------  
  
  if (care %in% c('IP','NF')){
    dt[, only_ed:=ifelse('IP' %ni% unique(toc) & 'NF' %ni% unique(toc), 1,0 ), by = 'encounter_id']
    dt[,p_only:=ifelse('F' %ni% unique(fac_prof_ind) & only_ed != 1,1,0), by = 'encounter_id']
    
    dt[, toc:=ifelse(p_only==1, 'AM',toc), by = 'encounter_id']
    dt$p_only <- NULL
    dt$only_ed <- NULL
    reassigned_to_am <<- length(unique(dt[toc=='AM']$claim_id))
    message('Reassigned ', round(reassigned_to_am/length(unique(dt$claim_id)), digits = 2)*100, '% of claims to AM type of care')
    
    dt[, n_toc:=length(unique(toc)), by = c('claim_id','bene_id')]
    if (dt[n_toc>1, .N] >0 ){
      stop('You have more than 1 toc per claim id indicating a processing issue')
    }
  }
  
  if (source %like% 'MDCR'){
    ## For the F claims that are now grouped with P claims these should all be 5% sample
    dt[, ENHANCED_FIVE_PERCENT_FLAG := ifelse('Y' %in% unique(ENHANCED_FIVE_PERCENT_FLAG), 'Y', 'N'), by = 'encounter_id']
  }
  
  if ('mc_ind' %in% colnames(dt)){
    # If any of the encounter claims are managed care, the entire encounter is marked as MC (mc_ind can be 0,1,2)
    dt[, mc_ind := ifelse(max(mc_ind) >0 , max(mc_ind), 0), by = 'encounter_id']
  }
  
  if ('race_cd_imp' %in% colnames(dt)){
    dt[, n_race:= length(unique(race_cd_imp)), by = 'encounter_id']
    if (nrow(dt[n_race>1]) >0){
      dt <- sync_race(orig_data = dt, id_var = 'bene_id',race_var = 'race_cd_imp')
      dt[, n_race:= length(unique(race_cd_imp)), by = 'encounter_id']
      if (nrow(dt[n_race>1]) >0){
        stop('Encounters are not unique on race_cd_imp')
      }
    }
    dt$n_race <- NULL
    dt$race_cd_synced <- NULL
  }
  
  
  dt[, n_claim := length(unique(claim_id)), by = c('encounter_id')] #number of claims per encounter
  dt[, n_row := .N, by = c('encounter_id','claim_id')] #number of DXs per claim
                  
  
  ## --------------------------------------------------
  ## 5. Add in all needed cols, reassign any ED visits that are now the same encounter as an IP visit
  ##    to IP type of care, make length of stay max claim los per encounter (NF and IP only), and do few computations for checks
  ## --------------------------------------------------
  if (source == 'KYTHERA' | source =='MSCAN'){
    dt[, n_tocs := length(unique(toc)), by = 'encounter_id'] #number of types of care per encounter
    dt[n_tocs >1, toc := 'IP'] # If there are more than 1 (as in there is IP and ED), set it equal to IP
    
  }
  

  dt[, n_tocs := length(unique(toc)), by = 'claim_id']
  num_service_lines <- nrow(dt)
  num_patients <- length(unique(dt[!is.na(bene_id)]$bene_id))
  num_claims <- length(unique(dt$claim_id))
  patients_dt <- unique(dt[,.(bene_id)])
  
  ## --------------------------------------------------
  ## 6. Assign length of stay and admission flag (NF and IP only)
  ##    1. Assign the encounter los to the max facility los across the claims within an encounter when the total charge amount is not NA
  ##    2. For IP and NF, count all admissions from discharge year - service date year, otherwise admission and encounter have 1:1 relationship 
  ## --------------------------------------------------
  
  ## Calculate length of stay, and create admission count if inpatient or nursing facility
  if (care %in% c('IP','NF')){
    dt[, service_yr := gsub("-.*", "", service_date)]
    dt[, encounter_los:= max(los[!is.na(los)]), by = 'encounter_id']


    dt[year_dchg<0, year_dchg:=NA]
    dt[is.na(year_dchg) | year_dchg<service_yr, year_dchg:= service_yr]
    dt[, encounter_discharge_yr := max(as.numeric(year_dchg)), by = 'encounter_id']
    dt[, service_yr:= min(as.numeric(service_yr)), by = 'encounter_id']
    dt$service_yr <- as.numeric(dt$service_yr)
    dt[, admission_count:= ifelse(toc %in% c('NF','IP'),
                                  (encounter_discharge_yr-service_yr)+1,
                                  1), by = 'encounter_id'] #plus one because if person was admitted and discharged in the same year we count as admission
    if ('nf_medpar' %in% colnames(dt)){
      dt[nf_medpar ==1, admission_count:=1]
    }
    dt$encounter_discharge_yr <- NULL

    ## If there are Infs or -Infs, reassign to NA
    dt[encounter_los== Inf | encounter_los == -Inf | encounter_los == 'Inf' | encounter_los == '-Inf', encounter_los:= NA]
    
    if (nrow(dt[admission_count<= 0]) >0) {
      stop('Have negative or zero admission count')
    }
    

    ## Check we just have 1 los per encounter
    dt[, n_los:= length(unique(encounter_los)), by = 'encounter_id']
    if (nrow(dt[n_los>1])>0){
      message('Warning, your encounters are not unique on los')
      stop()
    }

    ## Set los to encounter_los
    dt[, los:= encounter_los]
    dt$n_los <- NULL
    dt$service_yr <- NULL

    if ('year_dchg' %in% names(dt)){
      dt$year_dchg <- NULL
    }

  }else{
    dt$admission_count <- 1
  }


  if (source %like% 'MDCR' & care == 'HH'){
    dt[, flag := as.integer(rowid(claim_id) == 1L)]
    dt$CLM_HHA_TOT_VISIT_CNT <- as.numeric(dt$CLM_HHA_TOT_VISIT_CNT)
    dt[CLM_HHA_TOT_VISIT_CNT==0,CLM_HHA_TOT_VISIT_CNT:= 1]
    dt[is.na(CLM_HHA_TOT_VISIT_CNT),CLM_HHA_TOT_VISIT_CNT:= 1]
    dt[, admission_count := sum(CLM_HHA_TOT_VISIT_CNT[flag==1]), by = 'encounter_id']
    dt[admission_count > 365, admission_count:= 365]
    dt$CLM_HHA_TOT_VISIT_CNT <- NULL
    dt$flag <- NULL
  }


  ## --------------------------------------------------
  ## 7. For re-ranking DX later, assign the dollar_amt column to whichever of the tot_pay, tot_charge, or source_pay is not NA within a claim
  ## (Prioritizing tot_pay, then tot_charge, then source_pay)
  ## --------------------------------------------------

  dt[, dollar_amt := ifelse( !is.na(sum(tot_pay_amt[!is.na(tot_pay_amt)])),
                        ( ifelse( !is.na(sum(tot_chg_amt[!is.na(tot_chg_amt)])), source_pay_amt, tot_chg_amt)), tot_pay_amt), by = 'claim_id']

  dt$source_pay_amt <- NULL
  ## --------------------------------------------------
  ## 8. Re-assign primary payer and payer_2 to the encounter if there are multiple pri_payers within an encounter
  ##   - If there is more than 1 pri_payer per encounter, use the pri_payer that is one of the 4 main (ie mdcr,mdcd,priv,oop).
  ## --------------------------------------------------

  dt[, flag_pri_pay := ifelse(length(unique(pri_payer)) >1, 1,0), by = 'encounter_id']
  dt[flag_pri_pay!=1, pri_payer_encounter := pri_payer]

  if (nrow(dt[flag_pri_pay==1]) >0){
    print('You have pri pay conflicts in your zero charge/zero payment data')

    dt[flag_pri_pay==1, pri_payer_encounter := ifelse(length(unique(pri_payer[pri_payer <5])) ==1,
                                                unique(pri_payer[pri_payer %in% c(1:4)]),
                                                1001), by = c('encounter_id')]
    dt[pri_payer_encounter!= 1001, pri_payer := pri_payer_encounter]

    print(paste0('Reassigned pri_payer for ', nrow(dt[flag_pri_pay==1])/nrow(dt), '% of rows'))
  }


  dt$flag_pri_pay <- NULL


  ## ------------------------------------------------------------
  ## 9. Sum spending across total and facility (facility only for MDCR)
  ## - Need to do this before subsetting to only unique DXs otherwise will miss claim $$
  ## ------------------------------------------------------------
  save_dt <- copy(dt)

  # Get all columns for payment and charge
  spend_cols <- dt%>% dplyr::select(contains('_pay_')| contains('_chg_')) %>% names()
  print(spend_cols)

  ## Get first row of claim (it's long on DX so just want to sum the unique vals)
  spend_cols_and_ids <- c(spend_cols, 'claim_id','encounter_id','fac_prof_ind')

  dollar_dt <- unique(dt[,..spend_cols_and_ids])
  dollar_dt[, n_claim := length(unique(claim_id)), by = 'encounter_id'] #number of claims per encounter

  if (length(unique(dt$claim_id)) != length(unique(dollar_dt$claim_id))){
    message(paste0('Full data has ',length(unique(dt$claim_id)), ' unique claims, dollar dt has ',length(unique(dollar_dt$claim_id))))
    message('Warning, you missed some claims in your dollar dt')
    stop()
  }

  ## Sum spend function - sum ignoring NAs, except if all are NA, set to NA
  sum_ignore_na <-  function(x) if (all(is.na(x))) x[NA_integer_] else sum(x, na.rm = TRUE)


  ## Important to do this before summing regular spend cols, otherwise facility spend is duplicated
  ## Sum facility spending
  dollar_dt[fac_prof_ind=='F', paste0(spend_cols,'_facility') := lapply(.SD, sum_ignore_na), by = .(encounter_id), .SDcols = spend_cols]
  # Fill NAs with the group value
  new_cols <- paste0(spend_cols,'_facility')
  dollar_dt[,(new_cols) := lapply(.SD, function(x) unique(x[!is.na(x)])), by = .(encounter_id), .SDcols = new_cols]
  ## If any of the spend cols are negative, set to NA -- this must happen after aggregating to the encounter level in order to account for adjustments edited 8/9/23
  dollar_dt[, paste0(new_cols) := lapply(.SD, function(x){replace(x, which(x<0), NA)}), .SDcols = new_cols]
  dollar_dt[, paste0(spend_cols) := lapply(.SD, sum_ignore_na), by = .(encounter_id), .SDcols = spend_cols]

  ## If any of the spend cols are negative, set to NA -- this must happen after aggregating to the encounter level in order to account for adjustments edited 8/9/23
  dollar_dt[, paste0(spend_cols) := lapply(.SD, function(x){replace(x, which(x<0), NA)}), .SDcols = spend_cols]

  ## Fill in tot_pay_amt if its NA
  pay_cols <- dollar_dt %>% dplyr::select(contains('_pay_') & -contains('_facility')  & -contains('tot')) %>% names()
  if (length(pay_cols) >0){
    dollar_dt[is.na(tot_pay_amt),tot_pay_amt:= rowSums(.SD, na.rm = T), .SDcols = pay_cols]
  }

  ## Fix any encounters where a negative professional claim spend means that tot_pay_amt < tot_pay_amt_facility
  for (i in new_cols){
    print(i)
    non_fac_col <- str_remove(i,'_facility')
    print(non_fac_col)
    dollar_dt[get(i)>get(non_fac_col), (i):=get(non_fac_col)]
  }
  
  ## Merge spending back on to original data
  save_dt <- save_dt[,c(spend_cols):= NULL]
  save_dt <- unique(save_dt)
  
  dt <- merge(save_dt, dollar_dt, all = T)


  ## ------------------------------------------------------------
  ## 9. Order claims by dollar amount, within an encounter. (Uses dollar amount decreasing, then uses dx_level automatically).
  ## If 2 claims have the same diagnosis, keep the row with the highest charge. We only want unique DXs in the final data frame for ordering.
  ## It doesn't matter if the row removed is F or P because the charge and pay sums for both total and facility have already been calculated.
  ## ------------------------------------------------------------

  dt[,all_na:=ifelse(all(is.na(dollar_amt)), 1,0), by=c('encounter_id','dx') ]
  indices <- dt[, .I[which.max(dollar_amt)], by = c('encounter_id','dx')]$V1
  na_indices <- dt[, .I[which(all_na == 1)[1]], by = c('encounter_id','dx')]$V1
  indices <- c(indices,na_indices)
  indices <- indices[complete.cases(indices)]
  dt[,keep:=0]
  dt[indices,keep:=1]
  dt$all_na <- NULL
  print(paste0('Removing ',round(nrow(dt[keep==0])/nrow(dt) *100, digits=2), '% of rows that were duplicate diagnoses'))
  dt[,p:=ifelse('F' %ni% unique(fac_prof_ind),1,0), by = 'encounter_id']
  dt[,f:=ifelse('P' %ni% unique(fac_prof_ind),1,0), by = 'encounter_id']
  dt[,both:=ifelse(length(unique(fac_prof_ind))>1,1,0), by = 'encounter_id']


  ## Get rid of duplicate diagnoses
  dt <- dt[keep==1]
  ## Set new order based on remaining claims
  dt[, dx_level := order(order(dollar_amt, decreasing = T)), by = .(encounter_id)]


  dt[, `:=`(both = NULL, p = NULL, f = NULL)]
  

  ## Get rid of claim_id, DX1, and fac_prof_ind
  dt[,c('claim_id','DX1', 'fac_prof_ind'):= NULL]

  ## If any rows still had pri_payer or payer_2 to be determined, set those to dx_level 1 now.
  dt[, pri_payer_encounter := ifelse(pri_payer_encounter == 1001, pri_payer[dx_level == 1], pri_payer), by = 'encounter_id']
  dt[, pri_payer := pri_payer_encounter]
  dt$pri_payer_encounter <- NULL

  if ('payer_2' %in% colnames(dt)){
    dt[, payer_2 := payer_2[dx_level == 1], by = 'encounter_id']
  }

  dt <- dt[order(encounter_id,dx_level)]

  ## --------------------------------------------------
  ## 10. Computations for checks
  ## --------------------------------------------------
  num_service_lines_out <- nrow(dt)
  num_patients_out <- length(unique(dt[!is.na(bene_id)]$bene_id))
  num_encounter_out <- length(unique(dt$encounter_id))
  num_admissions <- sum(unique(dt[,.(encounter_id,admission_count)])$admission_count,na.rm=T)
  spend_sum <- sum(unique(dt[,.(encounter_id,tot_pay_amt)])$tot_pay_amt,na.rm=T)
  chg_sum <- sum(unique(dt[,.(encounter_id,tot_chg_amt)])$tot_chg_amt,na.rm=T)


  ## MDCR specific checks
  if (source %like% 'MDCR'){

    if ('Y' %in% unique(dt$ENHANCED_FIVE_PERCENT_FLAG) ){
      if ('nf_medpar' %in% unique(dt[ENHANCED_FIVE_PERCENT_FLAG == 'Y']$sub_dataset)){
        stop('You should not have medpar data in the 5% sample for MDCR')
      }
    }

  }

  if ('tot_pay_amt_facility' %in% names(dt)){
    dt[,fac_over_tot:= tot_pay_amt/tot_pay_amt_facility]
    if(nrow(dt[fac_over_tot<1]) >0){
      stop('You have encounters with more facility spend than total spend')
    }
    dt$fac_over_tot <- NULL
  }

  dt[,c('service_date','dollar_amt','keep','n_claim','dx_lvl_use','nf_medpar'):=NULL]

  if(num_service_lines < num_service_lines_out){
    stop('Warning, you have more data coming out than going in')
  }
  if(num_patients != num_patients_out){
    stop('Warning, you have an incorrect number of patients')
  }
  if((num_encounter_out - num_claims) >5 ){
    stop('Warning, you have more encounters than line level claims (>5)')
  }



  ##----------------------------------------------------------------------
  ## 11. Synchronize age for the encounter - take whole number median
  ##----------------------------------------------------------------------  
  dt[,age_group_years_start:= as.numeric(age_group_years_start)]
  dt <- assign_age(dt)


  ## ----------------------------------------------------------------------
  ## 12. Clean up and write out
  ## ----------------------------------------------------------------------
  keep_names <- names(dt)
  dt <- dt[,..keep_names]
  if (!interactive()){


    if (care == 'AM'){
      basename <- paste0("orig_toc_",care,"orig_age",age,"-{i}.parquet")
      base <- paste0(care,'_',yr,'_',age,'_',sex,'_',st)
    }else{
      basename <- paste0("orig_toc_",care,"-{i}.parquet")
      base <- paste0(care,'_',yr,'_',st,'_',sex)
    }

    arrow::write_dataset(dt, path = outdir,
                         basename_template = basename,
                         partitioning = c("toc","year_id","st_resi", "sex_id","age_group_years_start"))

  }

  ## ----------------------------------------------------------------
  ## 13. Check that the output is unique
  ## ----------------------------------------------------------------

  unique_cols <- c('encounter_id', 'toc', 'bene_id','pri_payer','admission_count',spend_cols)
  
  if ('race_cd_imp' %in% colnames(dt)){
    unique_cols1 <- c(unique_cols, 'race_cd_imp')
    unique_cols2 <- c(unique_cols, 'race_cd_raw')
    unique_cols <- unique_cols1
  }
  
  if (care %in% c('IP','NF')){
    unique_cols <- c(unique_cols, 'los')
  }
  if ('mc_ind' %in% colnames(dt)){
    unique_cols <- c(unique_cols, 'mc_ind')
  }

  '%ni%' <- Negate('%in%')
  if (source == 'KYTHERA' & 'mc_ind' %ni% unique_cols){
    message('Warning, mc_ind not in Kythera data')
    stop()
  }

  test <- dt[,..unique_cols]

  test <- unique(test)
  test[,n:=.N, by = 'encounter_id']
  max(test$n)

  if (max(test$n)>1){
    message('Warning, your data is not unique on encounter-toc-person-pripayer-los-raceimp-spendcols')
    print(head(test[n>1]))
    stop()
  }
  
  if ('race_cd_imp' %in% colnames(dt)){
    test2 <- dt[,..unique_cols2]
    
    test2 <- unique(test2)
    test2[,n:=.N, by = 'encounter_id']
    max(test2$n)
    
    if (max(test2$n)>1){
      message('Warning, your data is not unique on encounter-toc-person-pripayer-los-raceraw-spendcols')
      print(head(test2[n>1]))
      stop()
    }
  }

  if (nrow(dt[admission_count<0])>0){
    message('Warning, you have admission count values less than 0')
    print(dt[admission_count<0])
    stop()
  }
  
  
}else{
  print('No data')
  message('No data')
}

print(Sys.time() - t0)
print('Done')



