library(zoo)
'%ni%' <- Negate('%in%')


pull_data <- function(indir, care, yr, st){
  message('Pulling data')

  new_indir <- FILEPATH
  
  
  
  if (dir.exists(new_indir)){
    schema <- update_nulls_schema(new_indir)
    schema[["claim_id"]] <- arrow::string()
    schema[["discharge_date"]] <- arrow::string()
    schema[["service_date"]] <- arrow::string()
    dt <- arrow::open_dataset(new_indir, schema = schema)
    
    
    
    if (care == 'AM'){
      dt <- dt %>% 
        filter(sex_id ==sex & age_group_years_start==age) %>%
        as.data.table()
      dt <- dt %>% mutate(toc=care,year_id=yr,age_group_years_start=age,st_resi=st)%>%as.data.table()
    }else{
      dt <- dt %>% 
        filter(sex_id ==sex) %>%
        as.data.table()
      dt <- dt%>% mutate(toc=care,year_id=yr,st_resi=st)%>%as.data.table()
    }
    
    message('Got data')
    if (nrow(dt)<1){
      dt <- NULL
      message(paste0('No data for',new_indir))
    }else{
      
      if (source == 'MDCR' & care == 'NF'){
        dt <- dt[!(sub_dataset == 'hosp')]
      }
      
      ## ensure data is unique
      if (nrow(dt) != nrow(unique(dt))){
        stop('Data is not unique coming out of causemap!')
      }
      
    }
    
  }else{
    dt <- NULL
    message(paste0('No data for',new_indir))
  }
  
  return(dt)
  
}

clean_data <- function(dt){
  
  if ('CLM_FROM_DT' %in% names(dt)){ #we try to prioritize the actual admission date (which gets recoded to "service date") but when we dont have that we use clm_from_dt
    dt[,service_date:=as.character(service_date)]
    dt[service_date == 'NaT', service_date:=NA]
    dt[is.na(service_date) & !is.na(CLM_FROM_DT), service_date:=CLM_FROM_DT]
  }
  if ('discharge_date' %ni% names(dt)){
    dt[,discharge_date:=as.Date(service_date)+los]
  }

  if ('los' %ni% names(dt)){
    dt[,los:= as.Date(discharge_date) - as.Date(service_date)]
  }
  dt[los<0 | is.na(los),los:=ifelse(toc %in% c('IP','NF'), 1,0)] ## assigning negative or NA los values to 1 if its IP or NF and 0 if its all other TOCs

  dt[is.na(discharge_date), discharge_date:=as.Date(service_date)+los]
  dt[, service_date := gsub(".*/", "-", service_date)] ## in case any data are in the xx/xx/xxxx format instead of xx-xx-xxxx
  dt[, discharge_date := gsub(".*/", "-", discharge_date)] ## in case any data are in the xx/xx/xxxx format instead of xx-xx-xxxx
  dt[, service_yr := gsub("-.*", "", service_date)]

  
  if ('tot_pay_amt' %ni% names(dt)){
    dt[,tot_pay_amt:=NA]
  }
  return(dt)
}

make_dx1 <- function(data){
  message('Assigning DX1')
  dt <- as.data.table(data)
  
  dt[, dx_level := as.numeric(sub(".*_", "", dx_level))]
  dt[, dx_lvl_use := min(dx_level), by = c('claim_id')]
  dt[, DX1 := ifelse(unique(acause[dx_level == dx_lvl_use])[1] %notlike% '_gc', 
                     unique(acause[dx_level == dx_lvl_use])[1], 
                     unique(acause[dx_level == dx_lvl_use])[2]), by = c('claim_id')]
  dt[is.na(DX1), DX1 := unique(acause[dx_level == dx_lvl_use]), by = c('claim_id')]
  
  
  if (nrow(dt[is.na(DX1)])>0){
    message('Warning, you have rows not assigned a primary cause for encounter grouping')
    stop()
  }
  
  return(dt)
  
}


dedupe_admission <- function(data, buffer){ 
  ## Only relevant for IP and NF (and ED when getting grouped with IP)
  ## Resolve differences in length of stay
  ## Implement a buffer of 1 day on either side of the admission window.For example:
  ## Synchronize claims that overlap in dates to have the same min admission date and max discharge date

  dedupe_dt <- unique(data[,.(service_date,los,bene_id)])
  
  dedupe_dt[,n_dates:=.N, by = 'bene_id']
  
  if (buffer == T){
    dedupe_dt[,service_date:=as.Date(service_date)]
    dedupe_dt[,start_date:=service_date-1]
    dedupe_dt[,end_date:=service_date+los+1]
  }else{
    dedupe_dt[,service_date:=as.Date(service_date)]
    dedupe_dt[,start_date:=service_date]
    dedupe_dt[,end_date:=service_date+los]
  }

  
  # order the data by bene and then by start date so that date checking works with shift
  dedupe_dt <- dedupe_dt[order(bene_id,start_date)]
  
  # check if the start date was before or equal to the previous claim's end date
  dedupe_dt <- dedupe_dt[, in_range := start_date <= shift(end_date), by = 'bene_id']
  
  # if the claim service date is the first claim for that bene (in_range = NA), or its not in the time frame of the previous claim (in range = F),
  # then keep that service_date! otherwise, set to NA
  dedupe_dt[, new_service_date:=ifelse( in_range == FALSE | is.na(in_range), service_date, NA)] 
  
  dedupe_dt[, new_service_date := zoo::na.locf(new_service_date), by = 'bene_id'] #use the previous non-NA new service date, these will now be grouped together
  dedupe_dt[, new_service_date := as.Date(new_service_date)]
  if (buffer == T){
    dedupe_dt[, new_discharge_date := max(end_date)-1, by = c('bene_id','new_service_date')] # subtract 1 from end date because we added one to end date for the buffer
  }else{
    dedupe_dt[, new_discharge_date := max(end_date), by = c('bene_id','new_service_date')]
  }
  dedupe_dt[, new_los:= as.numeric(new_discharge_date - new_service_date)]
  dedupe_dt[, `:=` (service_date =as.character(service_date),
               new_service_date=as.character(new_service_date),
               new_discharge_date=as.character(new_discharge_date))]
  merged <- merge(dedupe_dt, data, all=T,by = c('los','bene_id','service_date'))
  if (nrow(merged) != nrow(data) ){
    stop('Admission dedupe dropped or added data')
  }
  

  merged[, assigned_new:=ifelse( service_date==new_service_date, 0, 1)]
  merged[,`:=`(service_date=NULL, los = NULL,n_dates = NULL, start_date = NULL, end_date=NULL, in_range = NULL, discharge_date = NULL)]
  setnames(merged, c('new_service_date','new_discharge_date','new_los'),c('service_date','discharge_date','los'))
  reassigned_adm_date <<- length(unique(merged[assigned_new==1]$claim_id))
  message('Reassigned ', round(reassigned_adm_date/length(unique(merged$claim_id)), digits = 2)*100, '% claims to a different admission date to account for overlapping dates')
  
  return(merged)
  
}


merge_prof_data <- function(facility_data, prof_data){
  message('Merging professional data')
  tot <- nrow(facility_data) + nrow(prof_data)
  ## ------------------------------------------------------------
  ## 1. Get the unique bene+date combos in the P claims and get the unique bene+date combos in the F claims
  ## -----------------------------------------------------------
  pd <- unique(prof_data[,.(bene_id,service_date)])[order(service_date)]
  setnames(pd, 'service_date','prof_service_date')

  pd[,bene_date:=paste0('date_',1:.N), by = 'bene_id']
  
  if (care %in% c('NF','IP')){
    if ('nf_medpar' %in% unique(facility_data$sub_dataset) ){
      fd_medpar <- facility_data[sub_dataset == 'nf_medpar']
      facility_data <- facility_data[sub_dataset!='nf_medpar']
    }
    fd <- unique(facility_data[,.(bene_id,los,service_date)])
    
  }else{
    fd <- unique(facility_data[,.(bene_id,service_date)])
  }
  ##----------------------------------------------------------------------
  ## 2. Merge the F and P claims on bene_id, allowing cartesian because we want to check if any P claim will fall in any F claim admission
  ##----------------------------------------------------------------------  

  fp_dt <- merge(fd,pd, by = 'bene_id', all=T,allow.cartesian = T)
  fp_dt[,service_date:=as.Date(service_date)][,prof_service_date:=as.Date(prof_service_date)]
  
  ##----------------------------------------------------------------------
  ## 3. Drop the claims that don't have a matching bene in both F and P
  ##----------------------------------------------------------------------  

  fp_dt <- fp_dt[!is.na(service_date) & !is.na(prof_service_date)]
  
  ##----------------------------------------------------------------------
  ## 4. Index on the facility service date and create a start/end window and check if the P claim is in that window
  ##----------------------------------------------------------------------  
  group_cols <- c('bene_id','service_date')
  if (nrow(fp_dt)==0){
    message('No matching benes in F and P')
    p_deduped <- dedupe_admission(data = copy(prof_data), buffer=F)
    both <- rbind(p_deduped,facility_data,fill=T)
    both[, temp_id := .GRP, by = group_cols]
    last_grp <- max(both$temp_id)
  }else{
    if (care %in% c('NF','IP')){
      fp_dt[,start_window:=service_date-1]
      fp_dt[,end_window:=service_date+los+1]
    }else if (care == 'ED'){
      fp_dt[,start_window:=service_date-1]
      fp_dt[,end_window:=service_date+1]
    }else{
      fp_dt[,start_window:=service_date]
      fp_dt[,end_window:=service_date]
    }
  
    date_bins <- fp_dt %>%
      mutate(in_window = case_when(
        prof_service_date >= start_window & prof_service_date <= end_window ~ 1,TRUE ~ 0
      )) %>% as.data.table()
    
    ##----------------------------------------------------------------------
    ## 5. Check if the professional service date is in the window of multiple facility admission periods
    ##----------------------------------------------------------------------  
    date_bins[,sum_matches:=sum(in_window), by = c('bene_id','prof_service_date')]
    date_bins[,in_multiple:=ifelse(sum_matches>1, 1,0)]
    if (nrow(date_bins[in_multiple==1]) > 0 ){
      
      message('You have a professional service date matching to more than one facility admission,
              reassigning P claim to F claim with most recent discharge')
      date_bin_fix <- date_bins[in_multiple==1 & in_window == 1]
      date_bins <- date_bins[!(in_multiple == 1 & in_window == 1)]
      
      date_bin_fix[,in_window:=ifelse(end_window == max(end_window), 1, 0), by = c('bene_id','prof_service_date') ]
      date_bin_fix[,sum_matches:=sum(in_window), by = c('bene_id','prof_service_date')]
      date_bin_fix[,in_multiple:=ifelse(sum_matches>1, 1,0)]
      
      date_bins <- rbind(date_bins, date_bin_fix[!(in_multiple == 1 & in_window == 1)])
      date_bin_fix <- date_bin_fix[in_multiple==1 & in_window == 1]
      
      if (nrow(date_bin_fix)> 0 ){
        message('You still have a P claim matching to more than F admission,
                reassigning P claim to F claim with earliest admission')
        date_bin_fix[, in_window:=ifelse(start_window == min(start_window), 1, 0), by = c('bene_id','prof_service_date') ]
        date_bin_fix[,sum_matches:=sum(in_window), by = c('bene_id','prof_service_date')]
        date_bin_fix[,in_multiple:=ifelse(sum_matches>1, 1,0)]
        date_bins <- rbind(date_bins, date_bin_fix[!(in_multiple == 1 & in_window == 1)])
  
        if (nrow(date_bin_fix[in_multiple==1]) > 0 ){
          stop('Reassigning to most recent end window did not work; still have P claim matching to more than F admission')
        }
      }
      
      
    }
    date_bins[,service_date:=as.character(service_date)][,prof_service_date:=as.character(prof_service_date)]
  
    ##----------------------------------------------------------------------
    ## 6. Assign a temp_id to the claims that are "in the window", grouping by bene_id and service_date
    ##----------------------------------------------------------------------  
    
    #keep only the claims that have a P claim in F claim admission 
    grouped <- date_bins[in_window==1] 
    
    if (nrow(grouped)>0){
      # Grouping columns and assigning a temporary unique id (nf-medpar has to be done separately because it is missing bene_id)
      grouped[, temp_id := .GRP, by = group_cols]
      last_grp <- max(grouped$temp_id)
      
      ## merge the grouped F claims back onto the ungrouped F claims
      if (care %in% c('NF','IP')){
        grouped_f <- unique(grouped[,.(bene_id,service_date,los,temp_id)]) 
        all_f <- merge(grouped_f, facility_data, by = c('bene_id','service_date','los'), all = T)
      }else{
        grouped_f <- unique(grouped[,.(bene_id,service_date,temp_id)]) 
        all_f <- merge(grouped_f, facility_data, by = c('bene_id','service_date'), all = T)
      }
      
      ## pull out the P claims that have a matching F claim and merge back onto full P claim table (prof_data)
      grouped_p <- unique(grouped[,.(bene_id,bene_date,prof_service_date,temp_id)])
      setnames(grouped_p,'prof_service_date','service_date')
      all_p <- merge(grouped_p, prof_data, by = c('bene_id','service_date'), all = T)
      all_p[,nf_medpar:=0] # we will never have nf_medpar in the P data because nf_medpar does not have bene_id, thus we'd never be able to find matching P claims
      all_p[,bene_date:=NULL]
      ##----------------------------------------------------------------------
      ## 7. Dedupe the P claims (testing no buffer to start with, may need to add buffer later)
      ##----------------------------------------------------------------------  
      p_deduped <- dedupe_admission(data = copy(all_p), buffer=F)
      
  
      ##----------------------------------------------------------------------
      ## 8. Assign temp ids to the remaining ungrouped claims
      ##----------------------------------------------------------------------  
      both <- rbind(all_f, p_deduped, fill = T)
      
      if (nrow(both[is.na(temp_id)]) >0 ){
        both[is.na(temp_id), temp_id:=.GRP+last_grp, by = group_cols]
        last_grp <- max(both$temp_id)
      }
      
      message('Prof data merged')
    }else{
      p_deduped <- dedupe_admission(data = copy(prof_data), buffer=F)
      both <- rbind(p_deduped,facility_data,fill=T)
      both[, temp_id := .GRP, by = group_cols]
      last_grp <- max(both$temp_id)
      
      message('No P claims fell in the F claim window')
      
    }
  }
  
  if (exists('fd_medpar')==T ){
    fd_medpar[,temp_id:=.GRP+last_grp, by = 'claim_id']
    both <- rbind(both,fd_medpar)
  }
  both[,nf_medpar:=ifelse(sub_dataset == 'nf_medpar', 1, 0)] 
  
  if (nrow(both) != tot ){
    stop('You dropped or added some data in the carrier merge')
  }
  
  if (nrow(both[is.na(temp_id)]) >0){
    stop('Some claims not assigned a temp id')
  }
  return(both)
}


assign_age <- function(dt){
  dt[,age_group_years_start_new:= round(median(age_group_years_start)/5)*5, by = 'encounter_id']

  dt[,age_group_years_start_new:= ifelse(min(age_group_years_start) ==1 &
                                       min(age_group_years_start_new) == 0, 1, age_group_years_start_new), by = 'encounter_id']
  dt$age_group_years_start <- NULL
  setnames(dt, 'age_group_years_start_new', 'age_group_years_start')
  

  return(dt)
}


## Synchronize race
sync_race <- function(orig_data, id_var, race_var){
  
  
  mult_race_data <- orig_data %>% 
    group_by(get(id_var)) %>%
    mutate(var = get(race_var)) %>%
    summarise(n_race = n_distinct(var)) %>%
    filter(n_race>1) %>% collect() %>% as.data.table()
  
  if (mult_race_data[,.N] >0){
    ids <- mult_race_data$`get(id_var)` # the bene_ids, or the encounter_ids
    
    sync_data <- orig_data %>% 
      filter(get(id_var) %in% ids) %>%
      group_by(bene_id,get(race_var)) %>%
      summarise(race_count = n()) %>% collect %>% as.data.table()
    
    setnames(sync_data, 'get(race_var)', 'race_cd_synced')
    
    ## 0. Deal with the benes that have only UNK/OTH
    sync_data[race_cd_synced == 'OTH', race_cd_synced := 'UNK']
    sync_data[race_cd_synced == 'UNK', race_count:=0]
    sync_data[,only_unk:= length(unique(race_cd_synced)), by = list(get(id_var))]
    synced0 <- sync_data[only_unk==1]
    sync_data <- sync_data[only_unk>1]
    
    ## 1. Determine race_cd_synced that is not UNK with highest count 
    sync_data[, max_race:=ifelse(race_count == max(race_count) & race_cd_synced != 'UNK', 1, 0), by = list(get(id_var))]
    sync_data[, num_race:=length(unique(race_cd_synced[max_race==1])), by = list(get(id_var))]
    
    synced <- sync_data[num_race==1]
    synced[, race_cd_synced:=race_cd_synced[max_race==1], by = list(get(id_var))]
    
    sync_data <- sync_data[num_race>1]
    
    ## 2. Prioritize HISP
    sync_data[, has_hisp:= ifelse('HISP' %in% unique(race_cd_synced), 1, 0), by = list(get(id_var))]
    synced2 <- sync_data[has_hisp==1]
    synced2[,race_cd_synced:='HISP']
    
    sync_data <- sync_data[has_hisp==0]
    
    ## 3. Randomly select one from the non UNK or OTH races
    if (sync_data[,.N]>0){
      sync_data[, sampled:=sample(race_cd_synced[max_race==1],1), by = list(get(id_var))]
      sync_data[,race_cd_synced:=sampled]
    }
    
    
    ## 4. Clean and append
    fully_synced <- rbind(synced0, synced, synced2, sync_data, fill = T)
    fully_synced[,`:=`(race_count = NULL,
                       max_race= NULL,
                       num_race = NULL,
                       has_hisp = NULL,
                       sampled = NULL,
                       only_unk = NULL)]
    fully_synced <- unique(fully_synced)
    fully_synced[,n:=.N, by = list(get(id_var))]
    if ( max(fully_synced$n) >1 ){
      stop('Error, did not fully sync race')
    }
    fully_synced$n <- NULL
    
    orig_data <- orig_data %>%
      left_join(fully_synced) %>% mutate(!!race_var := 
                                           ifelse(is.na(race_cd_synced), get(race_var), race_cd_synced))
  }
  
  return(orig_data)
}