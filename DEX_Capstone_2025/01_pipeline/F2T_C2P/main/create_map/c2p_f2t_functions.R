

mdcr_yrs <- c(2000, 2010, 2014:2016, 2019) ## ONLY CAN USE THESE YEARS BECAUSE ITS THE 5% SAMPLE WITH BOTH FACILITY AND PROF CLAIMS
chia_mdcr_yrs <- c(2015:2019,2021,2022)
kythera_yrs <- c(2016:2022)
mdcd_yrs <- c(2000,2010,2014,2016,2019) ##-- TAF years need F2T applied for ED only 1/30/24
mscan_yrs <- c(2010:2022)
meps_yrs <- c(2000:2021)



pull_f2t_data <- function(data, s, pp_num, test, causes){
  
  yrs <- get(paste0(tolower(s),'_yrs'))

  if (s == 'MDCR' & care == 'NF'){
    yrs <- 2019
  }
  if (s == 'MDCR' & care == 'ED'){
    yrs <- c(2000, 2010, 2015:2016, 2019)
  }
  if (s == 'MDCD' & care == 'ED'){  ##-- TAF years need F2T applied for ED only 
    yrs <- c(2000,2010,2014) 
  }
  print(paste0('Getting ',s))
  print(yrs)
  
  select_cols <- data %>% dplyr::select( contains('_pay_') | contains('_chg_') ) %>% names()
  select_cols <- c(select_cols, 'encounter_id', 'acause', 'year_id','primary_cause','pri_payer')
  
  if (s == 'MEPS'){
    mypipe <-. %>%
      filter(acause %in% causes) %>%
      filter(pri_payer %in% pp_num) 

  }
  if (s == 'MSCAN'){
    mypipe <-. %>%
      filter(acause %in% causes) %>%
      filter(pri_payer %in% pp_num) %>%
      filter(primary_cause == 1)
  }
  
  if (s == 'MDCD'){
    select_cols <- c(select_cols,'mc_ind')
    mypipe <-. %>%
      filter(acause %in% causes) %>%
      filter(pri_payer %in% pp_num) %>%
      filter(primary_cause == 1)
  }
  if (s == 'MDCR'){
    select_cols <- c(select_cols,'mc_ind','ENHANCED_FIVE_PERCENT_FLAG')
    mypipe <-. %>%
      filter(acause %in% causes) %>%
      filter(pri_payer %in% pp_num) %>%
      filter(primary_cause == 1) %>%
      filter(ENHANCED_FIVE_PERCENT_FLAG == 'Y')
  }

  if(test==T){
    filtered <- data.table()
    for (yr in yrs){
      yr_filtered <- data %>% filter(year_id == yr) %>% mypipe %>% head(1000L)%>% dplyr::select(all_of(select_cols))%>%as.data.table() 
      filtered <- rbind(filtered, yr_filtered)
    }
  }else{
    filtered <- data %>% mypipe %>% dplyr::select(all_of(select_cols))%>%as.data.table() 
  }
    
  if (s == 'MEPS'){
    filtered <- filtered[primary_cause == '1'] ## MEPS doesn't need primary cause or C2E, thus just doing this for column consistency
    pay_cols <- filtered %>% dplyr::select( contains('_pay_') & contains('_fac')| contains('_chg_') & contains('_fac') ) %>% names()
    filtered[, paste0(pay_cols) := lapply(.SD, function(x){replace(x, which(x==1), NA)}), .SDcols = pay_cols]
    ## drop rows where facility info is unknown
    filtered <- filtered[!(is.na(tot_fac_chg_amt))]
    
  }
  if (s %in% c('MDCR','MDCD')){
    filtered <- filtered[mc_ind == 0] ## Do not want managed care data to inform ratios
  }
  
  
  return(filtered)
  
}

pull_c2p_data <- function(data, s, pp_num, test, causes){
  

  yrs <- get(paste0(tolower(s),'_yrs'))
  
  print(paste0('Getting ',s))
  print(yrs)
  
  select_cols <- data %>% dplyr::select((contains('_pay_') & -contains('_facility')| contains('_chg_') & -contains('_facility')) ) %>% names()
  select_cols <- c(select_cols, 'encounter_id', 'acause', 'year_id','primary_cause')

  if (s == 'MEPS'){
    mypipe <-. %>%
      filter(pri_payer == pp_num) %>%
      filter(acause %in% causes)
  }
  if (s == 'KYTHERA'){
    mypipe <-. %>%
      filter(pri_payer == pp_num) %>%
      filter(acause %in% causes) %>%
      filter(primary_cause == 1)
  }
  
  if (s == 'MDCD' | s == 'CHIA_MDCR'){
    select_cols <- c(select_cols,'mc_ind')
    mypipe <-. %>%
      filter(pri_payer == pp_num) %>%
      filter(acause %in% causes) %>%
      filter(primary_cause == 1)
  }
  if (s == 'MDCR'){
    select_cols <- c(select_cols,'mc_ind','ENHANCED_FIVE_PERCENT_FLAG')
    mypipe <-. %>%
      filter(pri_payer == pp_num) %>%
      filter(acause %in% causes) %>%
      filter(primary_cause == 1) %>%
      filter(ENHANCED_FIVE_PERCENT_FLAG == 'Y')
  }
  
  if(test==T){
    filtered <- data.table()
    for (yr in yrs){
      yr_filtered <- data %>% filter(year_id == yr) %>% mypipe %>% head(1000L)%>% dplyr::select(all_of(select_cols))%>%as.data.table() 
      filtered <- rbind(filtered, yr_filtered)
    }
  }else{
    filtered <- data %>% mypipe %>% dplyr::select(all_of(select_cols))%>%as.data.table() 
  }
  
  if (s == 'MEPS'){
    filtered <- filtered[primary_cause == '1'] ## MEPS doesn't need primary cause or C2E, thus just doing this for column consistency
  }
  
  if (s %in% c('MDCR','MDCD', 'CHIA_MDCR')){
    filtered <- filtered[mc_ind == 0] ## Do not want managed care data to inform ratios
    filtered$mc_ind <- NULL
  }
  
  ## If Kythera, subset to only where we have both charge and payment
  if (s == 'KYTHERA'){
    filtered <- filtered[year_id %in% kythera_yrs]
    filtered <- filtered[!is.na(tot_pay_amt) & !is.na(tot_chg_amt)]
  }

  return(filtered)
  
}



