
library(tidyr)
get_job_params <- function(sources){  
  
  metadata <- data.table()
  
  mdcr_carrier_years <- c(2000,2010,2014:2016,2019)
  chia_mdcr_carrier_years <- c(2015:2022) 
  for (source in sources){
    
    print(source)
    indir <- paste0(c2e_config$data_input_dir[[source]],'data/')
    outdir <- paste0(c2e_config$data_output_dir[[source]],'data/')
    diagnostics_outdir <- paste0(c2e_config$data_output_dir[[source]],'diagnostics/')

    
    if (source == 'MSCAN' | source == 'KYTHERA' ){
      tocs <- str_extract(list.files(indir),'\\b\\w+$')
      tocs <- tocs[!tocs %in% c('ED')] # Removing ED because it will read in ED along with IP in order to combine across these ToCs for ED to IP admits
      
    }
    if (source == 'MDCR' | source == 'CHIA_MDCR'){
      tocs <- str_extract(list.files(paste0(indir,'/carrier=false/')),'\\b\\w+$')
      tocs <- tocs[!tocs %in% c('DV','RX')]
      indir <- paste0(indir, 'carrier=false/')
    }
    if (source == 'MDCD'){
      tocs <- str_extract(list.files(indir),'\\b\\w+$')
      tocs <- tocs[!tocs %in% c('DV','RX')]
    }
  
    source_meta <- data.table()
    
    for (toc in tocs){
      
      years <- str_extract(list.files(paste0(indir,'toc=',toc,'/')), '\\b\\w+$')
      
      yr_dt <- data.table()
      
      for (year in years){

          sexes <- c('1','2')
          
          if (source == 'CHIA_MDCR'){
            states <- 'MA'
          }else{
            states <- c("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", 
                        "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", 
                        "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", 
                        "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
                        "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY","-1")
          }
          
          yr_toc_dt <- crossing(name = source,
                                indir = indir,
                                outdir = outdir,
                                diagnostics_outdir =diagnostics_outdir,
                                toc = as.character(toc),
                                year = as.numeric(year),
                                sex = sexes,
                                state = states)
          if (toc == 'AM'){
            ages <- as.character(c(0,1,5*1:17))
            yr_toc_dt <- crossing(yr_toc_dt, age = ages)
          }
          
        yr_dt <- rbind(yr_dt,yr_toc_dt, fill= T)
      }
      source_meta <- rbind(source_meta,yr_dt, fill= T)
    }
    metadata <- rbind(metadata, source_meta, fill = T)
  }
  
  metadata[,carrier_indir := ifelse(name == 'MDCR' & year %in% mdcr_carrier_years, gsub('false','true',indir), NA)]
  metadata[,carrier_indir := ifelse(name == 'CHIA_MDCR' & year %in% chia_mdcr_carrier_years, gsub('false','true',indir), carrier_indir)]
  
  ##Removing years 1999-2001 for MDCR toc =NF because this data is unusable in current form
  metadata <- metadata[!(name=='MDCR' & toc=='NF' & year %in% c(1999:2001))]
  print("Got params")
  return(metadata)
}

pull_relaunch <- function(dir,job_id,usesrname){
  
  job_tasks <- fread(paste0(dir, job_id,'.csv'))
  jid <- job_id
  
  # Pull task ids of the jid that failed due to memory, timeout, failing, or cancelled
  mem <-  str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ", username," --state=OUT_OF_MEMORY"), intern = T))), ".+[:space:]") %>% as.numeric()
  time <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ", username," --state=TIMEOUT"), intern = T))), ".+[:space:]") %>% as.numeric()
  failed <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ", username," --state=FAILED"), intern = T))), ".+[:space:]") %>% as.numeric()
  cancelled <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ", username," --state=CANCELLED"), intern = T))), ".+[:space:]") %>% as.numeric()
  cancelled <- cancelled[!is.na(cancelled)]
  # pending <- (max(cancelled)+1):nrow(job_tasks)
  
  # Find those task ids in the job task list
  oom_tasks <- job_tasks[mem][,relaunch:='oom']
  oot_tasks <- job_tasks[time][,relaunch:='oot']
  failed_tasks <- job_tasks[failed][,relaunch:='failed']
  cancelled_tasks <- job_tasks[cancelled][,relaunch:='cancelled']
  # pending_tasks <- job_tasks[pending][,relaunch:='pending']
  
  
  relaunch <- rbind(oom_tasks,oot_tasks,failed_tasks,cancelled_tasks) #,pending_tasks)
  
  return(relaunch)
}

