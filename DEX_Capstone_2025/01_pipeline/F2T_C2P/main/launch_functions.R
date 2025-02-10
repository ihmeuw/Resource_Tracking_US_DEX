# Create payer combos with sources using restrictions file
create_pay_combos <- function(map, sources, path, tocs){
  
  expected <- fread(path)
  expected <- expected[include==1]
  expected[,pri_payer:=NULL]
  setnames(expected, 'pri_payer_pre_collapse','pri_payer')
  expected <- unique(expected[toc %in% tocs,.(toc,pri_payer,payer,include)])
  
  ## All possible payer combinations for each data source
  if ('MDCR' %in% sources){
    mdcr_pay_combos <- data.table(data_source = 'MDCR', 
                                  pri_payer = c('mdcr','mdcr','mdcr','priv'),
                                  payer = c('mdcr','oop','priv','mdcr'))
  }else{
    mdcr_pay_combos <- data.table()
  }
  
  if ('CHIA_MDCR' %in% sources){
    chia_mdcr_pay_combos <- data.table(data_source = 'CHIA_MDCR', 
                                  pri_payer = c('mdcr','mdcr','mdcr','priv'),
                                  payer = c('mdcr','oop','priv','mdcr'))
  }else{
    chia_mdcr_pay_combos <- data.table()
  }
  
  if ('MDCD' %in% sources){
    mdcd_pay_combos <- data.table(data_source = 'MDCD', 
                                  pri_payer = c('mdcd','mdcd','mdcr','priv'),
                                  payer = c('mdcd','oop','mdcd','mdcd'))
  }else{
    mdcd_pay_combos <- data.table()
  }
  
  if ('MSCAN' %in% sources){
    mscan_pay_combos <- data.table(data_source = 'MSCAN',
                                   pri_payer = c('mdcr','mdcr','mdcr','priv','priv'),
                                   payer = c('mdcr','priv','oop','priv','oop'))
  }else{
    mscan_pay_combos <- data.table()
  }
  
  if ('KYTHERA' %in% sources){
    kythera_pay_combos <- data.table(data_source = 'KYTHERA',
                                     pri_payer = c('mdcr','mdcd','priv'),
                                     payer = c('mdcr','mdcd','priv'))
  }else{
    kythera_pay_combos <- data.table()
  }
  
  if ('MEPS' %in% sources){
    meps_pay_combos <- data.table(data_source = 'MEPS', 
                                  pri_payer = c('mdcr','mdcd','priv','mdcr','priv','oop'),
                                  payer = c('oop','oop','oop','priv','priv','oop'))
  }else{
    meps_pay_combos <- data.table()
  }
  
  
  source_pay_combos <- rbind(mdcr_pay_combos,chia_mdcr_pay_combos, mdcd_pay_combos,kythera_pay_combos, meps_pay_combos,mscan_pay_combos)
  
  spc <- dcast(source_pay_combos, ...~data_source, value.var = 'data_source')
  
  ## Ensure they're valid - confirm with restrictions file
  valid_combos <- expected
  valid_combos <- merge(valid_combos, spc, by = c('pri_payer','payer'), all.y = T)
  valid_combos <- valid_combos[!is.na(include)]
  
  if (map == 'f2t'){
    ## MEPS doesn't have facility information for types of care besides IP, ED and sometimes AM (so need to filter to only numerics)
    valid_combos[toc == 'NF', MEPS := '']
    valid_combos[toc == 'HH', MEPS := '']
    valid_combos[is.na(MDCR), MDCR:= '']
    valid_combos[is.na(CHIA_MDCR), CHIA_MDCR:= '']
    valid_combos[is.na(MSCAN), MSCAN:= '']
    valid_combos[is.na(MEPS), MEPS:= '']
    valid_combos[is.na(MDCD), MDCD:= '']
    valid_combos <- valid_combos[!(CHIA_MDCR == '' & MDCR =='' & MEPS == '' & MSCAN == '' & MDCD == '')]
  }
  
  return(valid_combos)
}




##F2T
get_f2t_apply_params <- function(f2t_targets){
  cl <- level_causelist[!(acause %like% '_NEC') & !(acause %like% '_gc')][,.(acause,family)]
  setnames(cl, c('acause','family'),c('c','f'))
  
  f2t_apply_params <- data.table()
  for (i in f2t_targets){
    
    print(i)
    indir <- paste0(f2t_config$data_input_dir[[i]],'data/')
    outdir <- paste0(f2t_config$data_output_dir[[i]],'data/')
    tocs <- str_extract(list.files(indir), '\\b\\w+$')
    for (toc in tocs){
      years <-  str_extract(list.files(paste0(indir,'/toc=',toc,'/')), '\\b\\w+$')
      target_meta <- expand.grid(target = i,
                                 indir = indir,
                                 outdir = outdir,
                                 care = toc,
                                 yr = as.numeric(years),
                                 f = families,
                                 map_version = f2t_config$map_output_dir,
                                 cause_map_path = cause_map_path)
      
      level_causelist <- fread(cause_map_path)[,.(acause,family,cause_name)]
      
      f2t_apply_params <- rbind(f2t_apply_params, target_meta, fill = T)
    }
    
    
  }
  
  return(f2t_apply_params)
}


##C2P
get_c2p_apply_params <- function(c2p_targets){
  c2p_apply_params <- data.table()
  for (i in c2p_targets){
    
    print(i)
    indir <- paste0(c2p_config$data_input_dir[[i]],'data/')
    outdir <- paste0(c2p_config$data_output_dir[[i]],'data/')
    tocs <- str_extract(list.files(indir), '\\b\\w+$')
    years <-  str_extract(list.files(paste0(indir,'/toc=',tocs[1],'/')), '\\b\\w+$')
    target_meta <- expand.grid(target = i,
                               indir = indir,
                               outdir = outdir,
                               care = as.character(tocs),
                               yr = as.numeric(years),
                               map_version = c2p_config$map_output_dir,
                               cause_map_path = cause_map_path)
    c2p_apply_params <- rbind(c2p_apply_params, target_meta, fill = T)
  }
  
  
  return(c2p_apply_params)
}



## Pull relaunch params
pull_relaunch <- function(dir,job_id){
  
  job_tasks <- fread(paste0(dir, job_id,'.csv'))
  jid <- job_id
  
  # Pull task ids of the jid that failed due to memory, timeout, failing, or cancelled
  mem <-  str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=OUT_OF_MEMORY"), intern = T))), ".+[:space:]") %>% as.numeric()
  time <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=TIMEOUT"), intern = T))), ".+[:space:]") %>% as.numeric()
  failed <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=FAILED"), intern = T))), ".+[:space:]") %>% as.numeric()
  cancelled <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=CANCELLED"), intern = T))), ".+[:space:]") %>% as.numeric()
  
  # Find those task ids in the job task list
  oom_tasks <- job_tasks[mem][,relaunch:='oom']
  oot_tasks <- job_tasks[time][,relaunch:='oot']
  failed_tasks <- job_tasks[failed][,relaunch:='failed']
  cancelled_tasks <- job_tasks[cancelled][,relaunch:='cancelled']
  
  relaunch <- rbind(oom_tasks,oot_tasks,failed_tasks,cancelled_tasks)
  
  
  return(relaunch)
}
