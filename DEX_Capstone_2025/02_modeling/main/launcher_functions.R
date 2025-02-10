##----------------------------------------------------------------------
## FUNCTION: pull_relaunch_params()
## Purpose: Get a set of parameters to relaunch in an array job that did not complete
## Input:   jid: Job ID of incomplete job
##          model_version: Model version of parameters you wish to relaunch
## Output:  A data table of the parameters that did not complete and can be saved as a new param list to relaunch
## Author:  Azalea Thomson
##----------------------------------------------------------------------  

pull_relaunch_params <- function(jid, model_version, user){

  dir <- paste0("FILEPATH/model_version_", model_version, "/")
  
  job_tasks <- fread(paste0(dir, "launched_job_tasks/", jid,".csv"))
  
  if (user == Sys.getenv('USER')){
    # Pull task ids of the jid that failed due to memory, timeout, failing, or cancelled
    mem <-  str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ",user," --state=OUT_OF_MEMORY"), intern = T))), ".+[:space:]") %>% as.numeric()
    time <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ",user," --state=TIMEOUT"), intern = T))), ".+[:space:]") %>% as.numeric()
    failed <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ",user," --state=FAILED"), intern = T))), ".+[:space:]") %>% as.numeric()
    cancelled <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 -u ",user," --state=CANCELLED"), intern = T))), ".+[:space:]") %>% as.numeric()
  }else{
    # Pull task ids of the jid that failed due to memory, timeout, failing, or cancelled
    mem <-  str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=OUT_OF_MEMORY"), intern = T))), ".+[:space:]") %>% as.numeric()
    time <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=TIMEOUT"), intern = T))), ".+[:space:]") %>% as.numeric()
    failed <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=FAILED"), intern = T))), ".+[:space:]") %>% as.numeric()
    cancelled <- str_remove(str_squish(gsub(".*_", "", system(paste0("sacct -n -X -j ", jid," -o jobid%20 --state=CANCELLED"), intern = T))), ".+[:space:]") %>% as.numeric()
  }
  
  # Find those task ids in the job task list
  oom_tasks <- job_tasks[mem][,relaunch:='oom']
  oot_tasks <- job_tasks[time][,relaunch:='oot']
  failed_tasks <- job_tasks[failed][,relaunch:='failed']
  cancelled_tasks <- job_tasks[cancelled][,relaunch:='cancelled']
  
  relaunch <- rbind(oom_tasks,oot_tasks,cancelled_tasks,failed_tasks)
  
  
  return(relaunch)
}



##----------------------------------------------------------------------
## FUNCTION: update_param_memsets()
## Purpose: Update an existing table with new mem_sets for any tasks provided.
## Input:   existing_params_path: Supply the path to a table with all parameters that may or may not include a mem_set column
##          new_memsets_dt: A data table of the parameters with new mem_sets that you wish to replace
## Output:  A data table of all parameters in your original params table, with updated mem_set column
## Author:  Azalea Thomson
##----------------------------------------------------------------------  
update_param_memsets <- function(existing_params_path, new_memsets_dt){
  cols <- c("geo", "acause", "toc", "metric", "pri_payer", "sex_id", "payer", "mem_set")
  
  '%ni%' <- Negate('%in%')
  existing_params <- fread(existing_params_path)
  if ('mem_set' %ni% names(existing_params)){
    existing_params[,mem_set:=NA]
  }
  
  # limit cols
  new_memsets_dt <- new_memsets_dt[, c(cols), with = F]
  
  setnames(new_memsets_dt,'mem_set','mem_set_new')
  params_with_mem <- merge(existing_params, new_memsets_dt, all.x = T)
  params_with_mem[!is.na(mem_set_new), mem_set:= mem_set_new]
  params_with_mem$mem_set_new <- NULL
  return(params_with_mem)
  
}


##----------------------------------------------------------------------
## FUNCTION: profile_params()
## Purpose: Profile a given model version convergence and return a table with the starting intercept, convergence, se quant outlier values, and other info
##          that can be used as priors for another model version.
## Input:   params_path: Supply the path to a table with all parameters. This can be a table with prior profiled information, or no profiled information.
##                       If there is prior profiled information, parameters will be updated to reflect the model version supplied.
##          model_version: The model version number you wish to profile convergence for
## Output:  A data table of all parameters in your original params table, with profiled information for the parameters that were run for that model version
## Author:  Azalea Thomson
##----------------------------------------------------------------------  

profile_params <- function(params_path, model_version, worker){

  ## Full set of model params to subset from
  param_map <- fread(params_path)
  
  convergence_dir <- paste0("FILEPATH/model_version_",model_version,"/convergence/")
  message('Pulling convergence for model version ', model_version)
  schema <- update_nulls_schema(convergence_dir)
  schema[["rho_a"]] <- arrow::float()
  schema[["rho_t"]] <- arrow::float()
  schema[["starting_intercept"]] <- arrow::float()
  
  model_output <- arrow::open_dataset(convergence_dir,schema = schema) %>% collect() %>% as.data.table()
  
  if (worker != 'mod0'){
    model_output[n_years==1,insufficient_yrs:= 1]
    model_output[n_ages==1,insufficient_ages:= 1]
    model_output[n_row <= 3,insufficient_data:= 1]
    model_output[convergence_note %like% 'Insufficient',insufficient_data:=1 ]
    keep_cols <- c('geo','acause','toc','metric','pri_payer','sex_id','payer',
                   'starting_intercept','convergence','convergence_note','insufficient_yrs',
                   'insufficient_ages','insufficient_data','se_quant_out')
    if ('icd9' %in% names(model_output)){
      keep_cols <- c(keep_cols, 'icd9')
    }
    model_output <- model_output[,..keep_cols]
    merge_cols <- c('acause','toc','metric','pri_payer','payer','sex_id','geo')
    secondary_cols <- model_output %>% select(-merge_cols) %>% colnames()
    setnames(model_output, secondary_cols, paste0(secondary_cols,'_new'))
    
    ## Merge profiled params onto full set of model params
    params_profiled <- merge(param_map, model_output, by = merge_cols, all.x = T)
    params_profiled[!is.na(convergence_new),convergence:=convergence_new]
    params_profiled[!is.na(convergence_note_new),convergence_note:=convergence_note_new]
    params_profiled[!is.na(starting_intercept_new),starting_intercept:=starting_intercept_new]
    params_profiled[!is.na(insufficient_yrs_new),insufficient_yrs:=insufficient_yrs_new]
    params_profiled[!is.na(insufficient_ages_new),insufficient_ages:=insufficient_ages_new]
    params_profiled[!is.na(insufficient_data_new),insufficient_data:=insufficient_data_new]
    params_profiled[!is.na(se_quant_out_new),se_quant_out:=se_quant_out_new]
    
  }else{
    
    keep_cols <- c('geo','acause','toc','metric','pri_payer','sex_id','payer',
                   'convergence','convergence_note','restriction','intercept')
    model_output <- model_output[,..keep_cols]
    merge_cols <- c('acause','toc','metric','pri_payer','payer','sex_id','geo')
    secondary_cols <- model_output %>% select(-merge_cols) %>% colnames()
    setnames(model_output, secondary_cols, paste0(secondary_cols,'_new'))
    params_profiled <- merge(param_map, model_output, by = merge_cols, all.x = T)
    params_profiled[!is.na(convergence_new),convergence:=convergence_new]
    params_profiled[!is.na(convergence_note_new),convergence_note:=convergence_note_new]
    params_profiled[!is.na(restriction_new),restriction:=restriction_new]
    params_profiled[!is.na(intercept_new),intercept:=intercept_new]
  }
    
  params_profiled[,paste0(secondary_cols,'_new'):=NULL]
  return(params_profiled)

}

##----------------------------------------------------------------------
## FUNCTION: pull_nonconv_params()
## Purpose: Pull non-converged tasks from a previous model version to supply for params to next model version in the cascade
## Input:   prev_mv: model version of the previous run model
## Output:  A data table of all parameters to run
## Author:  Azalea Thomson
##----------------------------------------------------------------------  

pull_nonconv_params <- function(prev_mv, user){
  convergence_dir <- paste0("FILEPATH/model_version_",prev_mv,"/convergence/")
  need_cols <- c('geo','acause','toc','metric','pri_payer','payer','sex_id','convergence')
  conv_dt <- open_dataset(convergence_dir) %>% select(all_of(need_cols)) %>% collect() %>% as.data.table()
  t <- conv_dt[convergence==1]
  t$convergence <- NULL
  
  ## also pull in any tasks that "failed" because they timed out or went oom or were cancelled
  jid_dir <- paste0('FILEPATH/model_version_',prev_mv,'/launched_job_tasks/')
  jids <- gsub('.csv','',list.files(jid_dir))
  relaunch_t <- data.table()
  for (jid in jids){
    relaunch_tasks <- pull_relaunch_params(jid, prev_mv, user)
    relaunch_t <- rbind(relaunch_t, relaunch_tasks)
  }

  need_cols <- need_cols[!need_cols == 'convergence']
  relaunch_t <-  unique(relaunch_t[,..need_cols])
  
  # Remove any tasks that eventually converged/are already tracked as unconverged during a relaunch from the list of relaunch params
  relaunch_t <- merge(relaunch_t, conv_dt, by = need_cols, all.x = T)
  relaunch_t <-  relaunch_t[is.na(convergence),..need_cols]
  
  # Combined unconverged params with those that don't have any output
  t <- unique(rbind(t, relaunch_t))
  return(t)

  
}


##----------------------------------------------------------------------
## FUNCTION: pull_intercept_params()
## Purpose: Pull non-converged tasks from all previous models to use for intercept model in the cascade
## Input:   prev_mv: list of previous model versions
## Output:  A data table of all parameters to run
## Author:  Azalea Thomson
##----------------------------------------------------------------------  

pull_intercept_params <- function(prev_mv_list){
  
  int_params <- data.table()
  for (i in prev_mv_list){
    print(i)
    convergence_dir <- paste0("FILEPATH/model_version_",i,"/convergence/")
    need_cols <- c('geo','acause','toc','metric','pri_payer','payer','sex_id','convergence')
    t <- open_dataset(convergence_dir) %>% 
      filter(convergence == 1) %>%
      select(all_of(need_cols)) %>%
      collect() %>% as.data.table()
    t$convergence <- NULL
    int_params <- rbind(int_params, t)
  }
  
  return(int_params)
}