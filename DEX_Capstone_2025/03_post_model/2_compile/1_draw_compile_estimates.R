##----------------------------------------------------------------
## Title: compile_estimates.R
## Purpose: Compile utilization (vol) and spend (spending) for counties and states
##          Perform adjustments for managed care, nursing facility, dental, and more
## IMPORTANT!! States must be compiled first so counties can then be raked to state!!
## Notes:
## - Currently parallelized on:
##     * geo 
##     * toc
##     * year_id
##     * draw (0-50) or if running means only, then just "draw 0"
## - Partitions output data on:
##     * geo
##     * toc
##     * year_id
##     * draw (0-50) or if running means only, then just "draw 0"
## Authors: Azalea Thomson and Haley Lescinsky
##----------------------------------------------------------------


adj_vol_too <- T # Permanently true

## --------------------
## 1. Setup
## --------------------                                                                 
Sys.umask(mode = 002)
t0 <- Sys.time()

'%ni%' <- Negate('%in%')
 
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())  

pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

source(paste0(here,'/function_pull_data.R'))
source(paste0(here,'/function_adjustments.R'))
source(paste0(here,'/function_rake.R'))

## --------------------
## 2. Parameters
## --------------------

if(interactive()){
  mc_adjust <- T
  days_metrics <- F
  rake_state_to_national <- T # pretty much always T
  use_imputation <- T # ON / OFF depending
  
  sv <- 'XX'
  mset_id <- 'setXX' 
  mv_spend <- mset_id
  mv_vol <- mset_id
  draw_num <- 0
  geog <- 'state' 
  care <- 'AM'
  yr <- 2010
  
  if (geog == 'county'){
   st <- 'FL'
  }
 
}else{
  args <- commandArgs(trailingOnly = TRUE)
  task_path <- args[1]
  rake_state_to_national <- as.logical(args[2])
  use_imputation <- as.logical(args[3])
  mc_adjust <- as.logical(args[4])
  m <- args[5]
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  tasks <- fread(task_path)
  if("mem_set" %in% colnames(tasks)){
   tasks <- tasks[mem_set == m]
  }
  tasks <- tasks[task_id,]
  for (i in names(tasks)){
   assign(i, as.character(tasks[, get(i)]))
   message(i,':', tasks[, get(i)])
  }
  draw_num <- as.numeric(draw_num)
  yr <- as.numeric(yr)

}

## --------------------
## 3. Constants
## --------------------
denoms_path <- paste0("/FILEPATH/")
shea_path <- "/FILEPATH/shea_adjusted_envelopes_allpayer.csv"
toc_cause_restrictions_path <- "/FILEPATH/toc_cause_age_sex.csv"

## Inputs
post_mod_dir <- '/FILEPATH/'
mod_dir <- '/FILEPATH/'
sp_input_source_path <- paste0(post_mod_dir,'model_version_',mv_spend,'/all_param_combos.csv')
vol_input_source_path <- paste0(post_mod_dir,'model_version_',mv_vol,'/all_param_combos.csv')                    
model_est_path_vol <- paste0(mod_dir,'model_version_',mv_vol,'/draws/')
model_est_path_spend <- paste0(mod_dir,'model_version_',mv_spend,'/draws/')
imputed_dir_vol <- paste0(post_mod_dir,'/model_version_',mv_vol,'/imputed_draws/' )
imputed_dir_spend <- paste0(post_mod_dir,'/model_version_',mv_spend,'/imputed_draws/') 

## Outputs
compile_dir <- paste0("/FILEPATH/scaled_version_",sv,"/compiled/")
stepwise_dir <- paste0(compile_dir, "../stepwise/")
diagnostic_dir <- paste0(compile_dir, "../diagnostics/")
missings_dir <- paste0(compile_dir, "../missings/")

save_cols <- c("geo", "toc", "state", "location", "year_id", "age_group_years_start", "sex_id", "pri_payer", 
               "payer", "acause", "denom", "spend", "vol", "draw")
if (days_metrics == T){
  save_cols <- c(save_cols,'spend2','vol2')
}


partition_cols <- c("geo","toc","year_id", "draw")
tmp_partition_cols <- partition_cols[partition_cols != 'draw']

if (draw_num == 0){
  draw_col <- paste0('mean') #changed from median to mean on 7/9/24 - AT
}else{
  draw_col <- paste0('draw_',draw_num) 
} 

## ------------------------------------------------------------------------------------------------------------
## 4. Get metadata for location, age, year, causes, types of care, thresholds for plausible vals, and metrics
## ------------------------------------------------------------------------------------------------------------
cl <- fread(toc_cause_restrictions_path)[include == 1 & gc_nec == 0 & toc == care]
causes <- unique(cl$acause)

national <- data.table(state = 'USA', state_name = 'United States')

states <- fread('/FILEPATH/states.csv')
states <- states[,.(abbreviation,state_name)]
setnames(states,'abbreviation','state')

counties <- fread('/FILEPATH/merged_counties.csv')
counties <- counties[current==1]
counties <- merge(unique(counties[,.(state_name,mcnty)]), states[,.(state_name,state)], by = 'state_name')


if (geog=='national'){
  locs <- 'USA'
}else if (geog=='state'){
  locs <- states$state
}else if (geog == 'county'){
  if (interactive()){
    counties <- counties[state == st] # just so we can more easily test compile county level
  }
  locs <- as.character(counties$mcnty)
}


ages <- fread(paste0(denoms_path, 'inputs/age_metadata.csv'))[,.(age_group_id, age_group_years_start)]


if (days_metrics == T){
  spend_metrics <- c('spend_per_encounter','spend_per_day')
  vol_metrics <- c('days_per_encounter','encounters_per_person')
}else{
 spend_metrics <- 'spend_per_encounter'
 vol_metrics <- 'encounters_per_person'
}
all_metrics <-c(spend_metrics, vol_metrics)


# if interactive, or if national model (where we don't have imputed data)
if( geog == 'national' | use_imputation == F){
  print("Not using imputation!")
  inputs_full <- fread(paste0(model_est_path_spend, "/../post_run_job_params.csv"))[metric %in% all_metrics & toc == care & geo==geog]
  inputs_full <- inputs_full[output_exists == TRUE & convergence == 'Yes']
  inputs_full <-  inputs_full[,.(acause, metric, pri_payer, sex_id, payer, read_from = "modeled")]
  
}else{
  ## Read in model combos with input source
  inputs_spend <- fread(sp_input_source_path)[metric %in% spend_metrics & toc == care & geo==geog]
  inputs_vol <- fread(vol_input_source_path)[metric %in% vol_metrics & toc == care &geo==geog]
  inputs_full <- rbind(inputs_spend,inputs_vol)
  inputs_full$toc <- NULL
  inputs_full$geo <- NULL
  inputs_full$impute_reason <- NULL
}

# Drop some combinations depending on geo/toc/run type
if(care %in% c('AM','HH','DV','RX') & geog %in% c('state', 'county')){
  # we don't have data on these so use national model, not imputed results
  inputs_full <- inputs_full[pri_payer != "oop"]
}

# If DV - only want P&R to inform DV private spend 
if(care == 'DV'){
  inputs_full <- inputs_full[!(payer %in% c("priv") | pri_payer == "priv")] 
}

# determine imp vs mod combos
imp_combos <- inputs_full[read_from == 'imputed']
mod_combos <- inputs_full[read_from == 'modeled']


## ----------------------------------------
## 5. Read in estimates for metrics 
## ----------------------------------------
merge_cols <- c('acause','pri_payer','payer','sex_id','metric')

## ----------------------------------------
message('Reading in imputed data...')
## ----------------------------------------
t1 <- Sys.time()
## 1. Imputed spend
if(nrow(imp_combos[metric %in% spend_metrics]) > 0 ){
  
  if ( geog != 'national' ){
    spend_impute <-  pull_data(dir=imputed_dir_spend, geog=geog, care=care, yr=yr, mets=spend_metrics, locs=locs)
    spend_impute <- merge(spend_impute, unique(imp_combos[metric %in% spend_metrics]),all.x=T, by = merge_cols)
    spend_impute <- spend_impute[read_from == 'imputed']
    spend_impute$read_from <- NULL
    spend_impute[, from := "impute"]
  }
  
}else{
  print("no imputed spend data")
  spend_impute <- data.table()
}

## 2. Imputed vol
if(nrow(imp_combos[metric %in% vol_metrics]) > 0 ){

  if ( geog != 'national'){ 
     vol_impute <-  pull_data(dir=imputed_dir_vol, geog=geog, care=care, yr=yr, mets=vol_metrics, locs=locs)
     vol_impute <- merge(vol_impute, unique(imp_combos[metric %in% vol_metrics]),all.x=T, by = merge_cols)
     vol_impute <- vol_impute[read_from == 'imputed']
     vol_impute$read_from <- NULL
     vol_impute[, from := "impute"]
  }
  
}else{
  print("no imputed volume data")
  vol_impute <- data.table()
}

print(Sys.time() - t1)
t2 <- Sys.time()
## ----------------------------------------
message('Reading in modeled data...')
## ----------------------------------------

spend_mod <-  pull_data(dir=model_est_path_spend, geog=geog, care=care, yr=yr, met=spend_metrics, locs=locs)
spend_mod <- merge(spend_mod, unique(mod_combos[metric %in% spend_metrics]),all.x=T, by = merge_cols)
spend_mod <- spend_mod[read_from == 'modeled']
spend_mod$read_from <- NULL
spend_mod[, from := "modeled"]
   
vol_mod <-  pull_data(dir=model_est_path_vol, geog=geog, care=care, yr=yr, met=vol_metrics, locs=locs)
vol_mod <- merge(vol_mod, unique(mod_combos[metric %in% vol_metrics]),all.x=T, by = merge_cols)
vol_mod <- vol_mod[read_from == 'modeled']
vol_mod$read_from <- NULL
vol_mod[, from := "modeled"]
   

print(Sys.time() - t2)
t3 <- Sys.time()
## ----------------------------------------
message('Casting wide on metric and duplicating mdcr-mdcr rates for spend...')
## ----------------------------------------

spend_dt <- rbind(spend_mod, spend_impute)
vol_dt <- rbind(vol_mod, vol_impute)

rm(vol_mod)
rm(vol_impute)
rm(spend_mod)
rm(spend_impute)

print(paste0('spend dt has ',nrow(spend_dt), ' rows and ', length(unique(spend_dt$acause)), ' causes'))
print(paste0('vol dt has ',nrow(vol_dt), ' rows and ', length(unique(vol_dt$acause)), ' causes'))

### Cast wide on metric
if (nrow(spend_dt)>0){
  spend_dt <- unique(spend_dt)
  spend_dt <- dcast(spend_dt,...~metric,value.var = 'val')
  spend_dt$age_group_years_start <- as.numeric(spend_dt$age_group_years_start)
}

## Use mdcr-mdcr spend rate for mdcr-mdcd - mdcr & mdcr_priv - mdcr
spend_dt <- rbind(spend_dt, 
                  spend_dt[pri_payer == 'mdcr' & payer == 'mdcr'][, pri_payer := 'mdcr_mdcd'],
                  spend_dt[pri_payer == 'mdcr' & payer == 'mdcr'][, pri_payer := 'mdcr_priv'])

## we only have FFS spending, but MC+FFS Util, so we duplicate out FFS spending and recode as MC prior to MC adjust
spend_dt <- rbind(spend_dt, 
                  spend_dt[pri_payer %in% c("mdcd", "mdcr")][, pri_payer := paste0(pri_payer, "_mc")],
                  spend_dt[pri_payer =="mdcr_mdcd"][, pri_payer := "mdcr_mc_mdcd"])

## Payer is always NA for vol metrics, so we remove the column and then when merging with spend, we get a duplicated vol row for each payer specific spend row (which is what we want)
if (nrow(vol_dt)>0){
  vol_dt$payer <- NULL 
  vol_dt <- unique(vol_dt)
  vol_dt <- dcast(vol_dt,...~metric,value.var = 'val')
  vol_dt$age_group_years_start <- as.numeric(vol_dt$age_group_years_start)
}


print(Sys.time() - t3) 
t4 <- Sys.time()


## ----------------------------------------
message('Next step joining spend and vol')
## ----------------------------------------

by_cols <- c("acause", "pri_payer", "sex_id", "location", "year_id","age_group_years_start", "state", "geo", "toc", "draw_num")

est_dt <- merge(spend_dt, vol_dt, by = by_cols)

### Save the rows that we lose when we don't have estimates for the same params of the corresponding metric
if (nrow(est_dt) != nrow(spend_dt) ){

  if (draw_num == 0){

    by_cols2 <- by_cols[by_cols != 'age_group_years_start' & by_cols != 'year_id']

    # save record of what drops out of the merge
    check_drops <- merge(spend_dt, vol_dt,by=by_cols, all=T)
    check_drops[, tot_rows := .N, by = by_cols2]
    check_drops <- check_drops[is.na(spend_per_encounter) | is.na(encounters_per_person)]
    check_drops[, tot_rows_compare := .N, by = by_cols2]

    # save only the jobs where we lose the whole combination, not just some ages!
    check_drops <- check_drops[tot_rows == tot_rows_compare]

    combos_dropped <- unique(check_drops[,.(acause,pri_payer,sex_id,location, state,geo, toc,payer)])

    arrow::write_dataset(check_drops, path = missings_dir, partitioning = tmp_partition_cols) 

    nrow(combos_dropped)
    print (paste0('Warning, ', nrow(combos_dropped),' combos were fully dropped in the spend vol merge'))

  }


}


if( (geog == 'state' & draw_col == "mean") | (geog == 'national' & draw_col == 'mean') ){
  compile_tmp_dir <- paste0(stepwise_dir,"/step1_rates_merged")
  if(!dir.exists(compile_tmp_dir)){
    dir.create(compile_tmp_dir, recursive = T)
  }

  setnames(est_dt, c("from.x", "from.y"), c("spend_from", "vol_from"))
  est_dt <- est_dt[with(est_dt, order(year_id,age_group_years_start,sex_id,acause)), ]

  arrow::write_dataset(est_dt, path = compile_tmp_dir,
                       partitioning = tmp_partition_cols)

  est_dt[, `:=` (spend_from = NULL, vol_from = NULL)]
}else{
  est_dt[, `:=` (from.x = NULL, from.y = NULL)]
}

rm(spend_dt)
rm(vol_dt)


## ----------------------------------------
## 6. If we have data, carry on
## ----------------------------------------

if (nrow(est_dt)>0){

  print(Sys.time() - t4)
  t5 <- Sys.time()
  print(paste0('Joined spend and vol, est_dt is ',nrow(est_dt),'rows, next step cleaning location'))

  ## ----------------------------------------------------------------------------------
  ## 7. Clean location column, check we have expected number of locs
  ## ----------------------------------------------------------------------------------

  num_data_locs <- length(unique(est_dt$location))
  if (length(locs) != num_data_locs){
    stop('Number of locations are not as expected')
  }

  ## ------------------------------------------------------------
  ## 8. Format and bring in population denominators
  ## ------------------------------------------------------------
  print(Sys.time() - t5)
  t6 <- Sys.time()
  print('Next step pulling in denoms')

  ## Read in pop denoms
  pop_denom_path <- paste0(denoms_path, "/denoms_for_compile/data/geo=", geog, "/toc=", care, "")
  

  sample_pop <- open_dataset(pop_denom_path) %>% 
    filter(location %in% locs) %>%
    filter(year_id == yr)%>%
    collect() %>% as.data.table()

  # drop total for mdcr + mdcd payers
  sample_pop <- sample_pop[!(type == 'total' & pri_payer %in% c('mdcd', 'mdcr', 'mdcr_mdcd'))]

  # drop any-mdcr
  sample_pop <- sample_pop[pri_payer!='any-mdcr']

  sample_pop[type == "mc", pri_payer := paste0(pri_payer, "_mc")]
  sample_pop[pri_payer == "mdcr_mdcd_mc", pri_payer := "mdcr_mc_mdcd"] # pasted _mc at the end

  ## Subset sample_pop to columns we want
  keep_cols <- c('location', 'state', 'age_group_years_start', 'year_id', 'sex_id', 'pri_payer', 'denom')
  sample_pop <- sample_pop[,..keep_cols]

  ## ------------------------------------------------------------
  ## 9. Merge denominators and estimates
  ## ------------------------------------------------------------

  print(Sys.time() - t6)
  t7 <- Sys.time()
  print('Next step merging on denoms')

  by_cols3 <- keep_cols[keep_cols != 'denom']

  est_with_denoms <- merge(est_dt, sample_pop, by = by_cols3)

  if (nrow(est_dt) != nrow(est_with_denoms)){
    message('Warning, some data have dropped out')
    stop()
  }
  

  ## ------------------------------------------------------------
  ## 10. Calculate spend and utilization
  ## ------------------------------------------------------------
  print(Sys.time() - t7)
  t8 <- Sys.time()
  print('Next step calculating spend and vol')


  if (days_metrics == T){
    est_with_denoms[, spend := denom * (encounters_per_person) * (days_per_encounter) * (spend_per_day)]
    est_with_denoms[, vol := denom * (encounters_per_person) * (days_per_encounter)]

    ## Drop the remaining NAs (this is where we have days metrics output but no encounters_per_person output)
    est_with_denoms <- est_with_denoms[!( is.na(spend) | is.na(vol) )]
    
  } else{
    est_with_denoms[, spend := denom * (encounters_per_person) * (spend_per_encounter)]
    est_with_denoms[, vol := denom * (encounters_per_person)]
  }

  setnames(est_with_denoms, 'draw_num','draw')

  summary(est_with_denoms$spend)
  summary(est_with_denoms$vol)



  if( (geog == 'state' & draw_col == "mean") | (geog == 'national' & draw_col == 'mean') ){

    print('saving intermediates')

    compile_tmp_dir <- paste0(stepwise_dir,"/step2_counts")
    if(!dir.exists(compile_tmp_dir)){
      dir.create(compile_tmp_dir, recursive = T)
    }

    est_with_denoms <- est_with_denoms[with(est_with_denoms, order(year_id,age_group_years_start,sex_id,acause)), ]
    
    arrow::write_dataset(est_with_denoms, path = compile_tmp_dir,
                         partitioning = tmp_partition_cols)

    print("saved intermediate outputs!")
  }

  ## ------------------------------------------------------------
  ## 11. Combine FFS and MC for mdcd + mdcr
  ## ------------------------------------------------------------
  print(Sys.time() - t8)
  t9 <- Sys.time()
  print('Next step managed_care')

  if (mc_adjust == T){
    mc_adjusted <- managed_care_adjust(data = est_with_denoms)
    print('managed_care_adjust done')
  }else{
    mc_adjusted <- managed_care_combine(data = est_with_denoms)
    print('managed_care_combine done')
  }

  ## ------------------------------------------------------------
  ## 12. DV adjust!
  ##     - pull in Fluent (P&R) data for private
  ##     - for OOP, we only have it as a secondary payer for mdcd, mdcr + priv, so prior to combining those
  ##      we use the SHEA scalar for the respective pri payer applied to the oop vol/spend
  ## ------------------------------------------------------------
  
  print(Sys.time() - t9)
  t10 <- Sys.time()

  dv_adjusted <- dv_adjust(data = mc_adjusted, shea_path = shea_path)

  ## ------------------------------------------------------------
  ## 13. Rake & save
  ## ------------------------------------------------------------
  print(Sys.time() - t10)
  t11 <- Sys.time()
  
  
  if ((rake_state_to_national == T & geog == 'state') | (geog == 'county')){
    
    if (geog == 'state'){
      
      ## Rake
      raked_data <- rake_wrapper(data = dv_adjusted)
      
      ## Now save
      print('Saving compiled data')
      basename <- 'part'
      final <- clean_and_check(data = raked_data)
      if (!interactive()){
        arrow::write_dataset(final,
                   path = compile_dir,
                   partitioning = partition_cols,
                   basename_template = paste0(basename,"-{i}.parquet"))
      }
    }
    
    if (geog == 'county'){
      
      for (st in unique(dv_adjusted$state)){
        print(st)
        ## Rake
        raked_data <- rake_wrapper(data = dv_adjusted[state==st], st = st)
        
        ## Now save
        final <- clean_and_check(data = raked_data)
        if (!interactive()){
          arrow::write_dataset(final,
                               path = compile_dir,
                               partitioning = partition_cols,
                               basename_template = paste0(st,"-{i}.parquet"))
        }
      }
    }

  }else{
    
    ## Now save
    print('Saving compiled data')
    basename <- 'part'
    
    
    final <- clean_and_check(data = dv_adjusted)
    if (!interactive()){
      arrow::write_dataset(final,
                           path = compile_dir,
                           partitioning = partition_cols,
                           basename_template = paste0(basename,"-{i}.parquet"))
    }
    
  }
  

  print("saved!")


}else{
  message('No data, stopping')
}




print("Done")

print(Sys.time() - t0)
