##----------------------------------------------------------------
## Title: function_adjustments.R
## Purpose: functions for key steps during compile, namely the managed care adjust and DV adjust
#           and checking output before saving
## 
## 
## Authors: Azalea Thomson and Haley Lescinsky
## Last update: 12/9/24
##----------------------------------------------------------------


# ADJUSTMENT FUNCTIONS
#--------------------
managed_care_adjust <- function(data){
  
  # separate into data (doesn't need ffs + mc combined) and mdc_est (does need combined)
  payers_needing_combined <- c("mdcd", "mdcd_mc",
                               "mdcr", "mdcr_mc",
                               "mdcr_mdcd", "mdcr_mc_mdcd")
  
  # check if there are payers present in data that we need to combine
  present_payers <- intersect(payers_needing_combined, data$pri_payer)
  
  if(length(present_payers) > 0 ){
    
    mdc_est <- copy(data)[, type := ifelse(pri_payer %like% "_mc", "mc", "ffs")]
    rm(data)
    
    if(length(setdiff(payers_needing_combined, mdc_est$pri_payer)) > 0){
      print(setdiff(payers_needing_combined, unique(mdc_est$pri_payer) ))
      stop("trying to combine FFS & MC but missing some of the expected payers in the data")
    }
    
    # Add helper columns
    mdc_est[, `:=` (unadj_spend = spend, unadj_vol = vol)]
    mdc_est[type == "mc", `:=` (pri_payer = gsub("_mc", "", pri_payer))]
    
    
    #   Notes
    #
    #    1 - First we calculate MDCR scalars for MC and FFS; compare agg medicare spend to envelope
    #    2 - We apply that scalar to spend + vol for all payers underneath pri payers: mdcr, mdcr_mdcd, and mdcr_priv
    #    3 - Then we calculate MDCD scalars for MC and FFS; compare agg medicaid spend to envelope
    #    4 - We apply that scalar to spend + vol for all payers underneath pri payers: mdcd and mdcr_mdcd
    #    5 - Save scalars for vetting
    #    6 - Sum up spending across 'type', removing mc status
    #
    #
    
    # Load mc and ffs envelopes
    if(geog %in% c("state", "national")){
      mc_adj_fractions <- fread("FILEPATH/best/mc_spend_adjustment_envelopes.csv")
      mc_adj_fractions[, `:=` (location = state)]
      
      mc_adj_fractions_national <- mc_adj_fractions[, .(envelope = sum(envelope), state = 'USA', location = 'USA'), by = c('year_id', 'type', 'payer')]
      mc_adj_fractions <- rbind(mc_adj_fractions, mc_adj_fractions_national)
    }else{
      mc_adj_fractions <- fread("FILEPATH/best/mc_spend_adjustment_envelopes_mdcr_county.csv")
      mc_adj_fractions[, `:=` (pri_payer = payer, location = as.character(location))]
    }
    
    # 
    # MDCR RATIOS   (compare observed - where payer = mdcr - to mdcr envelope, by MC vs FFS)
    #
    
    mdc_est_compare <- mdc_est[payer == 'mdcr', .(agg_spend =  sum(spend, na.rm = T)), by = c("state", "location", "year_id", "type", "payer", "toc")]
    mdcr_ratios <- merge(mdc_est_compare, mc_adj_fractions[,.(year_id, state,location, type, envelope, payer)], by = c("year_id", "state", 'location', "payer", "type"))
    mdcr_ratios[agg_spend > 0, ratio := envelope / agg_spend]  
    
    # mdcr ratios get applied to pri_payers: mdcr_mdcd, mdcr, mdcr_priv and priv
    ratios <- copy(mdcr_ratios)
    mdcr_ratios <- crossing(mdcr_ratios, data.table(pri_payer = c('mdcr', 'mdcr_mdcd', 'mdcr_priv', 'priv'))) %>% as.data.table()
    
    mdc_est <- merge(mdc_est, mdcr_ratios[,.(year_id, state, location, pri_payer, type, ratio)],by = c("year_id", "state", "location", "pri_payer", "type"), all.x = T)
    mdc_est[is.na(ratio) | is.nan(ratio) | is.infinite(ratio), ratio := 1]
    
    # we only want to apply MDCR ratio to MDCR_MDCD when payer = MDCR --> set ratio to 1 (no effect) for MDCD
    mdc_est[pri_payer == 'mdcr_mdcd' & payer == 'mdcd', ratio :=1]
    
    mdc_est[, `:=` (spend = spend * ratio, 
                    vol = vol*ratio)]
    mdc_est[, ratio := NULL]
    
    # mdcr_mdcd is always mdcd FFS, so now that the mdcr MC vs FFS adjust part is over, make sure it's all coded as ffs
    mdc_est[pri_payer == "mdcr_mdcd" & payer == "mdcd", type := 'ffs']
    
    # 
    # MDCD RATIOS   (compare observed - where payer = mdcd - to mdcd envelope, by MC vs FFS). 
    #    For county, ratios will all be NA and thus 1
    
    mdc_est_compare <- mdc_est[payer == 'mdcd', .(agg_spend =  sum(spend, na.rm = T)), by = c("state", "year_id", "type", "payer", "toc")]
    mdcd_ratios <- merge(mdc_est_compare, mc_adj_fractions[,.(year_id, state, type, envelope, payer)], by = c("year_id", "state", "payer", "type"))
    mdcd_ratios[agg_spend > 0, ratio := envelope / agg_spend]
    
    # mdcd ratios get applied to pri_payers: mdcr_mdcd, mdcd
    mdcd_ratios <- crossing(mdcd_ratios, data.table(pri_payer = c('mdcd', 'mdcr_mdcd'))) %>% as.data.table()
    
    mdc_est <- merge(mdc_est, mdcd_ratios[,.(year_id, state, pri_payer, type, ratio)],by = c("year_id", "state", "pri_payer", "type"), all.x = T)
    mdc_est[is.na(ratio) | is.nan(ratio) | is.infinite(ratio), ratio := 1]
    
    # we only want to apply MDCD ratio to MDCR_MDCD when payer = MDCD --> set ratio to 1 (no effect) for MDCR
    mdc_est[pri_payer == 'mdcr_mdcd' & payer == 'mdcr', ratio :=1]
    # same for oop
    mdc_est[pri_payer == 'mdcr_mdcd' & payer == 'oop', ratio :=1]
    
    mdc_est[, `:=` (spend = spend * ratio, 
                    vol = vol*ratio)]
    mdc_est[, ratio := NULL]
    
    
    ### SAVE RATIOS AND SOME DIAGNOSTICS FOR DUALS in MDCR PAYER
    if (geog %in% c('national') & draw_num ==0){
      tmp_partition_cols <- partition_cols[partition_cols != 'draw']
      by_cols <- c('pri_payer','state','year_id')

      ## Duals check
      duals_prop_check <- copy(mdc_est[type == 'ffs' & payer == 'mdcr']) 
      duals_prop_check[pri_payer == 'mdcr_priv',pri_payer := 'mdcr'] 
      duals_sum <- duals_prop_check %>%  
        group_by(across(all_of(by_cols)))%>% 
        summarise(spend=sum(spend)) %>% as.data.table()
      duals_sum[,toc:=care][,geo:=geog]

      write_dataset(duals_sum,
                    path = paste0(diagnostic_dir, "/mdcr_ffs_duals_props/"),
                    partitioning = tmp_partition_cols)
      
      ## General MDCR check
      by_cols <- c('pri_payer','state','type','year_id')
      keep_cols <- c('age_group_years_start','sex_id','state','pri_payer','type','denom','toc','year_id')

      other_check <- copy(mdc_est[payer == 'mdcr']) 
      spend_sum <- other_check %>%  
        group_by(across(all_of(by_cols)))%>% 
        summarise(spend=sum(spend),vol=sum(vol)) %>% as.data.table()
      
      denom_check <- copy(mdc_est[payer == 'mdcr'])
      denom_check <- unique(denom_check[,..keep_cols]) 
      denom_sum <- denom_check %>%  
        group_by(across(all_of(by_cols)))%>%
        summarise(denom=sum(denom)) %>% as.data.table()
      sum_merge <- merge(spend_sum,denom_sum, by = by_cols)
      sum_merge[,toc:=care][,geo:=geog]
      write_dataset(sum_merge,
                    path = paste0(diagnostic_dir, "/mdcr_props/"),
                    partitioning = tmp_partition_cols)
      
      ## Now just save ratios
      ratios <- rbind(ratios, mdcd_ratios[, `:=` (location = state, pri_payer = NULL)], fill = T)
      
      ratios[, `:=` (geo = geog, toc = care)]
      write_dataset(ratios,
                    path = paste0(diagnostic_dir, "/mc_adjust_ratios/"),
                    partitioning = tmp_partition_cols)
    }
    
    bycols_mc <- c("location", "geo", "state", "year_id", "age_group_years_start", 
                   "sex_id", "pri_payer", "acause", "payer", "toc", "draw")

    # now we sum up spending across ffs + mc !
    mdc_est <- mdc_est[,.(spend = sum(spend),
                          vol = sum(vol),
                          denom = sum(denom)), by = bycols_mc]
    
    
    data <- copy(mdc_est)
    rm(mdc_est)
    
  }else{
    print("no MC adjustment needed")
  }
  
  
  data[, `:=` (spend_per_encounter = NULL, encounters_per_person = NULL)]
  
  
  if(nrow(data[is.na(spend)]) > 0 ){
    print("NAs in spend!")
    warning("NAs in spend!")
  }
  if(nrow(data[is.na(vol)]) > 0 ){
    print("NAs in vol!")
    warning("NAs in vol!")
  }
  
  # Drop NAs, but save record
  nas_drop <- data[is.na(spend) | is.na(vol)]
  if(nrow(nas_drop) > 0 ){
    nas_drop[, step := "na_check"]
    tmp_part_cols <- c("geo","toc","state","step")

    arrow::write_dataset(nas_drop, path = missings_dir, partitioning = tmp_part_cols) 
    print(paste0("NA check dropped ", nrow(nas_drop), " rows"))  
  }
  
  data <- data[!(is.na(spend) | is.na(vol))]
  
  
  # save for testing_purposes
  if( (geog == 'state' & draw_col == "mean") | (geog == 'national' & draw_col == 'mean' & uninsured_split == F) ){
    print('saving intermediates')
    
    compile_tmp_dir <- paste0(stepwise_dir,"/step3_postMC")
    if(!dir.exists(compile_tmp_dir)){
      dir.create(compile_tmp_dir, recursive = T)
    }
    
    data <- data[with(data, order(year_id,age_group_years_start,sex_id,acause)), ]
    
    tmp_partition_cols <- partition_cols[partition_cols != 'draw']
    arrow::write_dataset(data, path = compile_tmp_dir, 
                         partitioning = tmp_partition_cols)
    
    print("saved intermediate outputs!") 
    
  }
  
  return(data)
}

managed_care_combine <- function(data){
  
  # separate into data (doesn't need ffs + mc combined) and mdc_est (does need combined)
  payers_needing_combined <- c("mdcd", "mdcd_mc",
                               "mdcr", "mdcr_mc",
                               "mdcr_mdcd", "mdcr_mc_mdcd")
  
  # check if there are payers present in data that we need to combine
  present_payers <- intersect(payers_needing_combined, data$pri_payer)
  
  if(length(present_payers) > 0 ){
    
    mdc_est <- copy(data)[, type := ifelse(pri_payer %like% "_mc", "mc", "ffs")]
    rm(data)
    
    if(length(setdiff(payers_needing_combined, mdc_est$pri_payer)) > 0){
      print(setdiff(payers_needing_combined, unique(mdc_est$pri_payer) ))
      stop("trying to combine FFS & MC but missing some of the expected payers in the data")
    }
    
    mdc_est[type == "mc", `:=` (pri_payer = gsub("_mc", "", pri_payer))]
    
    
    #  Sum up spending across 'type', removing mc status
    
    bycols_mc <- c("location", "geo", "state", "year_id", "age_group_years_start", 
                   "sex_id", "pri_payer", "acause", "payer", "toc", "draw")

    # now we sum up spending across ffs + mc !
    mdc_est <- mdc_est[,.(spend = sum(spend),
                          vol = sum(vol),
                          denom = sum(denom)),
                       by = bycols_mc]
    
    data <- copy(mdc_est)
    rm(mdc_est)
    
  }else{
    print("no MC combine needed")
  }
  
  
  data[, `:=` (spend_per_encounter = NULL, encounters_per_person = NULL)]
  
  
  if(nrow(data[is.na(spend)]) > 0 ){
    stop("NAs in spend!")
  }
  if(nrow(data[is.na(vol)]) > 0 ){
    stop("NAs in vol!")
  }
  
  return(data)
}

dv_adjust <- function(data, shea_path){
  
  if(care == "DV"){
    
    print('Next step DV adjust')
  
    # bring in P&R data for DV private payer
    dv_env <- open_dataset("/FILEPATH/pr_dental.parquet") %>% collect() %>% as.data.table() %>% mutate(draw = draw_num)
    
    # Create national and state aggregates
    dv_env_states <- dv_env[, .(vol = sum(as.numeric(vol)), spend =sum(spend), geo = "state"), by = c("state", "year_id", "age_group_years_start", "sex_id", "acause", "payer", "pri_payer", "toc", "draw")]
    dv_env_national <- dv_env[, .(vol = sum(as.numeric(vol)), spend =sum(spend), geo = "national", state = 'USA'), by = c("year_id", "age_group_years_start", "sex_id", "acause", "payer", "pri_payer", "toc", "draw")]
    
    dv_env <- rbind(dv_env,
                    dv_env_states,
                    dv_env_national, fill = T)
    dv_env[, mcnty := as.character(mcnty)]
    dv_env[is.na(mcnty), mcnty := state]
    setnames(dv_env, "mcnty", "location")
    dv_env <- dv_env[year_id == yr]
    
    # bring in SHEA envelope 
    shea_env <- fread(shea_path)[toc == "DV" & payer!="ALL"]
    shea_env[, payer := tolower(payer)]
    shea_env[, spend := spend * 1000000]
    setnames(shea_env, "year", "year_id")
    shea_env <- shea_env[year_id == yr]
    
    # get SHEA ratios for mdcd and private to use for combining oop
    if(geog != "county"){
      
      # MDCD + MDCR
      ref_sum <- data[,.(obs_spend = sum(spend)), by = c("year_id", 'state', 'toc', 'payer')]
      mdc_ratios <- merge(ref_sum[payer %in% c("mdcr", "mdcd")], shea_env, by = c("year_id", 'state', 'toc', 'payer'), all.x = T)
      mdc_ratios[, ratio := spend / obs_spend]
      mdc_ratios[is.na(ratio) | is.infinite(ratio), ratio := 1]
      
      # PRIV
      if(geog == 'state'){
        ref_sum <- dv_env_states[,.(pr_spend = sum(spend)), by = c("year_id", 'state', 'payer')]
      }else{
        ref_sum <- dv_env_national[,.(pr_spend = sum(spend)), by = c("year_id", 'state', 'payer')]
      }
      priv_ratios <- merge(ref_sum[payer == 'priv'], shea_env, by = c("year_id", 'state', 'payer'), all.x = T)
      priv_ratios[, ratio := spend / pr_spend]
      priv_ratios[is.na(ratio) | is.infinite(ratio), ratio := 1]
      
      ratios <- rbind(mdc_ratios, priv_ratios, fill = T)
      
      #  add on ratios for mdcr_mdcd, use mdcd ratio
      ratios <- rbind(ratios, ratios[payer == 'mdcd'][, payer := "mdcr_mdcd"])
      
      if(geog == 'state'){
        write.csv(ratios, paste0(diagnostic_dir, "/DV_state_shea_ratios_", yr, ".csv"), row.names = F)
      }else{
        write.csv(ratios, paste0(diagnostic_dir, "/DV_national_shea_ratios_",yr,".csv"), row.names = F)
      }
      
    }else{
      ratios <- fread(paste0(diagnostic_dir, "/DV_state_shea_ratios_", yr, ".csv"))
    }
    
    # COMBINE modeled with P&R and merge on ratios 
    data <- rbind(data, dv_env[geo == geog & location %in% locs], fill = T)
    data <- merge(data, ratios[,.(year_id, state, pri_payer = payer, ratio)], all.x = T)
    
    # We don't expect to have a ratio for pri payer oop, but we do expect it everywhere else
    if(nrow(data[is.na(ratio) & pri_payer!='oop'] > 0)){
      stop("Unexpected NAs in DV ratio adjust")
    }
    data[is.na(ratio), ratio := 1]
    
    data[, `:=` (spend = spend*ratio,
                 vol = vol*ratio)]
    data[, ratio := NULL]
    
    print('DV adjust done')
  }
  
  
  return(data)
  
}


# RAKING & SAVING FUNCTIONS
#--------------------
rake_wrapper <- function(data, st = 'NA'){
  if (geog == 'state'){
    
    message('Pulling in national')
    national_compiled_dir <- paste0(compile_dir, 'geo=national/toc=',care)
    
    ##1. Read in national spend/vol (all states)
    select_cols <- c('year_id','age_group_years_start', 'spend', 'vol', 'sex_id','pri_payer','payer', 'acause','draw')

    
    national_data <- arrow::open_dataset(national_compiled_dir) %>% filter(draw == draw_num & year_id == yr)
    
    national_data <- national_data %>% 
      select(all_of(select_cols)) %>%
      as.data.table()

    setnames(national_data,c('spend','vol'),c('national_spend','national_vol'))
    

    # need to expand state_data to all locations in state so the resulting points will have state
    merge_cols <- select_cols[!(select_cols %in% c('spend','vol'))]
    est_with_nat <- merge(data, national_data, by = merge_cols, all = T)
    
    # account for NAs in est_with_nat now
    expand_to_locs <- est_with_nat[is.na(toc)]
    expand_to_locs[, `:=` (toc = NULL, location = NULL, geo = NULL)]
    expand_to_locs <- tidyr::crossing(expand_to_locs, 
                                      location = c(unique(data$location)),
                                      toc = unique(data$toc),
                                      geo = unique(data$geo)) %>% as.data.table()
    expand_to_locs[, state := location]
    
    est_with_nat <- rbind(est_with_nat[!is.na(toc)], expand_to_locs)
    
    ## Need to do this again for the new rows that bringing in the national data gives us
    ## Read in pop denoms
    pop_denom_path <- paste0(denoms_path, "/denoms_for_compile/data/geo=", geog, "/toc=", care, "")
    
    
    
    sample_pop <- open_dataset(pop_denom_path) %>% filter(location %in% locs & type == 'total' & year_id == yr) %>% collect() %>% as.data.table()
    
    pop_cols <- c('location', 'state', 'age_group_years_start', 'year_id', 'sex_id', 'pri_payer', 'denom')

    sample_pop <- sample_pop[,..pop_cols]
    
    est_with_nat[denom ==0,denom:=NA]
    
    no_denom <- est_with_nat[is.na(denom)][,denom:=NULL]
    has_denom <- est_with_nat[!is.na(denom)]
    no_denom$geo <- 'state'
    has_denom$geo <- 'state'
    
    pop_by_cols <- pop_cols[!pop_cols == 'denom']
    no_denom <- merge(no_denom, sample_pop, by = pop_by_cols, all.x = T)
    est_with_nat <- rbind(no_denom,has_denom)

    
    ##2. Rake by age, year 
    message('Next step raking to national')
    
    raked <- rake_state_to_nat(data = est_with_nat)
    raked_dt <- raked[[1]]
    weights <- raked[[2]]
    
    raked_dt[is.na(note), note := "added_in_raking"]
  }
  
  
  if (geog == 'county'){

    message('Pulling in state')
    
    ##1. Read in state spend/vol (all states)
    state_data <- arrow::open_dataset(paste0(compile_dir,'geo=state/toc=',care,'/year_id=',yr,'/draw=',draw_num,'/')) 
    
    select_cols <- c('age_group_years_start', 'spend', 'vol', 'sex_id','pri_payer','payer', 'acause', 'state')
    
    state_data <- state_data %>% 
      filter(state == st) %>%
      select(all_of(select_cols)) %>%
      collect() %>%
      as.data.table()
    state_data[,`:=`(year_id=yr, draw=draw_num)]
    
    select_cols <- c(select_cols,'year_id','draw')
    
    setnames(state_data,c('spend','vol'),c('state_spend','state_vol'))

    message('Got state')
    
    # need to expand state_data to all locations in county so the resulting points will have county 
    merge_cols <- select_cols[!(select_cols %in% c('spend','vol'))]
    est_with_st <- merge(data, state_data, by = merge_cols, all = T)
    
    
    # account for NAs in est_with_st now -- missing county estimates where we have state estimates -- expanding for each county
    # note that this is mostly oop-oop!
    # ultimately this expanded dt will have .N = nrow(est_with_st[is.na(loc)]) * # counties
    expand_to_locs <- est_with_st[is.na(toc)]
    expand_to_locs[, `:=` (toc = NULL, location = NULL, geo = NULL)]
    expand_to_locs <- tidyr::crossing(expand_to_locs, 
                                      data.table(location = c(unique(data$location)),
                                                 toc = unique(data$toc),
                                                 geo = unique(data$geo))) %>% as.data.table()
    
    
    
    est_with_st <- rbind(est_with_st[!is.na(toc)], expand_to_locs)
    
    
    ## Need to do this again for the new rows that bringing in the state data gives us
    ## Read in pop denoms
    message('Pulling in denoms')
    pop_denom_path <- paste0(denoms_path, "/denoms_for_compile/data/geo=", geog, "/toc=", care, "")
    
    sample_pop <- open_dataset(pop_denom_path) %>% filter(location %in% locs & type == 'total' & year_id == yr) %>% collect() %>% as.data.table()
    sample_pop <- sample_pop[,.(location, state, age_group_years_start, year_id, sex_id, pri_payer, denom)]
    message('Got denoms')
    
    no_denom <- est_with_st[is.na(denom)][,denom:=NULL]
    has_denom <- est_with_st[!is.na(denom)]
    no_denom$geo <- 'county'
    has_denom$geo <- 'county'
    
    message('Merging pop with county data that has no denom')
    no_denom <- merge(no_denom, sample_pop, by = c('location', 'state','year_id','age_group_years_start','sex_id', 'pri_payer'))
    est_with_st <- rbind(no_denom,has_denom)
    
    
    
    ##2. Rake by state, age, year
    message('Next step raking to state')
    
    raked_dt <- rake_county_to_state(data=copy(est_with_st))
    
    raked_dt <- raked_dt[,c('state_spend','state_vol'):=NULL]
    
    raked_dt[is.na(note), note := "added_in_raking"]
    
  }
  
  return(raked_dt)
  
}

clean_and_check <- function(data){
  print('Cleaning and checking...')
  
  if(length(setdiff(save_cols, colnames(data)) > 1)){
    stop(paste0("missing columns before saving: ", paste0(setdiff(save_cols, colnames(data)),  collapse = ",")))
  }
  data <- data[, ..save_cols]
  
  if(nrow(data[is.na(vol) | is.na(spend)]) > 0){
    stop("NAs in spend or vol right before saving national")
  }
  setorder(data, year_id, age_group_years_start, sex_id, acause)
  
  print('Checks done')
  return(data)
}






