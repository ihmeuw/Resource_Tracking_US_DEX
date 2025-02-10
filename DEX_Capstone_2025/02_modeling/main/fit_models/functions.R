# -------------------------------------------------------
#  FUNCTIONS USED IN TMB MODELING WORKER SCRIPTS
#   Authors: Haley Lescinsky & Azalea Thomson
# -------------------------------------------------------

## Note that the state models are always location IID, unless there are fewer than 51 states in the data, in which case they use the location LCAR (aka the county version of the model)
get_match_script_dict <- function(include_race){
  if (include_race == F){
    match_script_dict <- data.table(
      match_script =    c('glm',       'mod0',      'mod2',       'mod11',       'mod12',                'mod13',       'mod14',               'mod18',               'mod19'),
      interactions =    c('none',      'none',      'ad_td',      'ad_j_td',     'adj_j_tdj',            'ad_j_td',     'adj_j_tdj',           'adj_j_td',            'adj_j_td'), # a = age, d = dataset, t = time, j =location
      location_lcar =   c(NA,          NA,           NA,           T,             T,                     F,             F,                     F,                     T),
      mod_type =        c('intercept', 'intercept', 'no_loc',     'simple',      'complex',              'simple',      'complex',             'intermediate',        'intermediate'),
      geo =             c('any',       'any',       'national',   'county',      'county',               'state',       'state',               'state',               'county'), # we call anything with lcar on location a "county model" though sometimes state uses this.
      re1 =             c(NA,          NA,          'age_source', 'age_source',  'age_source_location',  'age_source',  'age_source_location', 'age_source_location', 'age_source_location'),
      re2 =             c(NA,          NA,          'year_source','location',    'location',             'location',    'location',            'location',            'location'),
      re3 =             c(NA,          NA,           NA,          'year_source', 'year_source_location', 'year_source', 'year_source_location','year_source',         'year_source'),
      re4 =             c(NA,          NA,           NA,           NA,            NA,                     NA,            NA,                    NA,                    NA)
    )
  }else{
    match_script_dict <- data.table(
      match_script =    c('glm',       'mod22',            'mod23',            'mod24',                     'mod25'),
      interactions =    c('none',      'adr_tdr',          'adr_j_tdr',        'adjr_j_tdjr',               'adjr_j_tdr'),
      location_lcar =   c(NA,          NA,                 F,                  F,                           F),
      mod_type =        c('intercept', 'no_loc',           'simple',           'complex',                   'intermediate'),
      geo =             c('any',       'national',         'state',            'state',                     'state'),
      re1 =             c(NA,          'age_source_race',  'age_source_race',  'age_source_location_race',  'age_source_location_race'),
      re2 =             c(NA,          'year_source_race', 'location',         'location',                  'location'),
      re3 =             c(NA,          NA,                 'year_source_race', 'year_source_location_race', 'year_source_race'),
      re4 =             c(NA,          NA,                 NA,                 NA,                          NA)
    )
  }
  return(match_script_dict)
}


library(actuar)

#
#   DATA PREP FUNCTIONS
#
pull_interactive_args <- function(run_id, met){
  L <- list(sim_data = F,
            num_draws = 50,
            drop_outliers = T,
            use_profiled_params = F,
            priors = T,
            set_se_floor = T,
            normalize_mad = F,
            rho_t_alpha = 80,
            rho_t_beta = 2,
            rho_a_alpha= 10,
            rho_a_beta= 2,
            rho_j_alpha= 10,
            rho_j_beta= 2,
            sigma_alpha = 1,
            sigma_beta = 4)

  return(L) 
  
}

pull_parallelized_args <- function(param_template){
  L = list(
    geog = param_template$geo,
    cause = param_template$acause,
    care = param_template$toc,
    met =  param_template$metric,
    pri_pay = param_template$pri_payer,
    sex = param_template$sex_id,
    pay = param_template$payer,

    rho_t_alpha = param_template$rho_t_alpha, 
    rho_t_beta = param_template$rho_t_beta, 
    rho_a_alpha = param_template$rho_a_alpha,
    rho_a_beta = param_template$rho_a_beta,
    rho_j_alpha = param_template$rho_j_alpha,
    rho_j_beta = param_template$rho_j_beta,
    sigma_alpha = param_template$sigma_alpha, 
    sigma_beta = param_template$sigma_beta,
    
    mad_cutoff = param_template$mad_cutoff,
    num_draws = as.numeric(param_template$num_draws),
    drop_outliers = as.logical(param_template$drop_outliers),
    use_profiled_params = as.logical(param_template$use_profiled_params),
    priors = as.logical(param_template$priors),
    set_se_floor = as.logical(param_template$set_se_floor),
    normalize_mad = as.logical(param_template$normalize_mad),
    sim_data = param_template$sim_data,
    model_type = param_template$model_type,
    include_race = param_template$include_race,
    extrapolate_ages = param_template$extrapolate_ages,
    full_age_span = param_template$full_age_span,
    max_model_year = param_template$max_model_year,
    days_metrics = param_template$days_metrics
    )
  
  return(L)
}

prep_data <- function(run_id, param_template, sim_data=F, orig_matching_script, include_race){
  
  #  Purpose: Load in data from post-collapse folder and prepare for modeling
  #
  #  Details: Implements data source restrictions, generates index variables, adds covariates,
  #           adds reference data source, can generate simulated data, checks if lots of zeros
  #  Required args: run_id (#) and param_template (data table with rows for acause, toc, metric, pri_payer, sex_id, payer, geo)
  # 
  #  Returns: a named list, first object is the prepped data
  #
  if (include_race == T){
    data_folder <- 'data_race/'
  }else{
    data_folder <- 'data/'
  }
  
  root_indir <- "FILEPATH"
  
  indir <- paste0(root_indir, data_folder,
                  'metric=',param_template$orig_metric,'/',
                  'geo=',param_template$geo,'/',
                  'toc=',param_template$toc,'/',
                  'pri_payer=',param_template$pri_payer,'/',
                  'payer=',param_template$payer,'/')
  

  raw_data <- arrow::open_dataset(indir)
  data <- raw_data %>%
    filter(acause == param_template$acause &
           sex_id == param_template$sex_id) %>%
    collect() %>%
    data.table()
  if("__index_level_0__" %in% names(data)){
    data[,`__index_level_0__` := NULL ]
  }
  
  if(param_template$pri_payer == 'priv' & include_race == F){
    message('adding MSCAN for 65+ from mdcr_priv')
    indir <- paste0(root_indir, 'data/',
                  'metric=',param_template$orig_metric,'/',
                  'geo=',param_template$geo,'/',
                  'toc=',param_template$toc,'/',
                  'pri_payer=mdcr_priv/',
                  'payer=',param_template$payer,'/')

    if(dir.exists(indir)){
      raw_data_add <- arrow::open_dataset(indir)
      data_add <- raw_data_add %>%
        filter(acause == param_template$acause &
                 sex_id == param_template$sex_id) %>%
        filter(age_group_years_start >= 65 & dataset == "MSCAN" ) %>%
        collect() %>%
        data.table()
      if("__index_level_0__" %in% names(data_add)){
        data_add[,`__index_level_0__` := NULL ]
      }
      data <- rbind(data, data_add)
    }
  
  }
  
  data[,`:=`(
    acause = param_template$acause,
    toc = param_template$toc,
    metric = param_template$orig_metric,
    pri_payer = param_template$pri_payer,
    sex_id = signif(as.numeric(param_template$sex_id), digits=0)
  )]
  
  
  if (sim_data==T){
    set.seed(12345)
    county_re_sd <- 1.5
    county_index <- data.table(location = sort(unique(data$location)))
    # Add a simulated random effect by country
    county_index[, true_county_re := rnorm(.N) * county_re_sd ]
    # Ensure effect has mean zero
    county_index[, true_county_re := true_county_re - mean(true_county_re) ]
    # Add country effects back to the data
    data <- merge(data,county_index, by = 'location') 
    
    age_dt <- data.table()
    age_dt[,age_group_years_start:=sort(unique(data$age_group_years_start))]
    age_dt[, age_index:= 1:.N]
    age_dt[,age_fe:=.25*age_index]
    
    year_dt <- data.table()
    year_dt[,year_id:=sort(unique(data$year_id))]
    year_dt[, year_index:= 1:.N]
    year_dt[,year_fe:=.5*year_index]
    
    data <- merge(data,age_dt,by='age_group_years_start')
    data <- merge(data,year_dt,by='year_id')
    
    # Simulate the raw val
    data[, raw_val := (1+year_fe + age_fe - true_county_re )] #1 + .... - true_county_re + .1
    data[,n_encounters:=rztpois(nrow(data),1)]
    data$se <- 1
    
  }
  

  if (nrow(data)<1){
    orig_ms <- paste0('mod',str_extract(orig_matching_script, "(\\d)+"))
    
    param_template_w_convergence <- copy(param_template)
    param_template_w_convergence[, metric:=orig_met][,orig_metric:=NULL]
    param_template_w_convergence[, `:=` (convergence = 1, 
                                         model_type = match_script_dict[match_script == orig_ms]$mod_type,
                                         match_script = orig_matching_script,
                                         convergence_note = 'Payer source restriction drops all data',
                                         starting_intercept = NA)]
    
    write_dataset(param_template_w_convergence,
                  existing_data_behavior = c("overwrite"),
                  path = paste0(outdir, "/convergence/"),
                  basename_template = paste0("acause_",params, "_part{i}.parquet"))
    message('Payer source restriction drops all data, stopping')
    return(list("data" = data))
  }else{
    
    # add intercept
    data$int <- 1
    
    # ## Add in covariates
    # ## add in columns for icd type and dataset dummies
    data[year_id <= 2014, icd9 := 1]
    data[year_id == 2015, icd9 := 0.75]
    data[year_id >= 2016, icd9 := 0]
    # 
    # ## Designate enrollees aged 65+ for Medicare fixed price effect
    data[age_group_years_start >= 65, a65 := 1]
    data[age_group_years_start < 65, a65 := 0]
    
    data[, a0 := ifelse(age_group_years_start ==0, 1,0)]
    return(list("data" = data))
  }
}

manual_outlier <- function(data, param_template){
  
  data[,outlier:=0]
  
  ## The following outliers are applied based on implausibly high or low values noted after vetting
  
  if (param_template$pri_payer %in% c('mdcr','mdcr_mc','mdcr_mc_mdcd')){
    message('Dropping data under age 20 for MDCR payers')
    data <- data[!(age_group_years_start < 20)]
  }

  if (param_template$toc == 'NF'){
    message('Outliering NF MDCR data for 2015')
    data <- data[dataset == 'MDCR' & year_id ==2015, outlier:=1]
  }
  if (param_template$toc == 'ED'){
    message('Outliering ED MDCR data for 2014')
    data <- data[dataset == 'MDCR' & year_id ==2014, outlier:=1]  
  }
  if (param_template$toc == 'DV'){
    message('Outliering DV MDCR data for 2000-2014')
    data <- data[dataset == 'MDCR' & year_id %in% c(2000,2011,2014), outlier:=1] 
  }
  
  if( (param_template$pri_payer == "mdcr_priv" & param_template$payer == "na") | 
      (param_template$pri_payer == "mdcr_priv" & param_template$payer == 'priv') ){ 
    message('Outliering MSCAN mdcr_priv for ages < 65')
    data <- data[!(dataset == "MSCAN" & age_group_years_start < 65)]
  }
  
  if ( (param_template$toc == 'IP' & param_template$pri_payer == 'mdcr_priv') | 
       (param_template$toc == 'IP' & param_template$pri_payer == 'oop')){
    message('Outerling IP SIDS data for 2016')
    data <- data[dataset == 'SIDS' & year_id ==2016 , outlier:=1]
  }
  
  if (param_template$toc == 'ED' & param_template$pri_payer == 'mdcr_priv'){
    message('Outliering ED SEDD data for 2016 & 2017 for pripayer mdcr_priv')
    data <- data[dataset == 'SEDD' & year_id %in% c(2016,2017) , outlier:=1]
  }
  
  
  if (param_template$toc == 'HH' ){
    data <- data[dataset == 'MDCR' & year_id %in% c(2000,2010) , outlier:=1]
    data <- data[dataset == 'MDCR' & year_id == 2000 & param_template$orig_metric == 'spend_per_encounter', outlier:=1]
  }

  if (param_template$toc == 'DV' & param_template$geo == 'state' & param_template$orig_metric == 'spend_per_encounter' &
      param_template$pri_payer == 'mdcd' & param_template$payer == 'oop'){
    data <- data[location == 'NC',outlier:=1]
  }

  if (param_template$geo == 'state' & param_template$orig_metric == 'encounters_per_person'){
    data <- data[location == 'RI' & year_id == 2016, outlier:=1]
  }
  
  if (param_template$geo == 'county' & param_template$orig_metric == 'encounters_per_person' & 
      param_template$acause == 'exp_donor' & param_template$pri_payer == 'priv' & 
      param_template$toc == 'IP' & param_template$sex_id == 1){
    data <- data[location == 1869 & dataset == 'KYTHERA' & age_group_years_start == 10 & year_id == 2018,outlier:=1]
  }

  
  ## Drop states with greater than 50% UNK in race
  if (include_race == T){
    message('Dropping states with UNK race > 50% (SIDS ME, MD, and NV, and MDCD RI)')
    race_hcup <- fread('FILEPATH')[,.(year,source,race_frac,st_resi)]
    race_cms <- fread('FILEPATH')[,.(year,source,race_frac,st_resi)]
    race_restrict <- rbind(race_hcup, race_cms)
    for (i in 1:nrow(race_restrict)){
      data <- data[!(dataset == race_restrict[i]$source & year_id == race_restrict[i]$year & location == race_restrict[i]$st_resi)]
    }
  }

  data[, outlier_reason:=ifelse(outlier==1, 'manual',NA)]
  if(nrow(data) == 0){
    message(paste0("All data dropped from manual outliering"))
  }
  print(table(data$outlier))
  return(data)
}

outlier_data <- function(data, param_template, mad_cutoff, quant_upper, quant_lower, outlier_group_vars, set_se_floor, normalize_mad){
  
  
  #  Purpose: Outlier data based on plausible data threshold and MADs, and supply a floor for standard deviation 
  #          (spend floor = 1, vol_floor = 25% of the median of the raw_val by age and dataset)
  #
  #  Details: outlier data according to logic below
  #  Required args: data, mad_cutoff, quant_lower/upper, outlier_group_vars
  #  Returns: list (data, p1, p2) |   data now with additional boolean "outlier" column, p1 + p2 are figures of all raw data
  
  if ('outlier' %ni% names(data)){
    data[,outlier:=0]
  }
  data[,outlier_reason:=as.character(outlier_reason)]
  
  
  if (set_se_floor == T){
    
    data[, orig_se := se]
    # ## 1) - set an SE floor (or SE min). Ensure this SE min won't cause data_upper to exceed the threshold
    #------------------------------------------------------
    data[, se_floor:= median(raw_val[raw_val>0])*.25, by = c('age_group_years_start','dataset')]
    data[, se_floor:= ifelse(is.na(se_floor), median(raw_val[raw_val>0])*.25, se_floor), by = c('age_group_years_start')]
    data[, se_floor:= ifelse(is.na(se_floor), median(raw_val[raw_val>0])*.25, se_floor)]
    
    data[, flag_se_floor:= ifelse(se<se_floor, 1, 0)]
    data[se<se_floor, se:= se_floor]
    
    data$se_floor <- NULL
  }
  
  if (param_template$metric %like% 'spend_per_encounter'){
    data[se<1, se:=1] 
  }
  # use SE to calculate data lower and upper
  data[, data_lower := raw_val -1.96*se][data_lower < 0, data_lower := 0]
  data[, data_upper := raw_val +1.96*se]
  
  ## 2) Plausible data threshold
  #---------------------------------------------------------------------------------------------------------
  ## check raw data against plausible thresholds
  threshold <- get_data_threshold(data, param_template)
  
  ## 1) Remove data points with insufficient variation by age/year (i.e. only 1 data point)
  if (param_template$geo != 'national' & model_type != 'intercept'){
    data[, num_age_row := .N, by = c('age_group_years_start')]
    data[, num_year_row := .N, by = c('year_id')]
    data[num_age_row == 1, `:=` (outlier=1, outlier_reason = 'insuff_age_var')]
    data[num_year_row == 1, `:=` (outlier=1, outlier_reason = 'insuff_yr_var')]
    data[,`:=`(num_age_row = NULL, num_year_row = NULL)]
    data[, outlier_no_var:= ifelse(outlier==1, 1, 0)]
  }
  
  if (param_template$orig_metric %like% 'spend' | param_template$toc == 'DV'){
    data[data_upper>=threshold,  `:=` (outlier=1, outlier_reason = 'over_threshold')]
  }else{
    multiplier <- 2
    data[data_upper>=multiplier*threshold, `:=` (outlier=1, outlier_reason = 'upper_over_2threshold')]
    data[raw_val >= threshold,  `:=` (outlier=1, outlier_reason = 'over_threshold')]
  }

 
  ## 3) - MAD outliering. calculate mad and median across non-zero groups (defined by outlier group vars)
  #       Then use mad_cutoff to decide how much to outlier
  #---------------------------------------------------------------------------------------------------------
  
  data[, only_zeros:= ifelse(max(raw_val[outlier==0]) ==0 , 1,0), by = outlier_group_vars]
  if (normalize_mad == T){
    data[,loc_ds_mean:=mean(raw_val[outlier!=1]), by = c('location','dataset')]
    if(param_template$geo == 'county'){
      data[,loc_ds_mean:=mean(raw_val[outlier!=1]), by = c('state','dataset')]
    }
    data[,normalized_raw_val:= ifelse(raw_val != 0, raw_val/loc_ds_mean,0)]
    data[,mad:=ifelse(only_zeros == 0, mad(normalized_raw_val[raw_val!=0 & outlier != 1]),0), by = outlier_group_vars]
    data[,med:=ifelse(only_zeros == 0, median(normalized_raw_val[raw_val!=0 & outlier != 1]),0), by = outlier_group_vars]
    data[,mad_upper := med + mad_cutoff*mad]
    data[,mad_lower := med - mad_cutoff*mad]
    data[normalized_raw_val>mad_upper | normalized_raw_val<mad_lower, `:=` (outlier=1, outlier_reason = 'mad')]
  }else{
    data[,mad:=ifelse(only_zeros == 0, mad(raw_val[raw_val!=0 & outlier != 1]),0), by = outlier_group_vars]
    data[,med:=ifelse(only_zeros == 0, median(raw_val[raw_val!=0 & outlier != 1]),0), by = outlier_group_vars]
    data[,mad_upper := med + mad_cutoff*mad]
    data[,mad_lower := med - mad_cutoff*mad]
    data[raw_val>mad_upper | raw_val<mad_lower,`:=` (outlier=1, outlier_reason = 'mad') ]
  }

  
  ## 4) - Track how many data points have been outliered and plot 
  #---------------------------------------------------------------------------------------------------------
  out <- nrow(data[outlier==1])
  out_due_to_insuff <- nrow(data[outlier_reason %like% 'insuff'])
  out_due_to_manual <- nrow(data[outlier_reason %like%'manual'])
  out_due_to_mad <- nrow(data[outlier_reason %like%'mad'])
  out_due_to_thresh <- nrow(data[outlier_reason %like%'threshold'])
  percent_kept <- round((nrow(data[outlier ==0])/(nrow(data))) *100, digits=2)

  outlier_stat <- ifelse(drop_outliers == T,
                         paste0(percent_kept, "% of data kept post outliering. Of outliered, ",
                                round(out_due_to_thresh/out*100, digits=2), "% due to over threshold, ",
                                round(out_due_to_mad/out*100, digits=2), "% due to MAD, ",
                                round(out_due_to_manual/out*100, digits=2), "% due to manual drops, ",
                                round(out_due_to_insuff/out*100, digits=2), "% due to insufficient variation in ages/years"),
                         paste0("outliering not implemented before fitting"))
  print(outlier_stat)
  
  p1 <- ggplot(data, aes(x = year_id))+
    {if(include_race==T) geom_point(aes(y = raw_val, color = as.factor(race_cd), shape = as.factor(outlier)))}+
    {if(include_race==F) geom_point(aes(y = raw_val, color = as.factor(outlier), shape = as.factor(icd9)))}+
    scale_shape_manual(values=c(1,2,4))+
    {if(param_template$metric =='spend_per_encounter') geom_errorbar(aes(ymin = data_lower, ymax = data_upper, color = as.factor(outlier)))}+
    facet_wrap(~age_group_years_start)+theme_bw()+
    labs(title = paste0("Raw data time trend"), 
         caption = paste0('Outliered data +- ',mad_cutoff, ' MADs\n'),
         subtitle = paste0(params, "\n", outlier_stat))
  
  p2 <- ggplot(data, aes(x = age_group_years_start))+
    {if(include_race==T) geom_point(aes(y = raw_val, color = as.factor(race_cd), shape = as.factor(outlier)))}+
    {if(include_race==F) geom_point(aes(y = raw_val, color = as.factor(outlier), shape = as.factor(icd9)))}+
    scale_shape_manual(values=c(16,17,18,4))+
    {if(param_template$metric =='spend_per_encounter') 
      geom_errorbar(aes(ymin = data_lower, ymax = data_upper, color = as.factor(outlier)))}+
    facet_wrap(~year_id)+theme_bw()+
    labs(title = paste0("Raw data age trend"), 
         caption = paste0('Outliered data +- ',mad_cutoff, ' MADs\n'),
         subtitle = paste0(params, "\n", outlier_stat))
  
  # drop all helper columns
  data[,`:=` (mad = NULL, med = NULL, mad_upper = NULL, mad_lower = NULL)] #, med_se = NULL, se_diff = NULL)]
  
  
  if (length(unique(data$outlier)) > 2){
    stop('Some data not categorized as outlier or not')
  }
  
  if ( nrow(data) == nrow(data[outlier==1]) ){
    warning('All data outliered')
  }
  message(paste0('Data has ', nrow(data[outlier==0]), ' non-outliered rows'))
  
  param_template[, n_ages:= length(unique(data[outlier==0]$age_group_years_start))]
  param_template[, n_years:= length(unique(data[outlier==0]$year_id))]
  if (include_race == T){
    param_template[, n_race:= length(unique(data[outlier==0]$race_cd))]
  }
  param_template[, n_row:= nrow(data[outlier==0])]
  param_template[, n_zeros:=nrow(data[raw_val==0 & outlier==0])]
  param_template[, outlier_mad := out_due_to_mad]
  param_template[, outlier_thresh := out_due_to_thresh]
  param_template[, outlier_insuff := out_due_to_insuff]
  param_template[, outlier_man := out_due_to_manual]
  
  return(list(data,p1,p2))
  
}


pull_other_metric_ages <- function(run_id, param_template, include_race, full_age_span, data){
  
  if (full_age_span == F){
    
    if ( (param_template$metric == 'spend_per_encounter') | 
         (param_template$metric == 'encounters_per_person' & param_template$geo != 'national')) {
      message('Pulling in national utilizaton data to get other ages')
      param_template2 <- copy(param_template)
      param_template2$payer <- 'na'
      param_template2$geo <- 'national'
      if (days_metrics == F){
        param_template2$metric <- 'encounters_per_person'
        param_template2$orig_metric <- 'encounters_per_person'
      }else{
        param_template2$metric <- 'encounters_per_person'
        param_template2$orig_metric <- 'days_per_encounter'
      }

      other_ages <- get_other_metric_data(param_template2)
    }else{
      other_ages <- c()
    }
    
  }else{
    # age sex restrictions
    ASR <- apply_asr(data, param_template$toc, param_template$acause)
    possible_ages <- ASR$possible_ages
    other_ages <- possible_ages
      
  }

  return(other_ages)
}

get_other_metric_data <- function(param_template2){
  if (include_race == T){
    data_folder <- 'data_race/'
  }else{
    data_folder <- 'data/'
  }
  
  root_indir <- "FILEPATH/"
  indir <- paste0(root_indir, data_folder,
                  'metric=',param_template2$metric,'/',
                  'geo=national/',
                  'toc=',param_template2$toc,'/',
                  'pri_payer=',param_template2$pri_payer,'/',
                  'payer=',param_template2$payer,'/')
  if (dir.exists(indir)){
  
    # Data - pulls data and returns it with merged covariates, int, icd9, a65
    other_data <- prep_data(run_id, param_template2, sim_data = F, orig_matching_script, include_race)
    other_data <- other_data$data
    
    if (include_race == T){
      other_data <- other_data[!(race_cd %in% c('OTH','MULT','UNK'))]
      other_data[race_cd == 'BLK', race_cd:='BLCK']
      if ( any(unique(other_data$race_cd) %ni% c('BLCK','WHT','API','AIAN','HISP')) ){
        stop('You have incorrect race/ethnicity values in your data')
      }
    }
    
    if (nrow(other_data)>0){
      
      ## Drop specific outliers
      other_data <- manual_outlier(other_data, param_template2)
      
      if (other_data[outlier==0,.N]>0){
        other_ages <- min(other_data[outlier==0]$age_group_years_start):max(other_data[outlier==0]$age_group_years_start)
        
        ## get full list of possible ages
        # age sex restrictions
        ASR <- apply_asr(other_data, param_template$toc, param_template$acause)
        possible_ages <- ASR$possible_ages
        
        other_ages <- other_ages[other_ages %in% possible_ages]
      }else{
        other_ages <- c()
      }

      
      
    }else{
      print('No data')
      other_ages <- c()
    }
  }else{
    print('No data')
    other_ages <- c()
  }
    
    
  
 
  return(other_ages)
  
}

index_data <- function(data, param_template, run_id, include_race, extrapolate_ages){
  
  # age sex restrictions
  ASR <- apply_asr(data, param_template$toc, param_template$acause)
  data <- ASR$data
  
  # Rectangularize age range for use in prediction index  
  ## Generate static ASR age range
  possible_ages <- ASR$possible_ages
  
  # ages +  years in data
  ages <- unique(min(data[outlier==0]$age_group_years_start):max(data[outlier==0]$age_group_years_start))

  ages <- intersect(ages, possible_ages)     # limits to just 5 year age bins
  years <- unique(data$year_id)
  
  races <- unique(data[outlier==0]$race_cd)
  index_key <<- create_index_key(param_template$geo, ages, races)
  
  
  data <- merge(data, index_key, by = c(intersect(colnames(data), colnames(index_key))), all.x = T)
  
  if (include_race == T){
    data <- data[order(age_index,location_index,year_index,race_index)]
  }else{
    data <- data[order(age_index,location_index,year_index)]
  }
  
  
  # ## pick reference dataset
  ref_dataset_dt <- fread("FILEPATH/static_files/RESTRICTIONS/toc_payer_reference_data.csv")
  ref_dataset_dt[reference_dataset == "", reference_dataset := NA]
  reference_dataset <- unique(ref_dataset_dt[toc == param_template$toc &
                                        pri_payer == param_template$pri_payer &
                                        payer == param_template$payer &
                                        geo == param_template$geo, reference_dataset])
  if(length(reference_dataset)!=1){
    warning("No reference dataset")
  }

  
  data[, reference_dataset := ifelse(dataset == reference_dataset, 1, 0)]
  
  if(nrow(data[reference_dataset == 1]) ==0 ){
    message(paste0("Preferred reference dataset (", reference_dataset, ") does not exist in data"))
    reference_dataset <- data[,.N, by = c("dataset")][N == max(N), dataset]
    if (length(unique(reference_dataset))>1){
      reference_dataset <- reference_dataset[2]
    }
    message(paste0("Updating reference dataset to ", reference_dataset, " due to data quantity"))
    data[, reference_dataset := NULL]
    data[, reference_dataset := ifelse(dataset == reference_dataset, 1, 0)]
  }
  
  
  # determine non reference datasets and add them as columns
  non_ref_datasets <- unique(data[reference_dataset == 0]$dataset)

  
  param_template[, ref_dataset := reference_dataset]
  param_template[, priors:= priors]
  param_template[, re_fix:=F]
  param_template[, draws_fix:=F]
  
  return(list("data" = data, "years" = years, "ages" = ages, 
              "ref_dataset" = reference_dataset, "non_ref_datasets" = non_ref_datasets))
  
}

apply_asr <- function(data, TOC,cause,data_acause_col = "acause",data_age_col = "age_group_years_start", data_sex_col = "sex_id"){
  
  #  Purpose: Some health conditions are restricted to only certain ages or sexes. Ensure data fits these restrictions. 
  #  Required args: data, toc, acause
  #  Returns: dt (with any excluded rows dropped)
  #
  
  ## get AS
  as_cols <- c("male", "female", "age_start", "age_end")
  as <- fread("FILEPATH/static_files/RESTRICTIONS/toc_cause_age_sex.csv")
  as <- as[acause == cause & toc == TOC, c("acause", as_cols), with = F]
  
  ## ensure correct types
  stopifnot(is.numeric(data[,get(data_age_col)]))
  stopifnot(is.numeric(data[,get(data_sex_col)]))
  
  setnames(data, data_acause_col, "acause") ## change acause name as needed
  if(any(as_cols %in% names(data))){
    stop(paste0("data can't have columns named ", paste0(as_cols, collapse = ", ")))
  }
  
  ## merge with data
  merged <- merge.data.table(data, as, by = "acause", all.x = TRUE) ## left merge
  setnames(merged, "acause", data_acause_col) ## reset acause name as needed
  
  missed <- merged[is.na(age_start)]
  if(nrow(missed) > 0){
    print(missed)
    stop("Restrictions information not found for rows printed above")
  }
  
  conditions <- paste0(c(
    "get(data_age_col) >= age_start", ## only ages above age start (inclusive)
    "get(data_age_col) <= age_end", ## only ages below age end (inclusive)
    "!(get(data_sex_col) == 1 & male == 0)", ## no male rows with male == 0
    "!(get(data_sex_col) == 2 & female == 0)" ## no female rows with female == 0
  ), collapse = " & ")
  
  ## apply ASR
  out <- merged[eval(parse(text = conditions))]
  dropped <- merged[eval(parse(text = paste0("!(", conditions, ")")))]
  
  ## drop extra columns
  out[,(as_cols) := NULL]
  dropped[,(as_cols) := NULL]
  
  ## show what we dropped
  if(nrow(dropped) > 0){
    print(dropped)
    message("Dropping the rows printed above")
  }
  
  ## get full list of possible ages
  possible_ages <- c(0,1,5*1:17)
  possible_ages <- possible_ages[possible_ages >= as$age_start & possible_ages <= as$age_end]
  
  ## save out
  return(list("data" = out, "possible_ages" = possible_ages))
}

create_index_key <- function(geo, ages, races){
  
  #  Purpose: Create one source of truth for indices
  #
  #  Details:  We use the same year range and location list every time (independent of which ages/locs are in the data)
  #            BUT we use the ages in the data
  #  Required args: geo (county or state) and ages in the data
  #  Returns: a grid that shows which age/year/loc responds to which index as well as all possible combinations
  #
  
  year_key <- data.table(year_id = 2000:max_model_year)
  year_key[, year_index := as.integer(year_id - min(year_id))]
  year_key[, year_index_1 := year_index + 1]
  
  
  age_key <- data.table(age_group_years_start = ages)
  age_key[, age_index := as.integer(factor(age_group_years_start, levels = ages)) - 1]
  age_key[, age_index_1 := age_index + 1]
  
  if(geo == "national"){
    location_key <- data.table(location = "USA", location_name = "USA", location_index = 0, location_index_1 = 1)
    
  }else if(geo == "county"){
    location_key <- fread("FILEPATH/static_files/RESTRICTIONS/county_names.csv")[,.(location, location_name)]
    location_key[, location := as.character(location)]
    location_key[, location_index := as.integer(location)] # order in matrix is based on mcnty id
    location_key[, location_index_1 := location_index + 1]
    
    # add on state
    location_key[, state := gsub(" - .*$", "", location_name)]
    location_key[, state_index := as.integer(factor(state)) - 1] # order in matrix is alphabetical by location_name 
    location_key[, state_index_1 := state_index + 1]
    
  }else if(geo == "state"){
    location_key <- fread("FILEPATH/static_files/RESTRICTIONS/state_names.csv")
    location_key[, location := as.character(location)]
    location_key[, location_index := as.integer(factor(location, levels = location_key[order(location_name)]$location)) - 1] # order in matrix is alphabetical by location_name 
    location_key[, location_index_1 := location_index + 1]
  }
  
  index_key <- tidyr::crossing(year_key, age_key, location_key) %>% as.data.table()
  
  if (include_race == T){
    race_key <- data.table(race_cd = races)
    race_key[,race_index:=  as.integer(as.factor(race_cd)) - 1]
    race_key[, race_index_1 := race_index + 1]
    index_key <- tidyr::crossing(index_key, race_key) %>% as.data.table()
  }
  return(index_key)
  
}

prep_matricies <- function(data, geo){
  
  #  Purpose: Load adjacency matricices
  #
  #  Details:  Read geo specific adjacency matricies, prep age and time by using the 'ages' and 'years' variables
  #  Required args:  data (dt) & geo (string); also must have ages and years loaded
  #  Returns: a list of the 3 matricices: graph_j, graph_a, graph_t
  #
  
  ## Set up adjacency matrices
  if(geo == "county"){
    adjmat_file <- "/FILEPATH/adjacency_matrix_county.rds"
    adjmat <- readRDS(adjmat_file)
    
    graph_j <- diag(apply(adjmat, 1, sum)) - adjmat
    graph_j <- as(graph_j, "dgTMatrix")
  }else if (geo == 'state'){
    adj_mat <- readRDS("/FILEPATH/adjacency_matrix_state.rds") # State level adjacency with AK/WY, CA/HI/WA, ME/VT as forced adjacency
    graph_j <- diag(apply(adj_mat, 1, sum)) - adj_mat
    graph_j <- as(graph_j, "dgTMatrix") # TMB-ready sparse state adjacency matrix
  }
  
  if (geo != 'national'){
    num_j <- dim(graph_j)[[1]]
    if (length(unique(data[outlier==0]$location_index)) > num_j) stop("there are more locations in data than in loc adjacency matrix")
  }else{
    graph_j <- 0
  }
  
  if(length(years) > 1) {
    num_t <- max(index_key$year_index_1)
    graph_t <- matrix(rep(0, num_t^2), nrow = num_t, ncol = num_t)  # matrix of zeros
    graph_t[abs(row(graph_t) - col(graph_t)) == 1] <- 1
    graph_t <- diag(apply(graph_t, 1, sum)) - graph_t
    graph_t <- as(graph_t, "dgTMatrix")
  } else {
    num_t <- 1
    graph_t <- matrix(rep(1, num_t^2), nrow = num_t, ncol = num_t)  # matrix of zeros
    graph_t <- diag(apply(graph_t, 1, sum)) - graph_t
    graph_t <- as(graph_t, "dgTMatrix")
  }
  
  if (length(ages) > 1) {
    num_a <- max(data[outlier==0]$age_index) + 1
    graph_a <- matrix(rep(0, num_a^2), nrow = num_a, ncol = num_a)
    graph_a[abs(row(graph_a) - col(graph_a)) == 1] <- 1
    graph_a <- diag(apply(graph_a, 1, sum)) - graph_a
    graph_a <- as(graph_a, "dgTMatrix")
  } else {
    num_a <- 1
    graph_a <- matrix(rep(1, num_a^2), nrow = num_a, ncol = num_a)  # matrix of zeros
    graph_a <- diag(apply(graph_a, 1, sum)) - graph_a
    graph_a <- as(graph_a, "dgTMatrix")
  }
  
  
  return(list("graph_a" = graph_a,
              "graph_j" = graph_j,
              "graph_t" = graph_t))
}

count_variables <- function(graph_a,graph_j,graph_t) {
  
  #  Purpose: count the dimensions of a, j, and t matricies
  #
  #  Details: 
  #  Required args: graph_a, graph_j, graph_t (all square)
  #  Returns: named list with number
  #
  
  num_a <- dim(graph_a)[[1]] 
  num_j <- dim(graph_j)[[1]] #max(data$location_index) + 1
  num_t <- dim(graph_t)[[1]] #max(data$year_index) + 1
  
  if(is.null(num_a)){
    print("no variation in age!")
    num_a <- 1
  }
  
  if(is.null(num_j)){
    print("no variation in location!")
    num_j <- 1
  }
  
  if(is.null(num_t)){
    print("no variation in year!")
    num_t <- 1
  }
  
  
  return(list("num_a"=num_a, "num_j" = num_j, "num_t" = num_t)) #"num_j" = num_j,
  
}

get_data_threshold <- function(data, param_template){
  
  #  Purpose: Determine maximum data value
  #
  #  Details: In some cases this is acause specific
  #  Required args: nothing (needs met, care, cause loaded in memory)
  #  Returns: number 
  #
  
  thresholds_dt <- fread('FILEPATH')
  threshold_dt <- thresholds_dt[metric==param_template$orig_metric & toc ==param_template$toc]
  if(cause %in% threshold_dt$acause){
    threshold <- threshold_dt[acause == cause]$threshold
  }else{
    threshold <- threshold_dt[is.na(acause)]$threshold
  }
  
  message(paste0('Threshold is: ',threshold))
  
  return(threshold)
  
}

assess_match_script <- function(data, matching_script, geog, years, ages, param_template, model_type){
  
  use_locs <- length(unique(data[outlier==0]$location_index_1))
  use_years <- length(unique(data[outlier==0]$year_id))
  use_ages <- length(unique(data[outlier==0]$age_group_years_start))

  
  ## Added due to poor MEPS quality
  if(geog == 'national' & param_template$orig_metric == 'spend_per_encounter' & param_template$pri_payer == 'oop' & param_template$toc == 'HH'){
    print("for national HH oop spend models, not doing a time trend")
    use_years <- 1
  }
  
  restriction <<- c()
  if (param_template$n_row ==0){
    restriction <- 'all_outliered'
  }
  ## 1. Determine matching script
  if (model_type != 'intercept'){
    
    # Year & age restriction
    if(use_ages == 1 & use_years == 1 ){
      restriction <- paste0(restriction, 'age_year')
      # model_type <- 'intercept'
    }
    
    # Zeros restriction
    if ( param_template$n_zeros == nrow(data[outlier==0]) & param_template$n_row != 0){
      restriction <- paste0(restriction,'only_zeros')
    }
    
    # We want dental models to start at intermediate
    if(model_type == 'complex' & care == 'DV'){
      print("Running a complex dental model, but want to use intermediate")
      restriction <- paste0(restriction, 'use_intermediate')
    }
    
    # If there isn't data coverage for all states, we should use LCAR
    if(use_locs < 51 & geog == 'state' & include_race == F){
      print("Not enough location coverage for IID, using LCAR")
      matching_script <- paste0(match_script_dict[location_lcar == T & mod_type == model_type]$match_script)
    }
    
    # if more than 1 age and 2 or 3 years and a county or state model remove year-loc interaction
    if (model_type == 'complex' & use_ages > 1 & use_years < 4 & use_years > 1 & geog %in% c('county','state') ){
      print("Less than 4 years but more than 1 year, halting now, will use intermediate model")
      restriction <- paste0(restriction, 'use_intermediate')
    }
    
    # Add priors
    if (priors == T){
      matching_script <- paste0(matching_script, '_priors')
    }
    
    # If only one age or year, remove age or year variation
    if (use_ages == 1 & use_years > 1 & model_type != 'intermediate'){
      print("Only one age - removing age from model")
      matching_script <- paste0(matching_script, '_noage')
    }else if (use_ages == 1 & use_years > 1 & model_type == 'intermediate'){
      print("Only 1 age, need to use simple")
      restriction <- paste0(restriction, 'use_simple')
    }
    
    if (use_years == 1 & use_ages > 1 & model_type != 'intermediate'){
      print("Only one year - removing year from model")
      matching_script <- paste0(matching_script, '_noyear')
    }else if (use_years == 1 & use_ages > 1 & model_type == 'intermediate'){
      print("Only 1 year, need to use simple")
      restriction <- paste0(restriction, 'use_simple') 
    }
    
    
  }else {
    
    if (include_race == T){
      matching_script <- 'glm'
    }else{
      ## Intercept model
      if (nrow(data[outlier==0])>100 | param_template$n_zeros == nrow(data[outlier==0]) ){
        matching_script <- 'glm'
      }else{
        matching_script <- 'mod0'
        ## Add priors
        if (priors == T){
          matching_script <- paste0(matching_script, '_priors')
        }
      }
    }

    
  }

  ## 2. Determine sufficient number of data points for model complexity
  if(model_type == "complex"){
    sufficient_data_to_model_threshold <- 100
  }else if (model_type == 'intercept'){
    sufficient_data_to_model_threshold <- 1
  }else if(geog!='national'){
    sufficient_data_to_model_threshold <- 50
  }else{
    # lower threshold for national models
    sufficient_data_to_model_threshold <- 5
  }
  
  if (nrow(data[outlier==0]) < sufficient_data_to_model_threshold){
    restriction <- paste0(restriction, 'insufficient_data')
  }
  
  ## 3. Update param template
  if (matching_script != 'glm'){ms <- paste0('mod',str_extract(matching_script, "(\\d)+"))}else{ms <- 'glm'}
  
  param_template[, match_script := matching_script]
  param_template[, model_type:= match_script_dict[match_script == ms]$mod_type]


  if(drop_outliers){
    nrowdata <- nrow(data[outlier==0])
    param_template[, percent_outliered:= round(nrow(data[outlier == 1])/nrow(data), digits=4)]
    param_template[, se_range:= round(max(data[outlier!=1]$se) - min(data[outlier!=1]$se), digits=3)]
    param_template[, encount_people := nrow(data[outlier ==0 & n_encounters>n_people])]
  }
  
  if (include_race == T){
    race_codes <- c("WHT", "BLCK", "HISP", "AIAN", "API")
    # Dynamically create binary indicator columns
    for (race in race_codes) {
      param_template[, (race) := 0]
      param_template[, paste0('need_',(race)) := 0]
    }
  }

  
  ## Return 
  if ( is.null(restriction) ){
    
    if (include_race == T){
      use_races <-   unique(data[outlier==0]$race_cd)
      
      race_codes <- c("WHT", "BLCK", "HISP", "AIAN", "API")
      # Dynamically create binary indicator columns
      for (race in race_codes) {
        param_template[, (race) := ifelse(race %in% use_races, 1,0)]
      }
      
      ## Read in payer pop denoms with only 1 race with pop >0 for a stratification to ensure we have data for this race 
      denoms_with1race <- arrow::open_dataset('FILEPATH') %>% collect() %>% as.data.table()
      if (param_template$metric == 'spend_per_encounter'){
        if (param_template$pri_payer == 'mdcr' & param_template$payer == 'mdcr'){
          denoms_with1race <- denoms_with1race[pri_payer%in% c('mdcr_mdcd','mdcr_priv','mdcr') & sex_id==sex & toc ==care]
        }
        if (param_template$pri_payer == 'mdcd'){
          denoms_with1race <- denoms_with1race[pri_payer%in% c('mdcd','mdcd_mc') & sex_id==sex & toc ==care]
        }
        if (param_template$pri_payer == 'mdcr'){
          denoms_with1race <- denoms_with1race[pri_payer%in% c('mdcr','mdcr_mc') & sex_id==sex & toc ==care]
        }
        if (param_template$pri_payer == 'mdcr_mdcd'){
          denoms_with1race <- denoms_with1race[pri_payer%in% c('mdcr_mdcd','mdcr_mc_mdcd') & sex_id==sex & toc ==care]
        }
      }else{
        denoms_with1race <- denoms_with1race[pri_payer==pri_pay & sex_id==sex & toc ==care]
      }
      
      denoms_with1race <- unique(denoms_with1race[age_group_years_start %in% unique(index_key$age_group_years_start),.(race_cd,age_group_years_start)])[,need_race:=1]
      check <- merge(data,denoms_with1race, all = T, by = c('race_cd','age_group_years_start'))
      need_races <- setdiff(unique(check[is.na(raw_val)]$race_cd),use_races)
      
      if ( length(need_races)>0){
        message(paste0('Need: ',toString(need_races)))
        for (race in race_codes) {
          param_template[, paste0((race),"_need") := ifelse(race %in% need_races, 1,0)]
        }
      }
      
      
    }
    message('Returning updated matching script and param_template')
    message('Matching script is ', matching_script)
    return(list('matching_script'=matching_script,
                'param_template' = param_template))
  }else{
    message('Data not fit for modeling due to: ', restriction,'. Writing out non convergence param template.')
    param_template_w_convergence <- copy(param_template)
    param_template_w_convergence[, metric:=orig_met][,orig_metric:=NULL]
    param_template_w_convergence[, `:=` (convergence = 1,
                                         convergence_note = paste0('Restriction: ',restriction),
                                         starting_intercept = NA,
                                         rho_t = NA,
                                         rho_a = NA)]
    
    write_dataset(param_template_w_convergence,
                  existing_data_behavior = c("overwrite"),
                  path = paste0(outdir, "/convergence/"),
                  basename_template = paste0("acause_",params, "_part{i}.parquet"))

    
    return(list('restriction' = restriction))
  }
  
}

#
#   TMB SET UP FUNCTIONS
#

parse_re_names <- function(matching_script){
  
  ms <- paste0('mod',str_extract(matching_script, "(\\d)+"))
  
  if (ms != 'mod0'){
    ## Takes the match_script_dict re column and splits into a vector at the underscore
    re1_names <- if (is.na(match_script_dict[match_script == ms]$re1)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re1, '_') }
    re2_names <- if (is.na(match_script_dict[match_script == ms]$re2)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re2, '_') }
    re3_names <- if (is.na(match_script_dict[match_script == ms]$re3)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re3, '_') }
    re4_names <- if (is.na(match_script_dict[match_script == ms]$re4)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re4, '_') }
    
    ## Remove NAs
    re_name_list <- list(re1_names, re2_names, re3_names, re4_names)
    re_name_list <- re_name_list[!(is.na(re_name_list))]
    
    ## Remove age or year
    if (matching_script %like% 'noage'){
      # re_name_list <- lapply(1:length(re_name_list), function(x) re_name_list[[x]][re_name_list[[x]] != "age"])
      re_name_list <- lapply(re_name_list, function(x) if('age' %in% x) x <- NA else x)
    }
    if (matching_script %like% 'noyear'){
      # re_name_list <- lapply(1:length(re_name_list), function(x) re_name_list[[x]][re_name_list[[x]] != "year"])
      re_name_list <- lapply(re_name_list, function(x) if('year' %in% x) x <- NA else x)
    }
    
    num_res <- length(re_name_list)
  }else{
    if (include_race == T){
      re_name_list <- list('iid_error','race_iid')
    }else{
      re_name_list <- list('iid_error')
    }

    num_res <- 0
  }

  
  return(list('re_name_list' = re_name_list,
              'num_res' = num_res))
}

parse_res <- function(matching_script){
  ms <- paste0('mod',str_extract(matching_script, "(\\d)+"))
  
  if (ms != 'mod0'){
    ## Takes the match_script_dict re column and splits into a vector at the underscore
    re1_names <- if (is.na(match_script_dict[match_script == ms]$re1)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re1, '_') }
    re2_names <- if (is.na(match_script_dict[match_script == ms]$re2)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re2, '_') }
    re3_names <- if (is.na(match_script_dict[match_script == ms]$re3)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re3, '_') }
    re4_names <- if (is.na(match_script_dict[match_script == ms]$re4)){ NA}else{ str_split_1(match_script_dict[match_script == ms]$re4, '_') }
    
    ## Remove NAs
    re_name_list <- list(re1_names, re2_names, re3_names, re4_names)
    re_name_list <- re_name_list[!(is.na(re_name_list))]
    
    ## Remove age or year
    if (matching_script %like% 'noage'){
      re_name_list <- lapply(re_name_list, function(x) if('age' %in% x) x <- NA else x)
    }
    if (matching_script %like% 'noyear'){
      re_name_list <- lapply(re_name_list, function(x) if('year' %in% x) x <- NA else x)
    }
    
    ## Pulls all num_j, num_d, num_a, num_t objects and replaces them in the list
    re_dim_list <- lapply(re_name_list, function(x) gsub("location", get("num_j"), x))
    re_dim_list <- lapply(re_dim_list, function(x) gsub("source", get("num_d"), x))
    re_dim_list <- lapply(re_dim_list, function(x) gsub("race", get("num_r"), x))
    re_dim_list <- lapply(re_dim_list, function(x) gsub("age", get("num_a"), x))
    re_dim_list <- lapply(re_dim_list, function(x) gsub("year", get("num_t"), x))
    re_dim_list <- lapply(re_dim_list, function(x) as.numeric(x))
    
    
    ## Makes the RE matrix of zeros with the appropriate dimensions!
    re_list <- list()
    for (i in 1:length(re_dim_list) ){
      
      if ( !(NA %in% re_name_list[[i]]) ){
        num_dims <- length(re_dim_list[[i]])
        if (num_dims == 4){
          zero_matrix <- rep(0,rev(re_dim_list[[i]])[1]*rev(re_dim_list[[i]])[2]*rev(re_dim_list[[i]])[3]*rev(re_dim_list[[i]])[4])
        }
        if (num_dims == 3){
          zero_matrix <- rep(0,rev(re_dim_list[[i]])[1]*rev(re_dim_list[[i]])[2]*rev(re_dim_list[[i]])[3])
        }
        if (num_dims == 2){
          zero_matrix <- rep(0,rev(re_dim_list[[i]])[1]*rev(re_dim_list[[i]])[2])
        }
        if (num_dims == 1){
          zero_matrix <- rep(0,re_dim_list[[i]])
          re <- zero_matrix
        }else{
          re <- array(zero_matrix, dim = re_dim_list[[i]])
        }
        
        re_list[[paste0('re',i)]] <- re
      }
    }
  }else{
    if (include_race == T){
      re_list <- list(iid_error = rep(0, nrow(data[ii])),
                      race_iid = rep(0, num_r))
                      
    }else{
      re_list <- list(iid_error = rep(0, nrow(data[ii])))
    }

  }
  
  
  return(re_list)
  
}

parse_re_priors <- function(matching_script, re_name_list){
  
  ms <- paste0('mod',str_extract(matching_script, "(\\d)+"))

  if (ms != 'mod0'){
    re_prior_list <- list()
    for (i in 1:length(re_name_list)){
      
      if ( !(NA %in% re_name_list[[i]]) ){
        
        re_prior_list[[paste0('re',i,'_log_sigma')]] <- 0
        if (length(re_name_list[[i]]) >1 ){
          if ('age' %in% re_name_list[[i]] ){
            re_prior_list[[paste0('logit_rho_',i,'a')]] <- 0
          }
          if ('year' %in% re_name_list[[i]] ){
            re_prior_list[[paste0('logit_rho_',i,'t')]] <- 0
          }
          if ('location' %in% re_name_list[[i]]){
            re_prior_list[[paste0('logit_rho_',i,'j')]] <- 0
          }
        }else{
          re_prior_list[[paste0('logit_rho_',i)]] <- 0
        }
        
      }
    }
  }else{
    if (include_race == T){
      re_prior_list <- list(log_sigma = 0)
    }else{
      re_prior_list <- list()
    }

  }
  
  return(re_prior_list)
}


#
#   DATA FIT + PREDICTION FUNCTIONS
#
get_intercepts <- function(use_profiled_params, met){
  if (use_profiled_params == F){ 
    start_val <- c() 
  }
  if (use_profiled_params == F | is.null(start_val)){ 
    lin_mod <- lm(as.formula(paste0("raw_val ~ int")), data)
    starting_fes <- lin_mod$coefficients
    start_val <- round(log(as.numeric(starting_fes["(Intercept)"])))
  }
  
  if (met %like% 'spend'){
    intercepts <- c(start_val, 0,1,max_starting_intercept)
  }else{
    intercepts <- c(start_val, 0,max_starting_intercept)
  }
  return(intercepts)
  
}

fit_model <- function(covars, data, tmb_data, tmb_par, starting_intercept, include_race){
  
  #  Purpose: fit TMB model with tmb_data and tmb_par
  #
  #  Required args: covars (vector), data (dt), tmb_data (list), tmb_par (list), starting_intercept (number)
  #  Returns: the fitted model object
  #
  
  
  res_to_fit <- names(tmb_par)
  res_to_fit <- res_to_fit[res_to_fit %like% "re[0-9]*$" | res_to_fit == 'iid_error']
  if(length(res_to_fit) == 0){
    res_to_fit <- NULL
  }
  
  
    
  ## Fit with tmb function--------------------
  if (include_race == T){
    tmb_template_path <- paste0(here, '/race_templates/', matching_script, ".cpp")
  }else{
    tmb_template_path <- paste0(here, '/templates/', matching_script, ".cpp")
  }
  
  
  tmb_function <- set_up_tmb_function(
    data_list = tmb_data,
    params_list = tmb_par,
    tmb_random = res_to_fit,  
    tmb_map = list(), # tmb_map is an advanced option that fixes some parameters at their starting values 
    custom_template = tmb_template_path,
    outer_fit_verbose = F,
    inner_fit_verbose = F)
  
  # Fit TMB model object
  model_fitted <- try(fit_tmb_model(
    tmb_adfunction = copy(tmb_function),
    get_joint_precision = TRUE,
    lower = -Inf,
    upper = Inf))
  
  # Save metadata 
  if(class(model_fitted) == "try-error"){
    print("Model did not finish fitting without error")
    convergence_status <- 1
    convergence_note <- as.character(model_fitted)
    
  }else{
    print("Model finished fitting!")
    convergence_status <- model_fitted$opt$convergence
    convergence_note <- model_fitted$opt$message
    
    if(any(is.nan(model_fitted$sdrep$cov.fixed)) ){
      convergence_status <- 1
      convergence_note <- paste0(model_fitted$opt$message, ", NANs")
    }
  }
  
  print(paste0("Model convergence status = ",convergence_status))
  
  #save
  convergence_status <<- convergence_status # make global variable to save
  convergence_note <<- convergence_note
  return(model_fitted)
    
  
}

fit_glm_model <- function(data, distribution,num_draws){
  ##----------------------------------------------------------------------
  ## Run a GLM/LM if TMB didn't converge
  ##----------------------------------------------------------------------  
  message('Running a GLM model')
  if (include_race == T & num_r>1 ){
    race_glm <- T
  }else{
    race_glm <- F
  }
  # Run a GLM model (no random effects)
  if (distribution == 'poisson'){
    
    if (race_glm == T){
      glm_results <- stats::glm(
        formula = "n_encounters ~ race_cd + offset(log(n_people))", family = poisson,
        data = data[ii]
      )
      intercept_to_add <- as.numeric(glm_results$coefficients[1])
      intercept <- as.numeric(glm_results$coefficients) #|> exp()
      intercept[-1] <- intercept[-1] + intercept_to_add
      se <- summary(glm_results)$coefficients[,2] %>%  as.numeric() 
    }else{
      glm_results <- stats::glm(
        formula = "n_encounters ~ 1 + offset(log(n_people))", family = poisson,
        data = data[ii]
      )
      intercept <- as.numeric(glm_results$coefficients) #|> exp()
      se <- summary(glm_results)$coefficients[[2]]
    }

    message("Estimate from stats::glm was ", intercept |> exp() |> round(5), " (SE = ", se |> round(5), ")")
    
  }else{

    if (race_glm == T ){
      lm_results <- lm(
        formula = raw_val ~ race_cd,
        weights = se,
        data = data[ii]
      )
      intercept_to_add <- as.numeric(lm_results$coefficients[1])
      intercept <- as.numeric(lm_results$coefficients)
      intercept[-1] <- intercept[-1] + intercept_to_add
      se <- as.numeric(summary(lm_results)$coefficients[,2])
      
    }else{
      lm_results <- lm(
        formula = raw_val ~ 1,
        weights = se,
        data = data[ii]
      )
      
      intercept <- as.numeric(lm_results$coefficients[1])
      se <- summary(lm_results)$coefficients[[1,2]]
    }

    message("Estimate from stats::glm was ", intercept |> round(5), " (SE = ", se |> round(5), ")")
    
  }
  
  fe_betas <- intercept
  ##----------------------------------------------------------------------
  ## Generate draws
  ##----------------------------------------------------------------------  
  
  # if only 1 row
  if('NaN' %in% se){
    message("se is NAN, setting to 05.*int")
    se <- 0.5 * intercept
  }
  
  if (race_glm==T){
    for (i in 1:length(intercept)){
      if(se[i] > 2*max(data$raw_val)){
        message('SE too big:', se[i], ', resetting se to ',2*max(data$raw_val))
        se[i] <- 2*max(data$raw_val)
      }
    }

  }else{
    if(se > 2*max(data$raw_val)){
      message('SE too big:', se, ', resetting se to ',2*max(data$raw_val))
      se <- 2*max(data$raw_val)
    }
  }

  
  # if max in data is 0 -- adding the [1] for race when we may have more than one intercept
  if('NaN' %in% se| any(se <= 0 ) ){
    message("se is <=0, setting to 05.*int")
    se <- 0.5 * intercept
  }

  if (distribution == 'poisson'){
    
    if (race_glm == T){
      summary_glm <- summary(glm_results)
      coefficients <- summary_glm$coefficients
      parameter_names <- str_remove(rownames(coefficients), 'race_cd')
      intercept_name <- unique(data[ii]$race_cd)[unique(data[ii]$race_cd) %ni% parameter_names]
      parameter_names <- parameter_names[!(parameter_names =="(Intercept)")]
      parameter_names <- c(intercept_name, parameter_names)
      
      int <- data.table()
      for (i in 1:length(parameter_names)){
        race_int <- rnorm(n=num_draws,mean= intercept[i], sd=se[i])  %>% exp() %>% as.data.table()
        setnames(race_int,'.',parameter_names[i])
        int <- cbind(int, race_int)
      }

    }else{
      int <- rnorm(n=num_draws,mean= intercept, sd=se)  %>% exp()
    }

  }else{
    
    if (race_glm == T){
      summary_lm <- summary(lm_results)
      coefficients <- summary_lm$coefficients
      parameter_names <- str_remove(rownames(coefficients), 'race_cd')
      intercept_name <- unique(data[ii]$race_cd)[unique(data[ii]$race_cd) %ni% parameter_names]
      parameter_names <- parameter_names[!(parameter_names =="(Intercept)")]
      parameter_names <- c(intercept_name, parameter_names)
      
      int <- data.table()
      for (i in 1:length(parameter_names)){
        race_int <- rnorm(n=num_draws,mean= intercept[i], sd=se[i]) %>% as.data.table()
        setnames(race_int,'.',parameter_names[i])
        int <- cbind(int, race_int)
        int[int < 0] <- 0
      }
      
    }else{
      int <- rnorm(n=num_draws,mean= intercept, sd=se)
      int[int < 0] <- 0
    }

  }

  convergence_status <- 0
  convergence_note <- 'glm model'
  return(list(convergence_status,convergence_note,int))
  
}

do_prediction <- function(){
  
  #  Purpose: use the fitted model and index key to generate prediction draws
  #
  #  Details: While doing prediction, it checks if REs have implausible SD. Before saving, compares draws to thresholds and does minor resampling if draws exceed threshold
  #  Required args (not currently passed as args): model_fitted OR out/opt (depending on which fit), "re1_dim", "re2_dim", index_key, etc
  #  Returns: a df with draws for each row, rows corresponding to different age/location/years 
  #
  
  threshold <- get_data_threshold(data, param_template)
  
  #
  #  Make draws template 
  #
  
  draws_template <- tidyr::crossing(
    index_key,
    source_index_1 = if (length(gold_standard)>0)source_index = 1:num_d else NA,
    int = 1
  ) %>% as.data.table()
  draws_template[, source_index := source_index_1 -1]
  

  if (include_race==T){
    index_cols <- c('age_index', 'location_index','year_index','race_index')
    draws_template <- draws_template[order(age_index, location_index,year_index,race_index)]
  }else{
    index_cols <- c('age_index', 'location_index','year_index')
    draws_template <- draws_template[order(age_index, location_index,year_index)]
  }
  
  if ('icd9' %in% covars){
    draws_template$icd9 <- 0
  }
  if ('a65' %in% covars){
    a65plus_indices <- unique(data[age_group_years_start >=65]$age_index)+1
    draws_template[,a65:=ifelse(age_index_1 %in% a65plus_indices, 1,0)]
  }
  if ('a0' %in% covars){
    a0_indices <- unique(data[age_group_years_start ==0]$age_index)+1
    draws_template[,a0:=ifelse(age_index_1 %in% a0_indices, 1,0)]
  }
  
  for(ds in intersect(covars, unique(data$dataset))){
    draws_template[, paste0(ds) := 0]
  }
  
  # create trigger for bad draws, which will cause the convergence to be marked as failed
  bad_draws <<- ""
  
  message('Predicting with TMB approach')
  mu <- c(model_fitted$sdrep$par.fixed, model_fitted$sdrep$par.random)
  message('Length mu: ',length(mu))
  message('rho a: ',exp(mu[grepl("logit_rho.*a$",names(mu))])/(1+exp(mu[grepl("logit_rho.*a$",names(mu))])))
  message('rho t: ',exp(mu[grepl("logit_rho.*t$",names(mu))])/(1+exp(mu[grepl("logit_rho.*t$",names(mu))])))
  
  if (length(exp(mu[grepl("logit_rho.*j$",names(mu))]))>0 ){
    for (i in 1:length(exp(mu[grepl("logit_rho.*j$",names(mu))])) ){
      message('rho j',i, ': ',exp(mu[grepl("logit_rho.*j$",names(mu))])[i]/(1+exp(mu[grepl("logit_rho.*j$",names(mu))])[i] ))
    }
  }

  parnames <- names(mu)
  message('# of unique params: ', length(unique(parnames)))
  prec_mat <- model_fitted$sdrep$jointPrecision
  if(any(colnames(prec_mat) != parnames)) stop("Issue with parameter ordering")
  
  # Drop any parameters not needed for prediction
  keep_params_idx <- grep('sigma|rho',parnames,invert=T)
  
  param_draws <- tryCatch(mvn_draws_from_precision(
    mu = mu[keep_params_idx],
    prec = prec_mat[keep_params_idx, keep_params_idx],
    num_draws = num_draws
  ),warning=function(w) w)
  
  
  if(is(param_draws,"warning") & model_type == 'intercept') {
    message('Convergence issue with TMB; running a glm')
    
    glm_fit <- fit_glm_model(data, distribution,num_draws)
    convergence_status <- glm_fit[[1]]  
    convergence_note <- glm_fit[[2]]  
    int <- glm_fit[[3]]  
    matching_script <<- 'glm'
    param_template[, match_script:='glm']
    draws <- generate_glm_draws(int)
    return(draws)
  }else if(is(param_draws,"warning") & model_type != 'intercept') {
    message('Convergence issue with TMB; returning bad draws')
    draws <- NULL
    return(draws)
  }else{
    message('Created draws from precision matrix, now pulling REs and FEs')
    parnames <- parnames[keep_params_idx]
    
    rownames(param_draws) <- parnames
    simvars <- paste0("V", 1:num_draws)
    ### FIXED EFFECT
    B <- param_draws[grepl("^B", parnames), ] # create / pull out B
    
    
    fe_by_data <- as.matrix(draws_template[, covars, with = F]) %*% B ##  draws of the FE for each row of fitted data -- would add covariates here as needed
    
    if (param_template$model_type != 'intercept'){
      ## RANDOM EFFECTS
      #
      #   For each re, first we find which values in param draws correspond to that re.
      #
      #   if the re was an interaction between a and b, the re matrix will have axb rows
      #      - in this case, we use the order that the parameter was constructed in tmb_par (either axb or bxa) to add on the appropriate indexes of a + b
      #      - then we use the order of those relevant indexes (a_index, b_index) from draws_template to extract the re draw corresponding to each combination
      #           in the same order as draws_template
      #
      #   if the re was just on a, the re matrix will have a rows
      #      - then the index of a is just 1:num_a
      #      - then we use the order of that index in draws_template to extract re draws in the same order
      #
      res_to_pull <- unique(parnames[grepl("^re[0-9]*$", parnames)])
      
      res <- lapply(res_to_pull, function(r){
        
        # extract re
        re_tmp <-data.table(as.matrix(param_draws[parnames == r, ]))
        
        re_dims <- get(paste0(r, "_dim"))
        
        if(length(dim(tmb_par[[r]])) == 2){
          # interaction within the RE
          i <- dim(tmb_par[[r]])[1]
          j <- dim(tmb_par[[r]])[2]
          
          re_tmp[, i_index := rep(1:i, j)]
          re_tmp[, j_index := rep(1:j, each=i)]
          
          #use the dims (required user input) to determine which index they correspond to
          i_name <- paste0(re_dims[1], "_index_1")
          j_name <- paste0(re_dims[2], "_index_1")
          
          
          print(paste0(r, " is an interaction between ", gsub('_index_1',"" ,i_name), " and ", gsub("_index_1","",j_name)))
          
          setnames(re_tmp, c("i_index", "j_index"), c(i_name, j_name))
          setkeyv(re_tmp, c(i_name, j_name))
          
          re_tmp <- explore_fix_extreme_res(re_tmp, r)
          
          re_by_data <- as.matrix(re_tmp[draws_template[, list(get(i_name), get(j_name))], simvars, with = F])
          
          
        }else if(length(dim(tmb_par[[r]])) == 3){
          # interaction within the RE
          i <- dim(tmb_par[[r]])[1]
          j <- dim(tmb_par[[r]])[2]
          k <- dim(tmb_par[[r]])[3]
          
          re_tmp[, i_index := rep(1:i, j * k)]
          re_tmp[, j_index := rep(rep(1:j, each=i), k)]
          re_tmp[, k_index := rep(1:k, each= i * j)]
          
          #use the dims (required user input) to determine which index they correspond to
          i_name <- paste0(re_dims[1], "_index_1")
          j_name <- paste0(re_dims[2], "_index_1")
          k_name <- paste0(re_dims[3], "_index_1")
          
          
          print(paste0(r, " is a 3-way interaction between ", gsub('_index_1',"" ,i_name), 
                       ", ", gsub("_index_1","",j_name),
                       ", and ", gsub("_index_1","",k_name)))
          
          setnames(re_tmp, c("i_index", "j_index", "k_index"), c(i_name, j_name, k_name))
          setkeyv(re_tmp, c(i_name, j_name, k_name))
          
          re_tmp <- explore_fix_extreme_res(re_tmp, r)
          
          re_by_data <- as.matrix(re_tmp[draws_template[, list(get(i_name), get(j_name), get(k_name))], simvars, with = F])
          
          
        }else if(length(dim(tmb_par[[r]])) == 4){
        
          
          # interaction within the RE
          i <- dim(tmb_par[[r]])[1]
          j <- dim(tmb_par[[r]])[2]
          k <- dim(tmb_par[[r]])[3]
          l <- dim(tmb_par[[r]])[4]
          
          re_tmp[, i_index := rep(1:i, j * k * l)]
          re_tmp[, j_index := rep(rep(1:j, each=i), l*k)]
          re_tmp[, k_index := rep(rep(1:k, each=i*j), l)]
          re_tmp[, l_index := rep(1:l, each= i * j * k)]
          
          #use the dims (required user input) to determine which index they correspond to
          i_name <- paste0(re_dims[1], "_index_1")
          j_name <- paste0(re_dims[2], "_index_1")
          k_name <- paste0(re_dims[3], "_index_1")
          l_name <- paste0(re_dims[4], "_index_1")
          
          print(paste0(r, " is a 4-way interaction between ", gsub('_index_1',"" ,i_name), 
                       ", ", gsub("_index_1","",j_name),
                       ", ", gsub("_index_1","",k_name),
                       ", and ", gsub("_index_1","",l_name)))
          
          setnames(re_tmp, c("i_index", "j_index", "k_index","l_index"), c(i_name, j_name, k_name, l_name))

          setkeyv(re_tmp, c(i_name, j_name, k_name, l_name))
          
          re_tmp <- explore_fix_extreme_res(re_tmp, r)
          
          re_by_data <- as.matrix(re_tmp[draws_template[, list(get(i_name), get(j_name), get(k_name), get(l_name))], simvars, with = F])
          
        }else{
          
          i <- length(tmb_par[[r]])
          re_tmp[, index := 1:i]
          i_name <- paste0(re_dims[1], "_index_1")
          print(paste0(r, " is on ", gsub('_index_1',"", i_name)))
          
          
          setnames(re_tmp, "index", i_name)
          setkeyv(re_tmp, i_name)
          
          re_tmp <- explore_fix_extreme_res(re_tmp, r)
          
          
          if(i_name == "location_index_1" & param_template$geo == "state"){
            # pull out maryland effect
            re_tmp_save <- merge(re_tmp, unique(index_key[,.(location_name,location_index_1)]), by = "location_index_1" )
            re_tmp_save$re_mean = rowMeans(re_tmp_save[,simvars, with = F])
            re_tmp_save$re_lower = rowQuantiles(as.matrix(re_tmp_save[,simvars, with = F]), probs = 0.025)
            re_tmp_save$re_upper = rowQuantiles(as.matrix(re_tmp_save[,simvars, with = F]), probs = 0.975)
            
            location_re_effects <<- re_tmp_save[,.(location_name, re_mean, re_lower, re_upper)]
            
          }else if(i_name == "state_index_1" & param_template$geo == "county"){
            # pull out maryland effect
            re_tmp_save <- merge(re_tmp, unique(index_key[,.(state,state_index_1)]), by = "state_index_1" )
            re_tmp_save$re_mean = rowMeans(re_tmp_save[,simvars, with = F])
            re_tmp_save$re_lower = rowQuantiles(as.matrix(re_tmp_save[,simvars, with = F]), probs = 0.025)
            re_tmp_save$re_upper = rowQuantiles(as.matrix(re_tmp_save[,simvars, with = F]), probs = 0.975)
            
            location_re_effects <<- re_tmp_save[,.(location_name = state, re_mean, re_lower, re_upper)]
          }else{
            location_re_effects <<- NULL
          }
          
          
          re_by_data <- as.matrix(re_tmp[(draws_template[[i_name]]), simvars, with = F])
          
          
        }
        
        return(re_by_data)
        
      })
      
      res <- Reduce('+', res)
      
      draws <- as.matrix(fe_by_data) + res
    }else{
      draws <- as.matrix(fe_by_data)
    }
    
    draws <- data.table(exp(draws))
    names(draws) <- paste0("draw_", 1:num_draws)
    
    draw_cols <- colnames(draws)[colnames(draws) %like% "draw_"]
    
    ## Fix extreme draws
    draws <- summarize_draws(draws)

    # If draws are implausible, we want to break out of prediction now!
    if(bad_draws != ""){
      percent_final_draws_fixed <<- 0
      return(draws)
    }
    
    # If draws are plausible until now - check against estimate threshold
    
    over_thresh <- as.numeric(nrow(draws[mean >= threshold | max_draw > 2*threshold]))
    
    if (over_thresh>0){
      bad_draws <- "draws above plausible threshold"
      
      if (model_type != 'intercept'){
        message(paste0(bad_draws, ', fixing them'))
        time_pre_adj <- Sys.time()
        draws <- explore_fix_extreme_draws(draws, threshold)
        print(Sys.time() - time_pre_adj)
        draws[, `:=` (mean= NULL, id = NULL, max_draw = NULL )]
        
        # Calculate fixed
        draws <- summarize_draws(draws)
      }else{ ## if intercept model has bad draws, break out of prediction now and run a glm
        message('TMB intercept model produced extreme draws; running a glm')
        glm_fit <- fit_glm_model(data, distribution,num_draws)
        convergence_status <- glm_fit[[1]]  
        convergence_note <- glm_fit[[2]]  
        int <- glm_fit[[3]]  
        matching_script <<- 'glm'
        param_template[, match_script:='glm']
        draws <- generate_glm_draws(int)
        return(draws)
      }

    }else{
      percent_final_draws_fixed <<- 0
    }
    
    # Decoding year and age based on row in template (1 indexed), data (0 indexed)
    draws[, age_index  := draws_template$age_index]
    draws[, year_index := draws_template$year_index]
    if (include_race==T){
      draws[, race_index := draws_template$race_index]
    }
    if (param_template$geo != 'national'){
      draws[, location_index := draws_template$location_index]
    }
    
    if (length(gold_standard)>0){
      draws[,source_index:=draws_template$source_index]
      if (length(unique(data$dataset))>1){
        draws <- draws[source_index==gold_standard]
      }
    }
    
    # FINAL TIME: Check against estimate threshold
    over_thresh <- as.numeric(nrow(draws[mean >= threshold | max_draw > 2*threshold])) 
    
    if (over_thresh>0){
      bad_draws <<- "draws above plausible threshold"
      message(bad_draws)
      message(paste0("threshold: ", threshold))
      message(over_thresh)
    }else{
      bad_draws <<- ''
    }
    
    
    any_inf_draws <- as.integer(sum(is.infinite(as.matrix(draws))) > 0)
    any_na_draws <- as.integer(sum(is.na(as.matrix(draws))) > 0)
    
    if(bad_draws == "" & any_inf_draws){bad_draws <<- "infs in draws"}
    if(bad_draws == "" & any_na_draws){bad_draws <<- "NAs in draws"}
    
    if (cause == 'lri_corona'){
      zero_yrs <- unique(index_key[year_id <=2019, year_index])
      draws_long <- melt(draws, measure.vars = colnames(draws)[grepl('draw_', colnames(draws))])
      draws_long[year_index %in% zero_yrs, `:=`(value = 0, mean = 0, lower = 0, upper = 0, median = 0, max_draw = 0)]
      draws <- dcast(draws_long, ... ~ variable, value.var = "value")
    }
    return(draws)
  }
}

explore_fix_extreme_res <- function(re_tmp, r, fix = T){
  
  #  Purpose: During prediction, do a quick scan at the REs and see if they have implausibly high SD
  #
  #  Details: For location REs (and year for national), check the RE draws and take the SD
  #           Since the draws are in log space, a SD(RE_draws) > 2 is really large so we want to identify that
  #           Encounters have much smaller numbers, and thus in log space the SD can be much wider, so we use a threshold of 5
  #           
  #           If there are only a few locations with SD > threshold, we attempt to fix the SD by resampling it (effectively shrinking the CI)
  #           If there are a lot of locs with SD > threshold (#  > loc_threshold), then the model fit is considered too bad to proceed
  #             - in this case, the bad_draws tag gets triggered!
  #
  #  Required args: re_tmp, r
  #  Returns: re_tmp post fixing
  #
  
  by_cols <- colnames(re_tmp)[colnames(re_tmp) %like% "index"]
  
  if("year_index_1" %in% by_cols & param_template$geo == "national"){
    re_tmp_long <- melt(re_tmp, id.vars = by_cols)
    examine1 <- re_tmp_long[, sd(value), by = c("year_index_1")][order(V1)]
    examine2 <- merge(examine1, unique(index_key[,.(year_id, year_index_1)]), by = "year_index_1")
    
    if(param_template$metric == "spend_per_encounter"){
      sd_threshold <- 2
    }else{
      sd_threshold <- 2.5}
    
    year_threshold <- 5
    
    examine2 <- examine2[V1 > sd_threshold]
    examine2[, re := r]
    if(nrow(examine2) > 1){
      
      # too bad to fix, treat as non convergence
      if(nrow(examine2) > year_threshold){
        
        print(paste0("Too many years have really high uncertainty in ", r, ", must stop"))
        
        bad_draws <<- "extreme draws"
        return(re_tmp)
      }
      
      print(paste0("Some years have really high uncertainty in ", r))
      message(paste0("Some years have really high uncertainty in ", r))
      print(examine2)
      
      if(fix){
        print("FIXING THEM")
        
        param_template <<- param_template[, re_fix:=T]
        
        # drop top and bottom 5% of draws
        fix_re_draws <- re_tmp_long[year_index_1 %in% examine1$year_index_1]
        fix_re_draws[, group := .GRP, by = by_cols]
        
        fixed_re_draws <- rbindlist(lapply(unique(fix_re_draws$group), function(g){
          
          group_fix <- fix_re_draws[group == g]
          
          while(sd(group_fix$value) > sd_threshold){ 
            
            group_fix[, lower_value := quantile(value, 0.025)]
            group_fix[, upper_value := quantile(value, 0.975)]
            
            group_fix[, to_fix := ifelse(value > lower_value & value < upper_value, 0, 1)] 
            group_fix[to_fix == 1, value := sample(group_fix[to_fix==0]$value, size = nrow(group_fix[to_fix == 1]), replace = T)]
            group_fix[, to_fix := NULL]
            
          }
          
          
          return(group_fix)
          
        }), fill = T)
        fixed_re_draws[, `:=` (lower_value = NULL, upper_value = NULL, group = NULL)]
        
        re_tmp_long <- rbind(re_tmp_long[!year_index_1 %in% examine1$year_index_1],
                             fixed_re_draws)
        
        re_tmp_fixed <- dcast(re_tmp_long, ... ~ variable, value.var = "value")
        stopifnot(nrow(re_tmp_fixed) == nrow(re_tmp))      
        return(re_tmp_fixed)
      }
      
    }
  }
  
  if("location_index_1" %in% by_cols){
    
    re_tmp_long <- melt(re_tmp, id.vars = by_cols)
    # calculate coefficient of variation 
    examine1 <- re_tmp_long[, sd(value), by = c("location_index_1")][order(V1)]
    
    examine2 <- merge(examine1, unique(index_key[,.(location_name, location_index_1)]), by = "location_index_1")
    
    if(param_template$metric =="spend_per_encounter"){
      sd_threshold <- 2 
    }else{
      sd_threshold <- 5} 
    
    if(param_template$geo == "state"){
      loc_threshold <- 10
    }else{
      loc_threshold <- 100
    }
    
    
    examine2 <- examine2[V1 > sd_threshold]
    examine2[, state := gsub(" - .*$", "", location_name)]
    examine2[, re := r]
    
    if(nrow(examine2) > 1){
      
      # too bad to fix, treat as non convergence
      if(nrow(examine2) > loc_threshold | model_type == "complex"){
        
        print(paste0("Too many locations have really high uncertainty in ", r, ", must stop"))
        
        bad_draws <<- "extreme draws"
        return(re_tmp)
      }
      
      print(paste0("Some locations have really high uncertainty in ", r))
      message(paste0("Some locations have really high uncertainty in ", r))
      print(examine2)
      
      if(fix){
        print("FIXING THEM")
        
        param_template <<- param_template[, re_fix:=T]
        
        # drop top and bottom 5% of draws
        fix_re_draws <- re_tmp_long[location_index_1 %in% examine1$location_index_1]
        fix_re_draws[, group := .GRP, by = by_cols]
        
        fixed_re_draws <- rbindlist(lapply(unique(fix_re_draws$group), function(g){
          
          group_fix <- fix_re_draws[group == g]
          
          while(sd(group_fix$value) > 2){ 
            
            group_fix[, lower_value := quantile(value, 0.025)]
            group_fix[, upper_value := quantile(value, 0.975)]
            
            group_fix[, to_fix := ifelse(value > lower_value & value < upper_value, 0, 1)] 
            group_fix[to_fix == 1, value := sample(group_fix[to_fix==0]$value, size = nrow(group_fix[to_fix == 1]), replace = T)]
            group_fix[, to_fix := NULL]
            
          }
          
          
          return(group_fix)
          
        }), fill = T)
        fixed_re_draws[, `:=` (lower_value = NULL, upper_value = NULL, group = NULL)]
        
        re_tmp_long <- rbind(re_tmp_long[!location_index_1 %in% examine1$location_index_1],
                             fixed_re_draws)
        
        re_tmp_fixed <- dcast(re_tmp_long, ... ~ variable, value.var = "value")
        stopifnot(nrow(re_tmp_fixed) == nrow(re_tmp))      
        return(re_tmp_fixed)
      }
      
    }
  }
  
  return(re_tmp)
  
}

summarize_draws <- function(draws){
  
  #  Purpose: replace bottom 2.5% of draws and top 2.5% of draws with the 2.5% and 97.5% value
  #           lower and upper 95% CI will be quite similar, but ensures mean is not calculated with extreme draws
  #
  #  Details: 
  #  Required args: draws
  #  Returns: draws with columns for mean, lower, upper, median, max_draw
  #
  message('Calculating mean and quantiles')
  
  draw_cols <- colnames(draws)[colnames(draws) %like% "draw_"]
  
  draws$lower_value <- rowQuantiles(as.matrix(draws),cols = draw_cols, probs = .025, na.rm = T)
  draws$upper_value <- rowQuantiles(as.matrix(draws),cols = draw_cols, probs = .975, na.rm = T)
  
  draws[,(draw_cols) := lapply(.SD, function(x){
    
    vec <- copy(x)
    vec[vec < draws$lower_value] <- draws$lower_value[vec < draws$lower_value]
    vec[vec > draws$upper_value] <- draws$upper_value[vec > draws$upper_value]
    
    return(vec)
    
  }), .SDcols = draw_cols]
  
  draws$mean <- rowMeans(draws[,..draw_cols],na.rm = T)
  draws$lower <- rowQuantiles(as.matrix(draws),cols = draw_cols, probs = .025, na.rm = T)
  draws$upper <- rowQuantiles(as.matrix(draws),cols = draw_cols, probs = .975, na.rm = T)
  draws$median <- rowQuantiles(as.matrix(draws),cols = draw_cols, probs = .5, na.rm = T)
  draws[, max_draw:= max(.SD), by= 1:nrow(draws), .SDcols = draw_cols]
  
  draws[, `:=` (lower_value = NULL, upper_value = NULL)]
  
  return(draws)
}

explore_fix_extreme_draws <- function(draws, threshold){
  
  #  Purpose: Resample at the draw level to remove extreme draws (identified as those over the threshold)
  #
  #  Details: 
  #  Required args: draws, threshold
  #  Returns: draws 
  #
  
  
  '%ni%' <- Negate('%in%')
  
  draws <- draws[,id:= 1:.N]
  draw_names <- colnames(draws)[colnames(draws) %like% "draw_"]
  not_draw_names <- colnames(draws)[!colnames(draws) %like% "draw_"]
  
  draws_fix <- draws[mean>=threshold | max_draw > threshold]
  draws_fine <- draws[! (mean>=threshold | max_draw > threshold)]
  
  draws_fix$upper_value <- rowQuantiles(as.matrix(draws_fix),cols = draw_names, probs = .85, na.rm = T)
  if ( nrow(draws_fix[upper_value > threshold])>0 ){
    message('Warning, top 15th percentile of draws are still over the threshold, setting upper bound to 75th %')
    extra_fix <- draws_fix[upper_value > threshold]
    extra_fix$upper_value <- rowQuantiles(as.matrix(extra_fix),cols = draw_names, probs = .75, na.rm = T)
    draws_fix <- draws_fix[!(id %in% unique(extra_fix$id))]
    draws_fix <- rbind(draws_fix,extra_fix)
  }
  
  if(nrow(draws_fix) > 0){
    
    percent_final_draws_fixed <<- round((nrow(draws_fix)/nrow(draws))*100, digits=2)
    print(paste0("Needing to fix ", percent_final_draws_fixed, "% of draws"))
    
    param_template <<- param_template[, draws_fix:=T]
    
    
    draws_fixed <- rbindlist(lapply(1:nrow(draws_fix), function(i){
      
      draws_tmp <- draws_fix[i,]
      draws_tmp_long <- melt(draws_tmp, measure.vars = draw_names)
      
      draws_tmp_long[, to_fix := ifelse(value < upper_value, 0, 1)] 
      draws_tmp_long[to_fix == 1, value := sample(draws_tmp_long[to_fix==0]$value, size = nrow(draws_tmp_long[to_fix == 1]), replace = T)]
      
      
      draws_tmp_long[, `:=` (lower_value = NULL, 
                             upper_value = NULL, 
                             to_fix = NULL)]
      
      # reshape
      draws_tmp_fixed <- dcast(draws_tmp_long, ...~variable, value.var = "value")
      return(draws_tmp_fixed)
      
    }))
    
    new_draws <- rbind(draws_fixed, draws_fine)
    new_draws <- new_draws[order(id)]
    
    return(new_draws)
    
    
  }else{
    percent_final_draws_fixed <<- 0
    return(draws)
  }
  
}

generate_glm_draws <- function(int, tmb){
  
  #  Purpose: Do predictions for our intercept only TMB model
  #
  #  Details: Much simpler since no REs (RE is the iid effect by datapoint that we don't use in prediction)
  #  Required args: int, tmb
  #  Returns: draws (dt - decoded)
  
  # create trigger for bad draws, which will cause the convergence to be marked as failed
  bad_draws <<- "" 
  
  # note - we don't use source_index here because the intercept model doesn't distinguish sources.
  draws_template <- tidyr::crossing(
    index_key,
    int = 1
  ) %>% as.data.table()
  if (include_race == T){
    draws_template <- draws_template[order(age_index, location_index,year_index,race_index)]
  }else{
    draws_template <- draws_template[order(age_index, location_index,year_index)]
  }
  
  if (include_race == T & num_r>1 & param_template$n_zeros != nrow(data[outlier==0]) ){
    race_glm <- T
  }else{
    race_glm <- F
  }

  if (race_glm == T){
    int[,draw:=1:.N]
    int_melt <- melt(int,id.var = 'draw', variable.name = 'race_cd')
    
    int_cast <- dcast(int_melt, race_cd ~draw)
    setnames(int_cast, names(int_cast)[names(int_cast) %in% 1:num_draws], paste0('draw_',1:num_draws))
    draws <- merge(draws_template, int_cast, all.x = T) ##  draws of the FE for each row of fitted data -- would add covariates here as needed
    draw_cols <- names(draws)[names(draws) %like% 'draw']
    threshold <- get_data_threshold(data, param_template)
    
    # Check against estimate threshold
    over_thresh <- which(draws[,..draw_cols]  >= threshold, arr.ind=TRUE)
    if (nrow(over_thresh) >0){
      bad_draws <<- "draws above plausible threshold"
    }
    
    # summarize 
    draws_summary <- summarize_draws(draws[,..draw_cols])
    non_draw_cols <- names(draws)[names(draws) %ni% draw_cols]
    draws <- cbind(draws[,..non_draw_cols], draws_summary)
    
    
    any_inf_draws <- as.integer(sum(is.infinite(as.matrix(draws[,..draw_cols]))) > 0)
    any_na_draws <- as.integer(sum(is.na(as.matrix(draws[,..draw_cols]))) > 0)
    
    if(bad_draws == "" & any_inf_draws){bad_draws <<- "infs in draws"}
    if(bad_draws == "" & any_na_draws){bad_draws <<- "NAs in draws"}
    
  }else{
    int_by_data <- as.matrix(draws_template[, covars, with = F]) %*% int ##  draws of the FE for each row of fitted data -- would add covariates here as needed
  
    ### add together to form predictions
    draws <- as.matrix(int_by_data)
    
    draws <- data.table(draws)
    
    names(draws) <- paste0("draw_", 1:num_draws)
    
    threshold <- get_data_threshold(data, param_template)
  
    # Check against estimate threshold
    over_thresh <- which(draws  >= threshold, arr.ind=TRUE)
    if (nrow(over_thresh) >0){
      bad_draws <<- "draws above plausible threshold"
    }
    
    # summarize 
    draws <- summarize_draws(draws)
    
    # Decoding year and age based on row in template (1 indexed), data (0 indexed)
    draws[, age_index  := draws_template$age_index]
    draws[, year_index := draws_template$year_index]
    if (param_template$geo != 'national'){
      draws[, location_index := draws_template$location_index]
    }
  
    any_inf_draws <- as.integer(sum(is.infinite(as.matrix(draws))) > 0)
    any_na_draws <- as.integer(sum(is.na(as.matrix(draws))) > 0)
    
    if(bad_draws == "" & any_inf_draws){bad_draws <<- "infs in draws"}
    if(bad_draws == "" & any_na_draws){bad_draws <<- "NAs in draws"}
  
  }
  
  if (include_race == T & race_glm == F){
    if (num_r>1){
      library(tidyr)
      draws <- tidyr::crossing(draws, race_index = unique(data[outlier==0]$race_index) ) %>% as.data.table()
    }else{
      draws[, race_index:= unique(data[outlier==0]$race_index)]
    }

  }
  
  if (cause == 'lri_corona'){
    zero_yrs <- unique(index_key[year_id <=2019, year_index])
    draws_long <- melt(draws, measure.vars = colnames(draws)[grepl('draw_', colnames(draws))])
    draws_long[year_index %in% zero_yrs, `:=`(value = 0, mean = 0, lower = 0, upper = 0, median = 0, max_draw = 0)]
    draws <- dcast(draws_long, ... ~ variable, value.var = "value")
  }
  
  return(draws)
}

icd_shift_draws <- function(draws, data){
  
  #  Purpose: Shift icd9 time series up to meet icd10
  #
  #  Details: Specifically, calculate the intercept shift as 2016 - 2014, and apply it to 2000-2014. Also set 2015 equal to the 2016 value.
  #  Required args: draws (already summarized with index's), data (to determine threshold)
  #  Returns: draws
  
  if (include_race == T){
    index_cols <- c("location_index", "age_index", "source_index","race_index", "variable")
  }else{
    index_cols <- c("location_index", "age_index", "source_index", "variable")
  }
  # causes we don't want to do this adjustment for!
  excluded_cause_list <- c('hepatitis_c')
  
  if(param_template$acause %in% excluded_cause_list){
    return(draws)
  }
  
  if (is.null(draws)| bad_draws != ''){
    return(draws)
  }
  
  if (geog == 'national'){
    draws$location_index <- 0
  }
  
  if(model_type == 'intercept'){
    draws$source_index <- 1
  }
  
  print("ICD shift!")
  
  # everything is still in index space, so determine the index for the relevant years
  alt <- unique(index_key[year_id == 2014, year_index])
  ref <- unique(index_key[year_id == 2016, year_index])
  other_alt <- unique(index_key[year_id == 2015, year_index])
  
  draws_long <- melt(draws, measure.vars = colnames(draws)[grepl('draw_', colnames(draws))])
  

  get_shift <- merge(draws_long[year_index == alt], draws_long[year_index == ref], by = index_cols, all.x = T)
  
  # calculate shift off of the median
  get_shift[, shift := median.y - median.x] # ref - alt
  get_shift[,value_2016:=value.y]
  get_shift <- get_shift[,.SD,.SDcols = c(index_cols,'shift','value_2016')]
  draws_long <- merge(draws_long, get_shift, by = index_cols, all.x = T)
  
  # only apply shift to icd 9 years, and set 2015 to 2016
  draws_long[ year_index %in% unique(index_key[year_id <= 2014]$year_index), value := value + shift ]
  draws_long[ year_index == other_alt, value := value_2016]
  draws_long[value < 0 ,value := 0]
  
  #reshape back to wide
  if (include_race == T){
    draws <- dcast(draws_long, location_index + age_index + year_index + race_index + source_index ~ variable, value.var = "value")
  }else{
    draws <- dcast(draws_long, location_index + age_index + year_index + source_index ~ variable, value.var = "value")
  }
  
 
  # summarize again
  draws <- summarize_draws(draws)
  
  # do some final checks
  threshold <- get_data_threshold(data, param_template)
  over_thresh <- as.numeric(nrow(draws[mean >= threshold | max_draw > threshold]))
  if (over_thresh>0){
    bad_draws <<- "draws above plausible threshold"
    message(bad_draws)
    message(paste0("threshold: ", threshold))
    message(over_thresh)
  }else{
    bad_draws <<- ''
  }
  
  
  any_inf_draws <- as.integer(sum(is.infinite(as.matrix(draws))) > 0)
  any_na_draws <- as.integer(sum(is.na(as.matrix(draws))) > 0)
  
  if(bad_draws == "" & any_inf_draws){bad_draws <<- "infs in draws"}
  if(bad_draws == "" & any_na_draws){bad_draws <<- "NAs in draws"}
  
  note <<- paste0(note, "_W_ICD9_ADJ")
  
  return(draws)
  
}

adjust_zeros <- function(draws, data, include_race,geog){
  
  #  Purpose: Prevents zeros from being predicted in state-race models
  #
  #  Details: 
  #  1. Take the minimum non-zero value across draws and replace any zeros with that non-zero estimate
  #  2. If only zeros across draws, take the min non-zero value across years and draws
  #  3. If only zeros across draws & years, take the min non-zero value across ages and years and draws
  #  Required args: draws (already summarized with index's), data (to determine threshold)
  #  Returns: draws
  
  if(include_race == F | is.null(draws) | bad_draws != ''){
    return(draws)
  }
  if (param_template$n_zeros == nrow(data[outlier==0]) & param_template$n_row != 0){
    message('Only zeros and running intercept model so cannot adjust')
    return(draws)
  }

  if(model_type == 'intercept'){
    draws$source_index <- 1
  }
  
  draws_long <- melt(draws, measure.vars = colnames(draws)[grepl('draw_', colnames(draws))])
  
  plot_error <- draws[,.(mean = mean(mean),
                         upper = mean(upper),
                         lower = mean(lower)), by = .(age_index, year_index, race_index,source_index)][,version:='orig']
  
  gcols <- c('location_index', 'age_index', 'year_index', 'race_index') #variable is draw
  draws_long[,fix:=ifelse(value == 0,1,0)]

  if (draws_long[fix==1,.N]>0){
    message(paste0("Adjusting ",draws_long[fix==1,.N],"zeros with min non-zero value across draws!"))
    ## Impute with min non zero value across draws
    draws_long[,min_val:=min(value[value!=0]), by = gcols][is.infinite(min_val), min_val:=0]
    
    if (draws_long[min_val ==0, .N]>0 ){
      message("Adjusting zeros with min non-zero value across years!")
      ## Impute with min non zero value across years
      draws_long[,min_val2:=min(value[value!=0]), by = c(gcols[gcols!= 'year_index']) ][is.infinite(min_val2), min_val2:=0]
      draws_long[min_val ==0 & min_val2 !=0, min_val:=min_val2]
      
      if (draws_long[min_val ==0, .N]>0 ){
        message("Adjusting zeros with min non-zero value across ages!")
        ## Impute with min non zero value across ages
        draws_long[,min_val2:=min(value[value!=0]), by = c(gcols[gcols!= 'age_index']) ][is.infinite(min_val2), min_val2:=0]
        draws_long[min_val ==0 & min_val2 !=0, min_val:=min_val2]
      }
      if (draws_long[min_val ==0, .N]>0 ){
        stop('we have too many zeros for the non zero adjustment to work in this combo')
      }
      
    }
    
    draws_long[, value:=ifelse(fix==1,min_val,value)]
    
    if (draws_long[is.infinite(value), .N] > 0) {
      stop('NA/inf imputed values')
    }
    
    
    
    draws_long[,`:=`(min_val = NULL, min_val2 = NULL, fix = NULL)]
    
    #reshape back to wide
    if (include_race == T){
      draws <- dcast(draws_long, location_index + age_index + year_index + race_index + source_index ~ variable, value.var = "value")
    }else{
      draws <- dcast(draws_long, location_index + age_index + year_index + source_index ~ variable, value.var = "value")
    }
    
    # summarize again
    draws <- summarize_draws(draws)
    
    plot_error_adj <- draws[,.(mean = mean(mean),
                           upper = mean(upper),
                           lower = mean(lower)), by = .(age_index, year_index, race_index,source_index)][,version:='adj']
    plot_error <- rbind(plot_error,plot_error_adj)
    
    ui_plot <- ggplot()+
                geom_errorbar(plot_error[year_index==19],
                              mapping = aes(x=version, ymin = lower, ymax = upper, y = mean, color = as.factor(race_index)),
                              width=.5, position=position_dodge(width=.5))+
                geom_point(plot_error[year_index==19], mapping = aes(x=version, y = mean, color = as.factor(race_index)),
                           size = 1, position=position_dodge(width=.5))+
                labs(title = paste0(cause, ', predicted UI across locations, by age and race - year 2019'))+
                facet_wrap(~age_index, scales = 'free_y')
    if (save_plots == T){
      pdf(paste0(outdir, "/diagnostics/ui_", cause,"_",met,"_",geog, "_",matching_script,".pdf"), height = 8, width = 11)
      print(ui_plot)
      dev.off()
    }
    # Do some of the checks we did already, just to confirm!
    threshold <- get_data_threshold(data, param_template)
    over_thresh <- as.numeric(nrow(draws[mean >= threshold | max_draw > threshold]))
    if (over_thresh>0){
      bad_draws <<- "draws above plausible threshold"
      message(bad_draws)
      message(paste0("threshold: ", threshold))
      message(over_thresh)
    }else{
      bad_draws <<- ''
    }
    
    
    any_inf_draws <- as.integer(sum(is.infinite(as.matrix(draws))) > 0)
    any_na_draws <- as.integer(sum(is.na(as.matrix(draws))) > 0)
    
    if(bad_draws == "" & any_inf_draws){bad_draws <<- "infs in draws"}
    if(bad_draws == "" & any_na_draws){bad_draws <<- "NAs in draws"}
    
    
    return(draws)
  }else{
    
    return(draws)
    
  }
  
}

extrapolate_draws_for_age <- function(draws, index_key, additional_ages){
  ## In order to get the complete set of age predictions present across both metrics we hold constant
  ## at the tails the prediction present in the original metric data
  if (is.null(draws)| bad_draws != '' | extrapolate_ages == F | unique(is.na(additional_ages)) ){
    return(draws)
  } 
  
  print(paste0("Extrapolating tails for ages: ", toString(additional_ages)))
  
  min_draws_age <- min(draws$age_index)
  max_draws_age <- max(draws$age_index)
  
  begin_ages <- additional_ages[additional_ages < min(ages)]
  end_ages <- additional_ages[additional_ages > max(ages)]
  
  
  ## Deal with indices
  if (length(begin_ages)>0){
    begin_indices <- min_draws_age - (1:length(begin_ages))
  }else{
    begin_indices <- NULL
  }
  
  if (length(end_ages)>0){
    end_indices <- max_draws_age + (1:length(end_ages))
  }else{
    end_indices <- NULL
  }
  
  new_age_key <- data.table(age_group_years_start = additional_ages,
                            age_index = c(begin_indices, end_indices))
  new_age_key[,age_index_1 := age_index+1]
  
  copy <- copy(index_key)[,`:=`(age_group_years_start = NULL, age_index = NULL, age_index_1 = NULL)]
  new_index_key <- tidyr::crossing(copy,
                            new_age_key) %>% as.data.table()
  
  new_index_key <- rbind(index_key,new_index_key)
  
  ## Copy the draws forward and back
  if (length(begin_ages)>0){
    begin_draws <- copy(draws[age_index==min_draws_age])
    begin_draws[,age_index:=NULL]
    begin_draws <- tidyr::crossing(begin_draws,
                                   age_index = begin_indices) %>% as.data.table() 
    draws <- rbind(draws,begin_draws)
  }
  
  if (length(end_ages)>0){
    end_draws <- copy(draws[age_index==max_draws_age])
    end_draws[,age_index:=NULL]
    end_draws <- tidyr::crossing(end_draws,
                                 age_index = end_indices) %>% as.data.table() 
    draws <- rbind(draws,end_draws)
  }
  
  index_key <<- new_index_key
  note <<- paste0(note, "_W_EXTRAP_AGES")
  return(draws)
  
}


#
#   DATA VISUALIZATION AND SAVING 
#

combine_draws_data <- function(data, draws, index_key, param_template){
  
  #  Purpose: combine data and draws so we can compare draws to data
  #
  #  Details: uses index_key to identify correct location/age/year in draws so can be merged with data
  #  Required args: data, draws, index_key, param_template
  #  Returns: dataframe of data_with_preds
  #
  

  if (include_race == T){
    index_cols <- c("location_index", "age_index", "year_index","race_index")
    id_cols <- c("age_group_years_start", "year_id", "location", "location_name","race_cd")
  }else{
    index_cols <- c("location_index", "age_index", "year_index")
    id_cols <- c("age_group_years_start", "year_id", "location", "location_name")
  }
  keep_cols <- c('mean','median', 'lower', 'upper', id_cols)
  
  if (geog == 'national'){
    draws$location_index <- 0
  }
  
  ## subset draws to just the index and draw cols
  if (include_race == T){
    draw_cols <- colnames(draws)[colnames(draws) %like% "draw_"]
    draws_and_index <- c(index_cols, draw_cols, 'mean','median', 'lower', 'upper')
    draws <- draws[,..draws_and_index]
  }
  # merge draws with the index_key to identify correct location/age/year
  draws2 <- merge(draws, index_key, by = index_cols, all.x = T)
  # merge draws (now with full info) with the data
  data_with_preds <- merge(data, draws2[,..keep_cols], by = id_cols, all = T)
  
  data_with_preds[is.na(dataset), dataset := "pred only"]
  
  # use se to get data lower  + upper
  data_with_preds[, data_lower := raw_val-1.96*se][data_lower < 0, data_lower := 0]
  data_with_preds[, data_upper := raw_val +1.96*se]
  
  # add all the columns from the param_template onto the datas
  tmp <- sapply(colnames(param_template), function(c){
    data_with_preds[, paste0(c) := param_template[[c]]]
  })
  
  # drop helper columns we don't need anymore
  drop_cols <- c("int", colnames(data_with_preds)[colnames(data_with_preds) %like% "index"])
  data_with_preds[, c(drop_cols) := NULL]
  
  return(data_with_preds)
  
}

visualize_fit <- function(data, draws, ages, years, outdir, param_template, note, fe_report, drop_outliers){
  
  #  Purpose: plot model fit alongside data for a subset of counties/states
  #
  #  Details: first plots age and time trends for subset of counties/states, then looks across county/states
  #  Required args: data, draws, ages, years, 
  #  Returns: a list of a bunch of plots
  #
  
  data_with_preds <- combine_draws_data(data, draws, index_key, param_template)
  
  data_with_preds[, used_to_fit := ifelse(outlier==1 & drop_outliers == T, 'no', 'yes')]
  data_with_preds[reference_dataset==1 & used_to_fit =='yes', error := (raw_val - mean)^2]
  rmse <- sqrt(mean(data_with_preds$error,na.rm=T))
  data_with_preds$error <- NULL
  
  shape_vals <- c(4,19)
  names(shape_vals) <- c("no", "yes")
  
  if ( matching_script!= 'glm' ){
    fe_betas <- list(fe_report$Estimate)
  }else{
    fe_betas <- NA
  }
  
  
  # locations to plot
  if (param_template$geo == 'county' ){
      plot_loc_list <- c("NY - Jefferson County", "NY - Saint Lawrence County","NY - Hamilton County")
      state_plot <- "NY -"
      county_plot <- "NY - Jefferson County"
  }else if (param_template$geo=='state'){
    
    outlier_locs <- length(unique(data[outlier==1]$location)) ##want to plot states with outliers
    if (outlier_locs >= 5){
      loc_indices <- seq(1, outlier_locs, floor(outlier_locs/5))
      plot_loc_list <- unique(data[outlier==1]$location_name)[loc_indices]
    }else if (outlier_locs < 5 & outlier_locs >0 ){
      plot_loc_list <- unique(c(unique(data[outlier==1]$location_name), c('California','New York','District of Columbia','Rhode Island', 'Hawaii','Montana')))
    }else{
      plot_loc_list <- c('California','New York','District of Columbia','Rhode Island', 'Hawaii','Montana','Minnesota')
    }
    
    state_plot <- "."
    county_plot <- "Maine"
  }else{
    plot_loc_list <- "USA"
    state_plot <- "USA"
    county_plot <- "USA"
  }

  # state to visualize location effect
  if (matching_script %like% 'noage'){
    age_plot <- ages
  }else{
    age_plot <- ages[floor(length(ages) - length(ages)/2)]
  }
  
  if (matching_script %like% 'noyear'){
    year_plot <- years
  }else{
    year_plot <- years[floor(length(years) - length(years)/2)]
  }
  
  plot_loc_list <- intersect(plot_loc_list, data_with_preds$location_name)
  
  subtitle <- paste0(params, '\ncovars: ',toString(covars))

  loop_plots <- list()
  # year and time trend for individual locations
  for(loc in plot_loc_list){
    
    loc_yr <- paste0(loc,'yr')
    loc_age <- paste0(loc,'age')
    
    yrange <- max(c(data_with_preds[location_name == loc & used_to_fit == 'yes']$raw_data, data_with_preds[location_name == loc]$upper), na.rm = T)+5
    if (max(data_with_preds[location_name == loc & !is.na(upper)]$upper) > 3* max(data_with_preds[location_name == loc &!is.na(mean)]$mean)){
      yrange <- max(data_with_preds[location_name == loc]$mean)*1.5
    }
    
      
    p1 <- ggplot(data_with_preds[location_name == loc], aes(x = year_id))+
      {if (include_race==F)geom_ribbon(aes(ymin = lower,ymax = upper), fill = 'grey80')}+
      {if (include_race==T)geom_line(aes(y = mean, color = as.factor(race_cd)))else geom_line(aes(y = mean))}+
      {if (include_race==T)geom_point(aes(y = raw_val, color = as.factor(race_cd), shape = as.factor(used_to_fit)))else
        geom_point(aes(y = raw_val, color = paste0(as.factor(reference_dataset), ': ', dataset), shape = as.factor(used_to_fit)))}+
      scale_shape_manual(values=shape_vals)+
      facet_wrap(~age_group_years_start, scales = 'free')+theme_bw()+
      labs(title = paste0(loc, " - time trend"), 
           caption = paste0('fe_betas: ',fe_betas,'\nRMSE: ',rmse),
           subtitle = subtitle,shape = 'Used to fit')

    
    if(param_template$metric =='spend_per_encounter'){
      p1 <- p1 + 
        {if (include_race==T)geom_errorbar(aes(ymin = data_lower, ymax = data_upper, color =as.factor(race_cd)))else
          geom_errorbar(aes(ymin = data_lower, ymax = data_upper, color = paste0(as.factor(reference_dataset), ': ', dataset)))}+
        scale_y_continuous(oob = scales::squish, limits = c(0, yrange))
    }
    
    p2 <- ggplot(data_with_preds[location_name == loc], aes(x = age_group_years_start))+
      {if (include_race==F)geom_ribbon(aes(ymin = lower,ymax = upper), fill = 'grey80')}+
      {if (include_race==T)geom_line(aes(y = mean, color = as.factor(race_cd)))else geom_line(aes(y = mean))}+
      {if (include_race==T)geom_point(aes(y = raw_val, color = as.factor(race_cd), shape = as.factor(used_to_fit)))else
        geom_point(aes(y = raw_val, color = paste0(as.factor(reference_dataset), ': ', dataset), shape = as.factor(used_to_fit)))}+
      scale_shape_manual(values=shape_vals)+
      facet_wrap(~year_id, scales = 'free')+theme_bw()+
      labs(title = paste0(loc, " - age trend"),
           caption = paste0('fe_betas: ',fe_betas,'\nRMSE: ',rmse),
           subtitle = subtitle, shape = 'Used to fit')
    
    if(param_template$metric =='spend_per_encounter'){
      p2 <- p2 + 
        {if (include_race==T)geom_errorbar(aes(ymin = data_lower, ymax = data_upper, color =as.factor(race_cd)))else
          geom_errorbar(aes(ymin = data_lower, ymax = data_upper, color = paste0(as.factor(reference_dataset), ': ', dataset)))}+
        scale_y_continuous(oob = scales::squish, limits = c(0, yrange))
    }
    
    loop_plots[[loc_yr]] <- p1
    loop_plots[[loc_age]] <- p2
    
  }
  
  
  loc_plot_df_ex <- data_with_preds[location_name %like% county_plot]
  p_time <- NULL
  p_age <- NULL
  

  if(nrow(loc_plot_df_ex)){
    
    # year and time trend for all counties within a state (holding either year or age constant)
    p_time <- ggplot()+
      geom_line(data=loc_plot_df_ex, aes(x = year_id, y = mean, group = age_group_years_start, color=as.factor(age_group_years_start)))+
      geom_point(data=loc_plot_df_ex[used_to_fit!='no'], aes(x = year_id, y = raw_val, color = as.factor(age_group_years_start), shape = as.factor(icd9)))+
      scale_shape_manual(values=c(1,2,4))+
      {if (include_race == T)facet_wrap(~race_cd)}+
      {if (include_race == F)facet_wrap(~location_name)}+
      theme_bw()+
      labs(title = paste0(state_plot, " - loc time trend"), 
           subtitle = subtitle,
           color = 'Age', shape = 'ICD9')

    p_age <- ggplot()+
      geom_line(data=loc_plot_df_ex,aes(y = mean, x=age_group_years_start, group = year_id, color=as.factor(year_id)))+
      geom_point(data=loc_plot_df_ex[used_to_fit!='no'],aes(y = raw_val, x=age_group_years_start,color = as.factor(year_id), shape = as.factor(icd9)))+
      scale_shape_manual(values=c(1,2,4))+
      {if (include_race == T)facet_wrap(~race_cd)}+
      {if (include_race == F)facet_wrap(~location_name)}+
      labs(title = paste0(state_plot, " - loc age trend"), 
           subtitle = subtitle,
           color = 'Year', shape = 'ICD9')
    
  }
  p_1age_time <- NULL
  plot_df_age <- data_with_preds[age_group_years_start == age_plot & location_name %like% state_plot & used_to_fit != "no"]
  if(nrow(plot_df_age)){
    p_1age_time <- ggplot(plot_df_age, aes(x = year_id))+
      {if (include_race==F)geom_ribbon(aes(ymin = lower,ymax = upper), fill = 'grey80')}+
      {if (include_race ==F)geom_line(aes(y = mean)) else geom_line(aes(y = mean, color = as.factor(race_cd))) }+
      {if (include_race ==F)geom_point(aes(y = raw_val, color = paste0(as.factor(reference_dataset), ': ', dataset), shape = as.factor(icd9))) else
        geom_point(aes(y = raw_val, color = as.factor(race_cd), shape = as.factor(icd9))) }+
      scale_shape_manual(values=c(1,2,4))+
      facet_wrap(~location_name)+theme_bw()+
      labs(title = paste0(state_plot, " - time trend -  age ", age_plot),
           caption = paste0('fe_betas: ',fe_betas,'\nRMSE: ',rmse),
           subtitle = subtitle,shape = 'ICD9')

    
    if(param_template$metric =='spend_per_encounter'){
      # add vertical error bars to the data
      yrange <- max(c(plot_df_age[used_to_fit == 'yes']$raw_val, plot_df_age$upper), na.rm = T)+5
      p_1age_time <- p_1age_time + 
        {if(include_race==F)geom_errorbar(aes(ymin = data_lower, ymax = data_upper,color = paste0(as.factor(reference_dataset), ': ', dataset))) else
          geom_errorbar(aes(ymin = data_lower, ymax = data_upper,color = as.factor(race_cd)))}+
        ylim(0, yrange)
    }
    
    
  }
  
  p_1year_age <- NULL
  plot_df_year <- data_with_preds[year_id == year_plot & location_name %like% state_plot & used_to_fit!='no']
  if(nrow(plot_df_year)){
    
    p_1year_age <- ggplot(plot_df_year, aes(x = age_group_years_start))+
      {if (include_race==F)geom_ribbon(aes(ymin = lower,ymax = upper), fill = 'grey80')}+
      {if (include_race ==F)geom_line(aes(y = mean)) else geom_line(aes(y = mean, color = as.factor(race_cd))) }+
      {if (include_race ==F)geom_point(aes(y = raw_val, color = paste0(as.factor(reference_dataset), ': ', dataset), shape = as.factor(icd9))) else
        geom_point(aes(y = raw_val, color = as.factor(race_cd), shape = as.factor(icd9))) }+      
      scale_shape_manual(values=c(1,2,4))+
      facet_wrap(~location_name)+theme_bw()+
      labs(title = paste0(state_plot, " - age trend -  year ", year_plot), 
           caption = paste0('fe_betas: ',fe_betas,'\nRMSE: ',rmse),
           subtitle = subtitle,shape = 'ICD9')
    
    if(param_template$metric =='spend_per_encounter'){
      # add vertical error bars to the data
      yrange <- max(c(plot_df_year[used_to_fit == 'yes']$raw_val, plot_df_year$upper), na.rm = T)+5
      p_1year_age <- p_1year_age + 
        {if(include_race==F)geom_errorbar(aes(ymin = data_lower, ymax = data_upper,color = paste0(as.factor(reference_dataset), ': ', dataset))) else
          geom_errorbar(aes(ymin = data_lower, ymax = data_upper,color = as.factor(race_cd)))}+
        ylim(0, yrange)
    }
    
    
  }
  return(list(loop_plots,p_time, p_age, p_1age_time, p_1year_age))
  
}

plot_state_res <- function(location_re_effects){
  
  #  Purpose: if state model, pull out the REs and SD and make another plot
  #  Required args: location_re_effects as saved during 'do prediction'
  #  Returns: a plot
  #
  
  if(is.null(location_re_effects)){
    return(NULL)
  }
  loc_order <- location_re_effects[order(re_mean)]$location_name
  location_re_effects[, location_name_plot := factor(location_name, levels = loc_order)]
  
  re_plot<- ggplot(location_re_effects, aes(x = location_name_plot, y = re_mean))+
    geom_hline(aes(yintercept = 0), color = 'blue')+
    geom_errorbar(aes(ymin = re_lower, ymax = re_upper))+coord_flip()+labs(x = "", y = "RE")+
    geom_point()
  return(re_plot)
}

save_draws <- function(geog, draws, param_template, index_key, outdir){
  
  #  Purpose: Save draws to model output folder
  #
  #  Details: determine rows according to the location/age/year indexes, add on right location columns,
  #           round draw columns, set draw_schema, save
  #  Required args: geog, draws, param_template, index_key, outdir
  #  Returns: nothing, but draws are saved. There is a print statement to confirm this
  #
  if (include_race == T){
    draws_schema <- arrow::schema(
      acause = utf8(), 
      toc = utf8(), 
      metric = utf8(), 
      geo = utf8(), 
      payer = utf8(),
      pri_payer = utf8(), 
      location = utf8(), 
      state = utf8(), 
      race_cd = utf8(),
      age_group_years_start = uint16(),  
      year_id = uint16(),
      sex_id = int8(), 
      mean = float32(), 
      lower = float32(), 
      upper = float32(), 
      median = float32()
    )
  }else{
    draws_schema <- arrow::schema(
      acause = utf8(), 
      toc = utf8(), 
      metric = utf8(), 
      geo = utf8(), 
      payer = utf8(),
      pri_payer = utf8(), 
      location = utf8(), 
      state = utf8(), 
      age_group_years_start = uint16(),  
      year_id = uint16(),
      sex_id = int8(), 
      mean = float32(), 
      lower = float32(), 
      upper = float32(), 
      median = float32()
    )
    
  }
  
  
  if (geog == 'national'){
    draws$location_index <- 0
  }
  
  index_cols <- c("location_index", "age_index", "year_index")
  if (include_race==T){
    index_cols <- c(index_cols,'race_index')
  }
  
  if (include_race == T){
    draw_cols <- colnames(draws)[colnames(draws) %like% "draw_"]
    draws_and_index <- c(draw_cols,"location_index", "age_index", "year_index","race_index", 'mean','median', 'lower', 'upper')
    draws <- draws[,..draws_and_index]
  }
  
  draws <- merge(draws, index_key, by = index_cols, all.x = T)
  
  tmp <- sapply(colnames(param_template), function(c){
    draws[, paste0(c) := param_template[[c]]]
  })
  
  
  ## merge in state to draw data (for scaling)
  if(geog == "county"){
    cnty_state <- fread("FILEPATH")
    cnty_state[, location := as.character(location)]
    cnty_state[, state:= gsub("- .*", "", location_name)]
    draws <- merge(draws, cnty_state[,.(location)], by = "location", all.x = T)
  }else{
    draws[,state := location]
  }
  
  drop_cols <- c(colnames(draws)[colnames(draws) %like% "index"])
  draws[, c(drop_cols) := NULL]
  
  draws <- draws[order(location, age_group_years_start, year_id, sex_id)]
  draw_names <- colnames(draws)[grepl("draw_", colnames(draws))]
  stat_names <- c("mean", "lower", "upper", "median")
  
  ## round to make output parquet smaller
  if(param_template$metric =="spend_per_encounter"){
    # digits <- 2
    digits <- "%.2e"
  }else{
    # digits <- 8
    digits <- "%.8e"
  }

  draws[,(draw_names) := lapply(.SD, function(x) as.numeric(sprintf(digits, x))), .SDcols = draw_names]
  draws[,(stat_names) := lapply(.SD, function(x) as.numeric(sprintf(digits, x))), .SDcols = stat_names]
  
  ## Check
  if (draws[is.na(age_group_years_start) , .N]>0){
    stop('There are NA ages')
  }
  if (draws[is.na(mean) , .N]>0){
    stop('There are NA mean values')
  }

  
  draws_col_order <- c(
    "acause",
    "toc",
    "metric",
    "geo",
    "payer",
    "pri_payer",
    "location",
    "state",
    if (include_race == T) "race_cd",
    "age_group_years_start",
    "year_id",
    "sex_id",
    "mean",
    "lower",
    "upper",
    "median",
    draw_names
  )
  
  ## add draws in
  start_i <- length(draws_schema)
  for(i in 1:length(draw_names)){
    draws_schema[[start_i + i]] <- arrow::Field$create(paste0(draw_names[i]), float32())
  }
  
  
  draws <- draws[,draws_col_order, with = F] ## set order based on schema (and drop location_name, model name)
  draws_table <- arrow_table(draws) ## convert to arrow table
  draws_table <- draws_table$cast(target_schema = draws_schema) ## set schema
  draws_table %>% group_by(geo, toc, metric, pri_payer, payer) %>% 
    arrow::write_dataset(
      paste0(outdir, "/draws/"), 
      basename_template = paste0("acause_", cause, "_sex", sex,"-{i}.parquet"), 
      existing_data_behavior = "overwrite", 
      compression = "zstd", 
      compression_level = 19
    )
  
  print(paste0("Draws saved to: ", outdir, "/draws !"))
  
  
  
}
