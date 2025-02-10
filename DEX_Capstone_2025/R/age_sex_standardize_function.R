# --------------------------------------------------------------
#   AGE (+ SEX) STANDARDIZATION FUNCTION
#
#       What this function does:
#               - age and sex standardize data to one of two age-sex patterns: the national population structure (payer_spec = F) OR the payer-specific beneficiary structure (payer_spec != F)
#               - loops through causes to ensure that any age-restricted causes will still have weights sum to 1
#
#       What this function doesn't:
#               - work for anything more specific than age-sex standardization (ex age-sex-payer standardization)
#
#
#
#  Authors: Haley Lescinsky and Drew DeJarnatt
#
# -------------------------------------------------------------


'%nin%' <- Negate('%in%')


get_weights <- function(payer_spec = F){
  # 
  #  Get weights for age-standardization. Specifically weights refer to the reference age/sex population that all populations will be aligned with. 
  #  
  #  Inputs
  #    - payer_spec:   if F or if payer_spec = 'oop', the reference population will be the age/sex pattern in the USA population in 2019
  #                    if T, mdcd, mdcr, or priv, the reference population will be the age/sex pattern in the 2019 payer specific beneficiaries in 2019 (from population denominators)
  #
  #  Outputs
  #    - weights: dataset with columns: age, sex, as_weight, payer      the as_weights sum to 1 across all ages/sexes by payer
  #
  

  if(payer_spec == F){
    # 
    #  use USA population structure in 2019 from GBD as weights. Saved here for DEX.
    #
    weights <- fread("FILEPATH/pop_age_sex.csv")[year_id == 2019 & geo == "national"]
    weights[, as_weight := pop/sum(pop)]
    weights[, pri_payer := 'none']
  }else{
    # 
    #  use USA population payer structure in 2019 from population denominators as weights. 
    #
    print("pulling pop denoms")
    
    weights <- open_dataset("/FILEPATH/data/geo=national/") %>% filter(toc == "all" & type == 'total' & year_id == 2019 & pri_payer %in% c('mdcd','mdcr','priv','oop')) %>%
      collect() %>% as.data.table()
    
    if(nrow(weights) == 0){
      stop('something went wrong pulling weights from pop denoms')
    }
    
    weights[pri_payer != 'oop', as_weight := denom/sum(denom), by = 'pri_payer']
    
    # For OOP we want population, for others benes
    weights[pri_payer == 'oop', as_weight := pop/sum(pop), by = 'pri_payer']
    
  }

  return(weights[,.(age_group_years_start, sex_id, as_weight, payer = pri_payer)])
  
}

make_weights_data <- function(data_ages, weights){
  # 
  #  If age standardizing by cause and a cause is missing some ages, don't want to artificially lower the value of the cause rate, so drop any missing ages from the weights
  #      and recalculate age-sex weights that will sum to 1 under the correct age list
  #  
  #  Inputs
  #    - data_ages: list of age_start values in the data
  #    - weights: output of get_weights
  #
  #  Outputs
  #    - weights: dataset with columns: age, sex, as_weight, payer      the as_weights sum to 1 across all ages/sexes by payer
  #
  
  
  # Make sure weights will sum to 1, even if data doesn't have all ages
  drop_ages <- setdiff(weights$age_group_years_start, data_ages)
  
  if(length(drop_ages) > 0){
    weights <- weights[age_group_years_start %in% unique(data_ages)]
  }

  weights[, tot_weight := sum(as_weight), by = 'payer']
  weights[, as_weight := as_weight/tot_weight]

  weights <- weights[, .(age_group_years_start, sex_id, as_weight, payer)]
  
  return(weights)
}

age_sex_standardization <- function(df, value_col, type = 'rate',  by_cols = c('mcnty', 'year_id', 'acause'), payer_spec = F){

  # 
  #  Age-sex standardization function
  #  
  #  Inputs
  #    - df: dataset with columns: age_group_years_start, sex_id, `value_col`, and by_cols
  #    - value_col: name of column you want to standardize
  #    - type: one of 'rate' or 'count'
  #    - by_cols: columns to stratify standardization (and output) by. If using payer_spec = T with multiple payers, include payer here
  #    - payer_spec: character. If T the column 'payer' must exist in the data and the value_col will be age-sex standardized to the age-sex pattern of BENEs in that payer's population denoms
  #                             if F, the national age sex pattern will be used as the reference age pattern 
  #                             if mdcd,priv,mdcr, or oop: the value_col will be age-sex standardized to the age-sex pattern of BENEs in that payer's population denoms
  #
  #  Outputs
  #    - df: dataset with columns: by_cols & `value_col`_stndz
  #
  

  if(sum(! c('age_group_years_start', 'sex_id') %in% colnames(df)) >0){
    stop('missing age_group_years_start and sex_id columns in data')
  }
  
  if(type == 'count' & 'pop' %nin% colnames(df)){
      stop('in order to do count standardization, population must be merged on as `pop`')
  }
  
  if(payer_spec == T & 'payer' %nin% colnames(df)){
    stop("in order to use payer_spec = T, payer must be a column with values mdcr, mdcd, priv, or oop")
  }
  
  if(payer_spec %nin% c(T,F)){
    if(payer_spec %nin% c('mdcr', 'mdcd', 'priv', 'oop')){
      stop('payer_spec must be one of: T, F, mdcr, mdcd, priv, oop')
    }
    
    print(paste0("Age standardizing to the ", payer_spec, " bene pattern ONLY. Setting payer to ", payer_spec))
    
    # Set payer column in df to the payer_spec
    df[, payer := payer_spec]
    
  }
  if(payer_spec == F){
    df[, payer := 'none']
  }
  
  weights <- get_weights(payer_spec = payer_spec)
  
  if('acause' %in% by_cols){
    print('lapplying through acause')
    
    new_df_stndz <- rbindlist(lapply(unique(df$acause), function(c){
      
      df_c <- df[acause == c]
      
      # get weights
      weights <- make_weights_data(unique(df_c$age_group_years_start), weights)
      df_c <- merge(df_c, weights, by = c('age_group_years_start', 'sex_id', 'payer'))
      if(payer_spec == F){
        df_c[, payer := NULL]
      }
      
      if(type == 'rate'){
        
        
        # Weighted sum of the rate
        
        setnames(df_c, value_col, 'rate')
        df_stndz <- df_c[, .(adj_rate = sum(rate * as_weight)), by = by_cols]
        setnames(df_stndz, 'adj_rate', paste0(value_col, "_stndz"))
        return(df_stndz)
        
      }else if(type == 'count'){
        
        # Convert to rate
        # Multiply by age standardized county population to get #
        
        df_c[, county_pop := sum(pop), by = by_cols]
        df_c[, county_pop_stndz := as_weight*county_pop]
        
        setnames(df_c, value_col, 'num')
        df_stndz <- df_c[, .(adj_num = sum(num/pop * county_pop_stndz)), by = by_cols]
        setnames(df_stndz, 'adj_num', paste0(value_col, "_stndz"))
        
        return(df_stndz)
      }else{
        stop('type should be count or rate')
      }
      
    }))
  }else{
    
    # get weights
    weights <- make_weights_data(unique(df$age_group_years_start), weights)
    df <- merge(df, weights, by = c('age_group_years_start', 'sex_id', 'payer'))
    if(payer_spec == F){
      df[, payer := NULL]
    }
    
    if(type == 'rate'){
      
      
      # Weighted sum of the rate
      
      setnames(df, value_col, 'rate')
      df_stndz <- df[, .(adj_rate = sum(rate * as_weight)), by = by_cols]
      setnames(df_stndz, 'adj_rate', paste0(value_col, "_stndz"))
      return(df_stndz)
      
    }else if(type == 'count'){
      
      # Convert to rate
      # Multiply by age standardized county population to get #
      
      df[, county_pop := sum(pop), by = by_cols] # total (all-age, both-sex) population
      df[, county_pop_stndz := as_weight*county_pop] # what the pop count would be in each age/sex if standard population
      
      setnames(df, value_col, 'num')
      df_stndz <- df[, .(adj_num = sum(num/pop * county_pop_stndz)), by = by_cols] # keeps standardized value in count space
      setnames(df_stndz, 'adj_num', paste0(value_col, "_stndz"))
      
      return(df_stndz)
    }else{
      stop('type should be count or rate')
    }
    
  }
  
  return(new_df_stndz)

}