##----------------------------------------------------------------
## Title: function_rake.R
## Purpose: functions for ensuring estimates are internally consistent between geography levels
## 
## 
## Authors: Azalea Thomson and Haley Lescinsky
## Last update: 12/9/24
##----------------------------------------------------------------

rake_county_to_state <- function(data) {
  
  rake_by_cols <- c('year_id','age_group_years_start','state','sex_id','pri_payer','payer','acause','draw')
  
  # 0. If county estimates are missing, set to zero
  if (nrow(data[is.na(spend)])>0 | nrow(data[is.na(vol)])>0){
    message('Warning! You have missing county estimates where we have state estimates.
            Setting these to zero.')
  }
  data[is.na(spend), spend:= 0 ]
  data[is.na(vol), vol:= 0 ]

  
  if(nrow(data[state_spend < 0 | state_vol < 0]) > 0){
    print(data[state_spend < 0 | state_vol < 0])
    stop('There are negative state estimates')
  }
  
  # 1. sum spend and vol across counties
  data[, summed_county_spend := sum(spend), by = rake_by_cols]
  data[, summed_county_vol := sum(vol), by = rake_by_cols]
  
  # 2. Offset zeros, only when all counties are zero and the state is non zero
  # Use county-payer population as offset values 
  
  if (nrow(data[(summed_county_spend ==0 & state_spend >0) | (summed_county_vol ==0 & state_vol >0) ]) >0){
    print('Offsetting states where all counties are zero and state is non zero with payer pop denom as offset')
    data[summed_county_spend ==0 & state_spend >0,spend:=denom]
    data[summed_county_vol ==0 & state_vol >0,vol:=denom]
    
    # re-sum now that you've done offsetting if necessary
    data[, summed_county_spend := sum(spend), by = rake_by_cols]
    data[, summed_county_vol := sum(vol), by = rake_by_cols]
    
    if (nrow(data[(summed_county_spend ==0 & state_spend >0) | (summed_county_vol ==0 & state_vol >0) ]) >0){
      warning('You have a state where all counties have payer population of zero! Raking is using populations, not sample pop')
      
      # merge on county population estimates
      pop <- fread("/FILEPATH/pop_age_sex.csv")[geo == 'county']
      data <- merge(data, pop[,.(location, year_id, age_group_years_start, sex_id, pop)], by = c('year_id','age_group_years_start', 'sex_id','location'), all.x = T)
      
      data[(summed_county_spend ==0 & state_spend >0), spend := pop ]
      data[(summed_county_vol ==0 & state_vol >0) , vol := pop]
      
      # re-sum now that you've done offsetting if necessary
      data[, summed_county_spend := sum(spend), by = rake_by_cols]
      data[, summed_county_vol := sum(vol), by = rake_by_cols]
    }
    
  }
  
  if (nrow(data[is.na(state_spend)])>0 | nrow(data[is.na(state_vol)])>0){

    message('Warning! You have missing state estimates where we have county estimates.
          Dropping them from county!')
    in_county_not_state <- copy(data)[(is.na(state_spend) | is.na(state_vol))]
    data <- data[! (is.na(state_spend) | is.na(state_vol))]
    
  }
  
  
  # 2. generate raking weights by dividing the upper-level geo (e.g., state) spend or vol by the crude county sum
  data[, rake_weight_sp := state_spend / summed_county_spend]
  data[, rake_weight_vol := state_vol / summed_county_vol]
  
  # if state is zero or missing and county is not, rake weight will be NA
  # if county is zero or missing and state is not, rake weight will be Inf
  # Convert to zeros.
  data[is.na(rake_weight_sp), rake_weight_sp := 0]
  data[is.na(rake_weight_vol), rake_weight_vol := 0]
  data[rake_weight_sp == 'Inf', rake_weight_sp := 0]
  data[rake_weight_vol == 'Inf', rake_weight_vol := 0]
  
  # 3. generate raked estimates by multiplying the original spend or vol by the raking weights
  data[, raked_spend := spend * rake_weight_sp]
  data[, raked_vol := vol * rake_weight_vol]
  
  if (nrow(data[is.na(raked_spend)]) > 0) {
    stop('Warning, raking produced NAs')
  }
  ## Confirm raked totals equal state totals
  data[, summed_raked_county_spend := sum(raked_spend), by = rake_by_cols]
  data[, summed_raked_county_vol := sum(raked_vol), by = rake_by_cols]
  
  if (nrow(data[round(summed_raked_county_spend, digits = 2) != round(state_spend, digits = 2)]) >0){
    stop('Raked county sum does not equal state sum')
  }

  
  data[, c("spend","vol","summed_county_spend", "summed_county_vol", "rake_weight_sp", "rake_weight_vol","summed_raked_county_spend", "summed_raked_county_vol") := NULL]
  setnames(data, c('raked_spend','raked_vol'), c('spend','vol'))
  
  return(data)
}


rake_state_to_nat <- function(data){
  
  # 0. If state estimates are missing, set to zero
  if (nrow(data[is.na(spend)])>0 | nrow(data[is.na(vol)])>0){
    message('Warning! You have missing state estimates where we have national estimates.
            Setting these to zero.')
  }
  data[is.na(spend), spend:= 0 ]
  data[is.na(vol), vol:= 0 ]
  
  # 1. sum spend and vol across states
  rake_by_cols <- c('year_id','age_group_years_start','sex_id','pri_payer','payer','acause','draw')

  data[, summed_state_spend := sum(spend), by = rake_by_cols]
  data[, summed_state_vol := sum(vol), by = rake_by_cols]
  
  # 2 . Offset zeros (only important when we have a non zero national but we have a zero state)
  # Use denom as offset values 
    
  data[summed_state_spend ==0 & national_spend >0,spend:=denom]
  data[summed_state_vol ==0 & national_vol >0,vol:=denom]
  
  # re-sum now that you've done offsetting if necessary
  data[, summed_state_spend := sum(spend), by = rake_by_cols]
  data[, summed_state_vol := sum(vol), by = rake_by_cols]
  
  if (nrow(data[(summed_state_spend ==0 & national_spend >0) | (summed_state_vol ==0 & national_vol >0) ]) >0){
    stop('You have a state where payer population is zero! Raking will not work')
  }
  
  if(nrow(data[national_spend < 0 | national_vol < 0]) > 0){
    print(data[national_spend < 0 | national_vol < 0])
    stop('There are negative national estimates')
  }
  

  if (nrow(data[is.na(national_spend)])>0 | nrow(data[is.na(national_vol)])>0 ){
    message('Warning! You have missing national estimates where we have state estimates.')
    message('Since we are raking to national, we will zero out state')
      
    data[is.na(national_spend), national_spend:= 0]
    data[is.na(national_vol), national_vol:= 0]
  }
  
  # 2. generate raking weights by dividing the upper-level (e.g., national) spend or vol by the crude state sum
  data[, rake_weight_sp := national_spend / summed_state_spend]
  data[, rake_weight_vol := national_vol / summed_state_vol]
  
  # if national is zero or missing and state is not, rake weight will be NA
  # if state is zero or missing and national is not, rake weight will be Inf
  # Convert to zeros.
  data[is.na(rake_weight_sp), rake_weight_sp := 0]
  data[is.na(rake_weight_vol), rake_weight_vol := 0]
  data[rake_weight_sp == 'Inf', rake_weight_sp := 0]
  data[rake_weight_vol == 'Inf', rake_weight_vol := 0]
  
  # 3. generate raked estimates by multiplying the original spend or vol by the raking weights
  data[, raked_spend := spend * rake_weight_sp]
  data[, raked_vol := vol * rake_weight_vol]
  

  weights <- unique(data[,.(toc, acause, year_id, age_group_years_start, sex_id, pri_payer, payer, rake_weight_sp, rake_weight_vol)])
  
  
  if (nrow(data[is.na(raked_spend)]) > 0) {
    stop('Raking produced NAs')
  }
  
  ## Confirm raked totals equal national totals
  data[, summed_raked_state_spend := sum(raked_spend), by = rake_by_cols]
  data[, summed_raked_state_vol := sum(raked_vol), by = rake_by_cols]
  
  if (nrow(data[round(summed_raked_state_spend, digits = 2) != round(national_spend, digits = 2)]) >0){
    stop('Raked state sum does not equal national sum')
  }
  
  
  data[, c("spend","vol","summed_state_spend", "summed_state_vol", "rake_weight_sp", "rake_weight_vol","summed_raked_state_spend", "summed_raked_state_vol") := NULL]
  data[,c('national_spend','national_vol'):=NULL]
  setnames(data, c('raked_spend','raked_vol'), c('spend','vol'))
  
  return(list(data,weights))
}

