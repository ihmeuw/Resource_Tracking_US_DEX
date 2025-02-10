##############################################
#  Compile all prevalence predictions, bound variation within a state, and rake to state prevalence
# 
#
#  Authors: Drew DeJarnatt and Haley Lescinsky
#
##############################################

Sys.umask(mode = 002)
pacman::p_load(data.table, tidyverse, dplyr, ggplot2, arrow)
library(tidyverse)
library(data.table)
library(arrow)
source("/FILEPATH/get_cause_metadata.R")
source('/FILEPATH/get_age_metadata.R')

'%nin%' <- Negate('%in%')

#------
# Set paths
#------

parent_dir <- "/FILEPATH/"
work_dir <- paste0(parent_dir, "/inc_prev_prediction/")

#------
# Set versions
#------

if(interactive()){
  # ARGUMENTS FOR RUNNING INTERACTIVELY
  
  draws <- T# T or F
  draw_num <- 0
  version_id <- 'XX' 
  work_dir <- paste0(work_dir, version_id, '/')
  cap_rho <- T

}else{
  # ARGUMENTS PULLED THROUGH FROM LAUNCHER
  
  args <- commandArgs(trailingOnly = TRUE)
  message(args)
  
  version_id <- args[1]
  draws <- as.logical(args[2])
  cap_rho <- as.logical(args[3])
  
  
  if(draws == T){
    draw_num <-  as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID")) - 1
    print(paste0("draws = T. running with draw: ", draw_num))
  }else{
    draw_num <- 0
    print("means only!")
  }
  
  
  work_dir <- paste0(work_dir, version_id, '/')

}

#------
# Load table of lasso coefficients to determine which we can use
#------
threshold <- 0

if(draws==T){
  coefs <- rbindlist(lapply(list.files(paste0(work_dir, "/viz_by_cause"), pattern = 'lasso', full.names = T), fread))
}else{
  coefs <- fread(paste0(work_dir, "/FILEPATH"))
}

# check that causes have coefficient for Mx for all available sexes
ln_mx_coefs <- coefs[cov == 'ln_mx',.(acause, cov, beta = est_coeff.V1)]
ln_mx_coefs[, n := .N, by = acause]
sex_res <- c("neo_cervical", "neonatal_enceph", "neo_ovarian", "neo_prostate", "neo_uterine") #sex restricted causes
ln_mx_coefs[, sex_avail := ifelse(acause %in% sex_res, 1, 2)]
# some causes aren't in ln_mx_coefs because neither sex model kept Mx as a covariate
# drop causes that are in the causelist but aren't among those with Mx covariate in all available models
drop_causes1 <- setdiff(sort(unique(coefs$acause)), ln_mx_coefs[n == sex_avail, acause])

# drop causes where one or both sex models have an R2 < 0.9
r2_coefs <- distinct(coefs, acause, r2, sex_id, .keep_all = FALSE)
drop_causes <-unique(c(drop_causes1, unique(r2_coefs[r2 < 0.9, acause]))) 

valid_causes <- unique(coefs[acause%nin%drop_causes]$acause)
write.csv(ln_mx_coefs[acause %in% drop_causes & cov == 'ln_mx'], paste0(work_dir, "/causes_DROPPED_due_to_coef_on_mx_n",length(drop_causes),".csv"), row.names = F)
write.csv(data.table('acause' = valid_causes, 'threshold' = threshold), paste0(work_dir, "/included_causes_n",length(valid_causes),".csv"), row.names = F)

#------
# Load rho and mortality data
#------
if(draws == T){
  
  mort_col <- ifelse(draw_num == 0, "mortality_rate_mean", paste0("mx_draw_", draw_num))
  
  # takes a very long time
  pred_prev <- open_dataset(paste0(work_dir, "/data")) %>% filter(draw == draw_num) %>% collect() %>% as.data.table()
  mort <- open_dataset(paste0(parent_dir, "/mort_draws_pop_agesex")) %>% 
    select(all_of(c('mcnty','sex_id', 'year_id','age_group_years_start','acause','pop', mort_col))) %>% collect() %>% as.data.table()
 
  setnames(mort, mort_col, "mortality_rate")
  pred_prev <- pred_prev[acause %in% valid_causes]
           
  
}else{
  pred_prev <- open_dataset(paste0(work_dir, "/data")) %>% filter(acause %in% valid_causes) %>% collect() %>% as.data.table()
  mort <- open_dataset(paste0(parent_dir, "/mort_pop_agesex")) %>% collect() %>% as.data.table()
  
  
}

causelist_map <- fread("/FILEPATH/ushd_dex_causelist.csv")[shared == 1]

#------
# Use Add on pop and mort rate
#------

combine <- merge(pred_prev, mort[,.(mcnty, sex_id, age_group_years_start, year_id, acause, mortality_rate, pop)],
                 by = c('sex_id', 'mcnty', 'age_group_years_start', 'year_id', 'acause'))

combine[, prev := prev_rate * pop]
combine[, deaths := mortality_rate * pop]
combine[, rho := deaths/prev][is.nan(rho), rho := 0]

# add on map of what prevalence is
combine <- merge(combine, causelist_map[,.(acause = dex_acause, case_def, ushd_acause)], by = 'acause')

# calculate rho across all ages
combine_all_age <- combine[, .(prev = sum(prev), deaths = sum(deaths), pop=sum(pop)), by = c('acause','mcnty','year_id','location_name','case_def','ushd_acause')]
combine_all_age[, rho := deaths/prev]

#------
# Load state prevalence, rho, mort
#------

# RHO + mort (always at mean level)
st_mort <- open_dataset("/FILEPATH/mx_prev_inc_yll_yld") %>% collect() %>% as.data.table()
st_mort[is.na(mx), mx := 0]
st_mort[, rho := mx/state_rate][is.nan(rho), rho := 0]
st_mort[, case_def := ifelse(state_rate == prev, 'prevalence', 'incidence')]

# calculate rho across all ages
st_mort_all_age <- st_mort[, .(prev = sum(state_rate*pop, na.rm = T), deaths = sum(mx*pop, na.rm = T)), by = c('ushd_acause','year_id','location_name','case_def')]
st_mort_all_age[, rho := deaths/prev]

# prevalence (mean level OR draws)
if(draws == F){
  
  st_prev <- st_mort[,.(acause, ushd_acause, case_def, age_group_years_start, sex_id, location_name, state_rate, year_id)]
  
}else{
  
  st_prev <- open_dataset("/FILEPATH/prev_inc_draws") %>% filter(draw == as.character(draw_num)) %>% collect() %>% as.data.table()
  st_prev <- st_prev[,.(acause, ushd_acause, case_def, age_group_years_start, sex_id, location_name = state_name, state_rate, year_id)]
  
}

#------
# Impose upper / lower bound on rho 
#------
mod_z_score_threshold <- 3.5 # +/- this. standard is 3.5 for outlier detection

bound_rho <- function(mod_z_score_threshold, df){
  # calculate modified z scores
  df[, rho := deaths/prev]
  df[, `:=` (orig_rho = rho, orig_prev = prev)]
  rho_outlier_detect <- st_mort[, .(z_median = median(rho), z_mad = mad(rho)), by = c('ushd_acause', 'age_group_years_start','sex_id')]
  df <- merge(df, rho_outlier_detect, by = c('ushd_acause', 'age_group_years_start','sex_id'))
  df[, mod_z_score := 0.6745*(rho - z_median) / z_mad]
  df[z_mad == 0, mod_z_score := 0]
  df[, rho_outliered := ifelse(mod_z_score <= -mod_z_score_threshold | mod_z_score >= mod_z_score_threshold, 1, 0)]
  
  # percent outliered
  summary_stat_rho_bounded <- df[, .(percent_outliered = sum(rho_outliered)/.N), by = 'acause']
  summary_stat_rho_bounded[, approach := 'age specific']
  if(draws == F | draw_num == 0){
    write.csv(summary_stat_rho_bounded[, mod_z_score_threshold := mod_z_score_threshold], paste0(work_dir, "/summary_of_rho_outliering_impact.csv"), row.names = F)
  }
  
  # bound rho
  df[mod_z_score <= -mod_z_score_threshold, rho := (-mod_z_score_threshold*z_mad)/0.6745 + z_median]
  df[mod_z_score >= mod_z_score_threshold, rho := (mod_z_score_threshold*z_mad)/0.6745 + z_median]
  df[rho > 1, rho := 1]
  
  # calculate prevalence off of new rho
  df[, prev := deaths/rho][rho == 0, prev := 0]
  df[, prev_rate := prev/pop][pop == 0, prev_rate := 0]
  
  df[, `:=` (z_median = NULL, z_mad = NULL, mod_z_score = NULL, rho_outliered = NULL, orig_prev = NULL, orig_rho = NULL)]
  return(df)  
}
  
#------
# Apply function adjustments to data
#------
if(cap_rho == T){
  combine <- bound_rho(mod_z_score_threshold, df = combine)
  print("cap_rho = T")
}

#------
# Rake counties to state prevalence / incidence 
#------


# Aggregate county prev to state level
county_agg <- combine[, .(sum_prev = sum(prev), sum_pop = sum(pop)), by = .(acause, ushd_acause, sex_id, age_group_years_start, case_def, year_id, location_name )]

# Calculate ratio between state and county agg
state_estimates <- merge(county_agg, st_prev, by = c("ushd_acause","acause", "sex_id", "age_group_years_start","year_id", "location_name", "case_def"), all.x = TRUE)
# If case_def is prev - use state prev, if inc - use state inc as state_count
state_estimates[, state_estimate := state_rate * sum_pop]
state_estimates <- state_estimates[, ratio := sum_prev/state_estimate]
state_estimates[,`:=` (case_def = NULL,state_rate = NULL, mx = NULL)]

# Merge ratios on to county level prev 
prev_out <- merge(combine, state_estimates, by = c("acause", 'ushd_acause', "sex_id", "age_group_years_start","year_id", "location_name"), all.x = TRUE)

# Adjust prevalence
prev_out[, prev_unadj := prev]
prev_out[!is.na(ratio), prev := prev_unadj/ratio]
prev_out[, prev_rate := prev / pop]
prev_out[pop == 0, prev_rate := 0]

#------
# Cap prev rate at 0.99 and re-rake
#------
if(nrow(prev_out[prev_rate > .99]) > 0){

  print("Some prev rates are above 1, bounding those at 0.99 and reraking")
  
  prev_out[ prev_rate > 0.99, prev_rate := 0.99]
  prev_out[, prev := prev_rate * pop]
  
  # Rerake
  prev_out[, `:=` (sum_prev = NULL, sum_pop =  NULL, state_estimate = NULL, ratio = NULL)]
  
  # Aggregate county prev to state level
  county_agg <- prev_out[, .(sum_prev = sum(prev), sum_pop = sum(pop)), by = .(acause, ushd_acause, sex_id, age_group_years_start, case_def, year_id, location_name )]
  
  # Calculate ratio between state and county agg
  state_estimates <- merge(county_agg, st_prev, by = c("ushd_acause",'acause', "sex_id", "age_group_years_start","year_id", "location_name", "case_def"), all.x = TRUE)
  unique(state_estimates[is.na(state_rate)]$acause)  # actually missing! 
  state_estimates[, state_estimate := state_rate * sum_pop]
  state_estimates <- state_estimates[, ratio := sum_prev/state_estimate]
  state_estimates[,`:=` (state_inc = NULL, state_prev = NULL, case_def = NULL)]
  
  # Merge ratios on to county level prev 
  prev_out <- merge(prev_out, state_estimates, by = c("acause", 'ushd_acause', "sex_id", "age_group_years_start","year_id", "location_name"), all.x = TRUE)
  
  # Adjust prevalence
  prev_out[!is.na(ratio), prev := prev/ratio]
  prev_out[, prev_rate := prev / pop]
  
  prev_out[ prev_rate > 1, prev_rate := 0.9999]
  prev_out[, prev := prev_rate * pop]

}

# Fix NANs due to low population
prev_out[pop == 0, `:=` (prev_rate = 0, prev = 0)]


#------
# Save!
#------

# Clean a few columns and names
states <- fread('/FILEPATH/states.csv')
prev_out <- merge(prev_out, states[,.(location_name = state_name, abbreviation)], by = 'location_name', all = T)
prev_out <- prev_out[,.(acause, ushd_acause, sex_id, age_group_years_start, year_id, state_name = location_name, state = abbreviation, location = as.character(mcnty), prev, prev_rate, pop, case_def)]

if(nrow(prev_out[is.na(prev_rate) | is.nan(prev_rate)]) > 0){
  print(prev_out[is.na(prev_rate) | is.nan(prev_rate)])
  stop("NAs or NANs in prevalence rate")
}

if(cap_rho == F){
  filename = "/unadjust_"
}

if(draws == T){
  prev_out[, draw := draw_num]
  write_dataset(prev_out, paste0(work_dir, filename,"prev_agesex_draws/"), partitioning = 'year_id', basename_template = paste0("draw_", draw_num, "-{i}.parquet"))
  
}else{
  write_dataset(prev_out, paste0(work_dir, filename,"prev_agesex/"), partitioning = 'year_id')
  
}


print("Done!")
print(work_dir)
