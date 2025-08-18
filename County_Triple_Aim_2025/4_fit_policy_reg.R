# ----------------------------------------------------------------
#
#   Fit policy regressions
#         - county level
#         - unadj score + controls
#
#   Author: Haley Lescinsky
# ----------------------------------------------------------------

pacman::p_load(data.table, tidyverse, dplyr, ggplot2, arrow, gridExtra, sandwich, glmnet)
set.seed(3457)
library(MASS)
library(usmap)
library(sandwich)
library(lmtest)
code_dir <- "FILEPATH"

counties <- fread("FILEPATH/merged_counties.csv")
states <- fread("FILEPATH/states.csv")

parent_dir <- "FILEPATH"

# Set args-----------------
date_version <- "xxx"
draws <- T
adjusted_wo_controls <- F

# -------------------------

##------------------
##   STEP 1: LOAD COUNTY DATA
##
##------------------

sens <- T
triple_aim <- fread(paste0(parent_dir, date_version, "FILEPATH"))
if(adjusted_wo_controls == T){
  triple_aim <- fread(paste0(parent_dir, date_version, "FILEPATH"))
  
  # rename adjusted variables to regular
  setnames(triple_aim, c('composite_score', 'double_composite_score', 'composite_score_adj', 'double_composite_score_adj'),
           c('composite_score_unadj', 'double_composite_score_unadj', 'composite_score', 'double_composite_score'))
  
}

# # Option to instead run on a sensitivity result
# sens <- T
# sens_name <- "sens_w_prematuremort"
# triple_aim <- fread(paste0(parent_dir, date_version, "FILEPATH"))

# county covs
df_w_covs <- add_covs_to_data(triple_aim)
df_w_covs <- merge(df_w_covs, counties, by = 'mcnty')

county_covs_test <- c(
  'primarycare_phys_nonfed_ratio_to_mds', 
  'prop_medicare_mc',
  'percent_smokers',
  'race_nh_black',
  'prop_age_over_65',
  'race_nh_aian',
  'ethn_hisp',
  'race_nh_api',
  'race_nh_white',
  'rural',
  'edu_hs',
  'income_median',
  'obesity_prev')

df_w_covs <- df_w_covs[covariate %in% county_covs_test]

drop_years <- unique(df_w_covs[is.na(cov_value), .N, by = c('covariate', 'year_id')]$year_id) 
if(length(drop_years) > 0){
  print(paste0("dropping years ", paste0(drop_years, collapse = ","), " because missing in "))
  print(df_w_covs[is.na(cov_value), .N, by = c('covariate', 'year_id')])
  df_w_covs <- df_w_covs[!(year_id %in% drop_years)]
}

# log income
df_w_covs[covariate %in% c('income_median') & cov_value == 0, cov_value := 0.00001]
df_w_covs[covariate %in% c('income_median'), cov_value := log(cov_value)]

# standardize each covariate to mean 0 sd 1
covariate_units <- df_w_covs[, .('mean_cov' = mean(cov_value), 'sd_cov' = sd(cov_value)), by = c('covariate', 'draw')]
df_w_covs[, value_norm := (cov_value - mean(cov_value))/sd(cov_value), by = c('covariate', 'draw')]

# reshape
county_reg_data <- dcast(df_w_covs, state_name + mcnty + year_id + draw + composite_score + double_composite_score + spend + health_outcomes + patient_exp_pca ~ covariate, value.var = 'value_norm')


##------------------
##   STEP 2: LOAD STATE DATA
##
##------------------

hhi_state <-  read_feather("FILEPATH/data.feather") %>% as.data.table()
hhi_state <- hhi_state[!(location_id %like% 'NA'),.(state_name = location_name, year_id, drive_time_HOSPBD_30_minutes_all)]
hhi_state <- hhi_state[, .(value = mean(drive_time_HOSPBD_30_minutes_all),
                           variable = 'hospital_hhi'), by = c('state_name', 'year_id')]

# mc rate by state
mdcd_mc <- fread("FILEPATH")
mdcd_mc <- mdcd_mc[type == 'mc' & pri_payer == 'mdcd',.(state, year_id, prop_mdcd_mco = percent_of_tot )]
mdcd_mc <- merge(mdcd_mc, states[,.(state = abbreviation, state_name)], by = 'state')[, state := NULL]

covariate_units_mdcd_mco <- data.table('variable' = 'prop_mdcd_mco', mean_cov = mean(mdcd_mc$prop_mdcd_mco), sd_cov = sd(mdcd_mc$prop_mdcd_mco))
mdcd_mc[, prop_mdcd_mco  := (prop_mdcd_mco  - mean(prop_mdcd_mco ))/sd(prop_mdcd_mco )]


state_policy_vars <- fread("FILEPATH/policy_vars.csv")
setnames(state_policy_vars,  'location_name', 'state_name')
state_policy_vars <- melt(state_policy_vars, id.vars = c('state_name', 'year_id', 'population', 'nid'))
state_policy_vars[value == 'Yes', value := '1']
state_policy_vars[value == 'No', value := '0']
state_policy_vars[, value := as.numeric(value)]
state_policy_vars <- state_policy_vars[!is.na(value)]

state_policy_vars[, N := .N, by = c('variable', 'year_id')]
state_policy_vars <- state_policy_vars[N == 51]
state_policy_vars  <- rbind(state_policy_vars , hhi_state, fill = T)

unique(state_policy_vars$variable)


state_covs_test <- c(
  'hospital_hhi',
  'mdcd_expanded',
  "prev_obese",
  'ma_pct', 
  'hhi_individ',
  'hhi_small',  
  'hhi_large',  
  "max_elig_c", 
  "max_elig_p", 
  "max_elig_m",
  'prop_mdcd_mco')


state_covs_test <- setdiff(state_covs_test, 'prev_obese')

state_policy_vars <- state_policy_vars[variable %in% state_covs_test]
state_policy_vars <- state_policy_vars[year_id %in% unique(county_reg_data$year_id)]


# standardize
covariate_units_state <- state_policy_vars[, .('mean_cov' = mean(value), 'sd_cov' = sd(value)), by = c('variable')]
state_policy_vars[, value_norm := (value - mean(value))/sd(value), by = c('variable')]

state_reg_covs <- dcast(state_policy_vars, state_name + year_id ~ variable, value.var = 'value_norm')
state_reg_covs <- merge(state_reg_covs, mdcd_mc, by = c('year_id', 'state_name'))

##------------------
##   STEP 3: COMBINE STATE + COUNTY DATA
##
##------------------

full_reg_data  <- merge(county_reg_data, state_reg_covs, by = c('state_name', 'year_id'), all.x = T)
full_reg_data[!complete.cases(full_reg_data)]

# save out covariates correlations
cov_only <- full_reg_data[draw == min(draw), c(state_covs_test, county_covs_test), with = F]

cor_matrix <- cor(cov_only)
cor_matrix <- as.data.frame(cor_matrix)
var <- rownames(cor_matrix)
cor_matrix$var <- var
setcolorder(cor_matrix, "var")

# combine all covariate units! 
# ----
covariate_units_state <- rbind(covariate_units_state, covariate_units_mdcd_mco, fill = T)
covariate_units <- covariate_units[draw ==1][, draw := NULL]
setnames(covariate_units_state, 'variable', 'covariate')

covariate_units <- rbind(covariate_units[,geo := 'county'], 
                         covariate_units_state[, geo := 'state'], fill = T)

covariate_units[covariate %in% c('income_median'), note := 'logged']

##------------------
##   STEP 4: DO REGRESSION
##
##
##------------------

# SET PATHS
save_dir <- paste0(parent_dir, date_version, "FILEPATH")
if(adjusted_wo_controls == T){
  save_dir <- paste0(parent_dir, date_version, "FILEPATH")
}

dir.create(save_dir, recursive = T)
write.xlsx(cor_matrix, paste0(save_dir, "FILEPATH"))
write.csv(covariate_units, paste0(save_dir, "FILEPATH"), row.names = F)

# Fit reg
#----------------------------
fit_reg <- function(full_reg_data, 
                    reg_var = 'composite_score',
                    clustered_robust_se = T,
                    covariates_test){
  
  # Fit regressions with draws
  all_coefs <- data.table()
  for(d in unique(full_reg_data$draw)){
    reg_data <- full_reg_data[draw == d]
    
    # Year FEs
    formula <- as.formula(paste0(reg_var, " ~ (", paste(covariates_test, collapse = " + "), " + as.factor(year_id)) - 1"))
    
    reg <- lm(formula, data = reg_data)
    
    # Take draws from vcov matrix to capture data + model uncertainty
    for(i in 1:10){
      reg_sum <- summary(reg)
      coefs <- summary(reg)$coefficients
      coefs <- data.table(coefs)
      
      if(clustered_robust_se == F){
        model1_robust_stata <- coeftest(reg,
                                        vcov = vcovHC,
                                        type = "HC1")
        newbeta <- coef(reg) + chol(vcovHC(reg, type="HC1"))%*%rnorm(nrow(coefs))
      }else{
        
      #Clustered robust standard errors with lm()
        model1_robust_clustered <- coeftest(reg,
                                            vcov = vcovCL,
                                            type = "HC1",
                                            cluster = ~mcnty)
        newbeta <- coef(reg) + chol(vcovCL(reg, type="HC1", cluster = ~mcnty))%*%rnorm(nrow(coefs))
      }
      
      n_df <- newbeta[, 1]
      
      coef_table <- data.table('covariate' = names(n_df), 
                               'beta' = n_df, 
                               'draw' = d)

      coef_table[, geo := ifelse(covariate %in% state_covs_test, "state", "county")]
      all_coefs <- rbind(all_coefs, coef_table)
      
    }
    
  }
  
  return(all_coefs)
  
}

# SET POLICIES AND CONTROLS
policy_covs <- c('hospital_hhi', 'hhi_individ', 'hhi_small', 'max_elig_c', 'max_elig_p', 'max_elig_m', 'prop_mdcd_mco', 'prop_medicare_mc', 'mdcd_expanded', 'hhi_large')
control_covs <- c("edu_hs", "income_median", "obesity_prev", "percent_smokers", 
                  "prop_age_over_65", "rural")


if(adjusted_wo_controls== T){
  control_covs <- c()
}
n_controls <- length(control_covs)

# 
#  MAIN, run regressions for main results and a handful of sensitivity results
# #--------------------------


if(sens == F){

  
  all_coefs_clustered <- rbindlist(lapply(policy_covs, function(p){
    
    message(p)
    covariates_test <- c(control_covs, p)
    
    reg_results <- fit_reg(full_reg_data, reg_var = 'composite_score', covariates_test)
    reg_results[, main_policy_var := p]
    
    return(reg_results)
    
  }))
  write.csv(all_coefs_clustered, paste0(save_dir, "FILEPATH"), row.names = F)
  
  
  #  SENS - double composite
  # #--------------------------
  new_var <- 'double_composite_score'
  
  all_coefs_clustered <- rbindlist(lapply(policy_covs, function(p){
    
    message(p)
    covariates_test <- c(control_covs, p)
    
    reg_results <- fit_reg(full_reg_data, reg_var = new_var, covariates_test)
    reg_results[, main_policy_var := p]
    
    return(reg_results)
    
  }))
  write.csv(all_coefs_clustered, paste0(save_dir, "/sens_",new_var,"_",n_controls,"controls.csv"), row.names = F)
  
  
  #  SENS - no controls
  # #--------------------------
  
  all_coefs_clustered_nocontrols <- rbindlist(lapply(policy_covs, function(p){
    
    message(p)
    covariates_test <- c(p)
    
    reg_results <- fit_reg(full_reg_data, reg_var = 'composite_score', covariates_test)
    reg_results[, main_policy_var := p]
    
    return(reg_results)
    
  }))
  write.csv(all_coefs_clustered_nocontrols, paste0(save_dir, "/sens_nocontrols.csv"), row.names = F)

  
}else{
  
  all_coefs_clustered <- rbindlist(lapply(policy_covs, function(p){
    
    message(p)
    covariates_test <- c(control_covs, p)
    
    reg_results <- fit_reg(full_reg_data, reg_var = 'composite_score', covariates_test)
    reg_results[, main_policy_var := p]
    
    return(reg_results)
    
  }))

  write.csv(all_coefs_clustered, paste0(save_dir, "/sens_", sens_name, "_",n_controls,"controls.csv"), row.names = F)
  
  
}


print("Everything saved!")


