# -------------------------------
#  Comorb functions
#    - comorb_restrictions applies a set of restrictions on primary causes and comorbidity pairs that we restrict
#    - tune_regression fits a lasso regression with cross-validation to tune it
#    - get_coefs fits the lasso regression with the tuned value of lambda and returns non-zero coefficients
#    - calc_af calculates the conditional probability for a pricause-comorb pair, then uses that + RR from get_coefs to calculate the AF
#
#  Author: Haley Lescinsky
# -------------------------------
pacman::p_load(glmnet, doParallel, scales, RColorBrewer, ggplot2)


# load functions
comorb_restrictions <- function(data){
  
  # cannot have comorbidities -> drop from data
  data <- data[!(pri_cause %like% "neo_" | pri_cause %in% c("neonatal_preterm", "neonatal_enceph") | pri_cause %like% "exp_well_dental" | pri_cause %like% "lri_corona")]
  
  # cannot be comorbidities anywhere
  drop_everywhere <- c("maternal_indirect", "nutrition", "hepatitis_c", "_infect_agg", 
                       "meningitis", "septicemia", "rf_hypertension", "rf_hyperlipidemia", 
                       "rf_obesity", "rf_tobacco", "endo", "renal_failure", "cvd_other", 
                       "resp_other", "digest_other", "maternal_other", "neo_other_cancer", "neo_other_benign", 
                       "neonatal_other", "neuro_other", "mental_other",
                       "exp_well_pregnancy", "exp_well_dental", "exp_donor", "exp_family_planning", 
                       "exp_social_services", "exp_well_person", "sense_other")
  data[, (drop_everywhere):= 0]
  
  data[pri_cause %like% "cvd_", cvd_afib := 0]
  data[pri_cause %like% "cvd_", cvd_hf := 0]
  data[pri_cause %like% "diabetes_", skin := 0]
  data[pri_cause == "neuro_dementia", lri := 0]
  data[pri_cause == "neuro_dementia", inj_falls := 0]
  data[pri_cause %like% "inj|intent_agg", skin := 0]
  data[pri_cause %like% "inj|intent_agg", sense_hearing := 0]
  data[pri_cause %like% "inj|intent_agg", sense_visionlos := 0]
  inj_causes <- c("inj_falls", "inj_trans", "inj_suicide", "inj_mech" , "_unintent_agg", "_intent_agg")
  data[pri_cause %like% "inj|intent_agg", (inj_causes) := 0]
  data[pri_cause %like% "otitis", (setdiff(causelist$acause, c("lri", "uri"))) := 0]
  
  return(data)
}

tune_regression <- function(cause_dt, causes_keep, alpha_list = 1, constraints = T){
  
  cause_dt <- cause_dt[!is.na(spending_log)]
  predictors <- c(causes_keep, c("sex_id", year_vec))
  
  tuning_set <- data.table()
  for(a in alpha_list){
    
    if(constraints){
      cv.fit <- cv.glmnet(as.matrix(cause_dt[,..predictors]), cause_dt[,spending_log], 
                          alpha = a, nfolds = 5, parallel = T, lower.limits = 0, upper.limits = Inf)
    }else{
      cv.fit <- cv.glmnet(as.matrix(cause_dt[,..predictors]), cause_dt[,spending_log], 
                          alpha = a, nfolds = 5, parallel = T)
    }
    
    # calculate lambda 2se
    # 1.12.24: using CV but 1SE, not 2SE
    min_error <- cv.fit$cvm[which(cv.fit$lambda == cv.fit$lambda.min)]
    se_min_error <- cv.fit$cvsd[which(cv.fit$lambda == cv.fit$lambda.min)]
    #                 determine highest lambda that has error within 2 SEs of the min
    closest <- min_error + 2*(se_min_error)
    lambda.2se <- cv.fit$lambda[which(cv.fit$cvm == max(cv.fit$cvm[cv.fit$cvm < closest]))]
    
    
    # identify min lambda and error
    tuning <- data.table(alpha = a,
                         lambda = c(cv.fit$lambda.min, cv.fit$lambda.1se, lambda.2se), 
                         lambda_note = c("min", "1se", "2se"),
                         mse = c(cv.fit$cvm[which(cv.fit$lambda == cv.fit$lambda.min)], 
                                 cv.fit$cvm[which(cv.fit$lambda == cv.fit$lambda.1se)],
                                 cv.fit$cvm[which(cv.fit$lambda == lambda.2se)]),
                         nonzero = c(cv.fit$nzero[which(cv.fit$lambda == cv.fit$lambda.min)],
                                     cv.fit$nzero[which(cv.fit$lambda == cv.fit$lambda.1se)],
                                     cv.fit$nzero[which(cv.fit$lambda == lambda.2se)]))
    tuning_set <- rbind(tuning_set, tuning)
    
  }
  
  return(tuning_set)

    
    
    
  
  
}

get_coefs <- function(cause_dt, causes_keep, alpha, lambda, return_all = F, constraints = T){
  
  cause_dt <- cause_dt[!is.na(spending_log)]
  predictors <- c(causes_keep, c("sex_id", year_vec))
  
  if(constraints){
    fit <- glmnet(as.matrix(cause_dt[,..predictors]), cause_dt[,spending_log], 
                  alpha = alpha, lambda = lambda, lower.limits = 0, upper.limits = Inf)
  }else{
    fit <- glmnet(as.matrix(cause_dt[,..predictors]), cause_dt[,spending_log], 
                  alpha = alpha, lambda = lambda)
  }

  
  coefs = predict(fit,type="coefficients",newx=data)
  coefs = t(data.table(t(data.frame(coefs[,1]))))
  parameters <- rownames(coefs)
  colnames(coefs) <- "coef"
  coefs <- as.data.table(coefs)
  coefs$parameter <- parameters
  coefs$primary_cause <- unique(cause_dt$pri_cause)
  if(!return_all){coefs <- coefs[coef != 0 ]}
  coefs <- coefs[!(parameter %in% c("sex_id", year_vec, "(Intercept)"))]
  
  # add on dementia fix
  if(age_group_min >= 60){
    if(cause_mod == "lri"){
      coefs <- coefs[parameter != "neuro_dementia"]
      coefs <- rbind(coefs, data.table(coef = log(1.9), parameter = "neuro_dementia", primary_cause = cause_mod))
    }else if (cause_mod == "inj_falls"){
      coefs <- coefs[parameter != "neuro_dementia"]
      coefs <- rbind(coefs, data.table(coef = log(1.93), parameter = "neuro_dementia", primary_cause = cause_mod))
    }
  }
  
  if(nrow(coefs)==0){
    return(coefs[,.(primary_cause, parameter, coef)])
  }

  return(coefs[,.(primary_cause, parameter, coef)])
}

calc_af <- function(cause_dt, coefs){
  
  # num encounters
  num_encounters <- nrow(cause_dt)
  
  #----
  # conditional probability
  #----
  
  #  numerator: # encounters with Y as comorbidity
  #---------------------------------------------------
  cause_dt_prop <- colSums(cause_dt[,c(causes_keep), with = F])
  cause_dt_prop <- data.frame(cause_dt_prop)
  cause_dt_prop$comorb <- rownames(cause_dt_prop)
  
  cause_dt_prop <- as.data.table(cause_dt_prop)
  
  #  denominator: # encounters + # pairs   
  #              (if it's just # encounters, then total probability is greater than 1, since one encounter can have multiple comorbs)
  #              And adding # encounters just so that encounters with no comorbidities are captured
  #---------------------------------------------------
  denom <- nrow(cause_dt) + sum(cause_dt_prop$cause_dt_prop)
  
  cause_dt_prop[, cond_prob := cause_dt_prop/ denom ]
  
  #----
  # calculate AF 
  #----
  cause_dt_prop <- merge(cause_dt_prop, coefs[,.(primary_cause, comorb = parameter, coef, direction, rank)], by = "comorb")
  cause_dt_prop[, af := cond_prob*(exp(coef)-1)]
  
  return(cause_dt_prop[,.(primary_cause, comorb, direction, coef, cond_prob, af, rank)])
}
