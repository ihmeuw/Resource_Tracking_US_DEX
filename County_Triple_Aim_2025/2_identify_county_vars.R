# ----------------------------------------------------------------
#
#   Determine which county covariates are most associated with unadjusted performance score -> controls  
#
#   Author: Haley Lescinsky
# ----------------------------------------------------------------

pacman::p_load(data.table, tidyverse, dplyr, arrow)
library(BAS, lib.loc = 'FILEPATH')
library(usmap)
fips_county_map <- fread("FILEPATH/merged_counties.csv")
code_dir <- "FILEPATH"
set.seed(235)

cov_info <- read.xlsx("FILEPATH") %>% as.data.table()
parent_dir <- "FILEPATH"

# Set args-----------------
date_version <- "xxx"
#--------------------------

work_dir <- paste0(parent_dir, "/", date_version)
triple_aim <- fread(paste0(work_dir, "/FILEPATH"))

descriptive_cov_list <- c("income_median", "rural","unemployed",
              "edu_hs", "percent_smokers", "obesity_prev", 
              "prop_age_over_65", "prop_age_under_20")

log_vars <- c('income_median')

# calculate mean of the score
triple_aim_means <- triple_aim[,.(performance_score = mean(performance_score)), by = c('year_id', 'mcnty')]

df_w_covs <- add_covs_to_data(triple_aim_means)
df_w_covs_plot <- df_w_covs[covariate %in% descriptive_cov_list]

# add income as logged
log_covs <- df_w_covs_plot[covariate %in% log_vars]
log_covs[cov_value == 0, cov_value := 0.0001]
log_covs[,cov_value := log(cov_value)]
log_covs[,`:=` (full_name = paste0(full_name, " - logged"), covariate = paste0('log_', covariate))]

df_w_covs_plot <- rbind(df_w_covs_plot, log_covs)

# standardize all covariates and demean by year (psuedo time effects)
#---------------------------------

# demean by year
df_w_covs_plot[, performance_score_demeaned := performance_score - mean(performance_score), by = c('year_id')]
df_w_covs_plot[, cov_value_demeaned := cov_value - mean(cov_value), by = c('covariate', 'year_id')]

# scale
df_w_covs_plot[, cov_value_scaled := (cov_value - min(cov_value))/(max(cov_value) - min(cov_value)), by = c('covariate')]
df_w_covs_plot[, cov_value_demeaned_scaled := (cov_value_demeaned - min(cov_value_demeaned))/(max(cov_value_demeaned) - min(cov_value_demeaned)), by = c('covariate')]

# reshape
df_w_covs_plot_wide <- dcast(df_w_covs_plot, mcnty + year_id + performance_score ~ covariate, value.var = 'cov_value_scaled')
df_w_covs_plot_wide_demeaned <- dcast(df_w_covs_plot, mcnty + year_id + performance_score_demeaned ~ covariate, value.var = 'cov_value_demeaned_scaled')

descriptive_cov_list[descriptive_cov_list %in% log_vars] <- paste0('log_', descriptive_cov_list[descriptive_cov_list %in% log_vars])


#---------------------------------
# Fit regressions between variables and unadj performance score
#---------------------------------

# 1. BMS, main approach
#---
model_string <- paste0('performance_score_demeaned ~ ', paste0(descriptive_cov_list, collapse = " + "))

model_bms <- bas.lm(model_string,
                    data = df_w_covs_plot_wide_demeaned,
                    prior = "JZS", 
                    modelprior = uniform())
temp_coef <- coef(model_bms, estimator = 'HPM')
coef_ui <- confint(temp_coef)
bms_coefs <- data.table(covariate = temp_coef$namesx, bms_beta = temp_coef$postmean) 


# 2. SIMPLE OLS (bivariate), sensitivity
#---
ols_coefs <- rbindlist(lapply(descriptive_cov_list, function(c){
  
  reg <- lm(performance_score_demeaned ~ cov_value_demeaned_scaled, data = df_w_covs_plot[covariate == c])
  coefs <- data.table(covariate = c, bivariate_beta = summary(reg)$coefficients[2,1])
  
  return(coefs)
  
}))


# 3. LASSO, sensitivity (both min error and 1se)
#---
formula <- as.formula(paste0("~ ", paste0(descriptive_cov_list, collapse = " + ")))
x <- model.matrix(formula, data = df_w_covs_plot_wide_demeaned)
y <- df_w_covs_plot_wide_demeaned$performance_score
cv_model <- glmnet::cv.glmnet(x, y, alpha = 1)

# lambda min
best_lambda <- cv_model$lambda.min
model <- glmnet::glmnet(x, y, alpha = 1, lambda = best_lambda)
d <- coef(model)[, 1]
lasso_coefs1 <- data.table('covariate' = names(d), 'lasso_beta' = d)[covariate != '(Intercept)']

# lambda 1se
best_lambda <- cv_model$lambda.1se
model <- glmnet::glmnet(x, y, alpha = 1, lambda = best_lambda)
d <- coef(model)[, 1]
lasso_coefs2 <- data.table('covariate' = names(d), 'lasso_beta_1se' = d)[covariate != '(Intercept)']

lasso_coefs <- merge(lasso_coefs1, lasso_coefs2, by = 'covariate')


#---------------------------------
# Combine all coefs, determine subset
#---------------------------------
all_coefs <- merge(ols_coefs, bms_coefs)
all_coefs <- merge(all_coefs, lasso_coefs)

print(all_coefs[bms_beta!=0]$covariate)

write.xlsx(all_coefs, paste0(work_dir, "/FILEPATH.xlsx"))
