# ----------------------------------------------------------------
#
#   Adjust performance score for selected county controls
# 
#   Author: Haley Lescinsky
# ----------------------------------------------------------------


pacman::p_load(data.table, tidyverse, dplyr, ggplot2, arrow, gridExtra)
library(usmap)
fips_county_map <- fread("FILEPATH/merged_counties.csv")
code_dir <- "FILEPATH"
library(lbd.loader, lib.loc = sprintf("/FILEPATH/lbd.loader-%s", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

parent_dir <- "FILEPATH"

# Set args-----------------
date_version <- "xxx"

controls <- c("edu_hs", "income_median", "obesity_prev", "percent_smokers", 
              "prop_age_over_65", "rural")

note <- ''
sens <- F  # IMPORTANT if T, will be a sensitivity result


log_vars <- c('income_median')

#--------------------------
work_dir <- paste0(parent_dir, "/", date_version)

if(sens == F){
  triple_aim <- fread(paste0(work_dir, "/FILEPATH"))
  
}else{
  
  # specify which sensitivity version to run
  sens_version <- 'sens_w_prematuremort'
  
  triple_aim <- fread(paste0(work_dir, "/sens/",sens_version,"/FILEPATH"))
}


save_dir <- paste0(work_dir, "FILEPATH")

if(sens == T){
  save_dir <- paste0(save_dir, "/", sens_version, '/')
}
dir.create(paste0(save_dir, "/adjust_reg/"), recursive = T)

# merge on covariate data
df_w_covs <- add_covs_to_data(triple_aim)
df_w_covs <- df_w_covs[covariate %in% controls]

df_w_covs[covariate %in% log_vars & cov_value == 0, cov_value := 0.0001]
df_w_covs[covariate %in% log_vars, cov_value := log(cov_value)]

# # Flip variables we expect to negatively impact so interpretation is easier! 
df_w_covs[covariate %in% c('prop_age_under_20', 'rural', 'obesity_prev', 'percent_smokers', 'unemployed'), cov_value := - cov_value]

# standardize
df_w_covs[, value_norm := (cov_value - mean(cov_value))/sd(cov_value), by = c('covariate', 'draw')]

by_draw <- T
if(!'draw' %in% colnames(df_w_covs)){
  df_w_covs[, draw := 0]
  by_draw <- F
}

all_reg_data <- dcast(df_w_covs, mcnty + draw + year_id + performance_score + double_performance_score ~ covariate, value.var = 'value_norm')

if(sens == F){
  vars <- c('performance_score', 'double_performance_score')
}else{
  vars <- 'performance_score'
}

# fit regression, calculate difference between predicted and observed
for(var in vars){
    
  formula <- paste0(var, " ~ ", paste0(controls, collapse =  " + "), "+ as.factor(year_id)")
  
  draw_coef_table <- data.table()
  adjusted_performance_score <- data.table()
  
  for(d in unique(all_reg_data$draw)){
    
    reg_data <- all_reg_data[draw == d]
    reg <- lm(formula, data = reg_data)
    summary(reg)
    
    if(d > 0){
      
      # make coefficient table
      coef_tab <- summary(reg)$coef
      coef_order <- rownames(coef_tab)
      coef_tab <- as.data.table(coef_tab)
      coef_tab[, cov := coef_order]
      coef_tab[, draw := d]
      
      draw_coef_table <- rbind(draw_coef_table, coef_tab)
    }
    
    pred_data <- copy(reg_data)
    pred_data[, (controls) := 0 ]
    
    pred_data$normal_predict <- predict(reg, newdata = reg_data)
    pred_data[, residual1 := normal_predict - get(var)]     # difference between predicted score and score
    pred_data[, residual1 := -residual1]  # flip so positive is did better than expected given controls! 
    
    adjusted_performance_score <- rbind(adjusted_performance_score, pred_data[,.(mcnty, year_id, draw = d, adj_var = residual1)])
  
  }
  
  setnames(adjusted_performance_score, "adj_var", paste0(var, "_adj"))
  triple_aim <<- merge(triple_aim, adjusted_performance_score, by = c('year_id', 'mcnty', 'draw'), all.x = T)
  
}

# rescale adjusted score to aid interpretation (divide by SD)
if(sens == F){
  triple_aim[, `:=` (performance_score_adj_orig = performance_score_adj,
                     double_performance_score_adj_orig = double_performance_score_adj)]
  
  triple_aim[, `:=` (performance_score_adj = performance_score_adj_orig / sd(triple_aim$performance_score_adj_orig),
                     double_performance_score_adj = double_performance_score_adj_orig / sd(triple_aim$double_performance_score_adj_orig))]
  
}else{
  triple_aim[, `:=` (performance_score_adj_orig = performance_score_adj)]
  triple_aim[, `:=` (performance_score_adj = performance_score_adj_orig / sd(triple_aim$performance_score_adj_orig))]
  
}

write.csv(triple_aim, paste0(save_dir, "/FILEPATH"), row.names = F)
