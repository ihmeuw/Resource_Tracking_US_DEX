# Create Model Metadata ---------------------------------------------------
# This script can be used to populate a list of state spending models that contains relevant metadata about each model
# and is used in the dep_var_models.Rmd script to launch models.

rm(list = ls())
library(tidyverse)
library(data.table)
library(noncompliance)
repo_path <- dirname(dirname(dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())))
source(paste0(repo_path,'/state_env/envelope_functions.R'))

binary <- c(TRUE,FALSE) # binary for time trend & whether per capita is by user or population


# Aggregate table ----
spending <- c("tot_spending", "pc_spending", "fr_spending", "fpc_spending") # potential dependent variables
transformations <- c("linear","log") # transformations (logit tform for fr spending added later)

addl_covs <- data.table(spending = c("tot_spending","tot_spending","pc_spending","fr_spending"), # list of other potential covariates considered for models
                        other_covs = c("population","population,us_spending", "us_spending_pc", "pop_fr")) # aligned with space for total spending var

shea_covs <- c("mdcd","mdcr","nes","wages","meps","ec","crt","aha","oop","feds","bea") # shea covariates availabe
# create all possible combinations of shea covs, including NA 
shea_grid <- lapply(shea_covs, function(s){
  s <- c(s,NA)
  return(s)
})
shea_grid <- expand.grid(shea_grid) %>%
  unite("shea_covs", sep=",", na.rm=TRUE)


# Create the table
dt <- setDT(expand.grid(spending = spending, 
                        transformation = transformations, 
                        time_trend = binary,
                        shea_covs = shea_grid$shea_covs,
                        # per_user = binary,
                        ma = binary,
                        state_effect = binary,
                        difference_model = binary))
dt <- rbind(dt, merge(dt, addl_covs, by="spending"), fill=TRUE)

dt[(ma), other_covs := paste0("ma_pct,",other_covs)]
dt[other_covs == "ma_pct,NA", other_covs := "ma_pct"][, ma := NULL]

dt[spending == "fr_spending" & transformation == "log", transformation := "logit"]
# dt <- dt[!(per_user) | (spending == "pc_spending")]
# dt <- dt[!(per_user & shea_covs == "")]
dt <- dt[!state_effect | 
           (state_effect & spending %in% c("tot_spending","fr_spending","pc_spending"))]
dt <- dt[!str_detect(shea_covs,"ec") | !str_detect(shea_covs,"wages")]

# Organize table
# Set order to run models in, create index
setorder(dt, state_effect, shea_covs, spending, transformation, time_trend, other_covs, difference_model) # per_user
dt <- dt[,.(model = .I, spending, transformation, time_trend, shea_covs, other_covs, state_effect, difference_model)] # per_user

# Write table
fwrite(dt, "FILEPATH/state_dep_var_table.csv")

# Payer table ------
# Create a table of models based on the format & add'l covariates included to be used to launch models
model_types <- c("raking - linear","raking - logit","CLO","no oop - linear","no oop - logit")
addl_covs <- c("none","total population", "payer populations","year","total population, year",
               "payer populations, year","total population, payer populations",
               "total population, payer populations, year","ma","total population, ma",
               "payer populations, ma","year, ma","total population, year, ma",
               "payer populations, year, ma","total population, payer populations, ma",
               "total population, payer populations, year, ma")
state_effects = c("no state FE", "state FE")

model_grid <- expand.grid(model_types = model_types, addl_covs = addl_covs, state_effect = state_effects, difference_model = binary)
setDT(model_grid)
model_grid[, model := .I]

model_grid <- model_grid[model_types != "no oop - logit"][model_types != "no oop - linear"]
fwrite(model_grid, "FILEPATH/state_dep_var_table_payer.csv")

# Payer model list -----
# Each object is a list of 4 models, one for each payer. The lists below each contain 4 payer-specific equations,
# and vary by the covariates included.

m1_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc,
                 pr_spending_oop_logit ~ 1 + oop_pr_logit)
m2_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + pop_fr,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + pop_fr,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + pop_fr,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + pop_fr)
m3_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + pop65,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + ins_mdcd,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + ins_private,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + uninsured)
m4_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + year_id,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + year_id,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + year_id,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + year_id)
m5_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + pop_fr + year_id,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + pop_fr + year_id,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + pop_fr + year_id,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + pop_fr + year_id)
m6_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + pop65 + year_id,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + ins_mdcd + year_id,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + ins_private + year_id,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + uninsured + year_id)
m7_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + pop65 + pop_fr,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + ins_mdcd + pop_fr,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + ins_private + pop_fr,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + uninsured + pop_fr)
m8_logit <- list(pr_spending_mdcr_logit ~ 1 + mdcr_pr_logit + pop65 + pop_fr + year_id,
                 pr_spending_mdcd_logit ~ 1 + mdcd_pr_logit + ins_mdcd + pop_fr + year_id,
                 pr_spending_priv_logit ~ 1 + meps_pr_logit + feds_pc + ins_private + pop_fr + year_id,
                 pr_spending_oop_logit ~ 1  + oop_pr_logit + uninsured + pop_fr + year_id)


# Model sets are lists of the lists of models created above, where each value in the list is a set of covariates.
model_set_logit <- list(m1_logit, m2_logit, m3_logit, m4_logit, m5_logit, m6_logit, m7_logit, m8_logit)

ma_covariate <- lapply(model_set_logit, function(eq_list){
  eq_list[[1]]  <- as.formula(str_c(deparse(eq_list[[1]]),"+ ma_pct"))
  return(eq_list)
})
model_set_logit <- c(model_set_logit, ma_covariate)

names(model_set_logit) <- c("none","total population", "payer populations","year","total population, year",
                            "payer populations, year","total population, payer populations",
                            "total population, payer populations, year","ma","total population, ma",
                            "payer populations, ma","year, ma","total population, year, ma",
                            "payer populations, year, ma","total population, payer populations, ma",
                            "total population, payer populations, year, ma")

no_oop <- function(m_set){
  m_set <- lapply(m_set, function(eq_list){
    eq_list <- eq_list[1:3]
    return(eq_list)
  })
  return(m_set)
}

model_set_logit_no_oop <- no_oop(model_set_logit)
model_set <- lapply(model_set_logit, logit_to_linear)
model_set_no_oop <- no_oop(model_set)

m_nostates <- list("raking - linear" = model_set,
                   "raking - logit" = model_set_logit, 
                   "CLO" = model_set,
                   "no oop - linear" = model_set_no_oop,
                   "no oop - logit" = model_set_logit_no_oop)
m_states <- list("raking - linear" = lapply(model_set, state_effect),
                 "raking - logit" = lapply(model_set_logit, state_effect), 
                 "CLO" = lapply(model_set, state_effect),
                 "no oop - linear" = lapply(model_set_no_oop, state_effect),
                 "no oop - logit" = lapply(model_set_logit_no_oop, state_effect))

# The final model object is a list of list of lists of lists. The space/transform of each set of models is the first level, 
# under which is the state effect, then the set of 8 different sets of covariates, under which is the final list of models by each payer.
m <- list("no state FE" = m_nostates, "state FE" = m_states)
save(m, file="FILEPATH/payer_model_set.RData")

# TOC table -----
# Create a table of models based on the format & add'l covariates included to be used to launch models
model_types <- c("raking - linear","raking - log","CLO")
addl_covs <- c("none", "total population", "year","total population, year")
state_effects = c("no state FE", "state FE")

model_grid <- expand.grid(model_types = model_types, addl_covs = addl_covs, state_effect = state_effects, difference_model = binary)
setDT(model_grid)
model_grid[, model := .I]

fwrite(model_grid, "FILEPATH/state_dep_var_table_toc.csv")

# TOC model list -----
# Each object is a list of 4 models, one for each payer. The lists below each contain 4 payer-specific equations,
# and vary by the covariates included.

m1_log <- list(pr_spending_log_dent ~ 1 + cov_pr_log_ec_dent,
               pr_spending_log_hh ~ 1 + cov_pr_log_ec_hh,
               pr_spending_log_hosp ~ 1 + cov_pr_log_aha_tot,
               pr_spending_log_nf ~ 1 + cov_pr_log_ec_nf,
               pr_spending_log_oprof ~ 1 + cov_pr_log_ec_oprof,
               pr_spending_log_other ~ 1 + cov_pr_log_ec_other,
               pr_spending_log_pharma ~ 1 + cov_pr_log_crt_tot,
               pr_spending_log_phys ~ 1 + cov_pr_log_ec_phys)
m2_log <- list(pr_spending_log_dent ~ 1 + cov_pr_log_ec_dent + pop_fr,
               pr_spending_log_hh ~ 1 + cov_pr_log_ec_hh + pop_fr,
               pr_spending_log_hosp ~ 1 + cov_pr_log_aha_tot + pop_fr,
               pr_spending_log_nf ~ 1 + cov_pr_log_ec_nf + pop_fr,
               pr_spending_log_oprof ~ 1 + cov_pr_log_ec_oprof + pop_fr,
               pr_spending_log_other ~ 1 + cov_pr_log_ec_other + pop_fr,
               pr_spending_log_pharma ~ 1 + cov_pr_log_crt_tot + pop_fr,
               pr_spending_log_phys ~ 1 + cov_pr_log_ec_phys + pop_fr)
m3_log <- list(pr_spending_log_dent ~ 1 + cov_pr_log_ec_dent + year_id,
               pr_spending_log_hh ~ 1 + cov_pr_log_ec_hh + year_id,
               pr_spending_log_hosp ~ 1 + cov_pr_log_aha_tot + year_id,
               pr_spending_log_nf ~ 1 + cov_pr_log_ec_nf + year_id,
               pr_spending_log_oprof ~ 1 + cov_pr_log_ec_oprof + year_id,
               pr_spending_log_other ~ 1 + cov_pr_log_ec_other + year_id,
               pr_spending_log_pharma ~ 1 + cov_pr_log_crt_tot + year_id,
               pr_spending_log_phys ~ 1 + cov_pr_log_ec_phys + year_id)
m4_log <- list(pr_spending_log_dent ~ 1 + cov_pr_log_ec_dent + pop_fr + year_id,
               pr_spending_log_hh ~ 1 + cov_pr_log_ec_hh + pop_fr + year_id,
               pr_spending_log_hosp ~ 1 + cov_pr_log_aha_tot + pop_fr + year_id,
               pr_spending_log_nf ~ 1 + cov_pr_log_ec_nf + pop_fr + year_id,
               pr_spending_log_oprof ~ 1 + cov_pr_log_ec_oprof + pop_fr + year_id,
               pr_spending_log_other ~ 1 + cov_pr_log_ec_other + pop_fr + year_id,
               pr_spending_log_pharma ~ 1 + cov_pr_log_crt_tot + pop_fr + year_id,
               pr_spending_log_phys ~ 1 + cov_pr_log_ec_phys + pop_fr + year_id)

# Create model lists
# Model sets are lists of the lists of models created above, where each value in the list is a set of covariates.
model_set_log <- list("none" = m1_log,
                      "total population" = m2_log,
                      "year" = m3_log,
                      "total population, year" = m4_log)

model_set <- lapply(model_set_log, log_to_linear)

m_nostates <- list("raking - linear" = model_set,
                   "raking - log" = model_set_log, 
                   "CLO" = model_set)
m_states <- list("raking - linear" = lapply(model_set, state_effect),
                 "raking - log" = lapply(model_set_log, state_effect), 
                 "CLO" = lapply(model_set, state_effect))

# The final model object is a list of list of lists of lists. The space/transform of each set of models is the first level, 
# under which is the state effect, then the set of 8 different sets of covariates, under which is the final list of models by each payer.
m <- list("no state FE" = m_nostates, "state FE" = m_states)
save(m, file="FILEPATH/toc_model_set.RData")
