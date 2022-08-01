# Prep data for policy regressions - get state policy covariates and draws and write smaller files
# Split up to be faster for processing
# USERNAME Sept 2021

# Setup -----------------------------------------------------------------------------------
rm(list = ls())
library(tidyverse)
library(data.table)
library(parallel)
library(plm)
library(lmtest)
source("FILEPATH/envelope_functions.R")

load("FILEPATH/states.RData")

# Covariates -----------------------------------------------------------------------------------
covariates <- fread("FILEPATH/state_covariate_data.csv")
covariates[ma_pct == 0, ma_pct := NA]
covariates[covariates == "" | covariates == "NR"] <- NA
covariates[, mdcd_expanded := as.character(mdcd_expanded)]
covariates[mdcd_expanded == 0, mdcd_expanded := "No"][mdcd_expanded == 1, mdcd_expanded := "Yes"]
covariates[pct_mdcd_mco == 0, pct_mdcd_mco := 0.0001][phys_pc == 0, phys_pc := 0.5][pharm_pc == 0, pharm_pc := 0.5]
covariates[pharm_pc == 0, pharm_pc := 0.5][phys_pc == 0, phys_pc := 0.5][pct_mdcd_mco == 0, pct_mdcd_mco := 0.5]
covariates[, incpt := 1]

covariates[, mdcr_prop := insured*ins_medicare][, mdcd_prop := insured*ins_medicare]
covariates[, mdcr_aged_prop := mdcr_aged_pct_pop*ins_medicare][, mdcr_dbld_prop := mdcr_disabled_pct_pop*ins_medicare]

covs <- c("density_g.1000","ldi_pc","rpp_cpi","PA_mets","cig_pc_15","pop_female",str_subset(colnames(covariates), "^age")) # controls in the model

num_cols <- names(covariates)[sapply(covariates, class)=="numeric"|sapply(covariates, class)=="integer"]
num_cols <- setdiff(num_cols, c("location_id", "year_id", "incpt", covs)) # numeric policy vars to transform

covariates[,(covs) := lapply(.SD, log), .SDcols = covs][,(num_cols) := lapply(.SD, log), .SDcols = num_cols]
fwrite(covariates, "FILEPATH/all_covs.csv")

covariates <- melt(covariates, id.vars = c("location_id","year_id",covs))
covariates <- covariates[!str_detect(variable,"population|edu|sdi|density|pct_firms|cpi|rpp|pct_not|bmi|cig")]
covariates <- merge(covariates, states, by="location_id")

mclapply(unique(as.character(covariates$variable)), function(varb){
  cv <- covariates[variable == varb]
  fwrite(cv, paste0("FILEPATH/",varb,".csv"))
}, mc.cores = 10)

# Covariate - model grid -----------------------------------------------------------------------------------
grid <- expand.grid(variable = unique(covariates$variable), model = c("agg","priv","pub","oop")) %>% setDT()
grid <- grid[, index := .I]
fwrite(grid,"FILEPATH/grid.csv")

