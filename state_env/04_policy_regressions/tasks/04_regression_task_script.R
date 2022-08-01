# Run policy model on state spending draws by covariate, spending model
# Run in parallel as part of policy regression workflow
# USERNAME, Sept 2021

# Setup -----------------------------------------------------------------------------------
rm(list = ls())
library(tidyverse)
library(data.table)
library(parallel)
library(plm)
library(lmtest)
library(car)
source("FILEPATH/envelope_functions.R")

args <- commandArgs(trailingOnly = TRUE)
print(args)
varb <- args[1] %>% print()
dep_var <- args[2] %>% print()

draws <- fread(paste0("FILEPATH/",dep_var,"_standardized.csv"))

covs <- fread(paste0("FILEPATH/",varb,".csv"))
d <- merge(covs, draws, by=c("location_id","year_id"), allow.cartesian = TRUE)
d[value == ""] <- NA
d[, log_mean := log(spending_pc_standardized)][, log_mean_unst := log(spending_pc)]


cov_names <- c("density_g.1000","ldi_pc","PA_mets","cig_pc_15")
supply_and_util_vars <- c("beds1000", "inpt_days1000", "admissions1000", "outpt_days1000", "hosp_rate", "hw_pc", "phys_pc", "oth_hw_pc", "pharm_pc")

if(varb %in% supply_and_util_vars){
  d <- d[year_id %in% 2014:2019]
  eqn <- paste0("log_mean_unst ~ value + as.factor(year_id) + abbreviation")
}else{
  # d <- d[year_id >= 2010]
  eqn <- paste0("log_mean ~ value + ",paste(cov_names, collapse = " + ")," + as.factor(year_id) + abbreviation")
}

# Run models 1000x ----------------------------------------------------------------------------------------------
betas <- rbindlist(lapply(1:1000, function(i){
  mod <- lm(eqn, data = d[draw == i])

  ## Extract coefficients from this run
  pe <- coef(mod)
  ## Choose a variance-covariance matrix
  vc <- vcovHC(mod, 
               method = "white1")
  ## Draw simulated betas from this run
  simbetas <- data.table(covariate = names(pe), 
                         simbeta = MASS::mvrnorm(n = 1, mu = pe, Sigma = vc),
                         draw = i)
  
  return(simbetas)
}))

# De-mean and compute VIF --------------------------------------------------------------------------------------
demean <- function(x){
  x <- as.numeric(x)
  x <- x - mean(x, na.rm = TRUE)
  return(x)
}

if(!(varb %in% supply_and_util_vars)){
  demeaned <- d[,.(log_mean = mean(log_mean)), by=setdiff(colnames(d),c("spending_pc_standardized","draw","log_mean","spending_pc"))]
  varbs <- c(cov_names, "log_mean")
  if(!is.character(demeaned$value)) varbs <- c(varbs, "value")
  demeaned <- demeaned[,(varbs) := lapply(.SD, demean), by="location_id", .SDcols = varbs]
  
  vif_mod <- lm(paste0("log_mean ~ value + ",paste(cov_names, collapse = " + ")), data = demeaned)
  vif_df <- as.data.table(vif(vif_mod), keep.rownames = TRUE)
  colnames(vif_df) <- c("covariate","vif")
}


# Compile draws ----------------------------------------------------------------------------------------------
betas <- betas[,.(spending = dep_var,
                  variable = varb,
                  beta = mean(simbeta, na.rm = TRUE), 
                  std_err = sd(simbeta, na.rm = TRUE), 
                  beta_lower = quantile(simbeta, 0.025, na.rm = TRUE), 
                  beta_upper = quantile(simbeta, 0.975, na.rm = TRUE),
                  p_val = 2*pnorm(abs(mean(simbeta))/sd(simbeta), lower.tail = FALSE)),
               by="covariate"]
setcolorder(betas, "variable")

if(!(varb %in% supply_and_util_vars)){
  betas <- merge(betas, vif_df, by="covariate", all = TRUE)
}
save(betas, file = paste0("FILEPATH/",dep_var,"/",varb,".RData"))
