# Simple compilation of the outputs of the policy regressions
# USERNAME, Sept 2021

# Setup -----------------------------------------------
library(tidyverse)
library(data.table)
library(Hmisc)
library(plotly)

# Load and compile betas ------------------------------
files <- Sys.glob("FILEPATH/*.RData")
betas <- lapply(files, function(f){
  print(f)
  load(f)
  return(betas)
})
betas <- rbindlist(betas, fill = TRUE)
betas <- betas[!str_detect(covariate,"Intercept|abbreviation|year|bmi|pct_see|pop65")]
# betas <- betas[!is.na(model_set)]
betas <- betas[variable != "incpt"]
betas <- betas[!str_detect(variable,"incpt|pop65|diab|haqi|hhi|ins_|max_rate|mdcr|income|obese|premiums|insured|ma_pct")]

# Write output file ---------------------------------
fwrite(betas,"FILEPATH/predictor_betas.csv")

# Count years of data -------------------------------------
all_covs <- fread("FILEPATH/all_covs.csv")
all_covs <- melt(all_covs, id.vars = c("location_id","year_id"))
all_covs <- all_covs[!is.na(value), .(states = .N), by = c("year_id","variable")]
all_covs <- all_covs[states == 51, .(years = .N, rows = 51*.N), by="variable"]
fwrite(all_covs, "FILEPATH/covariate_sample_size.csv")

