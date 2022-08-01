#################################
# Shapley decomp on control model for state policy regressions
# Using function from here: https://github.com/elbersb/shapley
# USERNAME/USERNAME
# Nov 2021
#################################

rm(list = ls())
pacman::p_load(tidyverse, data.table)
library(shapley)

draw_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID")) %>% print()

# prep data set
covs <- fread("FILEPATH/all_covs.csv")
covs <- covs[,.(location_id, year_id, density_g.1000, ldi_pc, rpp_cpi, cig_pc_15, PA_mets)]

cost <- fread("FILEPATH/agg_standardized.csv")
cost <- cost[draw == draw_id]
cost[, log_mean := log(spending_pc_standardized)]
df <- merge(cost, covs, by = c("year_id", "location_id"))
df[, value := 1]
df_wide <- as.data.table(dcast(df, ... ~ year_id, value.var = "value"))
df_wide[is.na(df_wide)] <- 0

####
covs <- c("density_g.1000","ldi_pc","PA_mets","cig_pc_15", paste0("`",2010:2019,"`"))

reg <- function(factors, dv, data) {
  if (length(factors) == 0) return(0)
  formula <- paste(dv, "~", paste(factors, collapse = " + "))
  m <- lm(formula, data = data)
  summary(m)$r.squared
}


r2 <- reg(covs, dv = "log_mean", data = df_wide)
decomp <- shapley(reg, covs, silent = FALSE, dv = "log_mean", data = df_wide)
setDT(decomp)
decomp[factor %in% paste0("`",2010:2019,"`"), `:=`(factor = "year_id", value = sum(value))]
decomp <- unique(decomp)
decomp[, draw := draw_id]

save(decomp, file=paste0("FILEPATH/",draw_id,".RData"))
