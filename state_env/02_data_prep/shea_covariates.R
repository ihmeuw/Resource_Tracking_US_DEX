# Compile all covariates that are used in the CMS SHEA study.
# USERNAME
# Apr 15 2021
# This data will be used alongside the SHEA expenditures and other state-level covariates to produce 
# the state envelope

# Setup -----------------------------------------------------------------------------------------------------
rm(list = ls())
library(data.table)
library(tidyverse)
library(arrow)
library(Hmisc)

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

source("FILEPATH/currency_conversion.R")
source("FILEPATH/get_population.R")

format_dir <- "FILEPATH"

load("FILEPATH/states.RData")
state_years <- merge(expand.grid(location_id = unique(states$location_id), year_id = 1999:2019), states, by="location_id")
setDT(state_years)

population <- get_population(gbd_round_id = 7, decomp_step = "iterative",
                             sex_id = 3, age_group_id = 22, year_id = 1990:2019, location_id = unique(states$location_id))
population <- as.data.table(population)[,.(location_id, year_id, population)]

validate <- function(df){
  return(count(count(df, year_id, location_id), n))
}

# EC -----------------------------------------------------------------------------------------------------
ec_files <- Sys.glob("FILEPATH")

ec <- rbindlist(lapply(ec_files, function(f){
  toc <- str_remove(f, "FILEPATH") %>%
    str_remove("/best.csv")
  df <- fread(f)
  df <- df[,.(state_name, location_id, year_id, mean, toc = toc)]
  return(df)
}))

ec <- merge(ec, population, by=c("location_id","year_id"))
ec[, mean := mean*population][, population := NULL]

ec <- dcast(ec, location_id + year_id ~ toc, value.var = "mean")
ec[, ec_other := ec_other + ec_dme_optometry]
setnames(ec,c("ec_envelope","ec_dental","ec_home","ec_hospital","ec_nursing","ec_other_prof","ec_phys_clin_service"),
         c("ec_tot","ec_dent","ec_hh","ec_hosp","ec_nf","ec_oprof","ec_phys"))

# CRT -----------------------------------------------------------------------------------------------------
crt_files <- Sys.glob("FILEPATH/best.csv")

crt <- rbindlist(lapply(crt_files, function(f){
  df <- fread(f)
  df <- df[,.(location_id, year_id, mean)]
  return(df)
}))

crt <- merge(crt, population, by=c("location_id","year_id"))
crt[, crt_tot := mean*population][, mean := NULL][, population := NULL]

# AHA -----------------------------------------------------------------------------------------------------
aha <- arrow::read_feather("FILEPATH/kff_aha_hospital_spending.feather")
setDT(aha)
aha <- aha[,.(location_id, year_id, aha_tot = hospital_spending)]

# NES -----------------------------------------------------------------------------------------------------
nes_files <- Sys.glob(paste0(format_dir,"NES/state/*.feather"))
nes <- lapply(nes_files, function(f){
  df <- arrow::read_feather(f)
  df <- as.data.table(df)
  df <- df[str_detect(NAICS,"62\\d{1}$")]
  if("LFO" %in% colnames(df)) df <- df[LFO == "All Establishments"]
  df <- df[, .(RCPTOT = sum(RCPTOT)), by=c("year_id","location_id")]
  return(df)
})
nes <- rbindlist(nes, fill=TRUE)

nes <- nes[,.(year_id, location_id, nes_tot = RCPTOT)]
nes <- merge(state_years[,.(location_id, year_id)], nes, by=c("location_id","year_id"), all = TRUE)
nes <- rbindlist(lapply(split(nes, by="location_id"), simple_extrapolate))


# ggplot(nes) + geom_line(aes(x = year_id, y = nes_total)) + facet_wrap(.~state_name)

# QCEW (wages) -----------------------------------------------------------------------------------------------------
qcew <- arrow::read_feather("FILEPATH/QCEW_state_covariate.feather")
qcew <- qcew[, .(location_id, year_id, wages_pc = cv_wages_envelope)]
qcew <- merge(qcew, population, by=c("location_id","year_id"))
qcew[, wages_tot := wages_pc*population][, population := NULL]

# Medicare -----------------------------------------------------------------------------------------------------
mdcr_ffs <- fread(paste0(format_dir,"MDCR_FFS/mdcr_ffs_state.csv"))
mdcr_ffs <- merge(mdcr_ffs, state_years[,.(location_id,state,year_id)], by=c("location_id","year_id"), all=TRUE, allow.cartesian = TRUE)
mdcr_ffs <- mdcr_ffs[,.(location_id, year_id, mdcr_tot = total)]

mdcr_ffs <- rbindlist(lapply(split(mdcr_ffs, by="location_id"), simple_extrapolate))

mdcr_ma <- fread("FILEPATH/KFF_MA/kff_ma_enrollment.csv")
mdcr_ma[, enrollees := NULL][, state_name := NULL]

mdcr <- merge(mdcr_ffs, mdcr_ma, by=c("year_id","location_id"))

# Medicare FFS spending is scaled by the proportion of the Medicare population on 
# Medicare Advantage (i.e. assume MA population spends the same as the FFS, inflate
# spending to account for the MA pop)
mdcr[, mdcr_tot := mdcr_tot/(1 - ma_pct)]

# Medicaid -----------------------------------------------------------------------------------------------------
mdcd <- arrow::read_feather(paste0(format_dir,"CMS64/CMS_64.feather"))
setDT(mdcd)
mdcd$total <- as.numeric(mdcd$total)
# mdcd <- mdcd[,.(mdcd_tot = sum(total, na.rm = TRUE)), by=c("nid","year_id","location_id")]
mdcd <- mdcd[type == "Total",.(mdcd_tot = total), by=c("nid","year_id","location_id")]
mdcd[, iso3 := "USA"][, year := year_id]

mdcd <- currency_conversion(data = mdcd, col.loc = "iso3", col.value = "mdcd_tot",
                          currency = "usd", col.currency.year = "year", base.year = 2020)
mdcd[, iso3 := NULL][, nid := NULL]

mdcd_benes <- arrow::read_feather(paste0(format_dir,"MSIS/beneficiaries_by_program/MSIS_state.feather"))
setnames(mdcd_benes, "beneficiaries", "mdcd_benes")
mdcd_benes <- mdcd_benes[, .(year_id = as.integer(year_id), location_id, mdcd_benes)]

# MEPS IC -----------------------------------------------------------------------------------------------------
meps_cols <- arrow::read_feather(paste0(format_dir,"MEPS_IC/meps_ic_codebook.feather"))
meps <- arrow::read_feather(paste0(format_dir,"MEPS_IC/meps_ic.feather"))
setDT(meps)
meps <- meps[,.(location_id, year_id = as.integer(year_id), employees = B1, pct_employees_enrolled = B2B, premium_per_enrollee = C1)]
meps[, meps_tot := employees*(pct_employees_enrolled/100)*premium_per_enrollee]
meps <- meps[!is.na(location_id)]

# fix missingness
meps_07 <- meps[year_id %in% c(2006,2008)]
meps_07 <- meps_07[,lapply(.SD, median), by="location_id"]
meps <- rbind(meps, meps_07)

meps <- square_df(meps)
meps[, missing := 0][is.na(meps_tot), missing := 1]

meps_e <- copy(meps)
meps_e[, min_yr := min(year_id), by="location_id"]
meps_e <- meps_e[year_id == min_yr | year_id == min_yr + 1][min_yr != 1996]

meps[, enrollees := NULL][, missing := NULL]
meps <- rbindlist(lapply(split(meps, by="location_id"), simple_extrapolate))

# FEDs -----------------------------------------------------------------------------------------------------
feds <- arrow::read_feather("FILEPATH/N_employees_by_state.feather")
feds <- feds[,.(location_id, year_id, feds_tot = n_employees)]

# Marketplace -----------------------------------------------------------------------------------------------------
mkt <- arrow::read_feather("FILEPATH/marketplace/marketplace_spending.feather")
setDT(mkt)
mkt[, year := year_id][, iso3 := "USA"]
mkt <- currency_conversion(data = mkt, col.loc = "iso3", col.value = "spending",
                                currency = "usd", col.currency.year = "year", base.year = 2020)
mkt <- mkt[,.(location_id, year_id, mkt = spending)]

# CES -----------------------------------------------------------------------------------------------------
ces <- arrow::read_feather("FILEPATH/CPS_ASEC_imputed.feather")
ces <- ces[,.(location_id = as.integer(location_id), year_id = year, oop_pc, oop_tot = oop)]

# BEA -----------------------------------------------------------------------------------------------------------
bea <- fread("FILEPATH/bea.csv", header = TRUE)
bea <- melt(bea, id.vars = c("GeoFips","GeoName"), value.name = "bea_pm", variable.name = "year_id")
bea <- bea[,.(state_name = GeoName, year_id = as.integer(as.character(year_id)), bea_tot = bea_pm*1000000)]
bea <- merge(bea, states[,.(state_name, location_id)], by="state_name")
bea[, state_name := NULL][, year := 2021][, iso3 := "USA"]
bea <- currency_conversion(data = bea, col.loc = "iso3", col.value = "bea_tot",
                           currency = "usd", col.currency.year = "year", base.year = 2020)
bea[, iso3 := NULL]

# Combination -----------------------------------------------------------------------------------------------------
combine <- function(x,y){
    x <- as.data.table(x)
    y <- as.data.table(y) 
    x$location_id <- as.character(x$location_id)
    y$location_id <- as.character(y$location_id)
    df <- merge(x, y, by=c("year_id","location_id"), all = TRUE)
  return(df)
}

data <- Reduce(combine, list(population, mdcd, mdcr, nes, qcew, meps, ec, crt, aha, ces, feds, mkt, bea))
data[!is.na(mkt), meps_tot := mkt + meps_tot]

data <- data[year_id %in% c(1999:2019)]

shea <- fread("FILEPATH/SHEA.csv")
shea <- shea[,.(location_id = as.character(location_id), year_id, tot_spending, pc_spending, fr_spending, fpc_spending)]
nhea <- fread("FILEPATH/NHEA.csv")
nhea <- nhea[,.(year_id, us_spending = nhea)]

data <- merge(data, shea, by=c("year_id","location_id"), all.x = TRUE)
data <- merge(data, nhea, by="year_id", allow.cartesian = TRUE, all = TRUE)
data[, us_spending_pc := us_spending/sum(population), by="year_id"]
data[, nid := NULL]
# Plot of the time trends of each covariate prepped - useful for finding errors in processing
ggplot(melt(data, id.vars = c("year_id","location_id"))) + 
  geom_vline(aes(xintercept = 1999), color = "red") +
  geom_line(aes(x = year_id, y = value, group = location_id)) + 
  facet_wrap(.~variable, scales = "free_y") + 
  theme_bw()

# archive old file if it already exists
shea_file <- "FILEPATH/shea_covariates.csv"
if(file.exists(shea_file)){
  file.copy(shea_file, paste0("FILEPATH/shea_covariates",Sys.Date(),".csv"))
  file.remove(shea_file)
}
fwrite(data, "FILEPATH/shea_covariates.csv")
