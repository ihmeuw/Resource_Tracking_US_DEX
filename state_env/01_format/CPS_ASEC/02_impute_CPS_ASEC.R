## ---------------------------
## Purpose: Impute CPS data with CES data
## Author: USERNAME
## Date modified: 9/15/2021
## ---------------------------

rm(list = ls())
pacman::p_load(data.table, arrow, tidyverse, lme4)
d <- data.table
source("FILEPATH/get_population.R")
load("FILEPATH/states.RData")

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

NID <- fread("FILEPATH")
NID <- NID[,unique(dex_nid)]

## ---------------------------
## Get data
## ---------------------------
CPS <- d(read_feather("FILEPATH"))
CES <- d(read_feather("FILEPATH"))

## simplify data
CPS <- CPS[year %in% 1999:2019 & year != 2014,.(year, abbreviation, oop, series = "CPS")]
CES <- CES[,.(year, abbreviation, oop = OOP, series = "CES")]

## ---------------------------
## For CES, impute the missing years (for the states that have data) 
## by taking the mean of the two years adjacent to the gap.
## ---------------------------

## impute missing years
CES <- dcast(CES, series + abbreviation ~ paste0("yr_", year), value.var = "oop")
CES[,yr_1999 := (yr_1998 + yr_2000)/2]
CES[,c("yr_2010", "yr_2011") := (yr_2009 + yr_2012)/2]
CES <- melt(CES, id.vars = c("series", "abbreviation"), variable.name = "year", value.name = "oop")
CES[,year := as.numeric(str_remove(year, "yr_"))]

## filter years
CES <- CES[year %in% 1999:2019][order(year)]

## ---------------------------
## conver to per capita
## ---------------------------

## merge
dat <- rbind(CPS, CES)

## get location_ids
dat <- merge(dat, states, by = "abbreviation")

## get population
pop <- get_population(
  gbd_round_id =7, 
  decomp_step = "iterative",
  location_id = unique(dat$location_id),
  age_group_id = 22,
  sex_id = 3, 
  year_id = unique(dat$year)
)
pop <- pop[,.(location_id = as.character(location_id), year = year_id, population)]

## merge in 
dat <- merge(dat, pop, by = c("year", "location_id"))

## convert
dat[,oop_pc := oop/population]

## log
dat[,ln_oop_pc := log(oop_pc)]

## ---------------------------
## simplify data
## ---------------------------
dat <- dat[,.(
  year = factor(year, levels = 1999:2019), 
  abbreviation,
  series, 
  oop_pc,
  ln_oop_pc,
  population 
)][order(year, abbreviation)]

## make square dataset
dat_sq <- d(crossing(
  year = unique(dat$year), 
  abbreviation = unique(dat$abbreviation), 
  series = unique(dat$series))
)
dat <- merge(dat_sq, dat, by = names(dat_sq), all = T)

## ---------------------------
## get pct uninsured
## ---------------------------

pct_unins <- fread("FILEPATH")
pct_unins <- pct_unins[,.(abbreviation, year = factor(year_id, levels = 1999:2019), pct_uninsured = uninsured)]

dat <- merge(dat, pct_unins, by = c("abbreviation", "year"), all = T)

## ---------------------------
## Regress CES on CPS with state and year dummies (aka fixed effects) and ln(THEpc).
## ---------------------------
mod1_formula <- "ln_oop_pc ~ year + abbreviation + series + abbreviation*series + pct_uninsured"
mod1 <- lm(as.formula(mod1_formula), dat)

## ---------------------------
## Predict missing states/years
## ---------------------------

## predict
dat[,pred_ln_oop_pc := predict(mod1, dat)]

## convert to percap
dat[,pred_oop_pc := exp(pred_ln_oop_pc)]

## ---------------------------
## Save data
## ---------------------------

## fill missing data points with predictions
out <- dat[series == "CPS", -"series"]
out[is.na(oop_pc), oop_pc := pred_oop_pc]
out <- merge(out, states, by = c("abbreviation"), all = T)
out <- out[,c(names(states), "year", "oop_pc"), with = F]
out <- merge(out, pop, by = c("location_id", "year"))
out[,`:=`(oop = oop_pc*population, population = NULL)]

## add NIDs
out[,nid := NID]

write_feather(out, "FILEPATH")
