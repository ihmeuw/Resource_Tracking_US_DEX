## ---------------------
## Purpose: forecast national dex estimates at the age-sex level to 2019
## Author: USERNAME
## Date: 10/29/2021
## ---------------------

rm(list = ls())

pacman::p_load(data.table, arrow, tidyverse, lme4)
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

## ----------------------------
## get data
## ----------------------------

ndex_as <- fread("FILEPATH/ndex_by_age.csv")
ndex <- fread("FILEPATH/ndex_by_year.csv")

## get fraction of per capita spend
D <- merge(ndex, ndex_as, by = c("year_id", "draw","dep_var"))
D[,pc_fr := ndex_pc_as/ndex_pc]

## get population through 2019
age_groups <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6)
age_groups <- age_groups[!age_group_id %in% c(2,3,4,31,32,235)]
age_groups <- rbind(age_groups, 
                    data.table(age_group_id = c(28,160), age_group_years_start = c(0,85), age_group_years_end = c(1,125)), 
                    fill = TRUE)
population <- get_population(age_group_id = c(unique(age_groups$age_group_id),31,32,235), 
                             location_id = 102, 
                             year_id = min(ndex$year_id):2019, 
                             sex_id = c(1,2), 
                             gbd_round_id = 7,
                             decomp_step = "iterative",
                             status = 'best') 
population[age_group_id %in% c(31,32,235), `:=`(population = sum(population), age_group_id = 160), by=c("location_id","year_id")]
population <- unique(population)

## simplify
population <- population[,.(age_group_id, year_id, sex_id, population)]

## merge in
D <- merge(D, population, by = c("age_group_id", "year_id", "sex_id"), all = T)

## fill missing age groups
D[,age := unique(na.omit(age)), by = age_group_id]

## make age+sex categories
D[,age_sex := paste0(age, "_", sex_id)]

## make dummy variable for year >= 2016
D[,dummy_2016 := ifelse(year_id >= 2016, 1, 0)]

## ----------------------------
## run age-sex models for each draw
## ----------------------------

mod_fcn <- function(x, pred = T){
  TMP <- D[draw == x$d | is.na(draw)]
  TMP <- TMP[dep_var == x$dv | is.na(dep_var)]
  TMP[,draw := x$d][, dep_var := x$dv]
  mod <- lm(
    pc_fr ~ ## fraction (age-sex specific per capita spend) / (total per capita spend)
      age_sex + ## dummy for each age/sex
      year_id + year_id*age_sex + ## year trend, interacted with age/sex
      dummy_2016 + dummy_2016*age_sex, ## dummy variable on 2016 for each age/sex (to get fit to run through 2016)
    TMP[year_id >= 2000] ## only modeling with 2000+ data since earlier years make the time trend a bit weird
  )
  if(pred){
    TMP[,pred := predict(mod, TMP)]
    return(TMP)
  }else{
    coefs <- data.table(summary(mod)$coef, keep.rownames = T)
    coefs[,draw := d]
    return(coefs)
  }
}

## get predictions
grid <- expand.grid(d = 1:1000, dv = c("agg","pub","priv","oop")) %>% setDT()
grid[, index := .I]
ndex_as_pred <- lapply(split(grid, by="index"), function(x) mod_fcn(x))
ndex_as_pred <- rbindlist(ndex_as_pred)

## ----------------------------
## scale national total based on NHEA
## ----------------------------

## get NHEA
nhea <- fread("FILEPATH/NHEA.csv")
ndex <- ndex[dep_var == "agg"]

## get ratios dex/nhea
ratio <- merge(nhea, ndex, by = "year_id", all.x = T)
ratio[,dex_nhea_ratio := ndex_tot/nhea]

## get years of interest
ratio <- ratio[year_id >= min(ndex$year_id)]

## make year dummy
ratio[,dummy_2016 := ifelse(year_id >= 2016, 1, 0)]

## predict out ratio at the draw level
tot_mod_fcn <- function(d){
  TMP <- ratio[draw == d | is.na(draw)]
  mod <- lm(dex_nhea_ratio ~ year_id + dummy_2016, TMP) ## just use time trend with dummy through 2016
  TMP[, draw := d]
  TMP[, pred := predict(mod, TMP)]
  return(TMP)
}
ndex_pred <- lapply(1:1000, tot_mod_fcn)
ndex_pred <- rbindlist(ndex_pred)

## plot draw 1 for vetting
ndex_pred[draw == 1] %>%
  ggplot() + 
  geom_point(aes(x = year_id, y = dex_nhea_ratio)) +
  geom_line(aes(x = year_id, y = pred))

## merge in population
ndex_pred[,n_pop := NULL]
tot_pop <- population[,.(n_pop = sum(population)), by = year_id]
ndex_out <- merge(ndex_pred, tot_pop, by = "year_id", allow.cartesian = T)

## fill predictions
ndex_out[is.na(ndex_tot), ndex_tot := pred*nhea] ## multiply nhea by modeled ratio dex/nhea for 2017-2019
ndex_out[is.na(ndex_pc), ndex_pc := ndex_tot/n_pop] ## divide total above by population for 2017-2019

## match original ndex columns
ndex_out <- ndex_out[,.(
  year_id, 
  draw, 
  dep_var = "agg",
  ndex_tot, 
  n_pop, 
  ndex_pc
)]

## ----------------------------
## predict out the total payer fractions
## ----------------------------
ndex_payers <- fread("FILEPATH/ndex_by_year.csv")
ndex_payers <- ndex_payers[dep_var != "agg"]

# Get fractional payer spend
ndex_payers <- merge(ndex_payers, ndex[,.(year_id, draw, ndex_pc_tot = ndex_pc)], by=c("year_id","draw"))
ndex_payers[, payer_frac := ndex_pc/ndex_pc_tot]

## Square data to 2019
newer_years <- expand.grid(year_id = 1996:2019, draw = 1:1000, dep_var = c("pub","priv","oop")) %>% setDT()
ndex_payers <- merge(ndex_payers, newer_years, by=c("year_id","draw","dep_var"), all = TRUE)

## make year dummy
ndex_payers[,dummy_2016 := ifelse(year_id >= 2016, 1, 0)]

## predict out ratio at the draw level
payer_mod_fcn <- function(x){
  TMP <- ndex_payers[draw == x$d | is.na(draw)][dep_var == x$dep_var | is.na(dep_var)]
  mod <- lm(payer_frac ~ year_id + dummy_2016, TMP) ## just use time trend with dummy through 2016
  TMP[,draw := x$d][, dep_var := x$dep_var]
  TMP[,pred := predict(mod, TMP)]
  return(TMP)
}

## get predictions
grid <- expand.grid(d = 1:1000, dep_var = c("pub","priv","oop")) %>% setDT()
grid[, index := .I]
ndex_payer_pred <- lapply(split(grid, by="index"), function(x) payer_mod_fcn(x))
ndex_payer_pred <- rbindlist(ndex_payer_pred)

# Raking payers to the total NDEX for that year
ndex_payer_pred[, pay_tot := sum(pred), by=c("year_id","draw")][, pred := pred/pay_tot]

# Merge payer props onto total, calculate total & pc, then bind the aggregate back on
ndex_payer_pred <- ndex_payer_pred[,.(year_id, draw, dep_var, pred)]
ndex_payer_pred <- merge(ndex_payer_pred, ndex_out[,.(year_id, draw, ndex_tot, n_pop, ndex_pc)], by=c("year_id","draw"))
ndex_payer_pred[,`:=`(ndex_tot = pred*ndex_tot, ndex_pc = pred*ndex_pc, pred = NULL)]

ndex_out <- rbind(ndex_out, ndex_payer_pred)

## ----------------------------
## use scaled national spending to scale up age-sex specific spending
## ----------------------------
ndex_as_pred[,c("ndex_pc", "ndex_tot") := NULL] ## remove cols that would duplicate in merge
ndex_as_out <- merge(ndex_as_pred, ndex_out[,.(year_id, draw, dep_var, ndex_pc)], by = c("year_id", "draw", "dep_var"), all = T)

## fill predictions
ndex_as_out[is.na(pc_fr), pc_fr := pred] ## fill NA per capita fraction with modeled
ndex_as_out[is.na(ndex_pc_as), ndex_pc_as := pred*ndex_pc] ## multiply total percap by ratio (age-sex-specific percap)/(total percap)
ndex_as_out[is.na(ndex_tot_as), ndex_tot_as := ndex_pc_as*population] ## multiply the above by population to get total

## match original ndex_as columns
ndex_as_out <- ndex_as_out[,.(
  year_id, 
  age_group_id, 
  sex_id, 
  age,
  draw, 
  dep_var,
  ndex_tot_as,
  ndex_pc_as, 
  pred_ndex_pc_as = pred*ndex_pc, ## prediction in PC space (kept for vetting plots)
  n_pop_as = population, 
  pc_fr,
  pred_pc_fr = pred ## prediction in FR space (kept for vetting plots)
)]

## ----------------------------
## write out data
## ----------------------------

## remove unneeded cols
ndex_as_out[,c("pred_ndex_pc_as", "pred_pc_fr", "pc_fr") := NULL]
setcolorder(ndex_as_out, names(ndex_as))

fwrite(ndex_out, "FILEPATH/forecast_ndex_by_year.csv")
fwrite(ndex_as_out, "FILEPATH/forecast_ndex_by_age.csv")
