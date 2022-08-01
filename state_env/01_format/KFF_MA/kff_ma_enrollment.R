rm(list = ls())
library(tidyverse)
library(data.table)
library(boot)

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major)) 
suppressMessages(lbd.loader::load.containing.package())
load("FILEPATH/states.RData")

ma_enrol_files <- Sys.glob("FILEPATH/*TOTAL*.CSV")
ma_enrol <- rbindlist(lapply(ma_enrol_files, function(f){
  df <- fread(f, skip = 3, nrow = 52, col.names = c("state_name","enrollees"))
  df[, year_id := as.integer(str_extract(f, "\\d{4}"))]
  return(df)
}))

ma_p_files <- Sys.glob("FILEPATH/*PERCENT*.CSV*")
ma_p <- rbindlist(lapply(ma_p_files, function(f){
  df <- fread(f, skip = 3, nrow = 52, col.names = c("state_name","ma_pct"))
  df[, year_id := as.integer(str_extract(f, "\\d{4}"))]
  return(df)
}))

ma <- expand.grid(year_id = 1990:2019, location_id = unique(states$location_id))
ma <- merge(ma, states[,.(location_id, state_name)], by="location_id", allow.cartesian = TRUE)
ma <- merge(ma, ma_enrol, by=c("state_name","year_id"), all.x = TRUE)
ma <- merge(ma, ma_p, by=c("state_name","year_id"), all.x = TRUE)

setDT(ma)

# Short term solution - extrapolate to 2019
ma_logit <- copy(ma[,.(location_id, year_id, state_name, ma_pct_logit = logit(ma_pct))])
ma_logit <- rbindlist(lapply(split(ma_logit, by="location_id"), simple_extrapolate))
ma <- copy(ma_logit[,.(location_id, year_id, state_name, ma_pct = inv.logit(ma_pct_logit))])
ma <- ma[year_id >= 1999] # MA started in 1997

## add NID
NID <- fread("FILEPATH/kff_ma.csv")$dex_nid %>% unique()
ma[,nid := NID]

fwrite(ma,"FILEPATH/kff_ma_enrollment.csv")
