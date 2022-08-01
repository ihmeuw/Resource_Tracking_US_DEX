## ---------------------------
## Purpose: Prep QCEW as covariate for state models
## Author: USERNAME
## Date modified: 4/8/2021
## ---------------------------
rm(list = ls())

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, openxlsx, arrow, lme4)
dir <- "FILEPATH"
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

## ---------------------------
## get QCEW data
## ---------------------------
qcew_files <- Sys.glob("FILEPATH")
qcew <- rbindlist(
  lapply(
    qcew_files, 
    function(x) data.table(arrow::read_feather(x))
  )
)

## pull out NIDs
NIDS <- qcew[,.(year_id, nid = NID)] %>% unique()

## simplify
qcew <- qcew[,.(location_id, year_id, own_title, industry_code, total_annual_wages)]

## sum across ownership
qcew <- qcew[,.(total_annual_wages = sum(total_annual_wages)), by = .(location_id, year_id, industry_code)]

## mark types of care
health_sector_codes <- c(
  "621",      ## NAICS 621 Ambulatory health care services
  "622",      ## NAICS 622 Hospitals
  "623",      ## NAICS 623 Nursing care
  ## ------------
  "44611",    ## NAICS 44611 Pharmacies and drug stores
  "44613"     ## NAICS 44613 Optical goods stores
)

## for EC
hospital_codes <- c("622")
phys_clin_service_codes <- c("6211","6214","6215")
other_prof_codes <- c("6213")
dental_codes <- c("6212")
home_codes <- c("6216")
nursing_codes <- c("6231","623311")
dme_optometry_codes <- c("62132")
other_codes <- c("62321", "62322", "62191")

## for CRT
pharma_codes <- c("44611")

## ---------------------------
## prep to model to fill NAICS codes
## ---------------------------

## filter to NAICS codes we care about
qcew <- qcew[industry_code %in% as.numeric(
  c(
    hospital_codes,
    phys_clin_service_codes,
    other_prof_codes,
    dental_codes,
    home_codes,
    nursing_codes,
    dme_optometry_codes,
    other_codes, 
    pharma_codes
  ) 
)]

## standardize 0->NA
qcew[total_annual_wages == 0, total_annual_wages := NA]

## make square for state
qcew_sq <- data.table(
  crossing(
    location_id = unique(qcew$location_id),
    year_id = unique(qcew$year_id),
    industry_code = unique(qcew$industry_code)
  )
)
qcew <- merge(qcew_sq, qcew, by = intersect(names(qcew_sq), names(qcew)), all.x = T)

## ---------------------------
## model to fill states NAICS codes (model by state/NAICS)
## ---------------------------

## order by year
qcew <- qcew[order(year_id)]

yrs <- unique(qcew$year)

## make model function
model_fcn <- function(y){
  ## if no data, leave NA
  if(unique(sum(!is.na(y)) == 0)){
    return(rep(NA, length(yrs)))
  ## else, model
  }else{
    mod <- lm(log(y) ~ yrs)
    pred <- exp(predict(mod, data.table(yrs)))
    return(pred) 
  }
}

## model
qcew[,pred := model_fcn(total_annual_wages), by = .(location_id, industry_code)]

## fix fit for these years
qcew[location_id == 531 & industry_code == 623311 & year_id %in% c(2006, 2007, 2010, 2011, 2014, 2015), total_annual_wages := NA] 
fix <- qcew[location_id == 531 & industry_code == 623311]
qcew <- qcew[!(location_id == 531 & industry_code == 623311)]
fix[, roll := zoo::rollmean(total_annual_wages, k = 4, na.pad = TRUE, na.rm = T)]
fix[is.na(total_annual_wages), total_annual_wages := roll]
fix[,roll := NULL]
qcew <- rbind(qcew, fix)

## fill missing
qcew[is.na(total_annual_wages), `:=`(total_annual_wages = pred, flag = "imputed")]

## check revenue are either ALL missing or ALL present for each state/NAICS
stopifnot(
  all.equal(
    qcew[,.(test = sum(!is.na(total_annual_wages))), by = .(location_id, industry_code)][,unique(sort(test))], 
    c(0,length(yrs))
  )
)

## remove missing values
qcew <- qcew[!is.na(total_annual_wages)]

## remove predicted values
qcew[, pred := NULL]

## ------------------------------------
## assign types
## ------------------------------------

qcew[industry_code %in% as.numeric(hospital_codes), type := "hospital"]
qcew[industry_code %in% as.numeric(phys_clin_service_codes), type := "phys_clin_service"]
qcew[industry_code %in% as.numeric(other_prof_codes), type := "other_prof"]
qcew[industry_code %in% as.numeric(dental_codes), type := "dental"]
qcew[industry_code %in% as.numeric(home_codes), type := "home"]
qcew[industry_code %in% as.numeric(nursing_codes), type := "nursing"]
qcew[industry_code %in% as.numeric(dme_optometry_codes), type := "dme_optometry"]
qcew[industry_code %in% as.numeric(other_codes), type := "other"]
qcew[industry_code %in% as.numeric(pharma_codes), type := "pharma"]

## aggregate across types of care and add envelope
envelope <- qcew[industry_code %in% as.numeric(health_sector_codes), 
                 .(total_annual_wages = sum(total_annual_wages), 
                   type = "envelope", 
                   flag = paste0(unique(na.omit(flag)))), 
                 by = .(location_id, year_id)]

types <- qcew[!is.na(type), 
              .(total_annual_wages = sum(total_annual_wages), 
                flag = paste0(unique(na.omit(flag)))), 
              by = .(location_id, year_id, type)]

qcew <- rbind(envelope, types, use.names = T, fill = T)

## ------------------------------------
## Make square of all locations
## ------------------------------------

## list types
type <- qcew[,unique(type)]

## add in location ids to covariate computation
national_locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step = "iterative")
national_locs <- national_locs[level >= 3, .(location_id, ihme_loc_id)]
pop_square <- get_population(
  gbd_round_id = 7, 
  decomp_step = "iterative",
  location_id = national_locs$location_id,
  age_group_id = 22,
  sex_id = 3, 
  year_id = c(1990:2019)
)

## ensure all our states are in this group
stopifnot(qcew[!location_id %in% pop_square$location_id, .N == 0])

## Required columns are location_id, year_id, age_group_id, sex_id, and cv_{covariate_name} columns.
cv <- pop_square[,.(location_id, year_id, age_group_id, sex_id, cv_population = population)]

## make square by type
cv <- data.table(crossing(cv, type))

## merge in qcew
cv <- merge(cv, qcew, by = c("location_id", "year_id", "type"), all.x = T)

## merge in location_names
cv <- merge(cv, national_locs, by = "location_id", all.x = T)

## convert to per capita
cv[,total_annual_wages_pc := total_annual_wages/cv_population]

## ------------------------------------
## fill missing locations
## ------------------------------------

## set USA national to sum of subnational
usa <- cv[ihme_loc_id %like% "USA_"]
usa <- usa[,.(sum_total_annual_wages = sum(total_annual_wages), ihme_loc_id = "USA"), by = .(year_id, type)]
cv <- merge(cv, usa, by = c("ihme_loc_id", "year_id", "type"), all.x = T)
cv[!is.na(sum_total_annual_wages),
   `:=`(total_annual_wages = sum_total_annual_wages, 
        total_annual_wages_pc = sum_total_annual_wages/cv_population)]
cv[,sum_total_annual_wages := NULL]

## check for no missing usa 
stopifnot(cv[ihme_loc_id %like% "USA" & (is.na(total_annual_wages) | is.na(total_annual_wages_pc)), .N == 0])

## check mean wages are comparable
test <- cv[ihme_loc_id %like% "USA"]
test[, national := ifelse(ihme_loc_id == "USA",1,0)]
test[,mean(total_annual_wages_pc), by = national]

## rename types as "cv_wages_*"
cv[,type := paste0("cv_wages_", type)]

## set other locations to overall mean from USA (should be meaningless)
cv[is.na(total_annual_wages_pc), `:=`(
  total_annual_wages_pc = cv[ihme_loc_id == "USA", mean(total_annual_wages_pc)], 
  cv_population  = cv[ihme_loc_id == "USA", mean(cv_population)]
)]

## make wide
cv <- dcast(cv, location_id + year_id + age_group_id + sex_id + cv_population ~ type, value.var = "total_annual_wages_pc")

## convert population to thousands
cv[,cv_population_thou := cv_population/10^3][,cv_population := NULL]

## merge back in NID
cv <- merge(cv, NIDS, by = "year_id")
setcolorder(cv, "nid")

## write
arrow::write_feather(cv, paste0(dir, "/QCEW_state_covariate.feather"))
