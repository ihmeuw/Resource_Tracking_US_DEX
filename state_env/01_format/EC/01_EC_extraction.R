################################################
#' @description extract Economics census from raw data
################################################

rm(list=ls())

library(openxlsx)
library(data.table)
library(tidyverse)
library(arrow)

# Directory
in_dir<-"FILEPATH"
out_dir<-"FILEPATH"

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

source("FILEPATH/currency_conversion.R")

## ---------------------------
## NAICS codes of interest
## ---------------------------

## this is the total health spending
health_sector_codes <- c(
  "621",      ## NAICS 621 Ambulatory health care services
  "622",      ## NAICS 622 Hospitals
  "623",      ## NAICS 623 Nursing care
  ## ------------
  "44611",    ## NAICS 44611 Pharmacies and drug stores
  "44613"    ## NAICS 44613 Optical goods stores
  # MAYBE "532291",    ## NAICS12 532291 Home health equipment rental
  # MAYBE "42345",   ## NAICS 42345 Medical equipment merchant wholesalers
  # MAYBE "42346",   ## NAICS 42346 Ophthalmic goods merchant wholesalers
  # MAYBE "4242",    ## NAICS 4242 Druggists' goods merchant wholesalers
  # MAYBE "334510",  ## NAICS 334510 Electromedical apparatus manufacturing
  # MAYBE "339112",  ## NAICS 339112 Surgical and medical instrument manufacturing
  # MAYBE "92312"    ## NAICS 92312 Administration of public health programs
)

## the below are type of care speficic. 
hospital_codes <- c("622")
phys_clin_service_codes <- c("6211","6214","6215")
other_prof_codes <- c("6213")
dental_codes <- c("6212")
home_codes <- c("6216")
nursing_codes <- c("6231","623311")
dme_optometry_codes <- c("62132")
other_codes <- c("62321", "62322", "62191")

## ---------------------------
## Get location info
## ---------------------------

states <- fread("FILEPATH/states.csv")
counties <- fread("FILEPATH/merged_counties.csv")

## pad FIPS codes
states[,state := str_pad(state,width = 2, side = "left", pad = "0")]
counties[,state := str_pad(state,width = 2, side = "left", pad = "0")]
counties[,cnty := str_pad(cnty,width = 5, side = "left", pad = "0")]

## ---------------------------
## loop through years to read data
## ---------------------------

EC <-  data.table()
for (yr in c(2002,2007,2012,2017)){
  
  #find the file name
  if (yr %in% c(2012,2007)){
    file_names = list.files(paste0(in_dir,yr,"/"),pattern = "62A1",full.names = T)
    file_names = file_names[file_names %like% ".DAT$"]
  }else if(yr==2002){
    # the state and county in 2002 are splitted
    file_names = list.files(paste0(in_dir,yr,"/"),pattern = "62A1",full.names = T)
    file_names = file_names[file_names %like% ".DAT$"]
    st_file<-list.files(paste0(in_dir,yr,"/"),pattern = "62A1",full.names = T)
    st_file = st_file[st_file %like% ".CSV$"]
    st_file = st_file[!st_file %like% "META"]
  }else{
    file_names = list.files(paste0(in_dir,yr,"/"),pattern = ".DAT$",full.names = T)
    file_names <- file_names[!file_names %like% "SECTOR_44"]
  }
  
  # read in the county file
  print(paste0("loading ",yr," data, file name:", file_names))
  census_data <- fread(file_names)
  
  ## filter to only state/county date
  ## geotype 2,3 = state,county 
  ## "& COUNTY + PLACE + CSA + MSA == 0" is included to make sure we only get state level totals
  census_data <- census_data[(GEOTYPE == 2 & COUNTY + PLACE + CSA + MSA == 0) | GEOTYPE == 3]
  
  ## parse data for each year
  if (yr==2002){
    
    ## filter to all establishments (so we don't double count taxable and non-taxable)
    census_data <- census_data[OPTAX_MEANING == "All establishments"]
    
    # read in county and states separately, there are only 29 state in the county DAT file, so use CSV which contains 51 states
    
    ## STATE
    da_st <- fread(st_file,skip=1)
    
    ## filter to all establishments (so we don't double count taxable and non-taxable)
    da_st <- da_st[`Meaning of Type of operation or tax status code` == "All establishments"]
    
    ## simplify data
    da_st <- da_st[!`Geographic area name` == "United States",.( #there are national and states in this file, only keep states
      YEAR = yr,
      ST = str_remove(`Geographic identifier code`, ".+US"), ## get state FIPS code
      NAICS = `2002 NAICS code`, 
      NAICS_TTL = `Meaning of 2002 NAICS code`, #sector code(62), NAICS code, Meaning of NAICS economic sector code
      revenue = suppressWarnings(as.numeric(`Receipts/revenue ($1,000)`)), # when numeric, Receipts/Revenue ($1,000)
      revenue_flag = ifelse(`Receipts/revenue ($1,000)` == "D", "D", ""), # when D, flag
      geo_type = "State"
    )]
    
    ## COUNTY
    da_county <- census_data[GEOTYPE_MEANING == "County", .( #keep county only
      YEAR, 
      ST, COUNTY, ## FIPS
      NAICS = NAICS2002, NAICS_TTL = NAICS2002_MEANING, ## business codes
      revenue = as.numeric(RCPTOT),  ## revenue ($1000)
      revenue_flag = RCPTOT_F, ## FLAG
      geo_type = GEOTYPE_MEANING
    )]
    
    da_1 <- rbind(da_st,da_county, use.names = T, fill = T)
    
  }else if(yr==2007){
    
    ## filter to all establishments (so we don't double count taxable and non-taxable)
    census_data <- census_data[OPTAX_MEANING == "All establishments"]
 
    ## simplify data
    da_1 <- census_data[,.(
      YEAR, ST, COUNTY, ## demographic stuff
      NAICS = NAICS2007, NAICS_TTL = NAICS2007_MEANING, ## business codes
      revenue = as.numeric(RCPTOT),  ## revenue ($1000)
      revenue_flag = RCPTOT_F, ## FLAG
      geo_type = GEOTYPE_MEANING
    )]
    
    da_1[geo_type == "State or state equivalent", geo_type := "State"]
    
  }else if (yr==2012){
    
    ## filter to all establishments (so we don't double count taxable and non-taxable)
    census_data <- census_data[OPTAX_TTL == "All establishments"]
    
    ## simplify data
    da_1 <- census_data[,.(
      YEAR, ST, COUNTY, ## demographic stuff
      NAICS = NAICS2012, NAICS_TTL = NAICS2012_TTL, ## business codes
      revenue = as.numeric(RCPTOT),  ## revenue ($1000)
      revenue_flag = RCPTOT_F, ## FLAG
      geo_type = plyr::revalue(as.character(GEOTYPE), c("2" = "State", "3" = "County"))
    )]
    
  }else if (yr==2017){
    
    ## filter to all establishments (so we don't double count taxable and non-taxable)
    census_data <- census_data[TAXSTAT_TTL == "All establishments"]
    
    ## simplify data
    da_1 <- census_data[,.(
      YEAR, ST, COUNTY, ## demographic stuff
      NAICS = NAICS2017, NAICS_TTL = NAICS2017_TTL, ## business codes
      revenue = as.numeric(RCPTOT),  ## revenue ($1000)
      revenue_flag = RCPTOT_F, ## FLAG
      geo_type = plyr::revalue(as.character(GEOTYPE), c("2" = "State", "3" = "County"))
    )]
  }
  
  # merge year into empty table
  EC <- rbind(EC, da_1, use.names = T, fill = T)
}

## ---------------------------
## clean up
## ---------------------------

## rename ones we didn't already rename
setnames(
  EC, 
  c("YEAR", "ST", "COUNTY", 
    "NAICS", "NAICS_TTL"), 
  c("year_id", "state", "cnty", 
    "NAICS", "NAICS_title")
)

## pad and fill FIPS codes
EC[,state := str_pad(state,width = 2, side = "left", pad = "0")]
EC[,cnty := paste0(state, str_pad(cnty,width = 3, side = "left", pad = "0"))]

## clean up flag
stopifnot(EC[revenue_flag != "" & !is.na(revenue_flag), unique(revenue_flag) %in% c("D", "S")])
EC[,flag_D := ifelse(revenue_flag == "D", 1, 0)]
EC[,flag_S := ifelse(revenue_flag == "S", 1, 0)]
EC[,revenue_flag := NULL]

## ---------------------------
## prep to model to fill NAICS codes
## ---------------------------

## filter to NAICS codes we care about
EC <- EC[NAICS %in% as.numeric(
  c(
    hospital_codes,
    phys_clin_service_codes,
    other_prof_codes,
    dental_codes,
    home_codes,
    nursing_codes,
    dme_optometry_codes,
    other_codes
  ) 
)]

## simplify and order columns
EC <- EC[,.(year_id, geo_type, state, cnty, NAICS, revenue, flag_D, flag_S)]

## check no positive revenues have a data suppression flag
stopifnot(EC[revenue > 0 & (flag_D == 1 | flag_S == 1), .N == 0])

## check all zero/NA revenues have no data suppression flag
stopifnot(EC[(revenue == 0 | is.na(revenue)) & (flag_D == 0 & flag_S == 0), .N == 0])

## standardize 0->NA
EC[revenue == 0, revenue := NA]

## split state/county
EC_state <- EC[geo_type == "State"]
EC_state[,cnty := NULL]
EC_cnty <- EC[geo_type == "County"]

## make square for state
EC_state_sq <- data.table(
  crossing(
    year_id = unique(EC_state$year_id),
    state = unique(EC_state$state),
    NAICS = unique(EC_state$NAICS),
    geo_type = unique(EC_state$geo_type)
  )
)
EC_state <- merge(EC_state_sq, EC_state, by = intersect(names(EC_state_sq), names(EC_state)), all.x = T)

## ---------------------------
## model to fill states NAICS codes (model by state/NAICS)
## ---------------------------

## order by year
EC_state <- EC_state[order(year_id)]

## make model function
model_fcn <- function(y){
  ## if no data, leave NA
  if(unique(sum(!is.na(y)) == 0)){
    return(c(NA, NA, NA, NA))
  ## else, model
  }else{
    yrs <- c(2002,2007,2012,2017)
    mod <- lm(log(y) ~ yrs)
    pred <- exp(predict(mod, data.table(yrs)))
    return(pred) 
  }
}

## model
EC_state[,pred := model_fcn(revenue), by = .(state, NAICS)]

## fill missing
EC_state[is.na(revenue), revenue := pred]

## check revenue are either ALL missing or ALL present for each state/NAICS
stopifnot(
  all.equal(
    EC_state[,.(test = sum(!is.na(revenue))), by = .(state, NAICS)][,unique(sort(test))], 
    c(0,4)
  )
)

## remove missing values
EC_state <- EC_state[!is.na(revenue)]

## remove predicted values
EC_state[, pred := NULL]

## recombine with county
EC <- rbind(EC_state, EC_cnty, use.names = T, fill = T)

## ---------------------------
## assign types of care
## ---------------------------

EC[NAICS %in% as.numeric(hospital_codes), type := "hospital"]
EC[NAICS %in% as.numeric(phys_clin_service_codes), type := "phys_clin_service"]
EC[NAICS %in% as.numeric(other_prof_codes), type := "other_prof"]
EC[NAICS %in% as.numeric(dental_codes), type := "dental"]
EC[NAICS %in% as.numeric(home_codes), type := "home"]
EC[NAICS %in% as.numeric(nursing_codes), type := "nursing"]
EC[NAICS %in% as.numeric(dme_optometry_codes), type := "dme_optometry"]
EC[NAICS %in% as.numeric(other_codes), type := "other"]

## ---------------------------
## add NID
## ---------------------------
nid <- fread("FILEPATH/economic_census.csv") %>% select(-dataset)
EC <- merge(EC, nid, by="year_id",all.x = T)

## ---------------------------
## merge in location columns and split state/county
## ---------------------------

## split by state/county
ec_state <- EC[geo_type == "State", -c("cnty")]
ec_county <- EC[geo_type == "County", -c("state")]

## note what we're dropping
if(length(diff(ec_state$state, states$state)) > 0) warning("Dropping states: ", paste0(diff(ec_state$cnty, counties$cnty), collapse = ", "))
if(length(diff(ec_county$cnty, counties$cnty)) > 0) warning("Dropping counties: ", paste0(diff(ec_county$cnty, counties$cnty), collapse = ", "))

## merge in location stuff
ec_state <- merge(ec_state, states, by = "state")
ec_county <- merge(ec_county, counties, by = "cnty")

## ---------------------------
## aggregate across types of care
## ---------------------------

## sum across health sector
health_sector_state <- ec_state[
  NAICS %in% as.numeric(health_sector_codes), 
  .(
    revenue = sum(revenue, na.rm = T), type = "envelope", 
    flag_D = round(mean(flag_D),2), ## proportion of obs with flag D
    flag_S = round(mean(flag_S),2) ## proportion of obs with flag D
    ),
  by = c("nid", "year_id", names(states))
]
health_sector_county <- ec_county[
  NAICS %in% as.numeric(health_sector_codes), 
  .(
    revenue = sum(revenue, na.rm = T), type = "envelope", 
    flag_D = round(mean(flag_D),2), ## proportion of obs with flag D
    flag_S = round(mean(flag_S),2) ## proportion of obs with flag D
    ),
  by = c("nid", "year_id", names(counties))
]

## sum by type of care
type_state <- ec_state[
  !is.na(type), 
  .(
    revenue = sum(revenue, na.rm = T),
    flag_D = round(mean(flag_D),2), ## proportion of obs with flag D
    flag_S = round(mean(flag_S),2) ## proportion of obs with flag D
    ), 
  by = c("nid", "year_id", names(states), "type")
]

type_county <- ec_county[
  !is.na(type), 
  .(
    revenue = sum(revenue, na.rm = T),
    flag_D = round(mean(flag_D),2), ## proportion of obs with flag D
    flag_S = round(mean(flag_S),2) ## proportion of obs with flag D
    ), 
  by = c("nid", "year_id", names(counties), "type")
]

## bind
state <- rbind(health_sector_state, type_state, use.names = T, fill = T)
county <- rbind(health_sector_county, type_county, use.names = T, fill = T)

## fix NAs in flag columns
state[is.na(flag_D), flag_D := 0]
state[is.na(flag_S), flag_S := 0]
county[is.na(flag_D), flag_D := 0]
county[is.na(flag_S), flag_S := 0]

## order
setcolorder(county, c("year_id", names(counties)))
setcolorder(state, c("year_id", names(states)))
county <- county[order(year_id, cnty)]
state <- state[order(year_id, state)]

## convert to millions
county[,revenue := (revenue*1000)/10^6]
state[,revenue := (revenue*1000)/10^6]

## ---------------------------
## Currency convert
## ---------------------------

## Currency convert state data
state[, iso3 := "USA"][, year := year_id]
state <- currency_conversion(
  data = state, 
  col.loc = "iso3",
  col.value = "revenue",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)
state[,iso3 := NULL]

## Currency convert county data
county[, iso3 := "USA"][, year := year_id]
county <- currency_conversion(
  data = county, 
  col.loc = "iso3",
  col.value = "revenue",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)
county[,iso3 := NULL]

## ---------------------------
## write
## ---------------------------
write_feather(state, paste0(out_dir, "EC/EC_state.feather"))
write_feather(county, paste0(out_dir, "EC/EC_county.feather"))