## ---------------------------
## Purpose: Process NES (Census Bureau’s Non-employer Statistics program)
## Author: USERNAME
## Date modified: 4/12/2021
## ---------------------------
rm(list = ls())

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, openxlsx, arrow)
in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

source("FILEPATH/currency_conversion.R")

## NAICS codes of interest
NAICS_parent_codes <- c(
  
  "621",      ## NAICS 621 Ambulatory health care services
  "622",      ## NAICS 622 Hospitals
  "623",      ## NAICS 623 Nursing care 
  "44611",   ## NAICS 44611 Pharmacies and drug stores
  "44613"   ## NAICS 44613 Optical goods stores
) 

## make regex
NAICS_parent_regex <- paste0("^", paste0(NAICS_parent_codes, collapse = "|^"))

## hardcode NIDs
NIDS <- c(
  "1997" = "474824",
  "1998" = "474822",
  "1999" = "474821",
  "2000" = "474820",
  "2001" = "474819",
  "2002" = "474818",
  "2003" = "474817",
  "2004" = "474816",
  "2005" = "474815",
  "2006" = "474814",
  "2007" = "474813",
  "2008" = "474812",
  "2009" = "474811",
  "2010" = "474810",
  "2011" = "474809",
  "2012" = "474808",
  "2013" = "474807",
  "2014" = "474806",
  "2015" = "474805",
  "2016" = "474804",
  "2017" = "474803",
  "2018" = "474802"
)

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
## Function to read an NES state file
## ---------------------------
read_NES <- function(filepath){
  
  ## read
  tmp <- fread(filepath)
  
  ## set upper
  setnames(tmp, toupper(names(tmp)))
  
  ## standardize names
  if("ESTABF" %chin% names(tmp)) setnames(tmp, "ESTABF", "ESTAB_F")
  if("COUNTY" %chin% names(tmp)) setnames(tmp, "COUNTY", "county")
  if("CTY" %chin% names(tmp)) setnames(tmp, "CTY", "county")
  setnames(tmp, "ST", "state")
  
  ## add year based on filepath digits
  tmp[,year_id := as.numeric(str_extract(filepath, "[:digit:]{4}"))]

  ## convert Legal Form of Organization (if applicable)
  if("LFO" %chin% names(tmp)){
    tmp[year_id %in% 2014:2018, 
        LFO := plyr::revalue(
          LFO, 
          c(
            "-" = "All Establishments",
            "C" = "C-Corporations and other corporate legal forms of organization",
            "Z" = "S-Corporations",
            "S" = "Sole Proprietorships",
            "P" = "Partnerships"
          ))]
    
    tmp[year_id %in% 2008:2013, 
        LFO := plyr::revalue(
          LFO, 
          c(
            "-" = "All Establishments",
            "C" = "Corporations and other corporate legal forms of organization",
            "S" = "Sole Proprietorships",
            "P" = "Partnerships"
          ))]
  }
  
  ## pad and combine FIPS
  tmp[,state := str_pad(state,width = 2, side = "left", pad = "0")]
  if("county" %chin% names(tmp)){
    tmp[,county := str_pad(county,width = 3, side = "left", pad = "0")]
    tmp[,cnty := paste0(state, county)]
    tmp[,county := NULL]
  }
  return(tmp)
}

## ---------------------------
## read and write data
## ---------------------------

## State
state_files <- Sys.glob(paste0(in_dir, "/*/*_STATE_*TXT"))

for(i in state_files){

  print(i)
  
  ## read and clean
  out <- read_NES(i)
  
  ## filter to NAICS codes
  out <- out[NAICS %like% NAICS_parent_regex]
  
  ## merge with loc ids
  out[,state := str_pad(state,width = 2, side = "left", pad = "0")]
  out <- merge(out, states, by = "state")
  
  ## set order
  setcolorder(out, c("year_id", names(states)))
  out <- out[order(year_id)]
  
  ## add in NIDs for the given year
  out[,NID := NIDS[as.character(year_id)]]
  
  ## Currency convert
  out[, iso3 := "USA"][, year := year_id]
  out <- currency_conversion(
    data = out, 
    col.loc = "iso3",
    col.value = "RCPTOT",
    currency = "usd",
    col.currency.year = "year",
    base.year = 2020
  )
  out[,c("iso3") := NULL]
  
  ## write
  yr <- out[,unique(year_id)]
  arrow::write_feather(out, paste0(out_dir, "state/NES_", yr, ".feather"))
  
}

## County
county_files <- Sys.glob(paste0(in_dir, "/*/*_COUNTY_*TXT"))

for(i in county_files){
  
  print(i)
  
  ## read and clean
  out <- read_NES(i)
  
  ## filter to NAICS codes
  out <- out[NAICS %like% NAICS_parent_regex]
  
  ## merge with loc ids
  out <- merge(out, counties, by = c("cnty", "state"))
  
  ## set order
  setcolorder(out, c("year_id", names(counties)))
  out <- out[order(year_id)]
  
  ## add in NIDs for the given year
  out[,NID := NIDS[as.character(year_id)]]
  
  ## Currency convert
  out[, iso3 := "USA"][, year := year_id]
  out <- currency_conversion(
    data = out, 
    col.loc = "iso3",
    col.value = "RCPTOT",
    currency = "usd",
    col.currency.year = "year",
    base.year = 2020
  )
  out[,c("iso3") := NULL]
    
  ## write
  yr <- out[,unique(year_id)]
  arrow::write_feather(out, paste0(out_dir, "county/NES_", yr, ".feather"))
}
