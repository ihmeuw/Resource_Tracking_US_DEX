## ---------------------------
## Purpose: Process QCEW number of employees
## Author: USERNAME
## Date modified: 4/8/2021
## ---------------------------
rm(list = ls())

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, openxlsx, arrow)
in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

setwd(in_dir)

## hardcoding NIDS
NIDS <- c(
  "2019" = "474954",
  "2018" = "474953",
  "2017" = "474952",
  "2016" = "474951",
  "2015" = "474950",
  "2014" = "234615",
  "2013" = "234614",
  "2012" = "234613",
  "2011" = "234612",
  "2010" = "234611",
  "2009" = "234609",
  "2008" = "234607",
  "2007" = "234606",
  "2006" = "234605",
  "2005" = "234604",
  "2004" = "234602",
  "2003" = "234601",
  "2002" = "234600",
  "2001" = "234599",
  "2000" = "234598",
  "1999" = "234597",
  "1998" = "234567",
  "1997" = "234566",
  "1996" = "234565",
  "1995" = "234564",
  "1994" = "234563",
  "1993" = "234562",
  "1992" = "234561",
  "1991" = "234560",
  "1990" = "234248"
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
## Get list of data files
## ---------------------------
files <- list.files(in_dir, recursive = T, full.names = T, pattern = "ANNUAL_SINGLE_FILE_Y")

## ---------------------------
## Make a function to read and select columns of interest
## ---------------------------
read_qcew <- function(filename){
  
  ## read
  tmp <- fread(filename)
  
  ## select columns of interest 
  tmp <- tmp[,.(
    
    ## IDS
    year_id = year, area_fips, own_code, industry_code, agglvl_code, size_code, 
    
    annual_avg_emplvl
    
  )]
  
  return(tmp)
}

## ---------------------------
## Make a function to add titles to a variable
## ---------------------------
title_qcew <-  function(dt, var_name){
  
  ## get documentation file
  doc <- fread(paste0(in_dir, "USA_QCEW_", toupper(var_name), "_Y2021M04D08.csv"))
  
  ## add missing leading zeros in FIPS and conver to character
  if(var_name == "area_fips"){
    dt[,area_fips := as.character(area_fips)]
    doc[,area_fips := as.character(area_fips)]
    doc[, area_fips := str_pad(area_fips, width = 5, side = "left", pad = "0")]
  }
  
  ## get newnames from docfile
  title_col <- names(doc)[names(doc) != var_name]
  
  ## get order of columns in data
  cols <- names(dt)
  firsthalf <- cols[1:which(cols == var_name)]
  secondhalf <- cols[(which(cols == var_name)+1):length(cols)]
  
  ## merge docfile
  dt <- merge(dt, doc, by = var_name, all.x = T)
  
  ## fix order (to insert new column in the right spot)
  setcolorder(dt, c(firsthalf, title_col, secondhalf))
  
  return(dt)
}

## ---------------------------
## for each file, read, get titles, do some filtering, and write 
## ---------------------------
for (i in files){

  print(i)
  
  ## read 
  out <- read_qcew(i)
  
  ## add titles
  for (j in c("own_code", "industry_code", "agglvl_code", "size_code")){
    out <- title_qcew(out, j)
  }
  
  ## we want national totals by ownership sector
  out <- out[agglvl_code == 51]
  
  ## add in NIDs for the given year
  out[,NID := NIDS[as.character(year_id)]]
  
  ## split by state/county
  state <- out[area_fips %like% "000$"]
  state[,area_fips := str_remove(area_fips, "000$")]
  
  ## merge in location demographics (STATE)
  setnames(state, "area_fips", "state")
  state <- merge(state, states, by = "state")
  setcolorder(state, c("year_id", names(states)))
  
  ## write
  yr <- out[,unique(year_id)]
  arrow::write_feather(state, paste0(out_dir, "state/employment_pct_by_state/QCEW_", yr, "_EMPLOYMENT.feather"))
}
