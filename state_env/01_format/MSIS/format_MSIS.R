## ---------------------------
## Purpose: Process MSIS (Medicaid Statistical Information System) data 
##          -> these files include the number of beneficiaries by state/year/program type
## Author: USERNAME
## Date modified: 5/3/2021
## ---------------------------
rm(list = ls())

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, readxl, arrow)
in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

## get locs
states <- fread("FILEPATH/states.csv")
## pad FIPS codes
states[,state := str_pad(state,width = 2, side = "left", pad = "0")]

## hardcode NIDs
NIDS <- c(
  "1999" = "475959",
  "2000" = "475958",
  "2001" = "475957",
  "2002" = "475956",
  "2003" = "475955",
  "2004" = "475954",
  "2005" = "475953",
  "2006" = "475952",
  "2007" = "475951",
  "2008" = "475950",
  "2009" = "475949",
  "2010" = "475948",
  "2011" = "475947",
  "2012" = "475946"
)

## ---------------------------
## Get data
## ---------------------------
setwd(in_dir)
files <- list.files(recursive = T, pattern = "XLS")

read_fcn <- function(x){
  
  ## define function
  if(str_detect(x, "XLSX")){
    xlfcn <- read_xlsx
  }else{
    xlfcn <- read_xls
  }
  
  ## read
  tmp <- data.table(
    xlfcn(
      x,
      col_names = T,
      skip = 4
    )
  )
  
  ## rename columns of interest
  total_col <- names(tmp)[names(tmp) %like% "TOTAL"]
  setnames(tmp, total_col, "TOTAL BENEFICIARIES")
  
  ## add year
  tmp[,year_id := str_extract(x, "[:digit:]+")]
  
  return(tmp)
}

MSIS <- rbindlist(lapply(files, read_fcn), use.names = T, fill = T)

## ---------------------------
## clean data
## ---------------------------

MSIS <- MSIS[STATE != "TOTAL",.(
  year_id, 
  abbreviation = STATE, 
  beneficiaries = `TOTAL BENEFICIARIES`
)]

## merge in locs
MSIS <- merge(MSIS, states, all.y = T)

## set order
setcolorder(MSIS, c("year_id", names(states), "beneficiaries"))
MSIS <- MSIS[order(year_id)]

## add in NIDs for each year
MSIS[,NID := plyr::revalue(as.character(year_id), NIDS)]

## ---------------------------
## Write
## ---------------------------
write_feather(MSIS, paste0(out_dir, "MSIS_state.feather"))
