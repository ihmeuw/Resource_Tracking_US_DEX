## ---------------------------
## Purpose: Process Consumer Expenditure Survey Data
## Author: USERNAME
## Date modified: 8/24/2021
## ---------------------------

## survey documentation here
## https://www.bls.gov/cex/csxsurveyforms.htm#interview

rm(list = ls())

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package()) # load shared functions

load("FILEPATH")

source("FILEPATH/currency_conversion.R") # currency conversion function

NIDs <- c(
  "2020" = "484547",
  "2019" = "484546",
  "2018" = "484545",
  "2017" = "484544",
  "2016" = "484543",
  "2015" = "484542",
  "2014" = "484541",
  "2013" = "484540",
  "2012" = "484539",
  "2011" = "83015",
  "2010" = "83187",
  "2009" = "33518",
  "2008" = "81292",
  "2007" = "81293",
  "2006" = "83614",
  "2005" = "83681",
  "2004" = "83959",
  "2003" = "84213",
  "2002" = "84214",
  "2001" = "84319",
  "2000" = "84428",
  "1999" = "83625",
  "1998" = "84492",
  "1997" = "80563",
  "1996" = "83013",
  "1995" = "84537",
  "1994" = "84549"
)

## ---------------------------
## Setup
## ---------------------------
pacman::p_load(data.table, tidyverse, openxlsx, arrow, readstata13)
in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

## ---------------------------
## get FMLI data (for weights, state, etc)
## ---------------------------

## get years
setwd(in_dir)
files <- list.files(pattern = "INTERVIEW", recursive = T)
yrs <- unique(str_extract(files, "[:digit:]+"))

## loop through, unzip, read, etc. 
FMLI <- data.table()
for(i in yrs){
  
  ## get files
  f <- files[files %like% i & files %like% "FMLI" & files %like% "DTA$"]
  
  ## get data
  d <- lapply(
    f, 
    function(x){
      tmp <- data.table(read.dta13(x))
      tmp[,year := if(as.numeric(i < 50)) as.numeric(i) + 2000 else as.numeric(i) + 1900]
      tmp
    } 
  )
  
  ## bind together
  D <- rbindlist(d, use.names = T, fill = T)
  
  ## bind to big tables
  FMLI <- rbind(FMLI, D, use.names = T, fill = T)
  
  ## log
  print(i)
}

## select variables of interest and convert types
FMLI <- FMLI[,.(
  year,
  state,
  newid = as.character(newid),
  finlwt21, 
  qintrvmo = as.numeric(qintrvmo),
  qintrvyr
)]

## Generate a calendar-year population weight variable
FMLI[,popwt := ifelse(
  qintrvmo %in% 1:3 & qintrvyr == year,
  (qintrvmo - 1) / 3 * finlwt21 / 4,
  ifelse(
    qintrvyr == (year + 1),
    (4 - qintrvmo) / 3 *finlwt21 / 4,
    finlwt21 / 4
  )
)]

## remove cols we don't need
FMLI[,c("qintrvmo", "qintrvyr") := NULL]

## ---------------------------
## get MDB data (for OOP payments)
## ---------------------------

## get years
setwd(in_dir)
files <- list.files(pattern = "MDB", recursive = T)
yrs <- str_extract(files, "[:digit:]+")

MDB <- data.table()

for(i in yrs){
  
  ## get files
  f <- files[files %like% i & !files %like% "DIARY" & files %like% "DTA$"]
  
  ## get data
  d <- lapply(
    f, 
    function(x){
      tmp <- data.table(read.dta13(x))
      tmp[,year := if(as.numeric(i < 50)) as.numeric(i) + 2000 else as.numeric(i) + 1900]
      tmp
    } 
  )
  
  ## bind together
  D <- rbindlist(d, use.names = T, fill = T)
  
  ## bind to big tables
  MDB <- rbind(MDB, D, use.names = T, fill = T)
  
  ## log
  print(i)
}

## simplify data
MDB <- MDB[,.(
  newid = as.character(newid), 
  year,
  qyear, 
  medpcary,
  medpcy1,
  medpcy2,
  medpcy3,
  medpcy4,
  medpcy5,
  medpcy6,
  medpcy7,
  medpcy8,
  medpcy9,
  
  ## OOP PAYMENT
  medpmtx ## Amount of payment
)]


## fix years
MDB[as.numeric(qyear) < 1000, qyear := paste0(19, qyear)]

## convert year to remove quarter
MDB[,qyear := as.numeric(str_extract(qyear, ".{4}"))] 

# Filter for expenditures made in the given year
MDB <- MDB[qyear == year]

## sum item codes across IDS
MDB <- MDB[,.(medpmtx = sum(medpmtx, na.rm = T)), by = .(newid, year)]

## ---------------------------
## merge and calculate weighted OOP per capita
## ---------------------------
EXP <- merge(FMLI, MDB, by = c("newid", "year"))
EXP <- EXP[state != "", 
           .(OOP_pc = sum(medpmtx * finlwt21) / sum(popwt), ## per capita with survey pop
           OOP = sum(medpmtx * finlwt21)), ## per capita with normal pop
           by = .(year, state)]
EXP[,state := paste0("000", state)]
EXP <- merge(EXP, states, by = "state", all.x = T)

## function to find missing states
missed_state <- function(x){
  paste(unique(states$abbreviation[!states$abbreviation %in% x]), collapse = ",")
}

## check missing states
MSY <- EXP[,.(N_states = uniqueN(abbreviation), missed_state = missing(abbreviation)), by = year][order(year)]
MSY

## ---------------------------
## currency convert
## ---------------------------

EXP <- melt(EXP, measure.vars = c("OOP_pc", "OOP"))

EXP[, iso3 := "USA"][, year_id := year]
EXP <- currency_conversion(
  data = EXP, 
  col.loc = "iso3",
  col.value = "value",
  currency = "usd",
  col.currency.year = "year_id",
  base.year = 2020
)
EXP[,iso3 := NULL]

EXP <- dcast(EXP, state + year + abbreviation + state_name + location_id + region + division ~ variable, value.var = "value")

## ---------------------------
## Add NIDs
## ---------------------------
EXP[,nid := plyr::revalue(as.character(year), NIDs)]

## ---------------------------
## Write data
## ---------------------------
arrow::write_feather(EXP, "FILEPATH") 
