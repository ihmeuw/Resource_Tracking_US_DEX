## -------------------
## Clean up MDCD expansion data
## USERNAME
## Sep 29, 2021
## -------------------

rm(list = ls())

## Setup
pacman::p_load(data.table, tidyverse, openxlsx)

## get medicaid expansion data
mdcd_ex <- data.table(read.xlsx("FILEPATH/KAISER_FAMILY_FOUNDATION_MEDICAID_ADOPTION_STATUS/USA_STATUS_OF_STATE_ACTION_ON_MEDICAID_EXPANSION_2021_SEPT_08_MDCD_ADOPTION_STATUS_Y2021M09D03.XLSX"))

## fix years
mdcd_ex[,`:=`(
  date_expanded_mdcd = convertToDate(date_adopted),
  date_adopted = NULL
)]
mdcd_ex[,year_expanded_mdcd := year(date_expanded_mdcd)]

## simplify
mdcd_ex <- mdcd_ex[,.(state_name, date_expanded_mdcd, year_expanded_mdcd)]

## create variables
mdcd_ex[is.na(year_expanded_mdcd), c("expanded_mdcd_b4_16", "expanded_mdcd_b4_20") := FALSE]
mdcd_ex[year_expanded_mdcd < 2016, c("expanded_mdcd_b4_16", "expanded_mdcd_b4_20") := TRUE]
mdcd_ex[year_expanded_mdcd < 2020, expanded_mdcd_b4_20 := TRUE]
mdcd_ex[is.na(expanded_mdcd_b4_16), expanded_mdcd_b4_16 := FALSE]
mdcd_ex[is.na(expanded_mdcd_b4_20), expanded_mdcd_b4_20 := FALSE]

## add NID
mdcd_ex[,NID := 488378]

## merge in state data
load("FILEPATH/states.RData")
mdcd_ex <- merge(mdcd_ex, states, by = "state_name", all = T)

## write
fwrite(mdcd_ex, "FILEPATH/mdcd_expansion_status.csv")
