# MEDICARE FFS FORMAT FILE
# Total Medicare FFS expenditures for years 1998-2019
# This script produces total & PC spending as well as enrollment numbers for Medicare Parts A & B. Only spending on 
# aged and disabled enrollees is kept here - esrd coverage was introduced part way into the time series, and is captured differently.
library(tidyverse)
library(data.table)
source("FILEPATH/get_location_metadata.R")

# source currency conversion directly
source(paste0("FILEPATH/dex_currency_conversion.R"))


data_dir <- "FILEPATH"
input_dir <- "FILEPATH"

nids <- fread(paste0(input_dir, "mdcr_ffs_nids.csv"))

# County mapping from USHD
load("FILEPATH/states.RData")
counties <- fread("FILEPATH/merged_counties.csv")

counties <- counties[, location_id := NULL]
counties[, cnty := str_pad(cnty,width = 5, side = "left", pad = "0")]
counties[, county := str_remove(cnty_name," County| Census Area| Municipality| Borough| Parish")]

# GBD states
locs <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
locs <- locs[parent_id == 102, .(location_id,location_name,location_name_short,location_type)]

counties <- merge(counties, locs[,.(location_id, location_name)], by.x ="state_name", by.y = "location_name", all.x = TRUE)

# Spending is sometimes reported with dollar signs as characters - standardize here
make_money_numeric <- function(x){
  x <- as.character(x) %>%
    str_remove("\\$") %>%
    str_remove_all(",") %>%
    as.numeric()
  return(x)
}

data <- Sys.glob(paste0(data_dir, "/by_year/*"))
dfl <- lapply(data, function(x){
  demographic <- str_split(x,"[/_.]")[[1]][13]
  year <- str_split(x,"[/_.]")[[1]][14]
  df <- fread(x)
  spending_cols <- c("part.a.total","part.a.per.capita","part.b.total","part.b.per.capita")
  
  # Filter to columns of interest, make money numeric
  df <- df[,.(year_id = year, state, code, county, part.a.enrollment, part.b.enrollment, part.a.total, part.a.per.capita, part.b.total, part.b.per.capita)]
  df <- df[,lapply(.SD, make_money_numeric), by=c("year_id","state","code","county","part.a.enrollment","part.b.enrollment"),
           .SDcols = spending_cols]
  df <- dex_currency_conversion(df, spending_cols)
  df[, part.a.enrollment := str_remove_all(part.a.enrollment,",")][, part.b.enrollment := str_remove_all(part.b.enrollment,",")]
  df[, part.a.enrollment := as.numeric(part.a.enrollment)][, part.b.enrollment := as.numeric(part.b.enrollment)]
  df[, demo := demographic]

  return(df)
})
df <- rbindlist(dfl)

# Merge on county map from USHD
df <- Filter(function(x)!all(is.na(x)), df)
df$state <- str_to_title(df$state)
df[, state := str_replace(state, "N\\.", "North")]
df[, state := str_replace(state, "S\\.", "South")]
df[, state := str_replace(state, "W\\.", "West")]
df[, state := str_replace(state, "Dist. Of Col.|District Of Columbia", "District of Columbia")]
df[state == "Dist. Of Col.|District Of Columbia", state := "District of Columbia"]
df <- merge(states, df, by.x = "state_name", by.y = "state")

df[, year_id := as.integer(year_id)]
df <- merge(df, nids[,.(nid, year)], by.x = "year_id", by.y = "year")

df <- df[demo != "esrd"]
df[, total := part.a.total + part.b.total][, per.capita := part.a.per.capita + part.b.per.capita]

df_state <- df[,.(part.a.total = sum(part.a.total, na.rm = TRUE), part.b.total = sum(part.b.total, na.rm = TRUE), 
                  total = sum(total, na.rm = TRUE),
                  part.a.per.capita = weighted.mean(part.a.per.capita, w = part.a.enrollment, na.rm = TRUE),
                  part.b.per.capita = weighted.mean(part.b.per.capita, w = part.b.enrollment, na.rm = TRUE)),
               by=c("state_name","abbreviation","location_id","year_id")]

fwrite(df_state,paste0(data_dir,"mdcr_ffs_state.csv"))

