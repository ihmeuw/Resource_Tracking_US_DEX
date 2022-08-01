# Import CMS personal health spending data and reshape into long format
# The CMS personal health spending data is obtained from the following website:
# https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/NationalHealthExpendData/NationalHealthAccountsStateHealthAccountsResidence
# We downloaded the files for "Health expenditures by state of residence, 1991-2014"

# Setup ------------------------------------------------
rm(list = ls())
library(data.table)
library(tidyverse)
library(readxl)
library(unpivotr)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
repo_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
data_path <- "FILEPATH"

source("FILEPATH/currency_conversion.R")

gbd_round <- "ROUND"
gbd_decomp <- 'DECOMP'
locs <- fread("FILEPATH/states.csv")
locs[, state := str_pad(as.character(state), 5, side = "left", pad = "0")]

pop <- get_population(age_group_id = 22, 
                      location_id = locs$location_id, 
                      year_id = 1990:2014, 
                      sex_id = 3, 
                      gbd_round_id = gbd_round,
                      decomp_step = gbd_decomp,
                      status = 'best') 
pop[, run_id := NULL]

# Function to process excel sheets for SHEA data
# Expects a sheet that contains total spending in millions of dollars
process_shea <- function(sheet){
  df <- read_excel("FILEPATH/USA_HEALTHCARE_EXPENDITURES_BY_STATE_OF_RESIDENCE_1991_2014_ALL_TABLES_Y2020M05D22.XLSX",
                   sheet = sheet, skip = 1) %>% as.data.table()
  df[,names(df)[names(df) %like% "Growth"] := NULL]
  df <- melt(df, id.vars = "Region/state of residence", variable.name = "year_id", value.name = "tot_spending")
  df[, year_id := as.numeric(as.character(year_id))][, iso3 := "USA"][, year := year_id]
  setnames(df,"Region/state of residence","state_name")
  df <- merge(df, locs, by="state_name")
  df[, tot_spending := 1000000*tot_spending]
  
  df <- currency_conversion(data = df, col.loc = "iso3", col.value = "tot_spending",
                            currency = "usd", col.currency.year = "year", base.year = 2020)
  df[, nid := 448107][, iso3 := NULL]
  return(df)
}

# National health spending ---------------------------------------------------
nhea <- fread("FILEPATHUSA_NATIONAL_HEALTH_EXPENDITURE_ACCOUNTS_HISTORICAL_PROJECTED_1960_2028_Y2022M02D24.csv",
              skip = 6)
nhea <- nhea[CATEGORY == "Personal Health Care", .(year_id = YEAR, nhea = TOTAL*1000000)]
nhea[, year_id := as.integer(str_sub(year_id, start = 2, end = -1))]

nhea <- nhea[year_id %in% c(1990:2019)]

nhea[, year := year_id][, iso3 := "USA"]
nhea <- currency_conversion(data = nhea, col.loc = "iso3", col.value = "nhea",
                          currency = "usd", col.currency.year = "year", base.year = 2020)
nhea[, iso3 := NULL]

# filter to rows of interest
fwrite(nhea, paste0(data_path,"NHEA.csv"))

# Total health spending ------------------------------------------------------
shea <- process_shea("Table 1 Personal Health Care")
shea <- merge(shea, nhea, by="year_id", allow.cartesian = TRUE)

cms_data <- merge(shea, pop, by=c("location_id","year_id"))
cms_data[, tot_spending := (tot_spending/sum(tot_spending))*nhea, by="year_id"]
cms_data[, pc_spending := tot_spending/population]
cms_data[,`:=`(us_spending = nhea, us_spending_pc = nhea/sum(population)), by="year_id"][, nhea := NULL]
cms_data[, fr_spending := tot_spending/us_spending]
cms_data[, fpc_spending := pc_spending/us_spending_pc]

fwrite(cms_data, paste0(data_path,"SHEA.csv"))

# Payer-specific health spending ------------------------------------------------------
mdcr <- process_shea("Table 22 Medicare") %>% setnames("tot_spending","tot_spending_mdcr")
mdcd <- process_shea("Table 25 Medicaid") %>% setnames("tot_spending","tot_spending_mdcd")
priv <- process_shea("Table 28 Private Health") %>% setnames("tot_spending","tot_spending_priv")

payers <- left_join(shea, mdcr) %>% 
  left_join(mdcd) %>% 
  left_join(priv)

payers <- payers[year_id %in% c(2001:2014)]
payers[, nhea_tot_spending := (tot_spending/sum(tot_spending))*nhea, by="year_id"]
payers[, tot_spending_oop := tot_spending - tot_spending_mdcr - tot_spending_mdcd - tot_spending_priv]
cols <- str_subset(colnames(payers),"^tot_spending_")
payers[, (cols) := lapply(.SD, "*", nhea_tot_spending/tot_spending), by=c("year_id","location_id"), .SDcols = cols]
payers[, tot_spending := nhea_tot_spending][, nhea_tot_spending := NULL]

fwrite(payers, paste0(data_path,"SHEA_by_payer.csv"))

# Type of care-specific health spending -----------------------------------
hospital <- process_shea("Table 2 Hospital")[, toc := "hosp"]
physician <- process_shea("Table 3 Physician and Clinics")[, toc := "phys"]
other_prof <- process_shea("Table 4 Other Professionals")[, toc := "oprof"]
dental <- process_shea("Table 5 Dental")[, toc := "dent"]
home_health <- process_shea("Table 6 Home Health")[, toc := "hh"]
nursing <- process_shea("Table 7 Nursing")[, toc := "nf"]
pharma <- process_shea("Table 8 Drugs and Non-durables")[, toc := "pharma"]
dme <- process_shea("Table 9 Durables")[, toc := "dme"]
other <- process_shea("Table 10 Other Health")[, toc := "other"]

toc <- rbindlist(list(hospital, physician, other_prof, dental, home_health, nursing, pharma, dme, other))

toc <- merge(toc, shea[,.(location_id, nhea, shea = sum(tot_spending)), by="year_id"], by=c("year_id","location_id"))
toc <- toc[, tot_spending := tot_spending*nhea/shea]

fwrite(toc, paste0(data_path,"SHEA_by_toc.csv"))
