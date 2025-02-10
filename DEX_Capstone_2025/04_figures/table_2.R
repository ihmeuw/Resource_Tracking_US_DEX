# ---------------------
#  Manuscript Table 2 - Aggregated Cause table
#    
#   - Total Spending
#   - Growth rates
#   - Percent spending (by age group, type of care, and payer)
#
#   Author: Drew DeJarnatt
# ---------------------

## -----------------------------------
## Arguments and set up
## -----------------------------------
Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')

library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
#if it doesn't say: dex-db-load package has been succesfully loaded! it didn't work
suppressMessages(lbd.loader::load.containing.package())

pacman::p_load(data.table, tidyverse, arrow)
'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')
username <- Sys.getenv('USER')

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

# specify scaled_version 
scaled_version <- "XX"
draws <- T #T or F

# where to save final table
out_dir <- "FILEPATH"

if(!dir.exists(out_dir)){
  dir.create(out_dir, recursive = T)
}

# data paths to pull from
national_data_path <- "FILEPATH"



# Population
pop <- fread("FILEPATH")[geo == "national" & year_id %in% c(2010,2019)]

source("FILEPATH")
source("FILEPATH")

## Read in causelist 
cause_list <- fread("FILEPATH")[, .(acause, cause_name, cause_name_lvl2, cause_name_lvl1)] 
# Reset some level 2 causes to remove them from "Other non-communicable diseases"
# Also combine all injuries into one cause
cause_list[, cause_level2 := fcase(
  acause == "_oral", cause_name,
  acause == "nutrition","Other non-communicable diseases",
  cause_name_lvl1 == 'Injuries', 'Injuries',
  acause == "_ntd", "Other infectious diseases"
)]
cause_list[is.na(cause_level2), cause_level2 := cause_name_lvl2]


## ---------------------------------------------------------------------------------------------------------
## Percent of total spending
## 1. Calculate percent of total spending for each payer and toc for each cause
## 2. Calculate percent of spending in age groups <20 and >= 65
## ---------------------------------------------------------------------------------------------------------

## Read in data
# cause, age, toc, and payer specific
if(draws){
  pct_df <- open_dataset(national_data_path) %>%
    filter(payer != 'oth', year_id == 2019)%>%
    group_by(year_id, acause, age_group_years_start, toc, payer, draw) %>%
    summarize(spend = sum(spend)) %>%
    group_by(year_id, acause, age_group_years_start, toc, payer) %>%
    summarize(spend = mean(spend)) %>%
    collect() %>%
    setDT()
  
  # uncertainty range for total spending
  spend_range <- open_dataset(national_data_path) %>%
    filter(payer != 'oth', year_id == 2019)%>%
    left_join(cause_list, by = 'acause') %>%
    group_by(year_id, cause_level2, draw) %>%
    summarize(spend = sum(spend)) %>%
    collect() %>%
    group_by(cause_level2) %>%
    summarize(mean_spend = mean(spend),
              lower_spend = quantile(spend, .025),
              upper_spend = quantile(spend, .975)) %>%
    rename(spend = mean_spend) %>%
    setDT()
} else{
  pct_df <- open_dataset(national_data_path) %>%
    filter(payer != 'oth', year_id == 2019)%>%
    group_by(year_id, acause, age_group_years_start, toc, payer) %>%
    summarize(spend = sum(mean_spend)) %>%
    collect() %>%
    setDT()
}
# Merge causelist and aggregate to level 2 causes
pct_df <- merge(pct_df, cause_list[, .(acause, cause_level2)], by = 'acause', all.x = TRUE)
pct_df <- pct_df[, .(spend = sum(spend)), by = .(year_id, cause_level2, age_group_years_start, toc, payer)]


# Create row for all causes
all_causes_pct <- pct_df[, .(spend = sum(spend)), by = .(year_id, age_group_years_start, toc, payer)]
all_causes_pct[, ':='(acause = "All",
                      cause_name = "All_causes",
                      cause_name_lvl2 = "All causes",
                      cause_name_lvl1 = "All causes",
                      cause_level2 = "All causes")]

# Add all_causes to cause_data
pct_df <- rbindlist(list(pct_df, all_causes_pct), use.names = TRUE, fill = TRUE)

# Calculate proportions
pct_df_final <- pct_df[, .(Total_Spending = sum(spend)/1e9,
                           Under_20 = sum(spend[age_group_years_start < 20])/sum(spend),
                           Over_65 = sum(spend[age_group_years_start >= 65])/sum(spend),
                           Inpatient = sum(spend[toc == "IP"])/sum(spend),
                           EmergencyDepartment = sum(spend[toc == "ED"])/sum(spend),
                           Ambulatory = sum(spend[toc == "AM"])/sum(spend),
                           Pharmaceutical = sum(spend[toc == "RX"])/sum(spend),
                           Nursing_Facility = sum(spend[toc == "NF"])/sum(spend),
                           Medicare = sum(spend[payer == "mdcr"])/sum(spend),
                           Medicaid = sum(spend[payer == "mdcd"])/sum(spend),
                           Private_Insurance = sum(spend[payer == "priv"])/sum(spend),
                           Out_of_pocket = sum(spend[payer == "oop"])/sum(spend)),
                       by = cause_level2]
# Round to 3 decimal places
numeric_cols <- colnames(pct_df_final)[sapply(pct_df_final, is.numeric)]
pct_df_final[, (numeric_cols) := lapply(.SD, round, 3), .SDcols = numeric_cols]


## ---------------------------------------------------------------------------------------------------------
## Growth rates
## 1. AROC from 2010 - 2019
## 2. Age-Standardized AROC from 2010-2019
## ---------------------------------------------------------------------------------------------------------

## Read in data
# cause, age, sex specific
aroc_df_tmp <- open_dataset(national_data_path) %>%
  filter(payer != 'oth', year_id %in% c(2010, 2019))%>%
  group_by(year_id, acause, age_group_years_start, sex_id, draw) %>%
  summarize(spend = sum(spend)) %>%
  # group_by(year_id, acause, age_group_years_start, sex_id) %>%
  # summarize(spend = mean(spend)) %>%
  # filter(draw <= 5) %>%
  collect() %>%
  setDT()

# Merge causelist and aggregate to level 2 causes
aroc_df_tmp2 <- merge(aroc_df_tmp, cause_list[, .(acause, cause_level2)], by = 'acause', all.x = TRUE)


aroc_df <- aroc_df_tmp2[, .(spend = sum(spend)), by = .(year_id, cause_level2, age_group_years_start, sex_id, draw)]

# Create row for all causes
all_causes_aroc <- aroc_df[, .(spend = sum(spend)), by = .(year_id, age_group_years_start, sex_id, draw)]
all_causes_aroc[, ':='(cause_level2 = "All causes")]

# Add all_causes to cause_data
aroc_df <- rbindlist(list(aroc_df, all_causes_aroc), use.names = TRUE, fill = TRUE)

## 1. Calculate AROC
aroc_df1 <- aroc_df[, .(spend = sum(spend)), by = .(year_id, cause_level2, draw)]
# wide on year_id
aroc_df1 <- dcast(aroc_df1, cause_level2 + draw ~ year_id, value.var = 'spend')
#unadjust for inflation
# get deflator value for unadjusting for inflation
deflators <- fread("FILEPATH")
deflator <- deflators[year == 2010, annual_cpi]/deflators[year == 2019, annual_cpi]
aroc_df1[, `2010` := `2010` * deflator]
aroc_df1[, growth_rate := ((`2019`/`2010`)^(1/9) - 1)]

## 2. Calculate Age-Standardized AROC
aroc_df2 <- merge(aroc_df, pop, by = c('year_id', 'age_group_years_start', 'sex_id'))
aroc_df2 <- age_sex_standardization(aroc_df2, value_col = "spend", type = "count", by_cols = c("year_id", "cause_level2", "draw"))
# wide on year_id
aroc_df2 <- dcast(aroc_df2, cause_level2 + draw ~ year_id, value.var = 'spend_stndz')
aroc_df2[, as_growth_rate := ((`2019`/`2010`)^(1/9) - 1)]

## 3. Calculate Age-Standardized AROC of per capita spending
aroc_df3 <- merge(aroc_df, pop, by = c('year_id', 'age_group_years_start', 'sex_id'))
aroc_df3[, spend_pc := spend/pop]
setnames(aroc_df3, 'cause_level2', 'acause') #renaming here so the age standardization function will loop over cause
aroc_df3 <- age_sex_standardization(aroc_df3, value_col = "spend_pc", type = "rate", by_cols = c("year_id", "acause", "draw"))
setnames(aroc_df3, 'acause', 'cause_level2')
aroc_df3 <- dcast(aroc_df3, cause_level2 + draw ~ year_id, value.var = 'spend_pc_stndz')
aroc_df3[, as_per_cap_growth_rate := ((`2019`/`2010`)^(1/9) - 1)]

# Merge aroc_df1 and aroc_df2
aroc_df_final <- merge(aroc_df1, aroc_df3, by = c('cause_level2', 'draw'))[, .(cause_level2, growth_rate, as_per_cap_growth_rate, draw)]
aroc_df_final <- merge(aroc_df_final, aroc_df2, by = c('cause_level2', 'draw'))[, .(cause_level2, growth_rate, as_per_cap_growth_rate, as_growth_rate, draw)]

# aggregate over draws and keep uncertainty range
aroc_df_final <- aroc_df_final[, .(growth_rate = paste0(round(mean(growth_rate)*100, 1), "% (",
                                                        round(quantile(growth_rate, 0.025)*100, 1), " - ",
                                                        round(quantile(growth_rate, 0.975)*100, 1), ")"),
                                   as_growth_rate = paste0(round(mean(as_growth_rate)*100, 1), "% (",
                                                           round(quantile(as_growth_rate, 0.025)*100, 1), " - ",
                                                           round(quantile(as_growth_rate, 0.975)*100, 1), ")"),
                                   as_per_cap_growth_rate = paste0(round(mean(as_per_cap_growth_rate)*100, 1), "% (",
                                                                   round(quantile(as_per_cap_growth_rate, 0.025)*100, 1), " - ",
                                                                   round(quantile(as_per_cap_growth_rate, 0.975)*100, 1), ")")),
                               by = .(cause_level2)]


## ---------------------------------------------------------------------------------------------------------
## Combine and clean
## ---------------------------------------------------------------------------------------------------------

table2 <- merge(pct_df_final, aroc_df_final, by = 'cause_level2')
table2[, Spending := Total_Spending]
# adding uncertainty to total spending if using draw level data
if(draws){
  table2 <- merge(table2, spend_range, by = "cause_level2", all.x = TRUE)
  table2[, Spending := ifelse(cause_level2 == "All causes", paste0("$", round(Total_Spending, 2)), paste0("$", round(Total_Spending, 2), " (", round(lower_spend/1e9, 1), "-", round(upper_spend/1e9, 1), ")"))]
}
# descend by total spending 
table2 <- table2[order(-Total_Spending)]
table2 <- table2[, .("Aggregated Health Condition" = cause_level2,
                     "Total Spending (billions, 2019 dollars)" = Spending,
                     "Unadjusted annual growth rate; 2010-2019" = growth_rate,#not adjusted for inflation
                     "Inflation adjusted, age/sex-standardized total spending annual growth rate; 2010-2019" = as_growth_rate,
                     "Inflation adjusted, age/sex-standardized spending per capita annual growth rate; 2010-2019" = as_per_cap_growth_rate,
                     "Under 20" = Under_20,
                     "Over 65" = Over_65,
                     "Inpatient" = Inpatient,
                     "Emergency Department" = EmergencyDepartment,
                     "Ambulatory" = Ambulatory,
                     "Pharmaceutical" = Pharmaceutical,
                     "Nursing Facility" = Nursing_Facility,
                     "Medicare" = Medicare,
                     "Medicaid" = Medicaid,
                     "Private Insurance" = Private_Insurance,
                     "Out-of-pocket" = Out_of_pocket)]

## ---------------------------------------------------------------------------------------------------------
## Write out as csv - format in excel
## ---------------------------------------------------------------------------------------------------------
fwrite(table2, file = paste0(out_dir, "Table_2.csv"))
