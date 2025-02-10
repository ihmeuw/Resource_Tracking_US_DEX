# ---------------------
#  Manuscript Table 1 - Type of Care and Payer 
#    
#   Column 1: total spending in 2019
#   Column 2: Pct of total healthcare spending represented by toc/payer
#   Column 3: Growth rate of inflation UNADJUSTED total spending from 2010 to 2019
#   Column 4: Growth rate of inflation ADJUSTED total spending from 2010 to 2019
#   Column 5: Age-standardized growth rate of inflation ADJUSTED total spending from 2010 to 2019
#   Column 6: Payer only - spend per beneficiary
#
#   Author: Drew DeJarnatt
# ---------------------


## -----------------------------------
## Arguments and set up
## -----------------------------------
# specify scaled_version 
scaled_version <- "XX"

out_dir <- 'FILEPATH'

library(data.table)
library(tidyverse)
library(gridExtra)
library(scales)
library(arrow)

## -----------------------------------
## Reading in data
## -----------------------------------
# reading in toc, payer, age, sex, and year (2010 and 2019) estimates
# aggregate draws if using draw level data
national_data_path <- 'FILEPATH'
  
table1_df <- open_dataset(national_data_path) %>% 
  filter(year_id %in% c(2010, 2019), payer != "oth") %>%
  group_by(year_id, toc, payer, age_group_years_start, sex_id, draw) %>%
  summarize(spend = sum(spend)) %>%
  # filter(draw <= 10) %>%
  group_by(year_id, toc, payer, age_group_years_start, sex_id) %>%
  summarize(spend = mean(spend)) %>%
  collect()

# Population
pop <- fread("FILEPATH")[geo == "national" & year_id %in% c(2010,2019)]
national_pop <- pop[, .(pop = sum(pop)), by = c("year_id")]
pop_2019 <- national_pop[year_id == 2019, pop]
source("FILEPATH")

# Beneficiaries
benes <- open_dataset("FILEPATH") %>% 
  filter(year_id %in% c(2010, 2019), toc == "all", pri_payer %in% c("mdcr", "mdcd", "priv", "oop")) %>% 
  mutate(denom = ifelse(pri_payer == "oop", pop, denom)) %>% 
  select(payer = pri_payer, year_id, denom, age_group_years_start, sex_id) %>%
  group_by(year_id, payer, age_group_years_start, sex_id) %>% 
  summarize(denom = sum(denom)) %>%
  collect() %>%
  setDT()


## -----------------------------------
## Making table
## -----------------------------------

# pull total spending from 2019 to use for % of total
total_spend_19 <- table1_df %>%
  group_by(year_id) %>%
  summarize(total_spend = sum(spend)) %>%
  filter(year_id == 2019) %>%
  pull(total_spend)

# get deflator value for unadjusting for inflation
deflators <- fread("FILEPATH")
deflator <- deflators[year == 2010, annual_cpi]/deflators[year == 2019, annual_cpi]

# Function to aggregate across payer or toc and calculate total spending, % of total, and growth rates
gen_table_1 <- function(df, group){
  setDT(df)
  
  # year, toc/payer, age, and sex specific spending
  table1_group <- df %>%
    group_by(year_id, get(group), age_group_years_start, sex_id) %>%
    summarize(spend = sum(spend)) %>%
    rename(group = 'get(group)')
  setDT(table1_group)
  
  # join population for toc and beneficiaries for payer
  # spend_per_denom = spend per capita for toc and spend per beneficiary for payer
  if(group == "toc"){
    table1_group <- left_join(table1_group,
                              pop %>% select(year_id, age_group_years_start, sex_id, pop),
                              by = c("year_id", "sex_id", "age_group_years_start")) 
    table1_group[, spend_per_denom := spend/pop]
    setnames(table1_group, "pop", "denom")
  } else if(group == "payer"){
    table1_group <- left_join(table1_group,
                              benes %>% select(group = payer, year_id, age_group_years_start, sex_id, denom),
                              by = c("group", "year_id", "sex_id", "age_group_years_start")) 
    table1_group[, spend_per_denom := spend/denom]
  }
  
  ## Not age-standardized
  # aggregate over age and sex then make spending wide on year
  spend <- table1_group %>%
    group_by(year_id, group) %>%
    summarize(spend = sum(spend)) %>%
    mutate(year_id = paste0("spend_", year_id))
  spend <- dcast(spend, formula = group ~ year_id, value.var = "spend")
  
  # spend per denominator wide on year
  spend_pd <- table1_group %>%
    group_by(year_id, group) %>%
    summarize(spend = sum(spend), denom = sum(denom)) %>%
    mutate(spend_per_denom = spend/denom) %>%
    mutate(year_id = paste0("spend_per_denom_", year_id))
  spend_pd <- dcast(spend_pd, formula = group ~ year_id, value.var = "spend_per_denom")
  
  ## Age-sex standardized
  if(group == "toc"){
    stnd_spend_pd <- age_sex_standardization(table1_group,
                                             value_col = "spend_per_denom",
                                             type = "rate",
                                             by_cols = c("year_id", "group"))
  } else if(group == "payer") {
    stnd_spend_pd <- data.table()
    for(p in c("mdcr", "mdcd", "priv","oop")){
      p_df <- age_sex_standardization(table1_group[group == p],
                                      value_col = "spend_per_denom",
                                      type = "rate",
                                      by_cols = c("year_id", "group"),
                                      payer_spec = p)
      stnd_spend_pd <- rbind(stnd_spend_pd, p_df)
    }
  }
  
  stnd_spend_pd_group <- stnd_spend_pd %>%
    group_by(year_id, group) %>%
    summarize(stnd_spend_per_denom = sum(spend_per_denom_stndz)) %>%
    mutate(year_id = paste0("stnd_spend_per_denom_", year_id))
  
  stnd_spend_pd_group <- dcast(stnd_spend_pd_group, formula = group ~ year_id, value.var = "stnd_spend_per_denom")
  
  
  table1_tmp <- left_join(spend, spend_pd, by = c("group")) %>%
    left_join(stnd_spend_pd_group, by = c("group")) %>%
    mutate(unadj_spend_2010 = spend_2010 * deflator)
  
  table1_prep <- table1_tmp %>%
    mutate(unadj_growth_rate = ((spend_2019 /unadj_spend_2010)^(1/9) - 1), # Column 3 - Growth Rate
           growth_rate = ((spend_2019 /spend_2010)^(1/9) - 1), # Column 4 - Growth Rate (inflation adjusted)
           per_denom_growth_rate = ((spend_per_denom_2019 /spend_per_denom_2010)^(1/9) - 1), # Column 5 - Growth Rate of spend per beneficiary (payer) or per capita (TOC)
           stnd_per_denom_growth_rate = ((stnd_spend_per_denom_2019 /stnd_spend_per_denom_2010)^(1/9) - 1)) # Column 6 - Growth Rate of age-standardized spend per beneficiary (payer) or per capita (TOC)
  
  return(table1_prep)
}

# Make toc and payer tabled
toc_df <- gen_table_1(table1_df, group = "toc") %>% arrange(desc(spend_2019))
payer_df <- gen_table_1(table1_df, group = "payer") %>% arrange(desc(spend_2019))

# Use full payer name in plot titles and labels
payer_list <- list("mdcr" = "Medicare", 
                   "mdcd" = "Medicaid", 
                   "priv" = "Private Insurance", 
                   "oop" = "Out-of-Pocket")
# long names for types of care
toc_labels = c(
  "ED" = "Emergency Department",
  "AM" = "Ambulatory",
  "IP" = "Inpatient",
  "HH" = "Home Health",
  "NF" = "Nursing Facility",
  "DV" = "Dental",
  "RX" = "Pharmaceutical"
)
group_labels <- unlist(c(payer_list, toc_labels))

# join data and clean labels/names
table1 <- rbindlist(list(toc_df, payer_df), fill = TRUE) %>%
  mutate(frac_spend = paste0(round(spend_2019/total_spend_19 *100,1), "%"),
         spend_2019 = round(spend_2019/1e9,2),
         group = recode(group, !!!group_labels)) %>%
  select(group,
         "2019 spending (billions)" = spend_2019,
         "Fraction of Spending" = frac_spend,
         "2010-2019 unadjusted annual growth rate" = unadj_growth_rate,
         "2010-2019 inflation adjusted annual growth rate" = growth_rate,
         "2010-2019 inflation adjusted per capita or per beneficiary annual growth rate" = per_denom_growth_rate,
         "2010-2019 inflation adjusted, age/sex standardized per capita or per beneficiary annual growth rate" = stnd_per_denom_growth_rate)

## ---------------------------------------------------------------------------------------------------------
## Write out as csv - format in excel
## ---------------------------------------------------------------------------------------------------------
fwrite(table1, paste0(out_dir, "Table_1.csv"))
