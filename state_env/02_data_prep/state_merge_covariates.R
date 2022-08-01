# Generate Starting DF for state-level aggregated spending
# USERNAME
# Jan 27, 2021

# SETUP ------------------------------------------------------
rm(list=ls())

library(tidyverse)
library(data.table)
library(writexl)
library(DataCombine)
library(readxl)

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

data_path <- "FILEPATH"
policy_path <- paste0(data_path,"policy_vars/")
repo_path <- "FILEPATH"

load("FILEPATH/states.RData")
state_vec <- states$location_id; names(state_vec) <- states$state_name
state_vec2 <- states$state_name; names(state_vec2) <- as.character(states$location_id)

source("FILEPATH/get_ids.R")
source('FILEPATH/get_age_metadata.R')
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_age_weights.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/currency_conversion.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_age_weights.R")

# Set up the metadata
gbd_round <- "ROUND"
gbd_decomp <- 'DECOMP'

locs <- get_location_metadata(35, gbd_round_id = gbd_round, decomp_step = gbd_decomp)
locs <- locs[parent_id == 102 | location_id == 385, .(location_id,location_name,location_name_short,location_type)]

postal_codes <- fread(paste0(data_path,"postal_code.csv"))
locs <- merge(locs, postal_codes, by="location_name")

get_cov <- function(covariate_id){
  dt <- get_covariate_estimates(covariate_id = covariate_id,
                          age_group_id = 22,
                          location_id = locs$location_id,
                          year_id =  1980:2019,
                          sex_id = 3,
                          gbd_round_id = gbd_round,
                          decomp_step = gbd_decomp,
                          status = 'best')
  return(dt)
}

read_kff <- function(fpath){
  df <- suppressWarnings(suppressMessages(read_csv(fpath, skip=2)))
  df <- as.data.table(df)
  df[, year_id := as.numeric(str_extract(fpath,"\\d{4}"))]
  df <- merge(df, locs[,.(location_id, location_name)], by.x="Location", by.y="location_name")
  df[, Location := NULL]
  return(df)
}

# Population size
ages_df <- get_age_metadata(age_group_set_id = 12, gbd_round_id = 6) %>%
  rbind(data.table(age_group_id = 28, age_group_years_start = 0, age_group_years_end = 1), fill = TRUE)
ages_df <- ages_df[!(age_group_id %in% 2:4)]
ages <- c(ages_df$age_group_id,22,18,19,20,30,31,32,33,48,154) %>% unique()
population <- get_population(age_group_id = ages, 
                      location_id = locs$location_id, 
                      year_id = 1990:2019, 
                      sex_id = 1:3, 
                      gbd_round_id = gbd_round,
                      decomp_step = gbd_decomp,
                      status = 'best') 

pop <- population[age_group_id == 22 & sex_id == 3,.(location_id, year_id, population)]
pop <- merge(pop, locs, by="location_id")

sex_pop <- population[age_group_id == 22,.(location_id, year_id, sex_id, population)]
sex_pop <- dcast(sex_pop, location_id + year_id ~ sex_id, value.var = "population")
pop_female <- sex_pop[,.(location_id, year_id, pop_female=`2`/`3`)]

# DEMOGRAPHIC STRUCTURE ------------------------------------------------------
age_bins <- data.table(age_min = c(0,15,35,55,65), age_max = c(15,35,55,65,125))
age_bins[, age := paste0("age",age_min,"_",age_max)]
population <- merge(population, ages_df, by="age_group_id")
population <- population[age_bins, on = .(age_group_years_start >= age_min, age_group_years_end <= age_max)]
population <- population[,.(population = sum(population, na.rm = TRUE)), by=c("age","location_id","year_id","sex_id")]

pop65 <- population[age %in% c("age65_75","age75_85","age85_125"),
                    .(pop65 = sum(population)), by=c("location_id","year_id")]

setnames(population, "population","agepop")
population <- merge(population, pop, by=c("location_id","year_id"))
population[, population := agepop/population]
population <- dcast(population[sex_id == 3], location_id + year_id ~ age, value.var = "population")

# Prices ------------------------------------------------------
cpi <- data.table(
  openxlsx::read.xlsx(
    paste0(policy_path, "general price changes/Average Annual CPI - US -All items.xlsx"),
    startRow = 12
  )
)
cpi <- cpi[Year <= 2019, .(year_id = as.integer(as.character(Year)), cpi = Annual)]

rpp <- fread(paste0(policy_path, "general price changes/BEA Regional Price Parity by State - All Goods - 2008-2019.csv"))
rpp <- melt(rpp, id.vars = c("GeoFips", "GeoName"), variable.name = "year_id", value.name = "rpp")
rpp <- rpp[GeoName != "United States", .(state_name = GeoName, year_id = as.integer(as.character(year_id)), rpp)]
rpp[,location_id := as.integer(plyr::revalue(state_name, state_vec))]
rpp[,rpp := as.numeric(rpp)/100]

## fill pre-2008 with 2009
rpp_fill <- rpp[year_id == 2008, -"year_id"]
for(i in 1991:2007){
  tmp <- copy(rpp_fill)
  tmp[,year_id := i]
  rpp <- rbind(rpp, tmp, use.names = T, fill = T)
}
rpp <- rpp[order(year_id, state_name)]

## calculate with CPI
rpp <- merge(cpi, rpp, by = c("year_id"), all.y = T)
rpp[,rpp_cpi := rpp*cpi]


# DENSITY -------------------------------------------------------
# Proportion of the state with pop density < 150 ppl / sq.km
density_150 <- get_cov(covariate_id = 119) %>%
  mutate(density_l.150 = mean_value) %>%
  select(location_id, year_id, density_l.150)

# Proportion of the state with pop density > 1000 ppl / sq.km
density_1000 <- get_cov(covariate_id = 118) %>%
  mutate(density_g.1000 = mean_value) %>%
  select(location_id, year_id, density_g.1000)

# Income & education ------------------------------------------------------
# LDI: Lag distributed income per capita (I$): gross domestic product per capita that has been smoothed over the preceding 10 years
income  <- get_cov(covariate_id=57) %>%
  mutate(ldi_pc= mean_value) %>%
  select(location_id, year_id, ldi_pc)
income <- merge(income, rpp, by=c("location_id","year_id"))
income[, ldi_pc := ldi_pc/rpp][, rpp := NULL][, cpi := NULL][, rpp_cpi := NULL]

# EDUC: Avg years per capita of education, aggregated by age (15+) and sex
edu_yrs <- get_cov(covariate_id=1975) %>%
  mutate(edu_yrs = mean_value) %>%
  select(location_id, year_id, edu_yrs)

# SDI = Socio-demographic index 
sdi <- get_cov(covariate_id = 881) %>%
  mutate(sdi = mean_value) %>%
  select(location_id, year_id, sdi)

inc <- data.table(read.xlsx("FILEPATH/census_median_HH_income_by_state.xlsx",startRow = 63)) ## row 63 is 2020 USD

## reshape and recode
inc <- data.table::melt(inc, id.vars = "State", variable.name = "year_id", value.name = "med_income")
inc[,location_id := plyr::revalue(State, state_vec)]
med_income <- inc[
  i = location_id != "United States", 
  j = .(
    location_id = as.integer(location_id), 
    year_id = as.integer(as.character(year_id)), 
    med_income = as.numeric(med_income)
  )
]


# HAQi ------------------------------------------------------
haqi <- get_cov(covariate_id = 1099) %>%
  mutate(haqi = mean_value) %>%
  select(location_id, year_id, haqi)

# Cigs ------------------------------------------------------
# Cigarette consumption per capita 

# Number of cigarettes or cigarette equivalents consumed per adult aged 15+ per year
cig_pc <- get_cov(covariate_id = 14) %>%
  mutate(cig_pc = mean_value) %>%
  select(location_id, year_id, cig_pc)

# Cigarette consumption per capita - LAGGED by 5, 10, 15 yrs
cig_pc_lag <- setDF(copy(cig_pc)) %>%
  arrange(location_id, year_id) %>%
  slide(Var="cig_pc",GroupVar="location_id",slideBy=-5) %>%
  slide(Var="cig_pc",GroupVar="location_id",slideBy=-10) %>%
  slide(Var="cig_pc",GroupVar="location_id",slideBy=-15) %>%
  mutate(cig_pc_5=`cig_pc-5`,
         cig_pc_10=`cig_pc-10`,
         cig_pc_15=`cig_pc-15`) %>%
  select(location_id, year_id, cig_pc_5, cig_pc_10, cig_pc_15)

# Access & capacity ------------------------------------------------------
# Hospital beds per 1000 from KFF
beds1000 <- rbindlist(
  lapply(Sys.glob(paste0(policy_path,"num_hosp_beds/*csv")), function(f){
    dt <- read_kff(f)
    dt <- dt[, .(location_id, year_id, beds1000 = Total)]
    return(dt)
  })
)


## replacing the above with the below
hw <- fread("FILEPATH/healthcare_workers_pc_var.csv")
setnames(hw, c("phys_per_cap", "pharm_per_cap", "hwork_per_cap"), c("phys_pc", "pharm_pc", "hw_pc"))
hw[,oth_hw_pc := hw_pc - phys_pc - pharm_pc]

# Market concentration ------------------------------------------------------

# (HHI for hospitals)
# HCUP data available for 40 states, 5 years
hhi_hosp <- lapply(Sys.glob(paste0(policy_path,"market_concentration/hcup/HMS_*.csv")), function(f){
  dt <- fread(f)
  dt[, year_id := as.numeric(str_extract(f,"\\d{4}"))]
  dt[,radius_90pct_hhi := as.numeric(radius_90pct_hhi)]
  dt <- merge(dt, locs, by="state")
  dt <- dt[, .(hhi_90pct = base::mean(radius_90pct_hhi, na.rm = T)), by=c("location_id","year_id")]
  return(dt)
})
hhi_hosp <- rbindlist(hhi_hosp)

# Payer market concentration
# Payer HHI
market_conc <- lapply(Sys.glob(paste0(policy_path,"market_concentration/*.csv")), function(f){
  df <- read_kff(f)
  df[, m_size := paste0("hhi_",str_extract(f,"small|large|individ"))]
  setnames(df,"Herfindahl-Hirschman Index (HHI)","hhi")
  return(df)
})
market_conc <- rbindlist(market_conc)
market_conc <- dcast(market_conc, location_id + year_id ~ m_size, value.var = "hhi")

# Insurance ------------------------------------------------------
insurance <- lapply(Sys.glob(paste0(policy_path,"insurance_coverage/insurance_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, insured = 1-Uninsured)]
  return(df)
})
insurance <- rbindlist(insurance)

# Insurance coverage - disaggregated
insurance_disagg <- lapply(Sys.glob(paste0(policy_path,"insurance_coverage_disagg/insurance_*.csv")), function(f){
  df <- read_kff(f)
  df[,`:=`(ins_private = Employer + `Non-Group`, ins_public = Medicaid + Medicare + Military)]
  df <- df[, lapply(.SD,"/",Total), by=c("location_id","year_id"), 
     .SDcols = c("Employer","Non-Group","Medicaid","Medicare","Military","ins_private","ins_public")]
  setnames(df,c("Employer","Non-Group","Medicaid","Medicare","Military"),
           c("ins_employer","ins_non_group","ins_medicaid","ins_medicare","ins_military"))
  return(df)
})
insurance_disagg <- rbindlist(insurance_disagg)

# premiums and deductibles from commonwealth fund
premiums <- data.table(read.xlsx("FILEPATH/Premiums_Trend_Data_2010to2020.xlsx"))
premiums[, location_id := plyr::revalue(State, state_vec)]
# reshape and recode
premiums<- dcast(premiums, location_id + Year ~ Measure, value.var = "Avg.Cost.WTd.for.Houshold.Type.Distribution")

premiums <- premiums[
  i = location_id != "United States", 
  j = .(
    location_id = as.integer(location_id), 
    year_id = as.integer(as.character(Year)), 
    total_premiums = as.numeric(`Total Premium Cost`),
    total_deductibles = as.numeric(`Deductible`)
  )
]

# Utilization ------------------------------------------------------

# Care-seeking 
care_seeking <- lapply(Sys.glob(paste0(policy_path,"care_seeking/careseeking_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, pct_not_see_doctor_bc_cost = as.numeric(`All Adults`))]
  return(df)
})
care_seeking <- rbindlist(care_seeking)

# Hospital admissions per 1000 
hospital_admissions <- lapply(Sys.glob(paste0(policy_path,"care_seeking/hospital_admissions/ad_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, admissions1000=as.numeric(Total))]
  return(df)
})
hospital_admissions <- rbindlist(hospital_admissions)

# Hospital inpatient days per 1000 
inpatient_days <- lapply(Sys.glob(paste0(policy_path,"care_seeking/hospital_inpatient/inp_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, inpt_days1000=as.numeric(Total))]
  return(df)
})
inpatient_days <- rbindlist(inpatient_days)

# Outpatient days per 1000 
outpatient_days <- lapply(Sys.glob(paste0(policy_path,"care_seeking/hospital_outpatient/outp_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, outpt_days1000=as.numeric(Total))]
  return(df)
})
outpatient_days <- rbindlist(outpatient_days)

# Medicare ------------------------------------------------------
mdcr_util <- lapply(Sys.glob(paste0(policy_path,"medicare/mdcr_utilization/util_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, mdcr_util=`Acute Stays per 1,000 Enrollees`/1000)]
  return(df)
})
mdcr_util <- rbindlist(mdcr_util)

# Update May 10, 2020: Medicare Advantage Enrollment as percentage of Population > 65 years old
## Medicare part c enrollment
mdcr_c_enrollment <- fread("FILEPATH/kff_ma_enrollment.csv")
mdcr_c_enrollment[, state_name := NULL]

# MEDICARE PART D 
## Prescription Drug Plan Enrollment (pdp_pct)
mdcr_d_pct <- lapply(Sys.glob(paste0(policy_path,"medicare/pdp_enrollment/pdp_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, mdcr_d_pct=Overall)]
  return(df)
})
mdcr_d_pct <- rbindlist(mdcr_d_pct)

# Medicare premiums (premium_adj)
mdcr_prem <- lapply(Sys.glob(paste0(policy_path,"medicare/avg_premiums/premium_*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, premium=as.numeric(str_remove(Overall,"\\$")))]
  return(df)
})
mdcr_prem <- rbindlist(mdcr_prem)
setnames(mdcr_prem, "premium", "mdcr_prem")

## Medicare eligibility distribution
mdcr_elig <- lapply(Sys.glob(paste0(policy_path,"medicare/eligibility_distribution/*.csv")), function(f){
  df <- read_kff(f)
  df <- merge(df, pop[,.(location_id, year_id, P = population)], by = c("location_id", "year_id"))
  df <- df[,.(
    location_id, 
    year_id, 
    mdcr_aged_pct_pop = Aged/P,
    mdcr_disabled_pct_pop = Disabled/P
  )]
  return(df)
})
mdcr_elig <- rbindlist(mdcr_elig)

## ^ extend time series
## read SSI
ssi <- rbindlist(
  lapply(
    Sys.glob(paste0(policy_path, "ssi/*")),
    function(f){
      # f <- Sys.glob(paste0(policy_path, "ssi/*"))[5]
      df <- data.table(openxlsx::read.xlsx(f))
      colnames(df) <- as.character(df[2,])
      if(f %like% "2007"){
        df <- df[4:54,c(1,3:5)]
      }else{
        df <- df[4:54,c(1,4:6)]
      }
      colnames(df) <- c("State", "Total", "Aged", "Blind and disabled")
      df <- df[,.(
        year_id = as.numeric(str_extract(f, "[:digit:]{4}")),
        location_id = as.integer(plyr::revalue(State, state_vec)),
        ssi_disabled = as.numeric(`Blind and disabled`), 
        ssi_aged = as.numeric(Aged)
      )]
      return(df)
    }
  )
)

## get SSI annual rate of change
ssi <- ssi[order(year_id, location_id)]
ssi <- split(ssi, ssi$location_id)
ssi <- lapply(
  ssi,
  function(x){
    x[,d_lag := data.table::shift(ssi_disabled, type = "lag")]
    x[,a_lag := data.table::shift(ssi_aged, type = "lag")]
    x[,d_roc := d_lag/ssi_disabled]
    x[,a_roc := a_lag/ssi_aged]
  }
)
ssi <- rbindlist(ssi)
ssi <- ssi[,.(year_id, location_id, d_roc, a_roc)]

## backcast
yrs <- rev(unique(ssi$year_id[!ssi$year_id %in% mdcr_elig$year_id]))
for(i in yrs){
  tmp <- mdcr_elig[year_id == i + 1]
  tmp <- merge(tmp, ssi, by = c('location_id', 'year_id'))
  tmp[,`:=`(
    year_id = i,
    mdcr_disabled_pct_pop = mdcr_disabled_pct_pop*d_roc,
    mdcr_aged_pct_pop = mdcr_aged_pct_pop*a_roc,
    d_roc = NULL, a_roc = NULL
  )]
  mdcr_elig <- rbind(tmp, mdcr_elig, use.names = T, fill = T)
}

mdcr_elig[,`:=`(location_id = as.integer(location_id), year_id = as.integer(year_id))]

# Medicaid ------------------------------------------------------
# Medicaid expansion
mdcd_ex <- fread("FILEPATH/mdcd_expansion_status.csv")
mdcd_ex <- mdcd_ex[!is.na(year_expanded_mdcd), .(location_id, year_expanded_mdcd)]

## make time-specific dummy
mdcd_ex_sq <- data.table(crossing(
  year_id = 1990:2019, 
  location_id = locs$location_id
))
mdcd_ex <- merge(mdcd_ex_sq, mdcd_ex, by = "location_id", all.x = T)
missing <- mdcd_ex[is.na(year_expanded_mdcd), unique(location_id)]
mdcd_ex[year_id >= year_expanded_mdcd, mdcd_expanded := 1]
mdcd_ex[is.na(mdcd_expanded), mdcd_expanded := 0]
mdcd_ex <- mdcd_ex[,.(location_id, year_id, mdcd_expanded)]

# MEDICAID INCOME ELIGIBILITY,, % of federal poverty line (ma_elig)
# Since dates are given for policy changes, need to allocate to the correct year
# If policy change occurred July or before, then include for that year, if August or later include for the next year
# Manuallly add any missing years
read_max <- function(f){
  df <- read_kff(f)[, year_id := NULL]
  df <- melt(df, id.vars = "location_id", value.name = "max_elig")
  df <- df[!is.na(max_elig)]
  df <- separate(df, variable, into=c("month","year_id"))
  df[, year_id := as.numeric(year_id)][month %in% c("August","September","October", "November", "December"), year_id := year_id+1]
  df <- df[,.(location_id, year_id, max_elig = as.numeric(max_elig))][!is.na(max_elig)]
  return(df)
}
max_elig_c <- read_max(paste0(policy_path,"medicaid/income_eligibility_children/elig_00-20.csv"))
setnames(max_elig_c,"max_elig","max_elig_c")

max_elig_p <- read_max(paste0(policy_path,"medicaid/income_eligibility_parents/elig_02-20.csv"))
setnames(max_elig_p,"max_elig","max_elig_p")

max_elig_m <- read_max(paste0(policy_path,"medicaid/income_eligibility_pregnant/elig_03-20.csv"))
setnames(max_elig_m,"max_elig","max_elig_m")

# Medicaid rates increases (any, inpatient, outpatient, physician)
max_rates <- lapply(Sys.glob(paste0(policy_path,"medicaid/rate_increase/inc_*.csv")), function(f){
  df <- read_kff(f)
  setnames(df,c("Any Provider Rate Increases","Provider Rate Increases__Inpatient Hospital Rate Increases",
                "Provider Rate Increases__Outpatient Hospital Rate Increases","Provider Rate Increases__Physician Rate Increases"),
           c("max_rate_any","max_rate_inp","max_rate_out","max_rate_phys"))
  df <- df[,lapply(.SD, as.factor), by=c("location_id","year_id"), 
     .SDcols=c("max_rate_any","max_rate_inp","max_rate_out","max_rate_phys")]
  df$year_id <- as.numeric(df$year_id)
  return(df)
})
max_rates <- rbindlist(max_rates)

## MEDICAID MCOs
mdcd_mco <- lapply(Sys.glob(paste0(policy_path,"medicaid/MCO/*.csv")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, pct_mdcd_mco= as.numeric(`Percent of State Medicaid Enrollment`))]
  return(df)
})

mdcd_mco <- rbindlist(mdcd_mco)
## set NAs to zero (only for the years with pct data because  NAs mean no MCOs for that state)
mdcd_mco[, all_na := sum(!is.na(pct_mdcd_mco)), by = "year_id"]
mdcd_mco[is.na(pct_mdcd_mco) & all_na > 1 , pct_mdcd_mco := 0][, all_na := NULL] 

# Health Systems ------------------------------------------------------

# Number of hospitals per capita
hosp <- lapply(Sys.glob(paste0(policy_path,"/number_of_hospitals_by_state/n_hosp*")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, hospitals = `Total Hospitals`)]
  return(df)
})
hosp <- rbindlist(hosp)
hosp <- merge(hosp, pop, by=c("location_id","year_id"))
hosp <- hosp[,.(location_id, year_id, hosp_rate = 100000*hospitals/population)]

# Percent of firms offering health insurance
firms <- lapply(Sys.glob(paste0(policy_path,"/percent_of_firms_that_offer_health_insurance_to_employees/pct_*")), function(f){
  df <- read_kff(f)
  df <- df[,.(location_id, year_id, pct_firms = `Percent of Firms Offering Coverage`)]
  return(df)
})
firms <- rbindlist(firms)

# Health Status: obesity and diabetes -------------------------------------------------------


# get DALYs due to diabetes
age_outputs <- get_outputs("cause", 
                           cause_id=c(587, 974, 976), 
                           metric_id=3, #rate
                           measure_id=5, # Prevalence
                           gbd_round_id=6, # GBD 2019
                           decomp_step = "step5", # final GBD 2019 estimates
                           location_id = locs$location_id, 
                           year_id=seq(1990, 2019, 1),
                           sex_id = 3, #both
                           age_group_id = 27) # age standardized group

# subset and reshape
age_outputs <- age_outputs[!is.na(val),.(location_id, year_id, acause, sex_id, age_group_id, val)]
age_outputs[, acause := paste0(gsub("^_","", acause), "_prevr")]
prev_diabetes <- dcast(age_outputs, location_id + year_id ~ acause, value.var = "val")

# Age standardized set up
age_list <- get_age_weights(gbd_round_id = 7) # get list of age groups
as_weights <- get_population(age_group_id = age_list$age_group_id, 
                             location_id = 102, 
                             year_id = 2019, 
                             sex_id = 1:2, 
                             gbd_round_id = gbd_round,
                             decomp_step = gbd_decomp,
                             status = 'best') 
as_weights <- as_weights[, weight:=population/sum(population)][, .(sex_id, age_group_id, weight)]

age_sex_std <- function(df_as, value_col, as_weights, col_name) {
  
  # perform age standardization across any set of ages using global age weights
  # perform sex standardization using USA sex proportions
  
  # rescale weights based off age groups in data
  as_sum <- sum(as_weights[age_group_id %in% df_as$age_group_id & sex_id %in% df_as$sex_id, weight])
  as_weights <- as_weights[age_group_id %in% df_as$age_group_id & sex_id %in% df_as$sex_id] %>%
    .[, as_weight := weight/as_sum]
  
  df_as <- merge(df_as, as_weights, by = c("age_group_id", "sex_id"))
  df_as <- df_as[, .(age_std_val = sum(get(value_col)* as_weight)), by = c("location_id", "year_id"), ]
  
  # sex weights
  setnames(df_as, "age_std_val", col_name)
  
  return(df_as)
}

# prev obese - age standardized 20+
ob <- get_model_results(gbd_team = "epi", gbd_id = 24743, gbd_round_id = 6, decomp_step = "step4", location_id = locs$location_id, year_id = seq(1990,2019,1))
ob_std <- age_sex_std(ob, "mean", as_weights, "prev_obese")

# mean BMI - age standardized 20+
bmi_model <- get_model_results(gbd_team = "epi", gbd_id = 2548, gbd_round_id = 6, decomp_step = "step4", location_id = locs$location_id, year_id = seq(1990,2019,1))
bmi_std <- age_sex_std(bmi_model, "mean", as_weights, "bmi_std")

# PA - age standardized 
# Physical activity (METs) - age standardized 25+
mets <- get_model_results(gbd_team = "epi", gbd_id = 10729, gbd_round_id = 6, decomp_step = "step4", location_id = locs$location_id, year_id = seq(1990,2019,1))
mets_std <- age_sex_std(mets, "mean", as_weights, "PA_mets")

# MERGE ALL ------------------------------------------------------
LIST <- function(...) {
  nms <- sapply(as.list(substitute(list(...))), deparse)[-1]
  setNames(list(...), nms)
}

cov_merge <- function(x,y){
  x <- as.data.table(x)
  y <- as.data.table(y)
  x[, state_name := NULL]
  y[, state_name := NULL]
  dt <- merge(x, y, by=c("location_id","year_id"), all.x=TRUE)
  dt <- unique(dt)
  return(dt)
}

all_data <- LIST(pop[,.(location_id, year_id, population)], population, pop65, pop_female, density_1000, density_150, 
                 income, edu_yrs, sdi, cig_pc, cig_pc_lag, med_income, mets_std,
                 beds1000, hw, hhi_hosp, premiums,
                 market_conc, insurance, insurance_disagg, mdcr_util,
                 care_seeking, hospital_admissions, inpatient_days, 
                 outpatient_days, mdcr_c_enrollment, mdcr_d_pct,
                 mdcr_prem, max_rates, max_elig_c, max_elig_p, max_elig_m, 
                 hosp, firms, mdcd_ex, haqi, mdcd_mco, mdcr_elig, rpp,
                 prev_diabetes, ob_std, bmi_std)

# Save list elements so you don't have to rerun everything if you have one change
save(all_data, file="FILEPATH/state_merge_covariates.RData")
dt <- Reduce(cov_merge,all_data)

rpp[,.(mean(rpp), min(rpp), max(rpp))]

## filter to year range we care about
dt <- dt[year_id %in% 1991:2019]

## filter to states we report for
dt <- dt[location_id %in% states$location_id]

# Checks ------------------------------------------------------
## check histograms pct data
cols <- names(dt)
pdf("FILEPATH/state_covariate_data_histograms.pdf", width = 8, height = 6)
for(i in cols){
  print(i)
  if(is.numeric(dt$i)){
    hist(dt[,get(i)], main = i)
  }
}
dev.off()

## check duplicates
stopifnot(nrow(unique(dt[,.(location_id, year_id)])) == dt[,.N])

# Metadata
## check number of years of data
check <- dt[,lapply(.SD, function(x) sum(!is.na(x))), by = .(year_id)]
check <- melt(check, id.vars = "year_id")
check <- check[,.(has_data = sum(value)), by = .(variable, year_id)]
check[has_data > 0, has_data := 1]
fwrite(check, "FILEPATH/state_covariate_data_N_years.csv")

# WRITE OUT  ------------------------------------------------------
fwrite(dt,"FILEPATH/state_covariate_data.csv")

