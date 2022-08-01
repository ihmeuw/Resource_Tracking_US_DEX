#############################################
# Age standardize spending by national dex age patterns
# Indirect form of age standardization
# Acronyms: as = age/sex, pc = per capita
#############################################

# Setup ----------------------------------------------------------------------
rm(list = ls())
pacman::p_load(data.table, tidyverse)
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/currency_conversion.R")

model <- "mdcr"

# Load data --------------------------------------------------------------------
# States
load("FILEPATH/states.RData")

if(model %in% c("mdcr","mdcd")){
  pop <- fread("FILEPATH/cms_population_age_groups.csv")
  
  if(model == "mdcr"){
    pop <- pop[!(age_start == 65 & age_end == 84)][!(age_start == 85 & age_end == 125)]
  }else{
    pop <- pop[age_start < 65 | (age_start == 65 & age_end == 84) | (age_start == 85 & age_end == 125)]
  }
  
  pop <- pop[,.(year_id, sex_id, location_id, age = age_start, s_pop_as = s_pop)]
  
  ndex_as <- fread("FILEPATH/cms_by_age.csv")
  ndex_as <- ndex_as[dep_var == model]
  ndex <- fread("FILEPATH/cms_by_year.csv")
  ndex <- ndex[dep_var == model]
  
}else{
  age_groups <- get_age_metadata(age_group_set_id = 27, gbd_round_id = 7)
  age_groups <- rbind(age_groups[age_group_years_start < 85,.(age_group_id, age_group_years_start)],
                data.table(age_group_id = 160, age_group_years_start = 85))
  age_groups <- age_groups[,.(age_group_id, age = age_group_years_start)]
  # Population by state/age/sex
  pop <- get_population(age_group_id = unique(age_groups$age_group_id), 
                        location_id = unique(states$location_id), 
                        year_id = 1996:2019, 
                        sex_id = c(1,2), 
                        release_id = 7) 
  setnames(pop,"population","s_pop_as")
  pop <- merge(pop, age_groups, by="age_group_id")
  pop[, age_group_id := NULL]
  
  # National dex
  ndex_as <- fread("FILEPATH/forecast_ndex_by_age.csv")
  ndex_as <- ndex_as[dep_var == model]
  # ndex_as[, year_id := NULL]
  ndex <- fread("FILEPATH/forecast_ndex_by_year.csv")
  ndex <- ndex[dep_var == model]
}


# State estimates (draw space)
if(model == "agg"){
  sdex <- setDT(arrow::read_feather("FILEPATH/aggregate_draws.feather"))
  sdex <- sdex[, .(location_id = as.numeric(location_id), year_id, draw, sdex_pc = pred)]
}else{
  sdex <- setDT(arrow::read_feather("FILEPATH/payer_draws.feather"))
  sdex <- sdex[,.(location_id = as.numeric(location_id), year_id, draw, sdex_pc = pc, payer = model)]
  sdex <- sdex[payer == model][, payer := NULL]
  # sdex[, old_payer := model][model %in% c("mdcd","mdcr"), old_payer := "pub"]
  # sdex <- sdex[, .(sdex_pc = sum(pc), location_id = as.numeric(location_id)), by=c("year_id","population","draw","old_payer")]
  # sdex <- sdex[old_payer == model][, old_payer := NULL]
}

# RPP
rpp <- fread("FILEPATH/state_covariate_data.csv")
rpp <- rpp[,.(location_id, year_id, rpp, cpi = cpi/100)]
cpi2019 <- mean(rpp[year_id == 2019]$cpi)
rpp[, cpi := cpi/cpi2019] # standardize to 2019 instead of 1982-1984
rpp <- rpp[,.(location_id, year_id, rpp_cpi = rpp*cpi)]

# Age-sex standardization -----------------------------------------------------------------------
# Merge national proportions onto state population
df <- merge(ndex_as, pop[,.(age, year_id, sex_id, location_id, s_pop_as)], 
            by=c("age","sex_id","year_id"), allow.cartesian = TRUE)
# Estimate state age/sex-specific spending
df[, s_ndex_tot_as := ndex_pc_as*s_pop_as]
# Aggregate over age/sex
df <- df[, .(s_ndex_tot = sum(s_ndex_tot_as), s_pop = sum(s_pop_as)), by=c("year_id","location_id","draw")]
df[, s_ndex_pc := s_ndex_tot/s_pop]
# Add state estimates back in
df <- merge(df, sdex, by=c("year_id","location_id","draw"))
# Calculate standardized ratio
df[, st_ratio := sdex_pc/s_ndex_pc]

# National 
df <- merge(df, ndex[,.(year_id, draw, ndex_pc)], by=c("year_id","draw"), allow.cartesian = TRUE)
df[, s_stand_pc := ndex_pc*st_ratio]

# Add back intermediates
df <- merge(df, sdex[,.(location_id, year_id, draw, spending_pc = sdex_pc)], 
            by=c("location_id","year_id","draw"))
df <- df[,.(location_id, 
            year_id, 
            draw, 
            spending_pc,
            spending_pc_age_standardized = s_stand_pc,
            spending_pc_standardized = s_stand_pc)]

# Price standardization -----------------------------------------------------------------------
# Deflate from $2020 USD
price_df <- copy(df)
sdex_deflated <- rbindlist(lapply(min(df$year_id):2019, function(year){
  age_df <- price_df[year_id == year][, iso3 := "USA"][, yr := 2020]
  age_df <- currency_conversion(data = age_df, col.loc = "iso3", col.value = "spending_pc_standardized",
                            currency = "usd", col.currency.year = "yr", base.year = year)
  age_df[, iso3 := NULL]
  return(age_df)
}))

# State-time price adjustment from CPI-RPP
sdex_price_st <- merge(sdex_deflated, rpp, by=c("location_id","year_id"))
# Divide by CPI - apply inflation
sdex_price_st[, spending_pc_standardized := spending_pc_standardized/rpp_cpi][, rpp_cpi := NULL]

# Plotting --------------------------------------------------------------------------------------
ggplot(sdex_price_st[draw == 1]) + 
  geom_abline(aes(intercept = 0, slope = 1)) +
  geom_point(aes(x = spending_pc, y = spending_pc_age_standardized, color = as.factor(location_id)))

ggplot(sdex_price_st[draw == 1]) + 
  geom_point(aes(x = spending_pc_age_standardized, y = spending_pc_standardized, color = year_id))

# Write outputs -----------------------------------------------------------------------------------
as_file <- paste0("FILEPATH/",model,"_standardized.csv")
fwrite(sdex_price_st, as_file)
