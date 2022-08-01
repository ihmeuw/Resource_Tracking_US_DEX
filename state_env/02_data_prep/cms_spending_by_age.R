rm(list = ls())
library(openxlsx)

source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

load("FILEPATH/states.RData")

# Create weird cms data age bins
age_groups <- get_age_metadata(age_group_set_id = 27, gbd_round_id = 7)[,.(age_group_id, age_group_years_start, age_group_years_end)]
age_groups <- rbind(age_groups[age_group_id != 8], data.table(age_group_id = c(63,64,65,66,67), age_group_years_start = c(15,16,17,18,19), age_group_years_end = c(16,17,18,19,20)))
age_groups[, age_start := age_group_years_start]
age_groups[age_group_years_start < 19, age_start := 0][age_group_years_start < 45 & age_group_years_start >= 19, age_start := 19]
age_groups[age_group_years_start < 65 & age_group_years_start >= 45, age_start := 45]

age_groups[, age_end := age_start + 4]
age_groups[age_start == 0, age_end := 18][age_start == 19, age_end := 44]
age_groups[age_start == 45, age_end := 64][age_start == 95, age_end := 125]

mdcd_ages <- copy(age_groups)[age_start >= 65]
mdcd_ages[age_start < 85, `:=`(age_start = 65, age_end = 84)][age_start >= 85, `:=`(age_start = 85, age_end = 125)]

age_groups <- rbind(age_groups, mdcd_ages)

pop1 <- get_population(age_group_id = unique(age_groups$age_group_id),
                       location_id = unique(states$location_id),
                       year_id = 2000:2019,
                       sex_id = c(1,2),
                       release_id = 7)
pop2 <- get_population(age_group_id = c(63,64,65,66,67),
                       single_year_age = TRUE,
                       location_id = 102,
                       year_id = 2000:2019,
                       sex_id = c(1,2),
                       release_id = 7)
pop <- rbind(pop1, pop2)
pop[, run_id := NULL]
pop <- merge(pop, age_groups, by="age_group_id", allow.cartesian = TRUE)

pop <- pop[,.(s_pop = sum(population)), by=c("year_id","sex_id","location_id","age_start","age_end")]
pop[, n_pop := sum(s_pop), by=c("year_id","sex_id","age_start","age_end")]
fwrite(pop, "FILEPATH/cms_population_age_groups.csv")

population <- unique(pop[,.(year_id, sex_id, age_start, age_end, population = n_pop)])

data_folder <- "FILEPATH/CMS_HEALTH_CARE_EXPEDITURES/USA_NHEA_AGE_GENDER_2002_2014_TABLES_1_7_Y2020M09D16.xlsx"

mdcr_split <- fread("FILEPATH/USA/MEDICARE_PER_CAPITA_SPEND_BY_AGE/mdcr_spend_by_age.csv")
mdcr_split[, age_start := as.integer(str_sub(`Age (years)`, 1, 2))][, age_end := as.integer(str_sub(`Age (years)`, -2, -1))]
mdcr_split[age_start == 95, age_end := 125][age_start == 66, age_start := 65]
mdcr_split[, spend := as.numeric(str_remove(`Overall spending`,","))]
mdcr_split[, pop := as.numeric(str_remove_all(`Unweighted`,","))]
mdcr_split[, spend := spend*pop]
# mdcr_split[age_start < 85, bin := 65][age_start >= 85, bin := 85]
mdcr_split[, bin := 65]
mdcr_split <- mdcr_split[!is.na(spend),.(age_start, age_end, spend_fr = spend/sum(spend)), by="bin"]

process_sheet <- function(excel_path, sheet_number){
  
  # columns 1 -8 are spending (in millions)
  cms_data <- as.data.table(read.xlsx(excel_path, sheet = sheet_number))[, 1:8]
  
  # get proper column names
  colnames(cms_data) <- as.character(cms_data[3, ])
  
  # focus on sex specific estimates. males are listed first, then females
  cms_data <- cms_data[10:(.N-1),]
  cms_data[1:6, sex_id := 1]
  cms_data[7:12, sex_id := 2]
  cms_data <- cms_data[! `Age group` %in%  c("Males", "Females")]
  
  # reshape long
  cms_data <- melt(cms_data, id.vars = c("Age group", "sex_id"), variable.name = "year_id", value.name = "spending_millions")
  cms_data[, year_id := as.numeric(as.character(year_id))]
  cms_data[, spending_millions := as.numeric(as.character(spending_millions))]
  
  cms_data[, age_start := as.numeric(gsub("[-,+].*", "", `Age group`))]
  cms_data[, age_end := as.numeric(gsub("^.*[-,+]", "", `Age group`))]
  cms_data[ `Age group` %like% "\\+", age_end := 125]
  
  
  return(cms_data[, .(year_id, sex_id, age_start, age_end, spending_millions)])
  
}

medicare <- process_sheet(data_folder, 2)
medicare[age_start %in% c(65, 85), `:=`(spending_millions = sum(spending_millions), age_start = min(age_start), age_end = max(age_end))]
medicare <- unique(medicare)
setnames(medicare, c("age_start","age_end"), c("bin","old_age_end"))
medicare <- merge(medicare, mdcr_split, all = TRUE, allow.cartesian = TRUE)
medicare[is.na(age_start), age_start := bin][is.na(age_end), age_end := old_age_end][is.na(spend_fr), spend_fr := 1]
medicare <- medicare[!(age_start == 85 & age_end == 125)]
medicare[, spending_millions := spending_millions*spend_fr]
medicare[,`:=`(bin = NULL, old_age_end = NULL, spend_fr = NULL)]

medicaid <- process_sheet(data_folder, 3)

cms <- rbind(medicare[, payer := "mdcr"], medicaid[, payer := "mdcd"])
cms[, asy_tot := spending_millions*1000000][, y_tot := sum(asy_tot), by=c("year_id","payer")]
cms[, as_frac := asy_tot/y_tot]

cms_yrs <- tidyr::expand(cms, nesting(payer, age_start, age_end, sex_id), year_id = 2000:2014)
cms <- merge(cms, cms_yrs, all.y = TRUE)
setorder(cms, year_id)

cms <- rbindlist(lapply(split(cms, by=c("sex_id","age_start","age_end","payer")), simple_extrapolate))

cms <- rbindlist(lapply(split(cms, by=c("payer")), function(d){
  d[, age_sex := paste0(age_start, "_", sex_id)]
  d[year_id == 2014, dummy14 := 1][year_id != 2014, dummy14 := 0]
  frac_mod <- lm(as_frac ~ age_sex + year_id + year_id*age_sex + dummy14 + dummy14*age_sex, data = d)
  tot_mod <- lm(y_tot ~ year_id, data = d)
  # p <- predict(mod, year = 2000:2019)
  
  pred <- tidyr::expand(d, nesting(payer, age_start, age_end, sex_id, age_sex), year_id = 2000:2019) %>% setDT()
  pred[year_id < 2014, dummy14 := 0][year_id >= 2014, dummy14 := 1]
  pred$as_frac <- predict(frac_mod, newdata = pred)
  pred$y_tot <- predict(tot_mod, newdata = pred)

  pred[, age_sex := NULL][, dummy14 := NULL]
  return(pred)
}))
cms[, as_frac := as_frac/sum(as_frac), by=c("year_id","payer")][, asy_tot := as_frac*y_tot]
cms[, as_frac := NULL]

cms <- cms[population, on = .(sex_id, year_id, age_start, age_end), allow.cartesian = TRUE]
cms <- cms[,.(n_pop_as = sum(population), drawmaker = 1), by=c("year_id","sex_id","age_start","age_end","payer","asy_tot","y_tot")]
cms <- cms[data.table(draw = 1:1000, drawmaker = 1), on=.(drawmaker), allow.cartesian = TRUE]

cms <- dex_currency_conversion(cms, c("asy_tot","y_tot"))
cms_as <- cms[,.(year_id, age_group_id = NA, sex_id, age = age_start, draw, dep_var = payer, 
              ndex_tot_as = asy_tot, ndex_pc_as = asy_tot/n_pop_as, n_pop_as)]

cms <- cms[,.(n_pop = sum(n_pop_as)), by=c("year_id","payer","y_tot","draw")]
cms <- cms[,.(year_id, draw, dep_var = payer, ndex_tot = y_tot, n_pop, ndex_pc = y_tot/n_pop)]

fwrite(cms_as, "FILEPATH/cms_by_age.csv")
fwrite(cms, "FILEPATH/cms_by_year.csv")


