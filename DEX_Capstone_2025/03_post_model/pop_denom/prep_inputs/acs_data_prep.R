#---------------------------------------------------------
#  Format ACS county denominators and save
#
#   Author: Emily Johnson
#
#---------------------------------------------------------


rm(list = ls())
pacman::p_load(arrow, dplyr, openxlsx, ggplot2, data.table, tidyr, tidyverse)
library(wrapr, lib.loc = "/FILEPATH/")

z <- 1.645
t0 <- Sys.time()

if(interactive()){
  working_dir <- "/FILEPATH/"
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  working_dir <- args[1]
}


# set file paths
data_dir <- "/FILEPATH/"

viz_dir <- paste0(working_dir, "/intermediates/acs_data/")


load("/FILEPATH/merged_counties.RData")

age_groups <- qc(
  `Under 6 years` = "0_5",
  `6 to 17 years` = "6_17",
  `6 to 18 years` = "6_18",
  `Under 18 years` = "0_17",
  `Under 19 years` = "0_18",
  `18 to 24 years` = "18_24",
  `18 to 34 years` = "18_34",
  `19 to 34 years` = "19_34",
  `19 to 25 years` = "18_25",
  `25 to 34 years` = "25_34",
  `26 to 34 years` = "26_34",
  `35 to 44 years` = "35_44",
  `35 to 64 years` = "35_64",
  `45 to 54 years` = "45_54",
  `55 to 64 years` = "55_64",
  `65 to 74 years` = "65_74",
  `65 years and over` = "65_125",
  `75 years and over` = "75_125"
)

process_df <- function(file_list, geo_level){
  df <- rbindlist(lapply(file_list, function(f){
    year <- str_extract(f, "Y_\\d{4}\\_") %>% str_remove_all("[Y,_]")
    
    df <- fread(f, skip = 1)
    df <- suppressWarnings(melt(df, id.vars = c("Geography","Geographic Area Name")))
    df[, c("measure", "metric", "sex","age","population") := tstrsplit(variable, "!!", fixed = TRUE)]
    
    if(geo_level == "county"){
      df[, c("cnty_name","state_name") := tstrsplit(`Geographic Area Name`, ", ", fixed = TRUE)]
      df[, cnty := str_sub(Geography, -5L, -1L)]
    }else{
      df[, state_name := `Geographic Area Name`]
      df[, cnty_name := NA][, cnty := NA]
    }
    
    df <- df[!is.na(age) & !is.na(sex)]
    
    df <- df[!str_detect(measure, "Annotation")][measure != "V231"]
    df[is.na(population), population := "total"]
    df[str_detect(population, "^With"), population := "ins"]
    df[str_detect(population, "^No"), population := "non_ins"]
    
    # If we need uncertainty, should bootstrap here. Currently filtering out error.
    df <- df[measure == "Estimate"]
    
    df <- dcast(df, cnty + cnty_name + state_name + age + sex ~ population, id.vars = "value")
    df[, year_id := year]
    
    df[, age := str_remove(age,":")][, sex := str_remove(sex,":")]
    df[, age_group := age_groups[age]]
    
    df[, c("age_start","age_end") := tstrsplit(age_group, "_", fixed = TRUE)]
    df[, age_start := as.integer(age_start)][, age_end := as.integer(age_end)]
    
    df <- df[, .(year_id, cnty, state_name, cnty_name, sex, age_start, age_end, count = as.numeric(ins), 
                 total = as.numeric(total), 
                 prop = as.numeric(ins)/as.numeric(total))]
    return(df)
  }))
  return(df)
}

files <- list.files(data_dir, full.names = TRUE)
files <- files[grepl('.CSV', files)]

unins_cnty <- process_df(files[grepl("B27001_DATA_COUNTY", files)], "county")
unins_cnty[, `:=`(payer = "uninsured", prop = 1-prop, count = total - count)]
priv_cnty <- process_df(files[grepl("B27002_DATA_COUNTY", files)], "county")[, payer := "priv"]
mdcr_cnty <- process_df(files[grepl("B27006_DATA_COUNTY", files)], "county")[, payer := "mdcr"]
mdcd_cnty <- process_df(files[grepl("B27007_DATA_COUNTY", files)], "county")[, payer := "mdcd"]

unins_state <- process_df(files[grepl("B27001_DATA_STATE", files)], "state")
unins_state[, `:=`(payer = "uninsured", prop = 1-prop, count = total - count)]
priv_state <- process_df(files[grepl("B27002_DATA_STATE", files)], "state")[, payer := "priv"]
mdcr_state <- process_df(files[grepl("B27006_DATA_STATE", files)], "state")[, payer := "mdcr"]
mdcd_state <- process_df(files[grepl("B27007_DATA_STATE", files)], "state")[, payer := "mdcd"]

# mdcr_priv - B27010

process_df_combos <- function(file_list, geo_level){
  df <- rbindlist(lapply(file_list, function(f){
    year <- str_extract(f, "Y_\\d{4}\\_") %>% str_remove_all("[Y,_]")
    
    df <- fread(f, skip = 1, header = T)
    df <- suppressWarnings(melt(df, id.vars = c("Geography","Geographic Area Name")))
    # Estimate!!Total!!Under 18 years!!With two or more types of health insurance coverage!!With employer-based and Medicare coverage
    df[, c("measure", "metric","age", "num_insurances","population") := tstrsplit(variable, "!!", fixed = TRUE)]
    
    if(geo_level == "county"){
      df[, c("cnty_name","state_name") := tstrsplit(`Geographic Area Name`, ", ", fixed = TRUE)]
      df[, cnty := str_sub(Geography, -5L, -1L)]
    }else{
      df[, state_name := `Geographic Area Name`]
      df[, cnty_name := NA][, cnty := NA]
    }
    
    df <- df[!is.na(age)]
    
    df <- df[!str_detect(measure, "Annotation")][measure != "V231"]
    df[is.na(population), population := "total"]
    df[is.na(num_insurances), num_insurances := "total"]
    
    # pull out mdcr_priv
    mdcr_priv <- df[population %in% c("With employer-based and Medicare coverage",
                                      "With direct-purchase and Medicare coverage"), ][, pop_keep := "mdcr_priv"]
    df[grepl("Medicare", population), pop_keep := "mdcr"]
    
    # If we need uncertainty, should bootstrap here. Currently filtering out error.
    df <- df[measure == "Estimate" & !is.na(pop_keep)]
    mdcr_priv <- mdcr_priv[measure == "Estimate" & !is.na(pop_keep)]
    
    # sum up all mdcr rows
    df[, value := as.numeric(value)]
    df <- df[, .(value = sum(value)), by = c("cnty", "cnty_name", "state_name", "age", "pop_keep")]
    
    mdcr_priv[, value := as.numeric(value)]
    mdcr_priv <- mdcr_priv[, .(value = sum(value)), by = c("cnty", "cnty_name", "state_name", "age", "pop_keep")]
    
    df <- rbind(df, mdcr_priv, fill = T)
    
    df <- dcast(df, cnty + cnty_name + state_name + age ~ pop_keep, id.vars = "value")
    df[, year_id := year]
    
    df[, age := str_remove(age,":")]
    df[, age_group := age_groups[age]]
    
    df[, c("age_start","age_end") := tstrsplit(age_group, "_", fixed = TRUE)]
    df[, age_start := as.integer(age_start)][, age_end := as.integer(age_end)]
    
    # duplicate out to different sexes
    df <- rbind(df[, sex := "Female"],
                copy(df)[, sex := "Male"])
    
    df <- df[, .(year_id, cnty, state_name, cnty_name, age_start, age_end, 
                 sex,
                 count  = as.numeric(mdcr_priv), 
                 total = as.numeric(mdcr), 
                 payer = "mdcr_priv",
                 prop = as.numeric(mdcr_priv)/as.numeric(mdcr))]
    return(df)
  }))
  return(df)
}

mdcr_priv_cnty <- process_df_combos(files[grepl("B27010_DATA_COUNTY", files)], "county")
mdcr_priv_cnty <- mdcr_priv_cnty[!is.na(count)]
mdcr_priv_cnty[is.nan(prop), prop := 0]
mdcr_priv_state <- process_df_combos(files[grepl("B27010_DATA_STATE", files)], "state")
mdcr_priv_state[is.nan(prop), prop := 0]

df <- rbind(unins_cnty, priv_cnty, mdcr_cnty, mdcd_cnty,mdcr_priv_cnty, unins_state, priv_state, mdcr_state, mdcd_state, mdcr_priv_state)
df <- merge(df, counties[,.(cnty, mcnty)], by="cnty", all.x = TRUE) 
df <- df[state_name != "Puerto Rico"]
df[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2][, sex := NULL]

expected_places <- data.table(tidyr::expand(df, nesting(mcnty, cnty, cnty_name, state_name), year_id))[!is.na(mcnty)]
missing_counties <- setdiff(expected_places, df[!is.na(mcnty),.(mcnty, cnty, cnty_name, state_name, year_id)]) %>% setDT()
missing_counties[, s_counties := .N, by=c("state_name","year_id")]

# Back out county numbers when only 1 county in a state-year is missing
state_df <- df[is.na(cnty)]
state_subtotals <- df[!is.na(cnty), .(count_s = sum(count), total_s = sum(total)), 
                      by=c("year_id","state_name","sex_id","age_start","age_end","payer")]
state_df <- merge(state_df, state_subtotals, by=c("year_id","state_name","sex_id","age_start","age_end","payer"))
state_df[, marginal_pop := total - total_s][, marginal_count := count - count_s]

s_counties <- missing_counties[s_counties == 1][, s_counties := NULL]
s_counties <- merge(s_counties, 
                    state_df[,.(state_name, year_id, sex_id, age_start, age_end, payer, 
                                count = marginal_count, total = marginal_pop, prop = marginal_count/marginal_pop)], 
                    by=c("state_name","year_id"))

df <- rbind(df, s_counties)

# now check merged counties and recalculate
df <-df[, .(count = sum(count), total = sum(total)), by = c("year_id", "state_name", "age_start", "age_end", "payer", "mcnty", "sex_id")]
df[, prop := count/total]
df[is.nan(prop), prop := 0]

# add back cnty and mcnty
county_names <- counties[,.(cnty = unique(cnty)[1], cnty_name = unique(cnty_name)[1]), by = "mcnty"]
df <- merge(df, county_names, by = "mcnty", all.x = T)

# Fixing different age bins for different years here
unique(df[, .(age_start, age_end)])[order(age_start)]

df[year_id >= 2017 & age_end %in% c(18,25), age_end := age_end-1]
df[year_id >= 2017 & age_start %in% c(19,26), age_start := age_start-1]

df <- df[year_id <= 2019]

fwrite(df, paste0(working_dir, "/inputs/acs_state_and_county_raw.csv"))
print("1 - saved raw data")

#
#   Smooth across age to get DEX age groups
#

age_metadata <- fread(paste0(working_dir, "/inputs/age_metadata.csv")) 
expand_to_ages <- function(df, collapse_groups = T){
  
  # this function expands age groups out to single years, then assigns DEX age categories
  # means an age group that stratifies multiple will appear in multiple
  
  data_age_list <- unique(df[,.(age_start, age_end)])
  
  age_grid <- data.table()
  for(i in 1:nrow(data_age_list)){
    age_vec <- data_age_list[i, age_start]:data_age_list[i, age_end]
    age_grid <- rbind(age_grid, data.table("age_start" = data_age_list[i, age_start],
                                           "age_end" = data_age_list[i, age_end],
                                           "age" = age_vec))
  }
  
  age_grid_5 <- data.table()
  for(i in 1:nrow(age_metadata)){
    age_vec <- age_metadata[i, age_group_years_start]:(age_metadata[i, age_group_years_end-1])
    age_grid_5 <- rbind(age_grid_5, data.table("age_group_name" = age_metadata[i, age_group_name],
                                               "age" = age_vec))
  }
  
  # add on 5 year age groups to age grid
  age_grid <- merge(age_grid, age_grid_5, by = "age")
  df_big <- merge(df, age_grid, by = c("age_start", "age_end"), allow.cartesian = T)
  
  if(collapse_groups){
    
    df_big[, age := NULL]
    df_big <- unique(df_big)
  }
  
  
  return(df_big)
  
}

# expand age groups out to single age
df_big <- expand_to_ages(df, collapse_groups = F)

# add discontinuities for payer-specific ages (age 26 at private, age 20+65 for mdcr)
df_big[payer == "priv", grouping_var := ifelse(age <= 25, "groupa", "groupb")]
df_big[payer == "priv" & age >= 65, grouping_var := "groupc"]

df_big[payer == "mdcr", grouping_var := ifelse(age <= 20, "groupa", "groupb")]
df_big[payer == "mdcr" & age >= 65, grouping_var := "groupc"]

df_big[payer == "mdcr_priv", grouping_var := ifelse(age <= 20, "groupa", "groupb")]
df_big[payer == "mdcr_priv" & age >= 65, grouping_var := "groupc"]

df_big[payer == "uninsured", grouping_var := ifelse(age <= 18, "groupa", "groupb")]
df_big[payer == "uninsured" & age <= 65, grouping_var := "groupc"]

df_big[payer == "mdcd", grouping_var := ifelse(age <= 18, "groupa", "groupb")]
df_big[payer == "mdcd" & age >= 45 & sex_id == 2 , grouping_var := "groupc"]

# do loess smoother by group
print("2 - Loess smoother for age splitting!")
df_big[, group := .GRP, by = c("cnty", "year_id" ,"sex_id", "state_name", "payer", "grouping_var")]
df_big <- df_big %>%
  group_by(group) %>% 
  mutate(rate_smooth_age = predict(loess(prop ~ age, span = 0.5))) %>%
  as.data.table()

# truncate predictions outside 0,1
df_big[rate_smooth_age >1 | rate_smooth_age < 0, rate_smooth_age := ifelse(rate_smooth_age < 0, 0, 1) ]

# take average across ages within DEX age group
acs_rates <- df_big[, .(prop = mean(rate_smooth_age)),by = c("state_name", "cnty_name", "cnty", "mcnty", "year_id", "sex_id", "age_group_name", "payer")]

# plot age spec rates and df together
age_metadata[, age_mid := age_group_years_start + (age_group_years_end - age_group_years_start)/2]
acs_rates <- merge(acs_rates, age_metadata[,.(age_mid, age_group_name, age_start = age_group_years_start, age_end = age_group_years_end)], by = 'age_group_name')
pdf(paste0(viz_dir, "/acs_state_county_smoothed_age_split.pdf"), width = 11, height = 8)

for(s in unique(acs_rates$state_name)){
  
  print(ggplot()+
    geom_point(data = acs_rates[year_id == 2017 & state_name == s & is.na(cnty)], aes(x = age_mid, y = prop, color = payer))+
    geom_segment(data = df[year_id == 2017 & state_name == s & is.na(cnty)], aes(x = age_start, xend = age_end, y = prop, yend= prop))+
      scale_color_discrete(guide = "none")+
    facet_wrap(~payer+sex_id, ncol = 4 )+labs(title = paste0("Age splitting (loess) into DEX age bins - visual of 2017 state level data: ", s), 
                                   subtitle = "orig data = segments; split data = dots")+theme_bw())
}

dev.off()

#
#   Smooth across year to get smoother trend for denom process
#

acs_rates <- dcast(acs_rates, age_group_name + cnty + year_id + state_name + mcnty + cnty_name + sex_id + age_start + age_end ~ payer, value.var = "prop")
setnames(acs_rates, c("mdcr", "mdcd", "priv", "mdcr_priv", "uninsured"), c("mdcr_rate", "mdcd_rate", "priv_rate", "mdcr_priv_rate", "uninsured_rate"))
acs_rates[, group := .GRP, by = c("mcnty", "age_group_name" ,"sex_id", "state_name")]


# 2015 is missing for some medicare; removing the missingness works best
acs_rate_2014 <- acs_rates[year_id == 2014, .(group, mdcr_rate_2014 = mdcr_rate)]
acs_rates <- merge(acs_rates, acs_rate_2014, by = c('group'), all.x = T)
acs_rates[year_id == 2015 & is.na(mdcr_rate), mdcr_rate := mdcr_rate_2014]
acs_rates[, mdcr_rate_2014 := NULL]

print("3 - time smoothing for county imputation reg")
acs_rates <- acs_rates %>%
  group_by(group) %>% 
  mutate(uninsured_rate_smooth = predict(loess(uninsured_rate ~ year_id, span = 1)),
         priv_rate_smooth = predict(loess(priv_rate ~ year_id, span = 1)),
         mdcr_rate_smooth = predict(loess(mdcr_rate ~ year_id, span = 1)),
         mdcd_rate_smooth = predict(loess(mdcd_rate ~ year_id, span = 1))) %>% as.data.table()

# truncate to 0/1 now or later?
acs_rates[priv_rate_smooth >1 | priv_rate_smooth < 0, priv_rate_smooth := ifelse(priv_rate_smooth < 0, 0, 1) ]
acs_rates[uninsured_rate_smooth >1 | uninsured_rate_smooth < 0, uninsured_rate_smooth := ifelse(uninsured_rate_smooth < 0, 0, 1) ]
acs_rates[mdcd_rate_smooth >1 | mdcd_rate_smooth < 0, mdcd_rate_smooth := ifelse(mdcd_rate_smooth < 0, 0, 1)]
acs_rates[mdcr_rate_smooth >1 | mdcr_rate_smooth < 0, mdcr_rate_smooth := ifelse(mdcr_rate_smooth < 0, 0, 1)]

# many more counties missing for mdcr_priv than other payers, a couple things to address this
#   1) only smoothing where we have sufficient non-NAs
#   2) everything we don't want to smooth, we code with group = NA. it still goes through loess so we reset afterwards
#
tot_counties <- acs_rates[,.(total_rows = .N), by = "group"]
missing_counties <- acs_rates[is.na(mdcr_priv_rate), .(missing = .N), by = "group"][order(missing)]
missing_counties <- merge(missing_counties, tot_counties, by = "group", all = T )
too_missing_to_impute <- missing_counties[total_rows/missing >= 0.6 | total_rows < 4, group]
acs_rates[mdcr_rate ==0 & !(group %in% too_missing_to_impute), mdcr_priv_rate := 0]
acs_rates[group %in% too_missing_to_impute, group := NA]

acs_rates <- acs_rates %>%
  group_by(group) %>% 
  mutate(mdcr_priv_smooth = predict(loess(mdcr_priv_rate ~ year_id, span = 1, na.action = na.omit),
                                    newdata = data.table("year_id" = year_id))) %>% as.data.table()

acs_rates[is.na(group), mdcr_priv_smooth := NA]
acs_rates[is.na(mdcr_priv_smooth), mdcr_priv_smooth := mdcr_priv_rate]
acs_rates[mdcr_priv_smooth >1 | mdcr_priv_smooth < 0, mdcr_priv_smooth := ifelse(mdcr_priv_smooth < 0, 0, 1) ]

acs_rates[, group := NULL]

# add on age_start and age_end

write.csv(acs_rates, paste0(working_dir, "/inputs/acs_state_and_county_smoothed.csv"), row.names = F)

print("Done!")
print(Sys.time() - t0)
