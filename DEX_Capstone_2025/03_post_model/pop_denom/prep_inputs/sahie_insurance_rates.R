#---------------------------------------------------------
#  Format SAHIE data and save
#
#---------------------------------------------------------

pacman::p_load(arrow, dplyr, openxlsx, ggplot2, data.table, tidyr, tidyverse)
library(wrapr, lib.loc = "/FILEPATH/")
library(lbd.loader, lib.loc = 'FILEPATH')
library("ushd.dbr", lib.loc = lbd.loader::pkg_loc("ushd.dbr"))

sahie_input_dir <- "/FILEPATH/"

if(interactive()){
  save_dir <- "/FILEPATH/"
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  save_dir <- args[1]
}

files <- list.files(sahie_input_dir, full.names = T)
files <- files[grepl("*csv", files, ignore.case = T)]

load("FILEPATH/merged_counties.RData")

age_groups <- qc(
  `0` = "Under 65 years",
  `1` = "18 to 64 years",
  `2` = "40 to 64 years",
  `3` = "50 to 64 years",
  `4` = "Under 19 years",
  `5` = "21 to 64 years")
  
data <- lapply(files, function(f){
  print(f)
  df <- fread(f, skip = 78)
  df <- df[,.(year, sexcat, racecat, iprcat, geocat,
              agecat = as.character(agecat),
              cnty = paste0(str_pad(statefips,2,"left","0"),str_pad(countyfips,3,"left","0")),
              nipr = as.integer(NIPR), nipr_moe = as.integer(nipr_moe),
              nic = as.integer(NIC), nic_moe = as.integer(nic_moe),
              nui = as.integer(NUI), nui_moe = as.integer(nui_moe))]
  
  df[, age_group := age_groups[agecat]] # map age index to strings
  df <- df[sexcat != 0 & iprcat == 0] #filter all sexes out, filter to all income groups
  df <- df[racecat == 0] # filter to all race/ethnicities
  
  # FIXING AGES
  # swap data structure from wide on measure to wide on age, to make subtraction easier
  df <- melt(df, id.vars = c("year","sexcat","geocat","cnty","age_group"), 
             measure.vars = c("nipr","nipr_moe","nui","nui_moe","nic","nic_moe"))
  df <- dcast(df, year + sexcat + cnty + geocat + variable ~ age_group, value.var = "value")
  
  # transform age values so they're no longer cumulative
  df[geocat == 40 | !(year %in% c(2008,2009)), age_50_64 := `50 to 64 years`]
  df[geocat == 40 | !(year %in% c(2008,2009)), age_40_50 := `40 to 64 years` - `50 to 64 years`]
  df[geocat == 50 & year %in% c(2008,2009), age_40_64  := `40 to 64 years`]
  df[, age_18_40 := `18 to 64 years` - `40 to 64 years`]
  df[, age_0_18 := `Under 65 years` - `18 to 64 years`]
  
  # put data back into original shape
  df <- melt(df, id.vars = c("year","sexcat","cnty","variable"), measure.vars = patterns("^age_"),
             variable.name = "age_group")
  df <- df[!is.na(value)]
  df <- dcast(df, year + sexcat + cnty + age_group ~ variable, value.var = "value")
  
  df[, age_start := as.integer(str_remove_all(str_extract(age_group, "_\\d+_"),"_"))]
  df[, age_end := as.integer(str_remove(str_extract(age_group,"\\d+$"),"_"))]
  setnames(df, c("year","sexcat"), c("year_id","sex_id"))
  
  return(df)
}) %>% rbindlist()

data <- merge(data, counties[,.(cnty, mcnty, cnty_name, state_name)], by="cnty")
data[, prop_insured := round(nic/nipr, 5)][, prop_uninsured := round(nui/nipr, 5)]

setnames(data, c("nipr","nipr_moe","nui","nui_moe","nic","nic_moe"), 
         c("total","total_moe","uninsured","uninsured_moe","insured","insured_moe"))

fwrite(data, paste0(save_dir, "/inputs/sahie_insured_rates.csv"))
