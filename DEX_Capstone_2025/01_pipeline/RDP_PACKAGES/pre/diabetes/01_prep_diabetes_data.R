# #################
#
# 01_prep_diabetes_data: Get age+sex specific estimates of type 1 + type 2 diabetes from Marketscan and GBD
#
#
#  Author: Haley Lescinsky
#  
#
# ################

#--
# Set up
#--

rm(list = ls())

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH/python')
library(configr, lib.loc = "FILEPATH/r_library/")
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, lme4, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
source("/FILEPATH/get_outputs.R")
config <- get_config()

#--
# Arguments
#--
work_dir <- paste0(config$RDP$package_helper_dir, "/dex_package_helpers/diabetes/")
mscan_version_id <- 'XX'


#--
# PROPORTIONS FROM DEX DATA (MARKETSCAN)
#--

filepath <- config$CAUSEMAP$data_output_dir$MSCAN
filepath <- gsub("#", mscan_version_id, filepath)

# demographic grid of marketscan (only years with ICD10)
param_df <- as.data.table(expand.grid(year_id = seq(2015, 2019),
                                      toc = c("AM", "IP"),
                                      sex_id = c(1,2),
                                      age_group_years_start = c(0, 1, seq(5, 95 ,by = 5)) ))
# open dataset
df <- arrow::open_dataset(paste0(filepath, "/data"))


full_metadata <- rbindlist(lapply(1:nrow(param_df), function(i){
  
  params <- param_df[i, ]
  message(i)
  
  diabetes_df <- df %>% filter(year_id == params$year_id &
                                 toc == params$toc &
                                 sex_id == params$sex_id &
                                 age_group_years_start == params$age_group_years_start &
                                 code_system == "icd10" & grepl("diabetes", acause)) %>% as.data.table()
  
  denom <- length(unique(diabetes_df$claim_id))
  type1 <- length(unique(diabetes_df[acause == "diabetes_typ1"]$claim_id))
  type2 <- length(unique(diabetes_df[acause == "diabetes_typ2"]$claim_id))
  
  params[, `:=` (
    age_group_id = unique(diabetes_df$age_group_id),
    num_diabetes = denom,
    num_type1 = type1,
    num_type2 = type2,
    prop_type1 = type1 / (type1+type2),
    prop_type2 = type2 / (type1+type2)
  )]
  
  return(params)
  
}))

full_metadata[, mscan_run_id := mscan_version_id]

write.csv(full_metadata, paste0(work_dir, "/diabetes_proportions_icd10_dex.csv"), row.names = F )

#--
# PROPORTIONS FROM GBD PREVALENCE ESTIMATES (GBD 2019)
#--

dm_type12 <- get_outputs(topic = "cause",
                        measure_id = 5,  # prevalence 
                        metric_id = 1,   # number
                        location_id = 102,   # united states
                        cause_id = c(975, 976),   # diabetes type1,2
                        release_id = 6,   # GBD 2019
                        age_group_id = 'all',   # all-age
                        sex_id = c(1,2), # both sex
                        year_id = seq(1990, 2019))

dm_type1 <- dm_type12[cause_id==975]
dm <- dm_type12[, .(total_diabetes_prev = sum(val)), by = c("age_group_id", "location_id", "year_id", "age_group_name", "sex", "sex_id")]

setnames(dm_type1, "val", "diabetes1_prev")


dm <- merge(dm_type1, dm, by = c("location_id", "year_id", "age_group_id", "sex_id", "age_group_name"))
dm[, prop_type1 := diabetes1_prev/total_diabetes_prev]
dm[, prop_type2 := (total_diabetes_prev - diabetes1_prev)/total_diabetes_prev]


dm <- dm[, .(year_id, sex_id, age_group_id, age_group_name, diabetes1_prev, total_diabetes_prev, prop_type1, prop_type2)]


write.csv(dm, paste0(work_dir, "/diabetes_proportions_gbd_prevalence.csv"), row.names = F )


