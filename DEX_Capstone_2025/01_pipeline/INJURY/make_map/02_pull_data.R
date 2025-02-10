#-------------------------------------------------
## Pull data to inform injury adjustment
# 
#   Critical data pull:
#      - (step 3) How many observations of each INJ_specified are we seeing?    ---> use for the population distribution map for residual dxs
#       - (step 4) What are the co-occurrence rates of inj_NEC codes with specific inj causes     --> use for the main maps
#
#   Validation data pull:
#       - (step 1) What proportion of encounters with an N code also have an E code?
#       - (step 2) How many observations of each INJ_NEC are we seeing?
#
#
#  Author: Haley Lescinsky
#
#---------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

#---------------------------------------
config <- get_config()

full_inj_causes <- c("inj_NEC", "_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", "inj_suicide")
by_cols <- c("age_group_years_start", "sex_id", "year_id", "code_system", "toc", "source")
#-----------------------------------------

# Arguments
if(interactive()){
  
  params <- data.table(age_start = 35,
                       age_end = 40,
                       source_data_path = c("/FILEPATH/PRIMARY_CAUSE/data/"),
                       source = "NEDS",  # only temp
                       year_id = 2012,
                       code_system = "icd9",
                       toc = "ED")
  
  map_version <- "/FILEPATH/map_version_XX/"
  cm_map_version_id <- 'X'
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  #config_path <- args[1]
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  map_version <- args[2]
  cm_map_version_id <- args[3]
}

#-------------------------------------
# Read in the data and isolate just injury codes
#-------------------------------------
causemap_config <- parsed_config(config, key = "CAUSEMAP", map_version_id = as.integer(cm_map_version_id ))
dex_inj_code_list <- fread(causemap_config$injury_code_path)


#  Set data path by toc/year
data_path <- paste0(params$source_data_path, "/toc=", params$toc, "/year_id=", params$year_id)
params <- unique(params[, source_data_path:=NULL])
print("Reading in data!")

# Open and filter data
schema <- update_nulls_schema(data_path)
data <- arrow::open_dataset(data_path, schema = schema)
data <- data %>% 
  filter(age_group_years_start >= params$age_start &
           age_group_years_start < params$age_end & 
           code_system == params$code_system &
           acause %in% full_inj_causes) 

# Keep survey weight for HCUP
if(params$source %in% c('NIS', "NEDS")){
  data <- data %>%
    select(encounter_id, dx, acause, age_group_years_start, sex_id, code_system, survey_wt) %>% 
    mutate(year_id = params$year_id, toc = params$toc)
}else{
  data <- data %>%
    select(encounter_id, dx, acause, age_group_years_start, sex_id, code_system) %>%
    mutate(year_id = params$year_id, toc = params$toc, survey_wt = 1)
}


data <- as.data.table(data)
data$source <- params$source


if(nrow(data) > 0){
   
  #-------------------------------------
  # 1. What proportion of encounters with an N code also have an E code?
  #-------------------------------------
  ncodes <- dex_inj_code_list[code_system==params$code_system & code_type == "N code", icd_code]
  ecodes <- dex_inj_code_list[code_system==params$code_system & code_type == "E code", icd_code]
  
  # Add indicator by encounter as to if there is an E code/N code present
  data[, has_e_code := sum(dx %in% ecodes), by = c("encounter_id", by_cols)]
  data[, has_n_code := sum(dx %in% ncodes), by = c("encounter_id", by_cols)]
  
  # Filter to just encounters with N codes, and calculate proportion that has an E code
  tmp <- unique(data[has_n_code > 0, c("encounter_id", "has_e_code", "has_n_code", "survey_wt", by_cols), with = F])
  prop <- tmp[, .("prop" = sum((has_e_code > 1)*survey_wt)/(sum(survey_wt))), by = by_cols]
  
  data[, `:=` (has_e_code = NULL, has_n_code = NULL)]
  print("Calculated proportion N with E!")
  #-------------------------------------
  # 2. How many observations of each INJ_NEC are we seeing?
  #-------------------------------------
  count <- data[acause=="inj_NEC", .(N= sum(survey_wt)), by = c("dx", by_cols)]
  
  #-------------------------------------
  # 3. How many observations of each INJ_specified are we seeing? - use for the population distribution map for residual dxs
  #-------------------------------------
  count2 <- data[acause!="inj_NEC", c("encounter_id", "acause", "survey_wt", by_cols), with = F]
  count2 <- count2[, .(N= sum(survey_wt)), by = c("acause", by_cols)]
  
  #-------------------------------------
  # 4. What inj causes do specific inj_NEC codes co-occur with?
  #-------------------------------------
  
  data[, has_nec_code := sum(acause %like% "inj_NEC"), by = c("encounter_id", by_cols)]
  data[, has_inj := sum(!acause %like% "inj_NEC"),by = c("encounter_id", by_cols)]
  
  # only are interested in encounters that have both nec codes and specific injury causes
  data <- data[has_nec_code > 0 & has_inj > 0]
  data[, `:=` (has_nec_code = NULL, has_inj = NULL)]
  
  num_encounters <- length(unique(data$encounter_id))
  num_encounters <- cbind(params, data.table("n_encounters_used" = num_encounters))
  
  # split data up into inj_NEC and not inj_NEC
  data_nec <- data[acause %like% "inj_NEC"]
  data_inj <- unique(data[!acause %like% "inj_NEC"][, `:=` (dx = NULL, value = survey_wt, survey_wt = NULL)])
  inj_causes <- unique(data_inj$acause)
  # decast 6 injuries wide to get into matrix shape
  
  if(nrow(data_inj) > 0){
    data_inj_wide <- dcast(data_inj, ...~acause, value.var = "value", fill = 0)
    
    # now merge on NECs (long) 
    data <- merge(data_nec, data_inj_wide, by = c("encounter_id", by_cols), allow.cartesian = T)
    
    # now collapse to counts
    data[, `:=` (encounter_id = NULL, acause = NULL, survey_wt = NULL)]
    data[, (inj_causes) := lapply(.SD, sum), by = c(by_cols, "dx"), .SDcols = inj_causes]
    data <- unique(data)
    data[, tot_dx := rowSums(data[, c(inj_causes), with = F])]
    
    for(col in setdiff(setdiff(full_inj_causes, "inj_NEC"), colnames(data))){
      print(paste0(col, " not in this dataset, add on with all 0s"))
      data[, paste0(col) := 0]
    }
    
    print("Writing files!")
    #-------------------------------------
    # Write out results!
    #-------------------------------------
    
    if(!interactive()){
      
      # validation / metadata
      arrow::write_dataset(prop, path = paste0(map_version, "/inputs/validation/prop_n_with_e/"), partitioning = c("code_system", "toc", "source",  "year_id", "age_group_years_start"))
      arrow::write_dataset(count, path = paste0(map_version, "/inputs/validation/count_inj_nec//"), partitioning = c("code_system", "toc", "source",  "year_id", "age_group_years_start"))
      arrow::write_dataset(num_encounters, path = paste0(map_version, "/inputs/encounters_count/"), basename_template = paste0(paste0(params, collapse = "_"), "_part{i}.parquet"))
      
      # used for map
      arrow::write_dataset(count2, path = paste0(map_version, "/inputs/residual_distribution/"), partitioning = c("code_system", "toc", "source",  "year_id", "age_group_years_start"))
      arrow::write_dataset(data, path = paste0(map_version, "/inputs/dx_distribution/"), partitioning = c("code_system", "toc", "source",  "year_id", "age_group_years_start"))

      }
  
  }else{
    print(paste0("No non-inj NEC data in this data split - ", paste0(params, collapse = "-")))
  }
    
  


}else{
  print(paste0("No data in this data split - ", paste0(params, collapse = "-")))
  
}
