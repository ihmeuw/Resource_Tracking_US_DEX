#---------------------------------------------------
#
#  COMORB: reshape + save data. We want encounter-level data wide (on condition) for comorb, not long. 
#
#
#  Author: Haley Lescinsky
#---------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())


causelist <- fread("/FILEPATH/causelist.csv")
causelist <- causelist[!(acause == "_gc" | acause %like% "_NEC")]

spend_col <- c("tot_pay_amt", "priv_pay_amt", "oop_pay_amt", "mdcd_pay_amt", "mdcr_pay_amt")
t0 <- Sys.time()

if(interactive()){
  
  params <- data.table(age_start = 90,
                       age_end = 100,
                       source_data_path = c("/FILEPATH/F2T/data/"),
                       source = "MDCR",  # only temp
                       year_id = 2014,
                       toc = "ED",
                       state_id = NA)
  
  map_version <- "/FILEPATH/map_version_XX/"
  drop_RDP <- T
  
}else{
  
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  map_version <- args[2]
  drop_RDP <- args[3]
  
  
  if(Sys.getenv("SLURM_ARRAY_TASK_ID")== 1){
    
    note <- ifelse(drop_RDP, 
                   "RDP data was excluded from the prepped data in this version",
                   "RDP data was included in the prepped data in this version")
    write.table(note, file = paste0(map_version, "/README.txt") , sep = "")
  }

}

#----------------------------------
#  Find data and filter
#----------------------------------


data_path <- paste0(params$source_data_path, "/toc=", params$toc, "/year_id=", params$year_id, "/")

schema <- update_nulls_schema(data_path)
data <- arrow::open_dataset(data_path, schema = schema)
data <- data %>% 
  filter(age_group_years_start >= params$age_start &
           age_group_years_start < params$age_end) 

if(!is.na(params$state_id)){
  data <- data %>% filter(st_resi == params$state_id)
}

# Only use Medicare rows that are representative for AM + ED
if(params$source == "MDCR" & params$toc %in% c("AM", "ED")){
  if(params$year_id %in% c(2000,2010,2014,2015,2016,2019)){
    data <- data %>% filter(ENHANCED_FIVE_PERCENT_FLAG == "Y")
  }else{
    data <- data.table()
  }
}

#----------------------------------
#  Pull data into memory and do some processing
#----------------------------------

data <- as.data.table(data)
data <- data %>%
  mutate(year_id = params$year_id,
           toc = params$toc,
           source = params$source) 

id_vars <- c("source", "pri_payer", "age_group_id", "mcnty_resi", "encounter_id", "year_id", "toc", "age_group_years_start", "sex_id", spend_col, 'redistributed', 'primary_cause')
add_vars <- setdiff(id_vars, colnames(data))
tmp <- lapply(add_vars, function(v){
  data[, paste0(v) := NA]
})

if(drop_RDP){
  
  # drop redistributed comorbidities
  data <- data[!(redistributed == 1 & primary_cause == 0)]
  
  # drop full encounters where primary cause was redistributed
  enc_drop <- data[redistributed==1 & primary_cause ==1]$encounter_id
  print(paste0("There are ", length(enc_drop), " encounters with a redistributed primary cause, dropping them"))
  data <- data[!(encounter_id %in% enc_drop)]

}

if(nrow(data) == 0){
  print(paste0("No data in this data split - ", paste0(params, collapse = "-")))
}else{
  print("Data is read in!")
  

save_path <- paste0(map_version, "/prepped_data/")
dir.create(save_path, recursive = "T")

data <- data[,c(id_vars, "acause", "primary_cause"), with = F]
data <- data[, .(primary_cause = sum(primary_cause)), by = c(id_vars, "acause")]

# recoding because once we reshape wide, we want 1's to indicate a comorb
data[, primary_cause := ifelse(primary_cause == 1, 2, 1)]

data[primary_cause==2, pri_cause := acause]
data[is.na(pri_cause), pri_cause:=""]
data[, pri_cause := paste0(pri_cause, collapse = ""), by = c(id_vars)]

#----------------------------------
# Reshape data so it is wide on cause instead of long
#  This is pretty intensive and R has a max computation cap that gets hit with too many encounter ids
#   So for those instances we split up the data, reshape, and then merge back together
#----------------------------------

encounter_ids <- unique(data$encounter_id)
print(paste0("There are ", length(encounter_ids), " encounter_ids"))
if(length(encounter_ids) > 15000000){
  
  print("too many rows for dcast, splitting up in 4 chunks")
  
  split1 <- ceiling(length(encounter_ids)/4)
  split2 <- 2*ceiling(length(encounter_ids)/4)
  split3 <- 3*ceiling(length(encounter_ids)/4)
  
  encounter_ids1 <- encounter_ids[1:split1]
  encounter_ids2 <- encounter_ids[(split1+1):split2]
  encounter_ids3 <- encounter_ids[(split2+1):split3]
  encounter_ids4 <- encounter_ids[(split3+1):length(encounter_ids)]
  
  data_tmp1 <- dcast(data[encounter_id %in% encounter_ids1], ... ~ acause, value.var = "primary_cause", fill = 0)
  data_tmp2 <- dcast(data[encounter_id %in% encounter_ids2], ... ~ acause, value.var = "primary_cause", fill = 0)
  data_tmp3 <- dcast(data[encounter_id %in% encounter_ids3], ... ~ acause, value.var = "primary_cause", fill = 0)
  data_tmp4 <- dcast(data[encounter_id %in% encounter_ids4], ... ~ acause, value.var = "primary_cause", fill = 0)
  data <- rbind(data_tmp1, data_tmp2, data_tmp3, data_tmp4, fill = T)
  data[is.na(data)] <- 0 
  rm(data_tmp1)
  rm(data_tmp2)
  rm(data_tmp3)
  rm(data_tmp4)
  
}else{
  data <- dcast(data, ... ~ acause, value.var = "primary_cause", fill = 0)
}
data <- data[!(pri_cause == "")]

# add on zeros for any missing causes
acause_list <- setdiff(colnames(data), id_vars)
missing_causes <- setdiff(causelist$acause, acause_list)
data[, c(missing_causes):= 0]


#----------------------------------
# do minimal cause restrictions, mostly for data storage space. Full cause restriction list in calc_rr_afs.R
#----------------------------------

# # cannot have comorbs -> drop from data
 data <- data[!(pri_cause %like% "neo_" | pri_cause %in% c("neonatal_preterm", "neonatal_enceph") | pri_cause %like% "exp_well_dental")]
# 
# # cannot be comorbidities anywhere
drop_everywhere <- c("maternal_indirect", "nutrition", "hepatitis_c", "_infect_agg",
                    "meningitis", "septicemia", "rf_hypertension", "rf_hyperlipidemia",
                    "rf_obesity", "rf_tobacco", "endo", "renal_failure", "cvd_other",
                    "resp_other", "digest_other", "maternal_other", "neo_other_cancer", "neo_other_benign",
                    "neonatal_other", "neuro_other", "mental_other",
                    "exp_well_pregnancy", "exp_well_dental", "exp_donor", "exp_family_planning",
                    "exp_social_services", "exp_well_person", "sense_other")
data[, (drop_everywhere):= 0]


#----------------------------------
# SAVE
#----------------------------------

order_vars <- c("pri_cause", "sex_id", "age_group_id", "pri_payer", "mcnty_resi", acause_list)
data <- data[order(data[, c(order_vars), with = F])]

arrow::write_dataset(data, 
                     path = save_path, 
                     partitioning = c("toc", "age_group_years_start", "source"), 
                     basename_template = paste0("year_", params$year_id, "_st",params$state_id,"-{i}.parquet"), 
                     existing_data_behavior = "overwrite")

}
print("Done!")
print(Sys.time()-t0)


