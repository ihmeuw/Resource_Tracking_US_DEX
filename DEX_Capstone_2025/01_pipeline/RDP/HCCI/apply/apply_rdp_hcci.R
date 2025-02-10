#-------------------------------------------------
#
#  Apply the abridged RDP/Injury maps to HCCI
#
#  Author: Haley Lescinsky
#
#-----------------------------------------------------

rm(list = ls())
Sys.umask(mode = 002)
set.seed(5678)
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
Sys.setenv("RETICULATE_PYTHON" = '/FILEPATH/python')
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)
log_dir <- paste0("/FILEPATH/", Sys.info()['user'], "/")
t0 <- Sys.time()

config <- get_config()
causelist_path <- config$CAUSEMAP$causelist_path
causelist <- fread(causelist_path)
causes_to_fix <- causelist[acause %like% "NEC" | acause == "_gc"]$acause

if(interactive()){
  
  map_version <- "/FILEPATH/INJURY_RDP/"
  
  #-----------------------
  params <- data.table(source = "HCCI",
                       source_data_path = "/FILEPATH/CAUSEMAP/data/",
                       toc = "IP",
                       age_start = 65)
  #-----------------------
  
  phase_run_id <- 27 
  
  output_data_path <- "/FILEPATH/"
  output_data_path <- paste0(output_data_path, "/run_", phase_run_id, "/RDP/")
  dir.create(output_data_path, recursive = T)
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  map_version <- args[2]
  output_data_path <- args[3]
}


#----------------------
# LOAD DATA
#----------------------
hcci <- arrow::open_dataset(paste0(params$source_data_path, "/"))
hcci_filtered <- hcci %>% filter(
  toc == params$toc &
    age_group_years_start == params$age_start)

hcci_adjusted <- hcci_filtered %>% filter(
  !(acause %in% causes_to_fix)) %>%
  mutate(orig_acause = acause, redistributed = 0)

hcci_to_adjust <- hcci_filtered %>% filter(
  acause %in% causes_to_fix) %>% collect() %>% as.data.table()

#----------------------
# LOAD MAPS
#----------------------
maps <- arrow::open_dataset(paste0(map_version, "/maps/")) %>% 
  filter(toc == params$toc &
           age_group_years_start == params$age_start) %>%
  collect() %>% as.data.table()


#----------------------
# SELECT NEW CAUSES FOR NEC/GCs
#     Since we are redistributing collapsed data in this step, we expand a single row with N=# 
#     to a different row for each new acause, with sum(N)=#. Specifically we are distributing the "n_encounters" value, 
#     where encounters_per_person is calculated as n_encounters/n_people 
#----------------------

if(nrow(hcci_to_adjust) > 0){
  
  hcci_to_adjust[, `__index_level_0__` := NULL]
  hcci_to_adjust_wide <- dcast(hcci_to_adjust, ...~ metric, value.var = c("raw_val", "se"))
  hcci_to_adjust_wide[, group := .GRP, by = c("acause", "sex_id", "pri_payer", "geo", "location", "age_group_years_start", "year_id", "toc")]
  
  
  adjusted_data <- rbindlist(lapply(1:max(hcci_to_adjust_wide$group), function(i){
    # Iterate over a single group and use probability map to replace
    
    rows <- hcci_to_adjust_wide[group == i,]
    row <- rows[1]
    map_subset <- maps[nec == row$acause & sex_id == row$sex_id & age_group_years_start == row$age_group_years_start & toc == row$toc]
    
    # Use probability of each cause to make a new list of causes
    new_cause_vec <- sample(x = map_subset$acause,
                            size = ceiling(row$n_encounters), 
                            prob = map_subset$prop,
                            replace = T)
    
    # Split up fractionally
    new_cause_sum <- data.table("acause" = new_cause_vec)[, .(prop = .N/length(new_cause_vec)), by = "acause"]
    new_cause_sum[, `:=` ( n_obs = row$n_obs*prop, n_encounters = row$n_encounters*prop)]
    
    new_rows <- tidyr::crossing(new_cause_sum, rows[, `:=` (orig_acause = acause, acause = NULL, n_obs = NULL, n_encounters = NULL)]) %>% as.data.table()
    new_rows[, raw_val_encounters_per_person := n_encounters/n_people]
  
    new_rows_long <- melt(new_rows, measure.vars = colnames(new_rows)[grepl("raw_val_|se_", colnames(new_rows))])
    new_rows_long[, metric := gsub("raw_val_|se_", "", variable)]
    new_rows_long[, variable := ifelse(variable %like% "se_", "se", "raw_val")]
    new_rows_long <- dcast(new_rows_long, ...~variable, value.var = "value")
    
    new_rows_long[, `:=` (redistributed = 1, prop = NULL, group = NULL)]
    
    return(new_rows_long)
    
    
  }))
}else{
  adjusted_data <- data.table()
  print("No rows to adjust!")
}

#----------------------
# CHECK AND COMBINE WITH OTHER CAUSE ROWS
#----------------------

stopifnot(round(sum(hcci_to_adjust$n_encounters)) == round(sum(adjusted_data$n_encounters)))

rm(hcci_to_adjust)

# bring in data we didn't touch
hcci_adjusted <- hcci_adjusted %>% collect() %>% as.data.table()

hcci_adjusted <- rbind(hcci_adjusted, adjusted_data, fill = T)

bycols = c("acause", "sex_id", "pri_payer", "payer", "geo", "location", "location_name", "dataset", "age_group_years_start", "year_id", "toc", "metric")

# Now that we have applied RDP to collapsed data, we may have multiple rows for each cause, need to recollapse

#--
#-- simply collapse, recalculating the raw_val as n_encounters / n_people
hcci_adjusted_enc <- hcci_adjusted[metric == "encounters_per_person"]
hcci_adjusted_enc_collapsed <- hcci_adjusted_enc[, .(n_obs = sum(n_obs), n_encounters = sum(n_encounters), se = unique(se), n_people = unique(n_people)), by = bycols]
hcci_adjusted_enc_collapsed[, raw_val := n_encounters / n_people]

#---
#--- take a weighted mean to get raw val
hcci_adjusted_oth <- hcci_adjusted[metric != "encounters_per_person"]
hcci_adjusted_oth <- hcci_adjusted_oth[order(redistributed, n_obs)]

# Use weighted mean to get raw val
hcci_adjusted_oth[, prop := n_encounters/sum(n_encounters), by = bycols]
hcci_adjusted_oth_collapsed <- hcci_adjusted_oth[, .(n_obs = sum(n_obs), n_encounters = sum(n_encounters), se = se[1], raw_val = sum(raw_val*prop), n_people = unique(n_people)), by = bycols]

hcci_to_save <- rbind(hcci_adjusted_enc_collapsed, hcci_adjusted_oth_collapsed)

#----------------------
# SAVE
#----------------------

if(nrow(hcci_to_save) > 0){
  arrow::write_dataset(hcci_to_save, 
                       path = paste0(output_data_path, "/data/"), 
                       partitioning = c("toc", "year_id", "age_group_years_start", "sex_id"), 
                       existing_data_behavior = "overwrite")
  
  print("Saved data!")
  
}else{
  print("No data at all in this combination!")
}


print("Done!")
print(Sys.time() - t0)




