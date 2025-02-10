#---------------------------------------------------
#  Pull data for HCCI maps ! (RDP + Injury!)
#
#
#  Author: Haley Lescinsky
#---------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
t0 <- Sys.time()

#---------------------------------------
causelist <- fread("/FILEPATH/causelist.csv")
causes_to_use <- causelist[acause %like% "NEC" | acause == "_gc"]$acause
by_cols <- c("age_group_years_start", "sex_id", "year_id", "code_system", "toc", "source")
#-----------------------------------------

# Arguments
if(interactive()){
  
  params <- data.table(age_start = 55,
                       source_data_path = c("/FILEPATH/RDP/sankey_diagram_metadata/"),
                       source = "SOURCE",  
                       year_id = 2017,
                       toc = "HH")
  
  map_version <- "/FILEPATH/INJURY_RDP/"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  map_version <- args[2]
}

#-------------------------------------
# Read in the data
#-------------------------------------

# filter data

code_system <- if(params$year_id < 2016){
  code_system <- 'icd9'
}else{ code_system <- 'icd10'}

files <- list.files(params$source_data_path)
these_files_m <- files[grepl(paste0(params$toc, "_", params$year_id, "_",code_system,"_", params$age_start, "_1"), files)]
these_files_f <- files[grepl(paste0(params$toc, "_", params$year_id, "_",code_system,"_", params$age_start, "_2"), files)]


counts_by_nec_full <- data.table()

if(length(these_files_f) > 0 | length(these_files_m) > 0 ){

  
  for(i in 1:max(length(these_files_f), length(these_files_m))){
  
    if(length(these_files_m) >= i ){
      tmp_data_m <- arrow::open_dataset(paste0(params$source_data_path, these_files_m[i]))
      tmp_data_m <- tmp_data_m %>%  
        mutate(year_id = params$year_id, toc = params$toc, source = params$source, sex_id = 1, age_group_years_start = params$age_start)
      use_m <- T
    }else{
      use_m <- F
    }
    
    if(length(these_files_f) >= i ){
      tmp_data_f <- arrow::open_dataset(paste0(params$source_data_path, these_files_f[i]))
      tmp_data_f <- tmp_data_f %>%  
        mutate(year_id = params$year_id, toc = params$toc, source = params$source, sex_id = 2, age_group_years_start = params$age_start)
      use_f <- T
    }else{
      use_f <- F
    }
    
    if(use_f == F & use_m == F){next}
    
    #-------------------------------------
    # For each original NEC/GC code that went through RDP, calculate frequency that it was replaced with other codes in RDP
    #    Basically trying to capture overall effect of RDP
    #-------------------------------------
    
    counts_by_nec <- rbindlist(lapply(causes_to_use, function(cause){
      
      print(cause)
      
      possible_causes <- causelist[!(acause %in% causes_to_use)]$acause
      if(cause == "inj_NEC"){
        # only want injury causes to be possible for inj_NEC
        possible_causes <- causelist[family == "fam_inj" & acause!="inj_NEC"]$acause
        
      }
      # Look only at encounters that originally had a NEC/GC cause
      if(use_m){encounters_with_nec1 <- tmp_data_m %>% filter(orig_acause == cause & primary_cause==1 & acause %in% possible_causes) %>% collect() %>% as.data.table()
      }else{encounters_with_nec1 <- data.table()}
      if(use_f){encounters_with_nec2 <- tmp_data_f %>% filter(orig_acause == cause & primary_cause==1 & acause %in% possible_causes) %>% collect() %>% as.data.table()
      }else{encounters_with_nec2 <- data.table()}
      
      this_data <- rbind(encounters_with_nec1, encounters_with_nec2)
      
      # Get frequencies of each non NEC/GC cause
      count <- this_data[, .N, by = c("acause", by_cols)]
      count[, nec := cause]
      
      rm(this_data)
      
      return(count)
    }))
    
    setcolorder(counts_by_nec, c("nec", "acause"))
    counts_by_nec_full <- rbind(counts_by_nec_full, counts_by_nec, fill = T)
  
  }
}else{print("No data!")}

#-------------------------------------
# Write out results!
#-------------------------------------

if(!interactive() & nrow(counts_by_nec_full) > 0){
  
  counts_by_nec_full <- counts_by_nec_full[, .(N= sum(N)), by = c("nec", "acause", by_cols)]
  
  arrow::write_dataset(counts_by_nec_full, 
                       path = paste0(map_version, "/inputs/data/"), 
                       partitioning = c("toc", "source",  "year_id"),
                       basename_template = paste0("age_", params$age_start, "-{i}.parquet"))
}


print("Done!")
print(Sys.time() - t0)
