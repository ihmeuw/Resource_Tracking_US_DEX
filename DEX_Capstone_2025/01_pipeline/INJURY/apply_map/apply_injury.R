#----------------------------------------------
# 
#   Apply probabilistic injury map to adjust data with only N codes to align with E codes
#
#   Author: Haley Lescinsky
#
#----------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, ggplot2, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
set.seed(5279)
t0 <- Sys.time()

#---------------------------------------
config <- get_config()

inj_causes <- c("inj_NEC", "_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", "inj_suicide")
by_cols <- c("age_group_years_start", "sex_id", "year_id", "code_system", "toc", "source")
#-----------------------------------------


# Arguments
if(interactive()){
  
  params <- data.table(source = "MDCR",
                       source_data_path = c("/FILEPATH/PRIMARY_CAUSE/data/"),
                       output_data_path = c("/FILEPATH/INJURY/data/"),
                       year_id = 2010,
                       code_system = "icd9",
                       toc = "HH",
                       toc_use = "HH",
                       state_id = 'AR')
  
  map_version <- "/FILEPATH/map_version_XX/"
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  #config_path <- args[1]
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  map_version <- args[2]
}


#-------------------------------------
# Read in the data
#-------------------------------------
trunc_map <- fread(paste0(map_version, "/inj_trunc_map.csv"))

pred_vars <- paste0(gsub("^_", "", setdiff(inj_causes, "inj_NEC")), "_pred")

print("Reading in data!")

data_path <- paste0(params$source_data_path, "/toc=", params$toc, "/year_id=", params$year_id, "/")

schema <- update_nulls_schema(data_path)
all_data <- arrow::open_dataset(data_path, schema = schema)

if(!is.na(params$state_id)){
  all_data <- all_data %>% filter(st_resi == params$state_id)
}

###
# LOOP THROUGH AGE/SEX COMBINATIONS
###

age_group_years_start <- c(0, 1, seq(5, 95, by = 5))
sex_id <- c(1, 2)
demographic_grid <- as.data.table(crossing(age_group_years_start, sex_id))
orig_rows <- nrow(demographic_grid)

###
# IDENTIFY IF A AGE/SEX COMBO ALREADY HAS OUTPUT, IN WHICH CASE DON'T RERUN (allows for easy relaunching) 
###

output_data_path <- paste0(params$output_data_path, "/data/toc=", params$toc, "/year_id=",params$year_id, "/code_system=", params$code_system)
d <- list.files(output_data_path, recursive = T)

if(!is.na(params$state_id)){
  d <- d[grepl(paste0("/st_resi=", params$state_id),d)]
}

finished_grid <- data.table("finished_path" = d)
finished_grid[, age_group_years_start := as.numeric(gsub("age_group_years_start=","", gsub("/sex_.*$","",finished_path)))]
finished_grid[, sex_id := as.numeric(gsub("\\/.*$","",gsub("age_group_years_start=.+/sex_id=","", finished_path)))]

demographic_grid <- merge(demographic_grid, unique(finished_grid), by = c("age_group_years_start", "sex_id"), all = T)

# update demographic grid to just be those with no outputs
demographic_grid <- demographic_grid[is.na(finished_path)][, finished_path := NULL]
if(nrow(demographic_grid)<orig_rows){
  print("Looks like some age/sex splits already finished, just skipping to the new ones!")
}

if(nrow(demographic_grid) > 0){
  
  for(i in 1:nrow(demographic_grid)){
    
    print("--------------------------------------------------------")
    demo <- demographic_grid[i,]
    print(demo)
    as_start <- Sys.time()
    
    df <- all_data %>% 
      filter(age_group_years_start == demo$age_group_years_start &
               code_system == params$code_system &
               sex_id == demo$sex_id) %>% 
      mutate("toc" = params$toc, "year_id" = params$year_id, "orig_dx" = dx, "orig_acause" = acause)
    
    ts <- Sys.time()
    
    not_inj <- df %>% filter(acause != "inj_NEC")
    data <- df %>% filter(acause == "inj_NEC")
    
    # Pulling in to memory here since we need to
    data <- data %>% collect() %>% as.data.table()
    
    
    if(nrow(data) > 0 ){
    
      # add truncation map
      data <- merge(data, trunc_map[,.(code_system, dx = icd_code, trunc_code)], by = c("code_system", "dx"), all.x = T)
      
      if(nrow(data[is.na(trunc_code)])>0){
        print(data[is.na(trunc_code)])
        data <- data[!is.na(trunc_code)]
        stop("There seem to be some inj_NEC codes in the data that aren't in the truncation map!!")
      }
      
      
      # apply map
      # go through all the trunc codes
      
      splits <- unique(data[,.(age_group_years_start, sex_id, year_id, code_system)])
      
      adj_data <- rbindlist(lapply(1:nrow(splits), function(i){
        
        subdata <- data[age_group_years_start==splits[i, age_group_years_start] & sex_id == splits[i, sex_id] & year_id == splits[i, year_id] & code_system == splits[i, code_system]]
        
        map <- open_dataset(paste0(map_version, "/maps/")) %>% filter(age_group_years_start==splits[i, age_group_years_start] & 
                                                                        sex_id == splits[i, sex_id] & 
                                                                        year_id == splits[i, year_id] & 
                                                                        toc == params$toc_use &
                                                                        code_system == splits[i, code_system]) %>% as.data.table()
    
        missing <- setdiff(subdata$trunc_code, map$dx)
        if(length(missing) > 1){
          stop("There are codes in the data we don't have a map for!!")
        }
        
        map_long <- melt(map, measure.vars = pred_vars, value.name = 'prob', variable.name = 'new_acause')
        setnames(map_long, "dx", "trunc_code")
        map_long[, new_acause := gsub("_pred","", new_acause)]
        
        # Apply_map function changes data in place
        output <- apply_map(subdata, map_long, from_col = "trunc_code", to_col = "new_acause", prob_col = "prob")
        
        return(subdata)
        
      }))
      
      if(nrow(adj_data) != nrow(data)){
        stop("The number of rows of data changed during the adjustment! Not good. ")
      }
      
      
      # now some formatting
      adj_data[, `:=` (trunc_code = NULL, acause = NULL)]
      setnames(adj_data, "new_acause", "acause")
      
      adj_data[acause == "unintent_agg", acause:="_unintent_agg"]
      adj_data[acause == "intent_agg", acause:="_intent_agg"]
      
      # change dx to a placeholder that indicates it was adjusted!
      adj_data[, dx := paste0("adjusted", "_", acause)]
      
    
    }else{
      print("There is no inj_NEC data to adjust for this split!")
      adj_data <- copy(data)
    }
    
    rm(data)
    
    # Now save! 
    parquet_filepath <- paste0(params$output_data_path, "/data/")
    
    # don't pull non-inj data into memory
    print("saving non-inj data")
    
    if(!is.na(params$state_id)){
      arrow::write_dataset(not_inj, path = parquet_filepath, partitioning = c("toc", "year_id", "code_system","age_group_years_start", "sex_id", "st_resi"), existing_data_behavior = "overwrite", basename_template = "noninj-{i}.parquet")
    }else{
      arrow::write_dataset(not_inj, path = parquet_filepath, partitioning = c("toc", "year_id", "code_system","age_group_years_start", "sex_id"), existing_data_behavior = "overwrite", basename_template = "noninj-{i}.parquet")
    }
    
    
    # 
    print("saving inj data")
    
    if(!is.na(params$state_id)){
      arrow::write_dataset(adj_data, path = parquet_filepath, partitioning = c("toc", "year_id", "code_system","age_group_years_start", "sex_id", "st_resi"), existing_data_behavior = "overwrite", basename_template = "inj-{i}.parquet")
    }else{
      arrow::write_dataset(adj_data, path = parquet_filepath, partitioning = c("toc", "year_id", "code_system","age_group_years_start", "sex_id"), existing_data_behavior = "overwrite", basename_template = "inj-{i}.parquet")
    }
  

    print(Sys.time()-as_start)
  }
}


print("Done with entire injury adjustment!!")
print(Sys.time() - t0)
