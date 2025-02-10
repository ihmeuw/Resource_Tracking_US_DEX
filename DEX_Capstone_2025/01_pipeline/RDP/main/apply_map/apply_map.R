##--------------------------------------------------
#  Use map to apply RDP and save adjusted data; launched by launch_apply_map.R
# 
#
#  Author: Haley Lescinsky
#           
##--------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, tidyverse, sparklyr, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)
set.seed(14523)

residual_code <- "asr" # placeholder code; won't change
config <- get_config()

# Arguments
if(interactive()){
  
  params <- data.table(source_data_path = c("/FILEPATH/INJURY/data/"),
                       source = "MDCR",  # only temp
                       source_use = 'MDCR',
                       year_id = 2016,
                       code_system = "icd10",
                       toc = "NF",
                       state_id = 'NC')
  
  map_version <- "/FILEPATH/map_version_XX/"
  package_version <- "/FILEPATH/map_version_XX/"
  save_data_folder <- gsub("/INJURY.*$", "/RDP/",  params$source_data_path)
  dex_icd_map_path <- "/FILEPATH/map_version_XX/icd_map.feather"
  
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  map_path <- args[1]
  params <- fread(map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  package_version <- args[2]
  map_version <- args[3]
  save_data_folder <- args[4]
  dex_icd_map_path <- args[5]
  
}

t0 <- Sys.time()

# Read in data 
#-------------------------------------------
data_paths <- paste0(params$source_data_path, "/toc=", params$toc, "/year_id=",params$year_id, "/")

# filter data, immediately subset to just primary cause 
schema <- update_nulls_schema(data_paths)

all_data <- arrow::open_dataset(data_paths, schema = schema)
all_data <- all_data %>% 
  filter(code_system == params$code_system)

if(!is.na(params$state_id)){
  all_data <- all_data %>% filter(st_resi == params$state_id)
}

# Identify the RDP target extra codes
rdp_target_map <- fread(paste0(package_version, "/all_rdptarget_codes.csv"))

# load truncation map
trunc_map <- fread(paste0(package_version, "/trunc_map.csv"))[code_system == params$code_system , .(dx = icd_code, trunc_code)]

# load maps
year_map_path <- paste0(map_version, "/maps/toc=", params$toc, "/source=", params$source_use, "/year_id=", params$year_id)

if(file.exists(year_map_path)){
  all_map <- arrow::open_dataset(year_map_path)
}else{
  # We open the path anyway, without getting the error. Then below will attempt to use nearby year for this map after nrow map = 0 
  print("Attempting to use nearby year for all ages & packages!")
  all_map <- arrow::open_dataset(paste0(map_version, "/maps/toc=", params$toc, "/source=", params$source_use)) %>% filter(year_id == params$year_id)
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

output_data_path <- paste0(save_data_folder, "/data/toc=", params$toc, "/year_id=",params$year_id, "/code_system=", params$code_system)
d <- list.files(output_data_path, recursive = T)

if(!is.na(params$state_id)){
  d <- d[grepl(paste0("_", params$state_id, "-0"),d)]
}

finished_grid <- data.table("finished_path" = d)
finished_grid[, age_group_years_start := as.numeric(gsub("age_group_years_start=","", gsub("/sex_.*$","",finished_path)))]
finished_grid[, sex_id := as.numeric(gsub("[-,_].*$","",gsub("age_group_years_start=.+/sex_","", finished_path)))]

demographic_grid <- merge(demographic_grid, finished_grid, by = c("age_group_years_start", "sex_id"), all = T)

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
  
  data <- all_data %>% 
    filter(age_group_years_start == demo$age_group_years_start &
             sex_id == demo$sex_id) %>% collect() %>% as.data.table()
  
  # add on toc and year_id right away
  data[, toc := params$toc]
  data[, year_id := params$year_id]
  
  # if data is empty
  if(nrow(data) == 0){
    print("There is no data with these demographics! Stopping now")
    next
  }
  
  orig_n_rows <- nrow(data)
  data[, packages := ""]
  data[, redistributed := 0]
  
  # split into data with NEC/GC codes that needs fixing, and other
  other_data <- data[!(acause %like% "NEC" | acause %like% "_gc")]
  fixed_data <- data[acause %like% "NEC" | acause %like% "_gc"]
  rm(data)
  
  fixed_data <- merge(fixed_data, trunc_map, by = "dx", all.x = T)
  
  if(nrow(fixed_data[is.na(trunc_code)]) > 0){
    stop("Not all dxs are in the truncation map!")
  }
  
  # Get code system from the data so we can use the right packages
  code_system <- params$code_system
  package_version_code <- paste0(package_version, "/", code_system, "/")
  
  # load in icd map
  dex_icd_map <- as.data.table(arrow::read_feather(dex_icd_map_path))
  
  # data that we will modify and redistribute on
  fixed_data[, true_original_icd_code := trunc_code]
  
  # Function to redistribute the data
  #-------------------------------------------
  redistribute_data <- function(p, data, all_map){
    
    package_metadata <- rjson::fromJSON(file = paste0(package_version_code, p, "/metadata.json"))
    package_gcs <- package_metadata$codes
    
    # make sure the residual code is captured in the last 'catch all' package
    if(p == 1406 | p == 1401){
      package_gcs <- c(package_gcs, residual_code, NA)
    }
    
    # split data into data that needs adjusted and doesn't
    data_to_rdp <- data[trunc_code %in% package_gcs]
    data_not_to_rdp <- data[! (trunc_code %in% package_gcs)]
    
    # if no garbage codes from this package are in the data, then we don't need the map
    if(nrow(data_to_rdp)==0){
      return(data)
    }else{
      rm(data)
    }
    
    # Preserve original icd code
    data_to_rdp[, original_icd_code := trunc_code]
    data_to_rdp[, redistributed := 1]
    data_not_to_rdp[, original_icd_code := NA]
    
    # track packages used to adjust data
    data_to_rdp[packages != "", packages := paste0(packages, ",", p)]
    data_to_rdp[packages == "", packages := paste0(p) ]
    
    # read in map by filtering
    map <- all_map %>% 
      filter(code_system == params$code_system &
               age_group_years_start==demo$age_group_years_start &
               sex_id==demo$sex_id &
               package == p)
    map <- map %>% collect() %>% as.data.table()
    
    #Occasionally a code will need a map that we didn't have sufficient data for (EX: in 2015 when we split by code system)
    # Or when we use a different source map for the apply (ex: MDCR to CHIA_MDCR)
    if(nrow(map)==0){
      map <- arrow::open_dataset(paste0(map_version, "/maps/toc=", params$toc, "/source=", params$source_use)) %>% 
        filter(code_system == params$code_system &
                 age_group_years_start==demo$age_group_years_start &
                 sex_id==demo$sex_id &
                 package == p)
      map <- map %>% collect() %>% as.data.table()
      
        # In MDCR NF, we don't have data for 90 & 95 so we need to use the 85+ map
        if(nrow(map) == 0 & demo$age_group_years_start >= 90 & params$toc == 'NF'){
          map <- arrow::open_dataset(paste0(map_version, "/maps/toc=", params$toc, "/source=", params$source_use)) %>% 
            filter(code_system == params$code_system &
                     age_group_years_start==85 &
                     sex_id==demo$sex_id &
                     package == p)
          map <- map %>% collect() %>% as.data.table()
        }
        
        # if STILL no map, use the closest age
        if(nrow(map) == 0 & params$source_use =='MDCR'){
          map <- arrow::open_dataset(paste0(map_version, "/maps/toc=", params$toc, "/source=", params$source_use)) %>% 
            filter(code_system == params$code_system &
                     sex_id==demo$sex_id &
                     package == p)
          map <- map %>% collect() %>% as.data.table()
          
          if(params$code_system == 'icd9'){
            map <- map[year_id == max(year_id)]
          }else if(params$code_system == 'icd10'){
            map <- map[year_id == min(year_id)]
          }
          
          # find closest age with most observations
          map[, age_diff := abs(age_group_years_start - demo$age_group_years_start)]
          closest_age <- map[age_diff == min(age_diff), .N, by = 'age_group_years_start'][N == max(N), age_group_years_start][1]
          
          map <- map[age_group_years_start == closest_age]
        }
      
      map[, distance_from_pref_year := abs(params$year_id - year_id)]
      # Chose closest year to one you are missing
      map <- map[distance_from_pref_year == min(distance_from_pref_year)]
      map[, distance_from_pref_year := NULL]
      # maybe one year is available on either side
      if(params$code_system == 'icd9'){
        map <- map[year_id == max(year_id)]
      }else if(params$code_system == 'icd10'){
        map <- map[year_id == min(year_id)]
      }
    }
    
    if(nrow(map)==0){
      print(demo)
      print(paste0("package: ", p))
      print("No map found in an alternative year for this age/package combination")
      stop("No map found in an alternative year for this age/package combination")
    }
    
    # Check 
    if(max(map$proportion_id) > 1 ){
      stop("More than one proportion_id in data. not set to handle that yet!")
    }
    
    # Quick and simple redistribution using probabilities as weights
    data_to_rdp$trunc_code <- sample(x = as.character(map$trunc_code), 
                        size = nrow(data_to_rdp), 
                        replace = T, 
                        prob = map$probability)
    
    # combine adjusted data with data that didn't need adjusting
    new_data <- rbind(data_to_rdp, data_not_to_rdp, fill = T)
    
    return(new_data)
  }
  
  # Redistribute codes in data 
  #-------------------------------------------
  package_map <- rjson::fromJSON(file = paste0(package_version_code, "/_package_list.json"))
  
  for(p in package_map){
    # continually update data as it gets adjusted by packages in the package order (global variable)
    fixed_data <<- redistribute_data(p, fixed_data, all_map)
  }
  
  print("all data redistibuted, beginning checks + trunc map merge!")
  
  other_data[, trunc_code := dx]
  fixed_data <- rbind(fixed_data, other_data, fill = T)
  
  # Check and clean up redistributed data 
  #-------------------------------------------
  
  # quick function to make it very clear if one of these checks fails
  fail_check <- function(params, dir, check, df){
    dir.create(paste0(dir, "/failures/"))
    write.csv(df, paste0(dir, "/failures/", 
                         paste0(gsub(" ", "_", check)),
                         "__", params$source, "_", params$toc, "_", params$year_id, "_", demo$age_group_years_start, "_", demo$sex_id,"_",params$state_id,  ".csv"))
  }
  
  
  # check  - make sure all garbage in the data was distributed 
  lab <- code_system
  dex_icd_map <- dex_icd_map[code_system == lab]
  gc_codes <- dex_icd_map[acause=="_gc", icd_code]
  nec_codes <- dex_icd_map[acause %like% "NEC", icd_code]
  
  not_fixed_codes <- intersect(c(gc_codes, nec_codes), fixed_data$trunc_code)
  
  
  if(length(not_fixed_codes) > 0 ){
    
    return_df <- dex_icd_map[icd_code %in% not_fixed_codes]
    setnames(return_df, "icd_code", "not_fixed_codes")
    fail_check(params, save_data_folder, "not all gc codes redistributed", return_df)
    stop("not all gc codes redistributed !!")
  }
  
  # check  - confirm all rows of data still exist
  if(nrow(fixed_data) != orig_n_rows){
    fail_check(params, save_data_folder, "lost rows of data", params)
    stop("lost some rows of data in the process")
  }
  
  # check  - confirm there are no na's in trunc code
  if(nrow(fixed_data[redistributed ==1 & is.na(trunc_code)]) > 0){
    fail_check(params, save_data_folder, "NAs in new codes", fixed_data[is.na(trunc_code)])
    stop("there are some NA truncated codes")
  }
  
  # rdp targets map fix
  rdp_target_map <- rdp_target_map[code_system == lab]
  dex_icd_map <- rbind(dex_icd_map, rdp_target_map, fill = T)
  
  # clean up cause columns and use merge to get original column back
  fixed_data <- merge(fixed_data, dex_icd_map[, .(trunc_code = icd_code, new_acause = acause)], by = "trunc_code", all.x = T)
  fixed_data[redistributed == 1, `:=` (acause = new_acause)]
  
  if(nrow(fixed_data[is.na(acause)]) > 0){
    fail_check(params, save_data_folder, " new icd not in dex map", fixed_data[is.na(acause)])
  }
  
  print("making summary stats")
  
  # pull out summary stats
  rdp_summary <- fixed_data[ redistributed == 1 ][, .(count = .N), by = c("trunc_code", "acause","true_original_icd_code", "orig_acause", "packages", "code_system", "primary_cause")]
  rdp_summary <- rdp_summary[order(trunc_code)][, .(trunc_code, acause, true_original_icd_code, orig_acause, count, packages, code_system, primary_cause)]
  
  # remove helper columns from data
  fixed_data[, `:=` (original_icd_code = NULL, true_original_icd_code = NULL, new_acause = NULL, trunc_code = NULL)]
  
  # also remove dx since it is now out of sync with acause
  fixed_data[, dx := NULL]
  
  # Save redistributed data
  #-------------------------------------------
  
  print("saving data!")

  # Save parquet with correct partioning
  parquet_filepath <- paste0(save_data_folder, "/data/")
  # only partition by st_resi if jobs are parallelized by it

  if(!is.na(params$state_id)){
    fixed_data <- fixed_data[order(toc, year_id, code_system, age_group_years_start, sex_id, st_resi, encounter_id, acause)]
    arrow::write_dataset(fixed_data,
                         path = parquet_filepath,
                         partitioning = c("toc", "year_id", "code_system","age_group_years_start"),
                         basename_template = paste0("sex_",demo$sex_id,"_",params$state_id,"-{i}.parquet"))
  }else{
    fixed_data <- fixed_data[order(toc, year_id, code_system, age_group_years_start, sex_id, encounter_id, acause)]
    arrow::write_dataset(fixed_data,
                         path = parquet_filepath,
                         partitioning = c("toc", "year_id", "code_system","age_group_years_start"),
                         basename_template = paste0("sex_",demo$sex_id,"-{i}.parquet"))
  }

  # Same summary stats for vetting
  if(!dir.exists(paste0(save_data_folder, "/sankey_diagram_metadata"))){
    dir.create(paste0(save_data_folder, "/sankey_diagram_metadata"))
  }

  map_data <- paste0(params$source, "_", params$toc, "_", params$year_id, "_", params$code_system, "_", demo$age_group_years_start, "_", demo$sex_id, "_", params$state_id)
  rdp_summary[, `:=` (source = params$source, toc = params$toc, year_id = params$year_id, age_group_years_start = demo$age_group_years_start, sex_id = demo$sex_id, state_id = params$state_id)]

  write_dataset(rdp_summary, path = paste0(save_data_folder, "/sankey_diagram_metadata/"),
                basename_template = paste0(map_data, "-{i}.parquet"))


  print("Done with redistributing all codes for this data split!")
  print(Sys.time() - t0)
  
  rm(fixed_data)
  rm(map_data)
  rm(data)
}
}
print("DONE WITH ALL AGES!")
