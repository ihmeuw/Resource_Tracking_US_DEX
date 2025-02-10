##--------------------------------------------------
# Make RDP maps using packages; launched by launch_make_map.R
#    one map per age-sex-year-toc
# 
#  Author: Haley Lescinsky
#
##--------------------------------------------------

rm(list = ls())
source('/FILEPATH/get_age_metadata.R')
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, tidyverse, sparklyr, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
suppressMessages(lbd.loader::load.containing.package())
residual_code <- "asr"
by_state <- FALSE

# Arguments
if(interactive()){
  
  params <- data.table(source_data_path = c("/FILEPATH/INJURY/data/"),
                       source = "MDCD",  # only temp
                       year_id = 2010,
                       code_system = "icd9",
                       toc = "AM")
  
  map_version <- "/FILEPATH/map_version_XX/"
  package_version <- "/FILEPATH/map_version_XX/"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  map_path <- args[1]
  params <- fread(map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  package_version <- args[2]
  map_version <- args[3]

}
t0 <- Sys.time()

#-------------------------------------
# step 1. read in the data
#-------------------------------------
#
data_paths <- paste0(params$source_data_path, "/toc=", params$toc, "/year_id=",params$year_id, "/")

# filter data, immediately subset to just primary cause 
schema <- update_nulls_schema(data_paths)
  
all_data <- arrow::open_dataset(data_paths, schema = schema)
all_data <- all_data %>% 
    filter(code_system == params$code_system
           & primary_cause==1) 

###
# LOOP THROUGH AGE/SEX COMBINATIONS
###

age_group_years_start <- c(0, 1, seq(5, 95, by = 5))
sex_id <- c(1, 2)
demographic_grid <- as.data.table(crossing(age_group_years_start, sex_id))

# Since just one age group, look up age start and age end for the age group id, but need to merge those on
suppressWarnings(
  ages <- get_age_metadata(age_group_set_id = 27, release_id = 9)
)
ages <- select(ages, age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)


for(i in 1:nrow(demographic_grid)){
  
  demo <- demographic_grid[i,]
  print(demo)
  
  data <- all_data %>% 
    filter(age_group_years_start == demo$age_group_years_start &
             sex_id == demo$sex_id) %>% collect() %>% as.data.table()
  
  # Tweak columns
  data[,`:=` (age_group_id = NULL, age_start = age_group_years_start)]
  data[, toc := params$toc]
  data[, year_id := params$year_id]
  
  if(! params$source %in% c("NIS", "NEDS")){
    data[, survey_wt := 1]
  }

  # if data is empty
  if(nrow(data) == 0){
    print("There is no data with these demographics! Moving to next")
    next
  }
  
  # Identify the rdp extra codes
  rdp_target_map <- fread(paste0(package_version, "/all_rdptarget_codes.csv"))
  trunc_map <- fread(paste0(package_version, "/trunc_map.csv"))[code_system == params$code_system , .(dx = icd_code, trunc_code)]
  
  # Get code system so we can use the right packages
  code_system <- params$code_system
  package_version_code <- paste0(package_version, "/", code_system, "/")
  

  data <- inner_join(data,
                     ages,
                     by = "age_start",
                     copy = TRUE)
  
  
  # add on specific columns to make the process work better
  # add age mid so age restrictions are matched
  data <- data %>%
    select(year_id, sex_id, age_group_id, age_start, age_end, dx, survey_wt) %>%
    mutate(age = age_start+(age_end - age_start)/2,
           global = "G",
           super_region_id = 64, 
           region_id = 100, 
           country_id = 102)
  
  # Apply truncation map
  data <- merge(data, trunc_map[code_system == params$code_system], by = "dx", all.x = T)
  
  if(nrow(data[is.na(trunc_code)]) > 0){
    stop("Not all dxs are in the truncation map!")
  }
  
  data <- rename(data, cause=trunc_code)
  data[, dx := NULL]
  
  #-------------------------------------
  # step 2. Add proportion_id and calculate frequencies
  #-------------------------------------
  
  if(by_state){
    proportion_by_cols <- c('global','super_region_id','region_id','country_id','year_id','sex_id', 'age', 'age_group_id', 'state_name')
    
  }else{
    proportion_by_cols <- c('global','super_region_id','region_id','country_id','year_id','sex_id', 'age', 'age_group_id')
    
  }
  
  # Get metadata for the proportions (different "splits" in this dataset)
  proportion_metadata <- unique(data[, c(proportion_by_cols), with = F])[, proportion_id := 1:.N]
  
  # merge proportion_ids onto data
  input_data <- left_join(data, 
                          proportion_metadata, 
                          by = proportion_by_cols)
  
  # summarize frequencies by cause and proportion_ids
  new_data <- input_data %>% 
    group_by(proportion_id, cause) %>%
    summarize(freq = sum(survey_wt))
  
  # confirm everything is a data table now
  new_data <- as.data.table(new_data)
  proportion_metadata <- as.data.table(proportion_metadata)
  
  rm(data, input_data)
  #---------------------------------------
  # step 3. Evaluate cause map
  #---------------------------------------
  print("Data processing done, starting cause map eval!")
  icd_cause_map <- fread(paste0(package_version, "/icd_cause_map.csv"))
  icd_cause_map <- icd_cause_map[code_system == params$code_system & toc == params$toc]
  cause_map <- unique(icd_cause_map[, .(acause, restrictions)])
  # format the restrictions
  cause_map[, restrictions:=gsub("\\(", "(cause_map_wide[i,]$", restrictions)]
  cause_map[, restrictions:=gsub(" age", " cause_map_wide[i,]$age", restrictions)]
  cause_map[, restrictions:=gsub("\\(", "", gsub("\\)", "", gsub("and", "&", restrictions)))]
  
  # check out which causes are possible for each propotion id
  cause_map_wide <- merge(cause_map[, global := "G"], proportion_metadata, by = "global" , allow.cartesian = T)
  eval_col <- sapply(c(1:nrow(cause_map_wide)), function(i){
    
    result <- eval(parse(text = cause_map_wide[i, restrictions]))
    return(result)
  })
  
  cause_map_wide$restrictions_met <- eval_col
  
  cause_map_evaluated <- merge(cause_map_wide, icd_cause_map, by = "acause", allow.cartesian = T)
  cause_map_evaluated <- cause_map_evaluated[, .(icd_code, proportion_id, restrictions_met)]
  setnames(cause_map_evaluated, "icd_code", "cause")
  
  rm(cause_map_wide)
  
  #----------- Functions by package -------------------------
  
  #---------------------------------------
  # step 4. define weight group (evaluate with proportion metadata)
  #---------------------------------------
  
  get_weight_groups <- function(p, proportion_metadata){
    
    source <- as.data.table(arrow::read_parquet(paste0(package_version_code, p,"/source.parquet")))
    
    weight_group_df <- source[, .(restrictions = paste(paste0("weight_group_eval[i,]$",variable), operator, value, collapse = " & ")), by = c("shared_wgt_group_id", "shared_wgt_group_logic_set_id")]
    weight_group_df[, weight_group := shared_wgt_group_id]
    weight_group_df <- weight_group_df[, .(weight_group, restrictions)]
    
    # dummy variable to get a cartesian merge
    weight_group_eval <- merge(weight_group_df[, global := "G"], proportion_metadata, by = "global", allow.cartesian = T)
    
    eval_col <- sapply(c(1:nrow(weight_group_eval)), function(i){
      result <- eval(parse(text = weight_group_eval[i, restrictions]))
      return(result)
    })
    
    weight_groups <- weight_group_eval[eval_col, .(proportion_id, weight_group)]
    if(nrow(weight_groups) == 0){
      print("There are no weight groups for this package - will be null!")
    }
    return(weight_groups)
  
    
  }
  
  get_weights <- function(p){
    
    # note - the new weight packages don't have 0 weights included
    wgt <- as.data.table(arrow::read_parquet(paste0(package_version_code, "/", p,"/wgt.parquet")))
  
    setnames(wgt, c("shared_wgt_group_id", "wgt", "shared_group_id"),
             c("weight_group", "weight", "target_group"))
    
    return(wgt)
    
  }
  
  #---------------------------------------
  # step 5.  # Subset data split to just target codes  
  #---------------------------------------
  make_map <- function(new_data, proportion_metadata, p, cause_map_evaluated, create_targets = F){
    
    metadata <- rjson::fromJSON(file = paste0(package_version_code, p, "/metadata.json"))
    targets <- as.data.table(arrow::read_parquet(paste0(package_version_code, p,"/target.parquet")))
    
    # get weights for the package
    weights <- get_weights(p)
    weight_groups <- get_weight_groups(p, proportion_metadata)
    # merge proportion_id onto the weights table using weight_groups
    weights <- merge(weights, weight_groups, by = "weight_group", allow.cartesian = T)
    
    # Check
    if(nrow(weight_groups) == 0 ){
      print("no weight groups for this package, will skip")
      return()
    }
    
    # identify target codes in the data, by target group and proportion_id
    setnames(targets, "shared_group_id", "target_group")
    
    data_freq_tcs <- rbindlist(lapply(unique(targets$target_group), function(t){
      
      sub_data <- new_data %>%
        filter(cause %in% targets[target_group==t, icd_code]) %>%
        mutate(target_group = t)
      
      return(sub_data)
    }))
    
    # make a list of all possible target codes by target group and proportion_id
    # if create_targets ==1, then make those codes with small frequencies
    if(create_targets){
      print(paste0("Creating targets for package ", p))
    }
    
    all_tcs <- rbindlist(lapply(unique(targets$target_group), function(t){
      
      prop_df <- proportion_metadata %>% select(proportion_id) %>% as.data.frame()
      
      if(create_targets | nrow(targets[icd_code %in% rdp_target_map$icd_code] > 0)){
        comb <- expand.grid(proportion_id = prop_df$proportion_id, 
                            cause = targets[target_group==t, icd_code], 
                            freq =  0.001,
                            target_group = t)
      }else{
        comb <- expand.grid(proportion_id = prop_df$proportion_id, 
                            cause = targets[target_group==t, icd_code], 
                            freq = 0,
                            target_group = t)
      }
  
      return(comb)
    }))
    
    # combine data frequencies with all tcs
    sub_data <- rbind(data_freq_tcs, all_tcs)
    
    # merge on cause restrictions
    sub_data <- left_join(sub_data,
                          cause_map_evaluated,
                          by = c("cause", "proportion_id"),
                          copy = T)
    
    # if code isn't in cod eval list (NA), consider restriction NOT satisfied
    sub_data <- sub_data %>% 
      mutate(restrictions_met = ifelse(is.na(restrictions_met), FALSE, restrictions_met))
    
    # if restrictions are not met, replace frequency with 0 so no data is redistributed to it
    sub_data <- sub_data %>%
      mutate(freq = ifelse(restrictions_met == FALSE, 0, freq))
    
    # sum frequency by proportion_id & target_group
    sub_data <- sub_data[, .(freq = sum(freq)), by = c("proportion_id", "cause", "target_group")]
    
    # add weight groups on to data by expanding with proportion_ids
    sub_data <- inner_join(sub_data, 
                           weights, 
                           by = c("proportion_id", "target_group"),
                           copy = T)
    
    # calculate total frequency across weight + target groups
    sub_data <- as.data.table(sub_data)
    sub_data[, total := sum(freq), by = c("weight_group", "target_group", "proportion_id")]
    
    # If the total proportion for a given proportion id
    #  is 0, move to the residual code
    resid_add <- sub_data[, .(total_prop_id = sum(total)), by = "proportion_id"]
    resid_add <- resid_add[total_prop_id == 0, .(proportion_id)]
    resid_add[, `:=` (cause = residual_code, freq = 1, total = 1, weight = 1)]
    sub_data <- rbind(sub_data, resid_add, fill = T)
    sub_data <- sub_data[freq!=0]
    
    # calculate probability!
    sub_data[, probability := (freq/total)*weight]
    
    # make sure everything sums to 1
    #    if TGs are missing, this will upweight other TGs rather than replace with residual code
    sub_data[, probability := probability/sum(probability), by = "proportion_id"]
    
    # pull into memory for easier checking
    map <- as.data.table(sub_data)
    map <- map[probability!=0]
    
    # in case a code is in multiple target groups, sum up by code + proportion_id
    map <- map[, .(freq = sum(freq), 
                    probability = sum(probability)), by = c("cause", "proportion_id", "weight_group")]   # Q: any reason to not just remove weight group here?
    
    # Two checks just to make sure code is behaving as expected
    
    # check that probabilities sum to 1 for each proportion_id
    check1 <- map[, .(total_prob = sum(probability)), by = c("weight_group", "proportion_id")]
    
    if(nrow(check1[round(total_prob, digits = 4) != 1]) > 0 ){
      warning("Looks like weights don't add up")
      print("   Looks like weights don't add up")
    }
    
    # check that every proportion_id has at least 1 target_code
    prop_metadata <- as.data.table(proportion_metadata)
    check2  <- setdiff(prop_metadata$proportion_id, map$proportion_id)
    
    if(length(check2) > 0 ){
      warning("There isn't a TC for all proportion_ids")
      print("   There isn't a TC for all proportion_ids")
    }
    
    # reduce map before returning
    map <- merge(map, prop_metadata, by = "proportion_id")
     
    return(map)
    
  }
  
  #---------------------------------------
  # step 6. Make map by package
  #---------------------------------------
  run_save_map <- function(p){
    
    start_time <- Sys.time()
    print("-----------------")
    print(paste0("On package ", which(package_map==p), "/", length(package_map), ": ", p))
    
    map <- make_map(new_data, proportion_metadata, p, cause_map_evaluated)
    
    # check if map is null, skip if so
    if(is.null(map)){
      print("map is null!")
      return()
    }
    
    # if only one row (`asr`) then rerun with make targets
    if(nrow(map)==1 & sum(map$cause=='asr')){
      
      map <- make_map(new_data, proportion_metadata, p, cause_map_evaluated, create_targets = T)
    }
    
    # reduce to what is worth saving
    if(by_state){
      map <- map[,.(cause, freq, probability, state_name, year_id, sex_id, age_group_id, proportion_id)]
    }else{
      map <- map[,.(cause, freq, probability, year_id, sex_id, age_group_id, proportion_id)]
    }
   
    setnames(map, c("cause"), c("trunc_code"))
    
    # add on package name and code system
    map[, package := p]
    map[, toc := params$toc]
    map[, age_group_years_start := demo$age_group_years_start]
    
    return(map)
  }
  
  # load in list of packages and make all the maps
  package_map <- rjson::fromJSON(file = paste0(package_version_code, "/_package_list.json"))
  all_maps <- rbindlist(lapply(package_map, run_save_map))

  # for now need to add source
  all_maps[, source:= params$source]
  all_maps[, code_system := code_system]
  
  arrow::write_dataset(all_maps, path = paste0(map_version, "/maps/"), partitioning = c("toc", "source",  "year_id", "code_system", "age_group_years_start", "sex_id"))
  
  #
  
  print("Done with all packages for this data split")
  print(Sys.time() - t0)
  
  rm(all_maps)
  rm(sub_data)

}
