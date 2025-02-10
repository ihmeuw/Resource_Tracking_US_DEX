##----------------------------------------------------------------
## Title: 4_apply_ratios.R
## Purpose: Scale county and state DEX estimates by SHEA ratios
## Authors: Haley Lescinsky, Azalea Thomson
##----------------------------------------------------------------

t0 <- Sys.time()

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH')
library(configr, lib.loc = "FILEPATH")
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr)
library(lbd.loader, lib.loc = sprintf("FILEPATH", 
                                      R.version$major, 
                                      strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]))
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

## ARGS
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
run_at_draw_level <- args[2]
m <- args[3]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
if("mem_set" %in% colnames(tasks)){
  tasks <- tasks[mem_set == m]
}
tasks <- tasks[task_id,]
yr <- tasks$year_id
geog <- tasks$geog
if (geog == 'county'){
  pay <- tasks$payer
}
care <- tasks$toc
compile_dir <- tasks$compile_dir
ratio_path <- tasks$ratio_path
shea_path <- tasks$shea_path
final_dir <- tasks$final_dir
final_collapse_dir <- tasks$final_collapse_dir
st <- tasks$state
save_draws <- tasks$save_draws


##'----------------------------------------------------------------
##' 1. PULL IN DEX COMPILED DATA
##'----------------------------------------------------------------
message('Starting...')

t1 <- Sys.time()
dex <- arrow::open_dataset(paste0(compile_dir,'/geo=',geog,'/toc=',care,'/year_id=',yr,'/')) #, schema = schema)


if (geog == 'county'){
  message('County level so additionally parallelized on payer: ', pay)
  dex <- dex %>% filter(payer ==pay) 
}

dex <- dex %>% 
  filter(state == st) %>%
  as.data.table()
print(Sys.time()-t1)
message('Got dex data')

dex$state <- st
dex$year_id <- yr
if (geog == 'state'){
  dex[,location:=state]
}


##'----------------------------------------------------------------
##' 2. PULL IN SHEA RATIOS (TOC-STATE-YEAR SPECIFIC) AND MERGE WITH DEX
##'----------------------------------------------------------------

ratios <- arrow::open_dataset(paste0(ratio_path,'/toc=',care,'/')) %>% #, schema = schema)
  filter(state==st) %>% 
  filter(year_id==yr) %>%
  as.data.table()

message('Got ratios')

dex_scaled <- merge(dex, ratios,  all.x = T, by = c('payer','year_id','draw','state'))


if(nrow(dex_scaled[is.na(ratio)]) >0){
  print(head(dex_scaled[is.na(ratio), .(state, year_id, ratio,payer,spend)]))
  stop("Error there are some state/year/toc combinations we don't have a ratio for")
}


##'----------------------------------------------------------------
##' 3. APPLY SHEA RATIOS 
##'----------------------------------------------------------------

if(nrow(dex_scaled) > 0 ){
  
  ## Apply SHEA ratios to spend and utilization estimates
  dex_scaled[, spend := round(spend/ratio, digits = 2)]
  dex_scaled[, vol := round(vol/ratio, digits = 7)] 
  
  ## Checks
  if(nrow(dex_scaled[is.infinite(spend)]) >0){
    print(head(dex_scaled[is.infinite(spend), .(state, year_id, ratio,payer,spend)]))
    stop("Error there are some state/year/toc combinations with Inf spend")
  }
  if(nrow(dex_scaled[is.infinite(vol)]) >0){
    print(head(dex_scaled[is.infinite(vol), .(state, year_id, ratio,payer,vol)]))
    stop("Error there are some state/year/toc combinations with Inf vol")
  }
  
  if(nrow(dex_scaled[is.nan(vol)]) >0 | nrow(dex_scaled[is.nan(spend)]) >0){
    print(head(dex_scaled[is.nan(vol), .(state, year_id, ratio,payer,vol)]))
    message("Warning, there are some state/year/toc combinations with NaN vol or spend, setting those to zero")
    dex_scaled[is.nan(vol), vol:=0]
    dex_scaled[is.nan(spend), spend:=0]
  }
  
  if (dex_scaled[vol == 0 & spend > 0, .N] > 0 ){
    message('Warning, you have some rows where volume is zero but spend is not')
  }
  
  if (dex_scaled[vol > 0 & spend == 0, .N] > 0 ){
    message('Warning, you have some rows where volume is not zero but spend is')
  }
  
  if (dex_scaled[vol<0 | spend<0, .N] > 0){
    stop('Warning, you have some rows where volume or spend is negative')
  }
  
  ## Create well newborn from well person age zero
  dex_scaled[acause == 'exp_well_person' & age_group_years_start ==0, acause:= 'exp_well_newborn']
  
  
  ##'----------------------------------------------------------------
  ##' 4. COLLAPSE DATA IF RUNNING AT DRAW LEVEL
  ##'----------------------------------------------------------------
  
  if (run_at_draw_level == T){

    by_cols <- c('sex_id','pri_payer', 'age_group_years_start', 'location','payer') 
    causes <- unique(dex_scaled$acause)
    t2 <- Sys.time()
    results <- lapply(causes, function(cause) {
      dex_scaled[acause == cause, .(
        acause = cause,
        mean_spend = mean(spend),
        lower_spend = quantile(spend, 0.025),
        upper_spend = quantile(spend, 0.975),
        mean_vol = mean(vol),
        lower_vol = quantile(vol, 0.025),
        upper_vol = quantile(vol, 0.975)
      ), by = by_cols] 
    })

    dex_collapsed <- rbindlist(results)
    dex_collapsed[,`:=`(year_id = yr, state = st)]

    print(Sys.time()-t2)
    message('Collpased draws')
    
  }else{ 

    dex_collapsed <- dex_scaled[,.(year_id,sex_id,state,pri_payer,age_group_years_start,payer,acause,location,spend,vol)]
    dex_collapsed <- dex_collapsed[, `:=`(lower_vol = vol,
                                          upper_vol = vol,
                                          lower_spend = spend,
                                          upper_spend = spend)]
    setnames(dex_collapsed,c('vol','spend'), c('mean_vol','mean_spend'))
    
  }
  


  
  ##'----------------------------------------------------------------
  ##' 5. BRING IN OTHER PAYER FROM SHEA AND APPEND (SHEA OTH IS ONLY TOC-STATE-YEAR SPECIFIC)
  ##'----------------------------------------------------------------
  
  if (geog != 'county' | (geog == 'county' & pay == 'mdcr')){
    message('Bringing in OTH from SHEA')
    shea_oth <- fread(shea_path)[payer=='OTH' & toc == care & state == st]
    shea_oth <- shea_oth[,payer := tolower(payer)]
    setnames(shea_oth, c("year","spend"), c("year_id","mean_spend"))
    shea_oth[, mean_spend := mean_spend*1e6] ##multiply to get in millions like the rest of dex estimates
    shea_oth[,`:=` (lower_spend = mean_spend,
                    upper_spend = mean_spend)]
    
    dex_collapsed <- rbind(dex_collapsed, shea_oth, fill=T)
    
    if (run_at_draw_level == T){
      shea_oth[,`:=`(lower_spend=NULL,upper_spend=NULL)]
      setnames(shea_oth,'mean_spend','spend')
      dex_scaled <- rbind(dex_scaled, shea_oth, fill=T)
    }
  }
  
  
  dex_collapsed <- dex_collapsed[order(age_group_years_start, year_id, sex_id)]
  dex_collapsed$geo <- geog
  dex_collapsed$toc <- care
  
  if (run_at_draw_level == T){
    dex_scaled$geo <- geog
    dex_scaled$toc <- care
  }
  
  
  
  ##'----------------------------------------------------------------
  ##' 6. SAVE
  ##'----------------------------------------------------------------
  
  message('Saving out final collapsed data')

  arrow::write_dataset(dex_collapsed, path = paste0(final_collapse_dir, "/data/"), 
                       partitioning = c("geo","toc","state", "payer"),
                       basename_template = paste0("year_",yr,"-{i}.parquet"))
  
  
  ##'----------------------------------------------------------------
  ##' 7. IF RUNNING AT DRAW LEVEL, SAVE DRAWS
  ##' THESE WILL BE AGGREGATED TO CREATE NATIONAL
  ##'----------------------------------------------------------------
  if (run_at_draw_level == T & save_draws == T){
    
    message('Saving out draw data')
    dex_scaled[, ratio := NULL] 
    dex_scaled <- dex_scaled[with(dex_scaled, order(year_id,age_group_years_start,sex_id, draw)), ]

    arrow::write_dataset(dex_scaled, path = paste0(final_dir, "/data/"),
                         basename_template = paste0("year_",yr,"-{i}.parquet"),
                         partitioning = c("geo","toc","state", "payer"))
  }

  
  
}else{
  
  message("No final data, not saving anything!")
  
}


message(Sys.time() - t0)
message('Done')
##----------------------------------------------------------------