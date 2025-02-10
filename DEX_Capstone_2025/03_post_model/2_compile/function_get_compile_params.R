##----------------------------------------------------------------
## Title: function_get_compile_params.R
## Purpose: Make list of parameter combinations to parallelize compile over 
## 
## 
## Authors: Azalea Thomson
## Last update: 12/9/24
##----------------------------------------------------------------



get_compile_params <- function(scaling_version,
                               mset, model_set_data, tocs, geog, draws, n_draws, 
                               min_model_year, max_model_year,
                               add_mem_sets = F, rake_state_to_national){  
  
  years <- min_model_year:max_model_year
  
  states <- fread('FILEPATH')
  states <- unique(states$abbreviation)

  
  if (draws==T){
    draw_nums <- c(0:n_draws)
    print(paste0("running with ", n_draws, " draws!"))
  }else{
    draw_nums <- 0
  }
  
  meta <- expand.grid(geog = g,
                      care = as.character(tocs),
                      yr = years,
                      draw_num = draw_nums) %>% as.data.table()

  copy_msd <- copy(model_set_data)
  setnames(copy_msd,'geographic_granularity','geog')
  
  # use model set for spend and vol
  compile_params <- copy(meta)
  mv_spend <- mset
  mv_vol <- mset

  compile_params[, `:=`(sv = scaling_version,
                        mset_id = mset,
                        mv_spend = mv_spend,
                        mv_vol = mv_vol
                        )]

  if (geog == 'county'){
    compile_params[,mem_set:=ifelse(care == 'DV', '10G','150G')]
  }

  compile_param_path <- paste0(param_dir,'compile_',geog,'.csv')
  print("Got params")
  return(list(compile_params, compile_param_path))
}


