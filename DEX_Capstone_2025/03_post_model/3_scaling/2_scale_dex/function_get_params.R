
get_agg_params <- function(tocs, draws, n_draws, run_only_national){
  
  if (draws==T){
    draw_nums <- c(0:n_draws)
  }else{
    draw_nums <- 0
  }
  
  agg_params <- expand.grid(draw = draw_nums,
                             geog='state',
                             toc=tocs,
                             compile_dir = compile_dir,
                             agg_dir = agg_dir)%>% as.data.table()
  
  if (run_only_national == T){
    agg_params[,geog:='national']
  }
  agg_params_path <- paste0(param_dir,'/agg_params.csv')
  print("Got params")
  return(list(agg_params, agg_params_path))
}

get_ratio_params <- function(){
  ratio_params <- expand.grid(toc=tocs,
                             agg_dir = agg_dir,
                             shea_path = shea_path,
                             ratio_path = ratio_path)%>% as.data.table()
  ratio_params_path <- paste0(param_dir,'/ratio_params.csv')
  print("Got params")
  return(list(ratio_params, ratio_params_path))
}

get_apply_params <- function(geog, toc_cause_restrictions_path, save_draws = F,run_at_draw_level, tocs, years){
  
  cl <- fread(toc_cause_restrictions_path)[include == 1 & gc_nec == 0] 
  
  if (geog %in% c('county','state')){
    states <- fread('FILEPATH')
    states <- states$abbreviation
  }else{
    states <- 'USA'
  }

  apply_params <- expand.grid(state=states,
                              year_id = years,
                              geog = geog,
                              toc = tocs,
                              compile_dir = compile_dir,
                              ratio_path = ratio_path,
                              shea_path = shea_path,
                              final_dir = final_dir,
                              final_collapse_dir = final_collapse_dir,
                              param_dir = param_dir,
                              save_draws = save_draws) %>%as.data.table()
  if (geog == 'county'){
    apply_params <- crossing(apply_params,
                             payer = 'oop') %>% as.data.table()
  }

  
  apply_param_path <- paste0(param_dir,'apply_',geog,'.csv')
  print("Got params")
  return(list(apply_params, apply_param_path))
  
}

get_plot_params <- function(geog, tocs, missings_dir, scaling_version){
  

  if (geog %in% c('county','state')){
    states <- fread('FILEPATH')
    states <- states$abbreviation
  }else{
    states <- 'USA'
  }
  
  plot_params <- expand.grid(sv = scaling_version,
                             st=states,
                              geog = geog,
                              care = tocs,
                              missings_dir = missings_dir) %>%as.data.table()
  
  plot_param_path <- paste0(param_dir,'plot_',geog,'.csv')
  print("Got params")
  return(list(plot_params, plot_param_path))
  
}


