##----------------------------------------------------------------
## Title: function_pull_data.R
## Purpose:  function to pull modeled results for use in compile
## 
## 
## Authors: Azalea Thomson and Haley Lescinsky
## Last update: 12/9/24
##----------------------------------------------------------------


pull_data <- function(dir, geog, care, yr, mets, locs){

  full_data <- rbindlist(lapply(mets, function (met){
    
    dt <- arrow::open_dataset(paste0(dir,'geo=',geog,'/toc=',care,'/metric=',met,'/'))
    if (interactive()){
      dt <- dt %>% filter(location %in% locs)
    }
    

    data <- dt %>% 
      filter(year_id == yr)%>%
      select(c(get('draw_col',envir=.GlobalEnv),pri_payer,payer, acause,location,year_id,age_group_years_start,sex_id,state)) %>%
      rename(val = get('draw_col',envir=.GlobalEnv)) %>%
      as.data.table()
    
    data[,n:= .N, by = c('pri_payer','payer','acause','location','year_id','age_group_years_start','sex_id','state')]
    if (data[n>1, .N] >0){
      stop('Duplicates present. Stopping.')
    }
      
    
  
    if (NA %in% unique(data$age_group_years_start) | "NA" %in% unique(data$age_group_years_start) ){
      stop('Unexpected ages in data. Stopping.')
    }

    data$n <- NULL
    data[, `:=` (geo = geog,
                 toc = care,
                 metric = met,
                 draw_num = draw_num,
                 year_id = yr)]
    return(data)
    
  }))
  
  ## CHECK FOR Infs IN MOD DATA
  if ( full_data[val == Inf,.N] >0 ){
    stop('Data has infinite vals. Stopping.')
  }

  return(full_data)
}




