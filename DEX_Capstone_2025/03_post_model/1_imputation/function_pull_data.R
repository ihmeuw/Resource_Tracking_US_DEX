## PULL DATA FOR MEDIAN CALCULATION

pull_data <- function(level, dt, care, met, pp, p, draw_col){
  
  if (level ==3){
    summary_data <- dt %>% 
      rename(val = get('draw_col',envir=.GlobalEnv)) %>%
      filter(toc == care &
               pri_payer == pp &
               payer == p) %>% 
      group_by(sex_id) %>%
      summarise(min = min(val, na.rm = T),
                max = max(val , na.rm = T),
                med = median(val, na.rm = T),
                total_n = n()) %>% collect() %>%
      as.data.table()
    
  }
  
  if (level ==2){
    summary_data <- dt %>% 
      filter(toc == care &
               pri_payer == pp &
               payer == p) %>% 
      rename(val = get('draw_col',envir=.GlobalEnv)) %>%
      group_by(sex_id,acause) %>%
      summarise(min = min(val, na.rm = T),
                max = max(val , na.rm = T),
                med = median(val, na.rm = T),
                total_n = n()) %>% collect() %>%
      as.data.table()

  }
    
  if (level == 1){
      summary_data <- dt %>% 
        filter(payer == p) %>%
        rename(val = get('draw_col',envir=.GlobalEnv)) %>% 
        group_by(sex_id, acause) %>%
        summarise(min = min(val, na.rm = T),
                  max = max(val , na.rm = T),
                  med = median(val, na.rm = T),
                  total_n = n()) %>% collect() %>%
        as.data.table()
      
      
  }
  
  return(summary_data)
}





