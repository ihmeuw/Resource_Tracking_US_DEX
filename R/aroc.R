## Purpose: Calculate AROC (in percentage space)
## Owner: USERNAME

library(data.table)

aroc <- function(
  data,               ## data.table on which to perform aroc calculation
  yrs,                ## c(start_year, end_year)
  yr_col = "year_id", ## name of year column
  val_col,            ## which column to perform AROC calculation on
  id_cols             ## vector of variables names (to run the calculation "by") 
){  
  
  if(!"data.table" %in% class(data)){
    stop("data needs to be a data.table")
  }
  
  ## calculate AROC
  dt <- data[order(get(yr_col))]
  dt <- dt[get(yr_col) %in% yrs, .(
    yr = get(yr_col),
    aroc_yrs = paste0(yrs, collapse = "-"),
    aroc = ((get(val_col) / data.table::shift(get(val_col)))**(1 / (get(yr_col) - data.table::shift(get(yr_col)))) - 1)*100
  ), 
  by = id_cols
  ][yr == max(yr), -"yr"]

  return(dt)
}

