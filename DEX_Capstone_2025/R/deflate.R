## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Function to inflate/deflate USD 
## ==================================================

deflate <- function(
  data, ## dataset
  val_columns, ## column names of dollar values to deflate (vector)
  old_year = "year_id", ## column name (character) or year value (numeric) of old currency year
  new_year = 2019 ## column name (character) or year value (numeric) of new (desired) currency year
){
  ## get deflators
  deflators <- fread("{repo_root}/static_files/DEFLATORS/best.csv")
  
  ## copy dataset and set as data.table
  DT <- copy(data)
  setDT(DT)

  ## check for val columns
  if(!all(val_columns %in% names(DT))){
    stop("Not all val_columns are column names in data")
  }

  ## currency convert
  if(is.numeric(old_year) & is.numeric(new_year)){
    ## converting from year X to year Y (base to base)
    message(paste("Converting from", old_year, "USD to", new_year, "USD"))
    
    target_year_value <-  deflators[year == new_year, annual_cpi]
    deflators[,deflator_scalar := annual_cpi/target_year_value]
    deflator_scalar_val <- deflators[year == old_year, deflator_scalar]
    DT[,(val_columns) := lapply(.SD, function(x) x/deflator_scalar_val), .SDcols = val_columns]
    
  }else if(is.character(old_year) & is.numeric(new_year)){
    ## converting from year in column to year X (current to base)
    message(paste("Converting from currency year in column", paste0("'", old_year, "'"), "to", new_year, "USD"))
    
    if(!old_year %in% names(DT)){
      stop(paste0("'", old_year, "' not a column name in data"))
    }
    target_year_value <-  deflators[year == new_year, annual_cpi]
    deflators[,deflator_scalar := annual_cpi/target_year_value]
    deflators[,annual_cpi := NULL]
    setnames(deflators, "year", old_year)
    DT <- merge(DT, deflators, by = old_year, all.x = T)
    DT[,(val_columns) := lapply(.SD, function(x) x/deflator_scalar), .SDcols = val_columns]
    DT[,deflator_scalar := NULL]
    
  }else if(is.numeric(old_year) & is.character(new_year)){
    ## converting year X to year in column (base to current)
    message(paste("Converting from", old_year, "USD to currency year in column", paste0("'", new_year, "'")))
    
    if(!new_year %in% names(DT)){
      stop(paste0("'", new_year, "' not a column name in data"))
    }
    target_year_value <- deflators[year == old_year, annual_cpi]
    deflators[,deflator_scalar := target_year_value/annual_cpi]
    deflators[,annual_cpi := NULL]
    setnames(deflators, "year", new_year)
    DT <- merge(DT, deflators, by = new_year, all.x = T)
    DT[,(val_columns) := lapply(.SD, function(x) x/deflator_scalar), .SDcols = val_columns]
    DT[,deflator_scalar := NULL]
  }else{
    stop("Please check that old_year and new_year are specified correctly")
  }
  return(DT)
}
