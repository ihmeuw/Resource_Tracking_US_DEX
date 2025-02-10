## ==================================================
## Author(s): Sawyer Crosby, Max Weil
## Date: Jan 31, 2025
## Purpose: Function to replace NULL type with string type in parquet dataset schema
## ==================================================

library(arrow)

update_nulls_schema <- function(filepath){
  
  data_schema <- arrow::open_dataset(filepath)$schema
  null_idxs <- data_schema$names[unlist(lapply(data_schema$names, function(x) data_schema[[x]]$type == arrow::null()))]
  for(i in null_idxs){
    data_schema[[i]] <- arrow::string()
  }
  return(data_schema)
  
}
