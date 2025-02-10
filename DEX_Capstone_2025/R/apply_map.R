## ==================================================
## Author(s): Sawyer Crosby, Haley Lescinsky
## Date: Jan 31, 2025
## Purpose: Function to apply a probability map to data
## ==================================================

library(data.table)

## NOTE, the function adds a 'mapped' column in place by reference. `my_data` is modified directly
apply_map <- function(
  my_data, ## data table
  my_map, ## map
  from_col, ## column in dt and map
  to_col, ## column in map, to create in dt
  prob_col ## proportion in map
){
  
  if(!from_col %in% names(my_data)) stop(paste0("from_col '", from_col, "' not found in data"))
  if(!from_col %in% names(my_map)) stop(paste0("from_col '", from_col, "' not found in map"))
  if(!to_col %in% names(my_map)) stop(paste0("to_col '", to_col, "' not found in map"))
  if(!prob_col %in% names(my_map)) stop(paste0("prob_col '", prob_col, "' not found in map"))
  
  lapply(
    my_data[,unique(get(from_col))], 
    function(x){
      n <- my_data[get(from_col) == x, .N]
      props <- my_map[get(from_col) == x]
      my_data[
        get(from_col) == x, 
        (to_col) := props[,sample(get(to_col), size = n, prob = get(prob_col), replace = T)]
      ]
    }
  )
}
