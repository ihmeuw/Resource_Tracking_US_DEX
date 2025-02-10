## AGE BINNING FUNCTION
# Bins single-year age data into DEX age groups (age_group_set_id 27)
# Takes a df/dt, asssumes that "age" is the name of the single-year age column
# Returns a df of the same size with age_start, age_end and age_group_id columns attached

source('/FILEPATH/get_age_metadata.R')

age_bin <- function(data, age_column_name = "age"){
  dt <- copy(data)
  setnames(dt, age_column_name, "age")
  dtcols <- colnames(dt)
  
  ages <- get_age_metadata(age_group_set_id = 27, release_id = 9)
  ages <- select(ages, age_group_id, age_start = age_group_years_start, age_end = age_group_years_end)
  setorder(ages, age_start)
  
  dt$age_start <- cut(dt$age, breaks = c(ages$age_start, 125), labels = ages$age_start, right = FALSE)
  dt$age_start <- as.integer(as.character(dt$age_start))
  dt <- left_join(dt, ages, by="age_start")

  select(dt, c(all_of(dtcols), colnames(ages)))
  setnames(dt, "age", age_column_name)
  return(dt)
}
