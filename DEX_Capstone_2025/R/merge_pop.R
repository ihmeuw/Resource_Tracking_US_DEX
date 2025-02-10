


merge_pop <- function(data, yrs, geos, by_race){
  
  if ('location' %in% names(data)){
    message('Renaming `location` col to `state` for function operation. If `location` represents something other than states, double check this is what you want.')
    setnames(data,'location','state')
  }
  loc_key <- fread("/FILEPATH/merged_counties.csv")[current == 1][,.(mcnty,state_name)] %>% unique() ##there are some mcntys with multiple county_names so have to do a unique here
  st_abbrevs <- fread("/FILEPATH/states.csv")[,.(state_name,state=abbreviation)]
  loc_key <- merge(loc_key, st_abbrevs)
  if (by_race == F){
    pop <- fread("/FILEPATH/pop_age_sex.csv")[year_id %in% yrs][geo == 'county']
    setnames(pop,'location','mcnty')
    pop$mcnty <- as.integer(pop$mcnty)
    pop$state <- NULL
  }else{
    pop <- fread("/FILEPATH/county_population_age_sex_race.csv")[year_id %in% yrs]
  }
  
  pop <- merge(pop, loc_key, by = "mcnty", all.x=T) 
  
  ## Population will automatically be aggregated based on the most granular stratifications in your input data
  gcols <- names(data)[names(data) %in% c("state", "age_group_years_start", "sex_id", "year_id", "race_cd")] 
  
  pop_dt <- data.table()
  if ('county' %in% geos){
    gcols <- c(gcols, 'mcnty')
    
    county_pop <- pop %>%
      group_by( across(all_of(gcols)) ) %>%
      summarize(pop = sum(pop)) %>%  as.data.table()
    pop_dt <- rbind(pop_dt, county_pop)
  }
  if ('state' %in% geos){
    state_pop <- pop %>%
      group_by( across(all_of(gcols)) ) %>%
      summarize(pop = sum(pop)) %>%  as.data.table()
    pop_dt <- rbind(pop_dt, state_pop)
  }
  if ('national' %in% geos){
    us_pop <- pop %>%
      mutate(state = 'USA') %>%
      group_by( across(all_of(gcols)) ) %>%
      summarize(pop = sum(pop)) %>% as.data.table()
    pop_dt <- rbind(pop_dt, us_pop)
  }
  

  merged <- merge(data, pop_dt, all.x = T)
  setnames(merged, 'state','location')
  
  return(merged)
} 