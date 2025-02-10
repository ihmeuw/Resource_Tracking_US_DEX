rm(list = ls())
library(data.table)

here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)

## acause choices
a <- fread(paste0(here,"/../../static_files/RESTRICTIONS/toc_cause_age_sex.csv"))
choice_acause <- a[include == 1 & gc_nec == 0, unique(acause)]
choice_acause <- sort(choice_acause)
choice_acause <- c(choice_acause[choice_acause == "cvd_ihd"], choice_acause[choice_acause != "cvd_ihd"])
save(choice_acause, file = "../02_app/choice_acause.RData")

## location choices
state_dt <- fread(paste0(here,"/../../static_files/GEOGRAPHY/state_names.csv"))
states <- state_dt$location_name
counties <- fread(paste0(here,"/../../static_files/GEOGRAPHY/county_names.csv"))
counties <- counties$location_name
choice_location_name <- c("USA", states, counties)
save(choice_location_name, file = "../02_app/choice_location_name.RData")
save(counties, file = "../02_app/county_states.RData")

## location shape files
location_shape <- fread(paste0(here,"/../../static_files/GEOGRAPHY/mcnty_map_shape_df.csv"))
state_shape <- fread(paste0(here,"/../../static_files/GEOGRAPHY/state_map_shape_df.csv"))
load(paste0(here,"/../../static_files/GEOGRAPHY/states.RData"))
states_recode <- states$abbreviation; names(states_recode) <- states[,as.numeric(state)]
state_shape[,id := plyr::revalue(as.character(id), states_recode)]
save(location_shape, file = "../02_app/location_shape.RData")
save(state_shape, file = "../02_app/state_shape.RData")

## cause family 
dx_names <- fread(paste0(here,"/../../static_files/CAUSEMAP/causelist.csv"))
dx_names<-dx_names[acause != '_gc',.(acause,cause_name,family_name,cause_name_lvl1)] 
#Rename long lvl1 name to something shorter
dx_names[cause_name_lvl1 == 'Communicable, maternal, neonatal, and nutritional diseases',
         cause_name_lvl1 := 'Comm/maternal/neonat/nutrition disorders']
#set order we want the acuase level 1s to be in
dx_names[cause_name_lvl1 == 'Non-communicable diseases', group_order := 1]
dx_names[cause_name_lvl1 == 'Comm/maternal/neonat/nutrition disorders', group_order := 2]
dx_names[cause_name_lvl1 == 'Injuries', group_order := 3]
dx_names[cause_name_lvl1 == 'Risk factors', group_order := 4]
dx_names[cause_name_lvl1 == 'Well care', group_order := 5]

#add colors
dx_names[group_order == 1, color := "#56b4e9"] #ncd - blue, #"#cd661d"
dx_names[group_order == 2, color := "#EE4B2B"] #comm - red 
dx_names[group_order == 3, color := "#009E73"] #inj - green 
dx_names[group_order == 4, color := '#BF40BF'] #rf - purple
dx_names[group_order == 5, color := "#cd661d"] #well orange

#Arrange causes and Create number key to go with acause
dx_names<- dx_names %>% arrange(cause_name_lvl1, cause_name)
dx_names$number = seq(1, by = 1, length.out = nrow(dx_names))

tree_key<-dx_names %>% select(number, acause,cause_name, cause_name_lvl1,color)#, family_name
save(tree_key, file = "../02_app/tree_key.RData")
