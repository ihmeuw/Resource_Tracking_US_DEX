################################################
#' @description Write config file for Economics census STGPR for DENTAL
################################################
library(data.table)
rm(list = ls())

## *************************************************
notes <- "Extending to 1990."
## *************************************************

## set working directory to write config
setwd("FILEPATH")

## set type
type <- "dental"

##===========================##
## Read in or create config  ##
##===========================##

## check for exsiting config and read in if it exists; if not create empty data table

if(paste0(type, ".csv") %in% list.files("./02_config")) {
  config <- fread(paste0("./02_config/", type, ".csv"))
} else {
  config <- data.table()
}


##====================##
## Writing new config ##
##====================##

## a bunch of variables for the STGPR config - some of which are required
## check STGPR HUB page for required variables

## MAKE SURE THERE ARE NO DUPLICATE model_index_id's in config
model_index_id <- ifelse(is.infinite(max(config$model_index_id)), 1, max(config$model_index_id) + 1)


me_name <- "ec_interpolating" ## required
modelable_entity_name <- "Economic Census interpolating"
modelable_entity_id <- 24920 

data_transform <- "none" ## required

year_start <- 1990 ## required
year_end <- 2019 ## required
location_set_id<-22
prediction_units <-paste0("$ per capita")
description<-paste0("Economic Census (", type ,")")

st_lambda <- "0.5" ## required
st_omega <- "0.05" ## required
st_zeta <- "0.001" ## required

gpr_scale <- "10" ## required

rake_logit <- 0

decomp_step <- 'iterative' ## required

path_to_data <- paste0(getwd(), "/01_input/", type, ".csv") ## required
path_to_custom_covariates <- paste0(getwd(), "/01_input/covariates.csv")
stage_1_model_formula <- paste0("data ~ year_id + cv_population_thou + cv_wages_", type, " + (1|location_id)")
gbd_round_id <- 7
gpr_draws <- 1000

## aggregate the states UP to the USA, instead of raking. 
agg_level_4_to_3 <- 102

##===============================================##
## Bind row containing above variables to config ##
##===============================================##

## get all variable names except for config
vars <- ls()[!ls() == "config"]

## get list of variables
v_list <- mget(vars)

## bind all variables together and set names
new_data <- rbindlist(lapply(v_list, data.table))
new_data[, names := vars]

## get variables wide (appropriate format for config)
new_data <- data.table(dcast(new_data, . ~ names, value.var = "V1"))
new_data[, . := NULL] ## drop column that is creating when getting variables wide

## bind to config
config <- rbindlist(list(config, new_data), use.names = T, fill = T)

## set order
setcolorder(config, c("type", "model_index_id", "notes"))

##================================================##
## Check for duplciate model_index_id's and write ##
##================================================##

if (!any(duplicated(config$model_index_id))) {
  fwrite(config, paste0("./02_config/", type, ".csv"))
} else {
  warning("DUPLICATE MODEL_INDEX_ID'S. Check config.")
}
