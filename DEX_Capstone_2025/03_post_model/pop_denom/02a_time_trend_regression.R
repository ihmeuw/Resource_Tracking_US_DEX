# ---------------------------------------------
#  For ACS population denominators; backcast time trend to 2000
#
#  Author: Haley Lescinsky
#
# ------------------------------------------------

# LOAD PACKAGES
#----------------------------
library(arrow)
library(data.table)
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, lme4, parallel, stringr, ggplot2, splines)
library(lbd.loader, lib.loc = 'FILEPATH')
source('/FILEPATH/get_age_metadata.R')
source('/FILEPATH/get_population.R')
load("/FILEPATH/states.RData")
load("/FILEPATH/merged_counties.RData")
set.seed(123)

# SCRIPT SETTINGS
#--------------------------
if(interactive()){
  work_dir <- "FILEPATH"
  plot <- T
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  work_dir <- args[1]
  plot <- args[2]
}

if(plot){
  plot_counties <- c()
}else{ 
  plot_counties <- c()}

save_dir <- paste0(work_dir, "/FILEPATH/")
if(!dir.create(save_dir)){dir.create(save_dir)}

# BRING IN DATA SOURCES
#--------------------------
uninsured <- fread(paste0(work_dir, "/FILEPATH.CSV"))
private_insurance <- fread(paste0(work_dir, "/FILEPATH.CSV"))
mdcd <- fread(paste0(work_dir, "/FILEPATH.CSV"))
mdcr_priv <- fread(paste0(work_dir, "/FILEPATH.CSV"))

# ACS + CPS (state) - all payers
state_rates <- fread(paste0(work_dir, "/FILEPATH.CSV"))
state_rates <- dcast(state_rates, year_id+state_name+sex_id+age_group_name~payer, value.var = "scaled_rate")
# reshape wide
setnames(state_rates, c("mdcd", "mdcr", "priv", "uninsured"),
         c("mdcd_rate", "mdcr_rate", "pri_rate", "uninsured_rate"))

# covariates (county)
covariates <- fread(paste0(work_dir, "/FILEPATH.CSV"))

age_metadata <- fread(paste0(work_dir, "/FILEPATH.CSV")) # reference this with custom age group 85+


make_plot <- function(temp_data, yvar, datavar = NA, datalab = NA){
  
  tmp_data <- copy(temp_data)
  ymax <- max(tmp_data$prediction, na.rm = T)
  ymax <- ifelse(ymax >1, ymax, 1)
  
  plot <- ggplot(tmp_data, aes(x = year_id))+
    geom_line(aes(y = get(yvar), color = "observed"), data = tmp_data[!is.na(get(yvar))])+
    geom_line(aes(y = get("prediction"), color = "predicted"), data = tmp_data[!is.na(prediction)])+
    ylim(0,ymax)+
    facet_wrap(~age_start+sex_id)+theme_bw()+labs(title = paste0(unique(tmp_data$state_name), "-", unique(tmp_data$mcnty)), subtitle = "year trend", y = yvar)

  if(!is.na(datavar)){
    plot <- plot + 
      geom_point(aes(y = get(datavar), color = datalab))
  }
  
  print(plot)
  
  plot <- ggplot(tmp_data, aes(x = age_start, linetype = as.factor(sex_id)))+
    geom_line(aes(y = get(yvar), color = "observed"), data = tmp_data[!is.na(get(yvar))])+
    geom_line(aes(y = get("prediction"), color = "predicted"), data = tmp_data[!is.na(prediction)])+
    ylim(0,ymax)+
    facet_wrap(~mcnty+year_id)+theme_bw()+labs(title = paste0(unique(tmp_data$state_name), "-", unique(tmp_data$mcnty)), subtitle = "age trend", y = yvar)
  
  if(!is.na(datavar)){
    plot <- plot + 
      geom_point(aes(y = get(datavar), color = datalab))
  }
  
  print(plot)
  
  
  
  return(plot)
}


#--------
# Investigate that there are estimates for all counties
#--------

private_insurance <- unique(private_insurance[!is.na(priv_rate_step1), .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, priv_rate = priv_rate_step1)])
private_insurance_check <- private_insurance[, .N, by = c("year_id", "sex_id", "age_start", "age_end")]

if(length(unique(private_insurance_check$N)) > 1 ){
  stop("Check counties in private insurance results")
}

if(!setequal(private_insurance$mcnty, counties[current==1]$mcnty)){
  stop("Check counties in private insurance results")
}

mdcr_priv <- unique(mdcr_priv[!is.na(mdcr_priv_rate_step1), .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, mdcr_priv_rate = mdcr_priv_rate_step1)])
mdcr_priv_check <- mdcr_priv[, .N, by = c("year_id", "sex_id", "age_start", "age_end")]

if(length(unique(mdcr_priv_check$N)) > 1 ){
  stop("Check counties in mdcr private insurance results")
}

if(!setequal(mdcr_priv$mcnty, counties[current==1]$mcnty)){
  stop("Check counties in mdcr private insurance results")
}

uninsured <- unique(uninsured[!is.na(unins_rate_step1), .(state_name, mcnty, year_id, sex_id, age_group_name, unins_rate = unins_rate_step1)])
uninsured[unins_rate >1 | unins_rate < 0, unins_rate := ifelse(unins_rate < 0, 0, 1) ]
uninsured_check <- uninsured[, .N, by = c("year_id", "sex_id")]

if(!setequal(uninsured$mcnty, counties[current==1]$mcnty)){
  stop("Check counties in uninsured results")
}

mdcd <- unique(mdcd[!is.na(mdcd_rate_step1), .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, mdcd_rate = mdcd_rate_step1)])
mdcd[mdcd_rate >1 | mdcd_rate < 0, mdcd_rate := ifelse(mdcd_rate < 0, 0, 1) ]
mdcd_check <- mdcd[, .N, by = c("year_id", "sex_id", "age_start", "age_end")]

if(length(unique(mdcd_check$N)) > 1 ){
  stop("Check counties in MDCD results")
}

if(!setequal(mdcd$mcnty, counties[current==1]$mcnty)){
  stop("Check counties in MDCD results")
}

# add mdcd_expansion to state rates
state_rates <- merge(state_rates, unique(covariates[,.(year_id, state_name, mdcd_expanded)]), by = c("year_id", "state_name"))

#--------
# TIME TREND
#--------

time_trend_regression_FE <- function(df, df_pred, state, reg_dep, reg_indep, reg_pred_name, plot = F){
  
  tmp_data <- df[state_name == state]
  tmp_data_pred <- df_pred[state_name == state]
  
  if(state!='District of Columbia'){
  reg_indep <- paste0(reg_indep, "  + as.factor(mcnty)")
  }
  
  if(1 %in% df$mdcd_expanded){
    reg_indep <- paste0(reg_indep, " + mdcd_expanded")
  }
  
  mod <- glm(formula = paste0(reg_dep, " ~ ", reg_indep), data = tmp_data, family = "binomial")
  tmp_data$prediction <- predict(mod, newdata = tmp_data, type = "response")
  tmp_data_pred$prediction <- predict(mod, newdata = tmp_data_pred, type = "response")
  
  setnames(tmp_data_pred, "prediction", reg_pred_name)
  tmp_data[!is.na(get(reg_dep)), paste0(reg_pred_name) := get(reg_dep) ]
  tmp_data[is.na(get(reg_dep)), paste0(reg_pred_name) := prediction ]
  
  plot_counties_within_state <- intersect(tmp_data$mcnty, plot_counties)
  
  if(plot & length(plot_counties_within_state) > 0){
    
    for(c in plot_counties_within_state){
      mcnty_id <- c
      
      if(reg_dep == "mdcr_priv_rate"){
        d <- make_plot(temp_data = tmp_data[mcnty == c], yvar = reg_dep, datavar = "state_rate_priv", datalab = "ACS/CSP state priv rate")
        d <- make_plot(temp_data = tmp_data[mcnty == c], yvar = reg_dep, datavar = "state_rate_mdcr", datalab = "ACS/CSP state mdcr rate")
      }else{
        d <- make_plot(temp_data = tmp_data[mcnty == c], yvar = reg_dep, datavar = "state_rate", datalab = "ACS/CSP state rate")
        
      }
    }
    

    
  }
  
  return(tmp_data_pred)
  
}


get_square_grid <- function(df, age_cols){
  
  county_list <- unique(counties[current == 1, .(mcnty, state_name)])
  sex_id <- c(1,2)
  age_groups <- unique(df[, c(age_cols), with = F])
  year_id <- 1999:2019

  square_grid <- tidyr::crossing(county_list, sex_id, age_groups, year_id) %>% as.data.table()
  
   return(square_grid) 
}


#
# private
#
age_sex_priv <- get_square_grid(private_insurance, age_cols = c("age_group_name", "age_start", "age_end"))
state_rates_priv <- merge(state_rates, age_sex_priv, by = c("state_name", "year_id", "sex_id", "age_group_name"))
state_rates_priv <- state_rates_priv[,.(state_name, year_id, age_group_name, age_start, age_end, sex_id, mcnty,  state_rate = pri_rate, mdcd_expanded)]
private_insurance <- merge(private_insurance, state_rates_priv, by = c("state_name", "year_id", "age_group_name", "age_start", "age_end", "sex_id", "mcnty"), all = T)

if(plot){
  pdf(paste0(save_dir, "/private_insurance_counties.pdf"), width = 12, height = 8)
}


private_insurance_data <- rbindlist(lapply(unique(private_insurance$state_name), function(i){
  print(i)
  plot_cnty <- T
  time_trend_regression_FE(df = private_insurance, df_pred = state_rates_priv, state = i, reg_dep = "priv_rate", reg_indep = "state_rate + age_group_name*sex_id", reg_pred_name ="priv_rate_step2", plot = plot_cnty)
}))

if(plot){
  dev.off()
}

write.csv(private_insurance_data, paste0(save_dir, "/private_insurance_data.csv"), row.names = F)


#
# uninsured
#

age_sex_unins <- get_square_grid(uninsured, age_cols = c("age_group_name"))
state_rates_unins <- merge(state_rates, age_sex_unins, by = c("state_name", "year_id", "sex_id", "age_group_name"))
state_rates_unins <- state_rates_unins[,.(state_name, year_id, age_group_name, sex_id, mcnty, state_rate = uninsured_rate)]
uninsured <- merge(uninsured, state_rates_unins, by = c("state_name", "year_id", "age_group_name", "sex_id", "mcnty"), all = T)
uninsured <- merge(uninsured, age_metadata[, .(age_group_name, age_start = age_group_years_start)], by = "age_group_name")

if(plot){
  pdf(paste0(save_dir, "/uninsured_counties.pdf"), width = 12, height = 8)
}

uninsured_data <- rbindlist(lapply(unique(uninsured$state_name), function(i){
  print(i)
  plot_cnty <- T
  time_trend_regression_FE(df = uninsured, df_pred = state_rates_unins, state = i, reg_dep = "unins_rate", reg_indep = "state_rate + age_group_name*sex_id", reg_pred_name ="unins_rate_step2", plot = plot_cnty)
}))


if(plot){
  dev.off()
}

write.csv(uninsured_data, paste0(save_dir, "/uninsured_data.csv"), row.names = F)

#
# mdcd
#

age_sex_mdcd <- get_square_grid(mdcd, age_cols = c("age_group_name", "age_start", "age_end"))
state_rates_mdcd <- merge(state_rates, age_sex_mdcd, by = c("state_name", "year_id", "sex_id", "age_group_name"))
state_rates_mdcd <- state_rates_mdcd[,.(state_name, year_id, age_group_name, age_start, age_end, sex_id, mcnty, state_rate = mdcd_rate)]
mdcd <- merge(mdcd, state_rates_mdcd, by = c("state_name", "year_id", "age_group_name", "age_start", "age_end", "sex_id", "mcnty"), all = T)


if(plot){
  pdf(paste0(save_dir, "/mdcd_counties.pdf"), width = 12, height = 8)
}

mdcd_data <- rbindlist(lapply(unique(mdcd$state_name), function(i){
  print(i)
  plot_cnty <- T
  time_trend_regression_FE(df = mdcd, df_pred=state_rates_mdcd, state = i, reg_dep = "mdcd_rate", reg_indep = "state_rate + age_group_name*sex_id", reg_pred_name ="mdcd_rate_step2", plot = plot_cnty)
}))



if(plot){
  dev.off()
}

write.csv(mdcd_data, paste0(save_dir, "/mdcd_data.csv"), row.names = F)


#
# mdcr-priv
#

age_sex_mdcr_priv <- get_square_grid(mdcr_priv, age_cols = c("age_group_name", "age_start", "age_end"))
state_rates_mdcr_priv <- merge(state_rates, age_sex_mdcr_priv, by = c("state_name", "year_id", "sex_id", "age_group_name"))
state_rates_mdcr_priv <- state_rates_mdcr_priv[,.(state_name, year_id, age_group_name, age_start, age_end, sex_id, mcnty, state_rate_mdcr = mdcr_rate, state_rate_priv = pri_rate)]
mdcr_priv <- merge(mdcr_priv, state_rates_mdcr_priv, by = c("state_name", "year_id", "age_group_name", "age_start", "age_end", "sex_id", "mcnty"), all = T)


if(plot){
  pdf(paste0(save_dir, "/mdcr_priv_counties.pdf"), width = 12, height = 8)
}

# We use both mdcr and private rate here, but because of some instability in coefficients, we use the sum of the two rates instead of including them separately
mdcr_priv[, state_rate_mdcrpriv := state_rate_mdcr + state_rate_priv]
state_rates_mdcr_priv[, state_rate_mdcrpriv := state_rate_mdcr + state_rate_priv]

mdcr_priv_data <- rbindlist(lapply(unique(mdcr_priv$state_name), function(i){
  print(i)
  plot_cnty <- T
  time_trend_regression_FE(df = mdcr_priv, df_pred=state_rates_mdcr_priv, state = i, reg_dep = "mdcr_priv_rate", reg_indep = "state_rate_mdcrpriv + age_group_name*sex_id", reg_pred_name ="mdcr_priv_rate_step2", plot = plot_cnty)
}))



if(plot){
  dev.off()
}

write.csv(mdcr_priv_data, paste0(save_dir, "/mdcr_priv_data.csv"), row.names = F)


