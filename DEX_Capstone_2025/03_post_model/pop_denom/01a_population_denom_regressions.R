# ---------------------------------------------
#  For ACS population denominators, impute counties with low populations with regression (fit reg by state with relevant covs; predict for missing counties)
#
#  Author: Haley Lescinsky
#
# ------------------------------------------------

# LOAD PACKAGES ----------------------------
pacman::p_load(arrow, dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, lme4, parallel, 
               stringr, ggplot2, boot)
library(lbd.loader, lib.loc = 'FILEPATH')
source('/FILEPATH/get_age_metadata.R')
load("/FILEPATH/states.RData")
load("/FILEPATH/merged_counties.RData")
set.seed(3467)

# SCRIPT SETTINGS --------------------------

if(interactive()){
  work_dir <- "/FILEPATH/"
  plot <- F
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  work_dir <- args[1]
  plot <- args[2]
}

save_dir <- paste0(work_dir, "/FILEPATH/")
dir.create(save_dir)

# BRING IN DATA SOURCES --------------------------

# ACS (county) - all payers
acs_county <- fread(paste0(work_dir, "/FILEPATH.CSV"))
acs_county[, cnty_name := NULL]
acs_county <- acs_county[year_id <= 2019]

# SAHIE (county) - insured vs uninsured only
sahie_county <- fread(paste0(work_dir,"/FILEPATH.CSV"))
setnames(sahie_county, c("prop_insured", "prop_uninsured"), c("insured_rate", "uninsured_rate"))

# covariates (county)
covariates <- fread(paste0(work_dir,"/FILEPATH.CSV"))

age_metadata <- fread(paste0(work_dir, "/FILEPATH.CSV")) # reference this with custom age group 85+


# MAIN SCRIPT --------------------------

expand_to_ages <- function(df, collapse_groups = T){
  
  # this function expands age groups out to single years, then assigns DEX age categories
  # means an age group that stratifies multiple will appear in multiple
 
  data_age_list <- unique(df[,.(age_start, age_end)])
  
  age_grid <- data.table()
  for(i in 1:nrow(data_age_list)){
    age_vec <- data_age_list[i, age_start]:data_age_list[i, age_end]
    age_grid <- rbind(age_grid, data.table("age_start" = data_age_list[i, age_start],
                                           "age_end" = data_age_list[i, age_end],
                                           "age" = age_vec))
  }
  
  age_grid_5 <- data.table()
  for(i in 1:nrow(age_metadata)){
    age_vec <- age_metadata[i, age_group_years_start]:(age_metadata[i, age_group_years_end-1])
    age_grid_5 <- rbind(age_grid_5, data.table("age_group_name" = age_metadata[i, age_group_name],
                                               "age" = age_vec))
  }
  
  # add on 5 year age groups to age grid
  age_grid <- merge(age_grid, age_grid_5, by = "age")
  df_big <- merge(df, age_grid, by = c("age_start", "age_end"), allow.cartesian = T)
  
  if(collapse_groups){
    
    df_big[, age := NULL]
    df_big <- unique(df_big)
  }
  
  
  return(df_big)
  
}

make_plot <- function(temp_data, label, yvar, reg_map, datavar = NA, datalab = NA){
  
  tmp_data <- copy(temp_data)
  
  # add county name
  county_names <- counties[current == 1,.(mcnty, cnty_name, state_name)]
  county_names <- county_names[,.(cnty_name = min(cnty_name)), by=c("mcnty","state_name")]
  
  tmp_data <- merge(tmp_data, county_names, by = c("mcnty", "state_name"))
  
  ymax <- max(tmp_data[,get(paste0(yvar, "_pred"))], na.rm = T)
  ymax <- ifelse(ymax >1, ymax, 1)
  
  plot <- ggplot(tmp_data, aes(x = year_id))+
    geom_line(aes(y = get(yvar), color = "observed"), data = tmp_data[!is.na(get(yvar))])+
    geom_line(aes(y = get(paste0(yvar, "_pred")), color = "predicted"), data = tmp_data[!is.na(get(paste0(yvar, "_pred")))])+
    ylim(0,ymax)+
    facet_wrap(~cnty_name)+theme_bw()+labs(title = paste0(label, " - ", reg_map$state_name), 
                                           subtitle = paste0(ifelse(reg_map$sex_id==1, "males", "females"), " - ", reg_map$age_group_name),
                                           x = "", y = yvar)
  if(!is.na(datavar)){
    plot <- plot + 
      geom_point(aes(y = get(datavar), color = datalab))
  }
  
  return(plot)
}



#--------
# FILL IN COUNTIES --------
#--------

# Add on sex + age groups to the covariates, so they merge
cov_demo <- unique(acs_county[, .(state_name, sex_id, age_start, age_end, age_group_name)])
covariates <- merge(covariates, cov_demo, by = "state_name", allow.cartesian = T)
prepped_data <- merge(acs_county, covariates, by = c("state_name", "mcnty", "year_id", "sex_id", "age_start", "age_end", "age_group_name"), all = T)


#
# private --------
#

priv_reg_data <- prepped_data[!is.na(mcnty) & year_id %in% unique(acs_county$year_id)]
priv_reg_maps <- unique(priv_reg_data[!is.na(sex_id),.(state_name, age_group_name, sex_id)])[order(state_name)]

pri_reg <- function(i, plot = F, variant = "logit"){

  print(i)
  reg_map <- priv_reg_maps[i,]
  
  tmp_data <- priv_reg_data[state_name == reg_map$state_name &
                         age_group_name == reg_map$age_group_name &
                         sex_id == reg_map$sex_id]
  
  # Only one county in DC, so don't model it
  if(reg_map$state_name == "District of Columbia"){
    return(tmp_data[, priv_rate_step1 := priv_rate_smooth])
  }
  
  # Don't include dummy var if medicaid has not expanded
  if(1 %in% tmp_data$mdcd_expanded){
    indep_vars <-  "unemployed + income_pc + age65 + mdcd_expanded"
  }else{
    indep_vars <-  "unemployed + income_pc + age65"
  }
  
  # fit model
  if(variant == "logit"){
    mod <- glm(formula = paste0("priv_rate ~", indep_vars) , data = tmp_data, family = binomial())
  }else if(variant == "linear"){
    mod <- lm(formula = paste0("priv_rate ~", indep_vars) , data = tmp_data)
  }
  
  # predict model
  tmp_data$priv_rate_pred <- predict(mod, newdata = tmp_data, allow.new.levels = T, type = "response")
  
  # only plot for numbers divisible by 5
  if(plot & i%%5==0 & reg_map$sex_id==1){
    print(make_plot(tmp_data, "Private insurance rates", "priv_rate", reg_map))
  }
  
  # use prediction where we don't have data, and smoothed where we do
  tmp_data[!is.na(priv_rate_smooth), priv_rate_step1 := priv_rate_smooth]
  tmp_data[is.na(priv_rate_smooth), priv_rate_step1 := priv_rate_pred]
  
  return(tmp_data)
  
}

if(plot){
  pdf(paste0(save_dir, "/FILEPATH.pdf"), width = 12, height = 8)
  pri_data <- rbindlist(lapply(1:nrow(priv_reg_maps), pri_reg, plot = T), fill = T)
  dev.off()
}else{
  pri_data <- rbindlist(lapply(1:nrow(priv_reg_maps), pri_reg), fill =T)
}

pri_data <- pri_data[, .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, priv_rate, priv_rate_step1)]

write.csv(pri_data, paste0(save_dir, "/FILEPATH.CSV"), row.names = F)

#
# mdcr + private --------
#

mdcr_priv_reg_data <- prepped_data[!is.na(mcnty) & year_id %in% unique(acs_county$year_id)]
mdcr_priv_reg_maps <- unique(mdcr_priv_reg_data[!is.na(sex_id),.(state_name, age_group_name, sex_id)])[order(state_name)]

mdcr_pri_reg <- function(i, plot = F, variant = "linear"){
  
  print(i)
  reg_map <- mdcr_priv_reg_maps[i,]
  
  tmp_data <- mdcr_priv_reg_data[state_name == reg_map$state_name &
                              age_group_name == reg_map$age_group_name &
                              sex_id == reg_map$sex_id]
  # drop cols
  c("mdcd_rate",  "mdcr_rate", "priv_rate", "uninsured_rate", "uninsured_rate_smooth",
    "priv_rate_smooth")
  
  # Only one county in DC, so don't model it
  if(reg_map$state_name == "District of Columbia"){
    return(tmp_data[, mdcr_priv_rate_step1 := mdcr_priv_smooth])
  }
  
  # Don't include dummy var if medicaid has not expanded
  if(1 %in% tmp_data$mdcd_expanded){
    indep_vars <-  "unemployed + income_pc + age65 + mdcd_expanded"
  }else{
    indep_vars <-  "unemployed + income_pc + age65"
  }
  
  # low data coverage locations get simpler intercept-only model
  if(reg_map$state_name %in% c("Alaska", "Nevada", "North Dakota", "Vermont", "Wyoming")){
    indep_vars <-  "intercept"
    tmp_data[, intercept := 1]
  }
  
  # fit model
  if(variant == "logit"){
    mod <- glm(formula = paste0("mdcr_priv_smooth ~", indep_vars) , data = tmp_data, family = binomial())
  }else if(variant == "linear"){
    mod <- lm(formula = paste0("mdcr_priv_smooth ~", indep_vars) , data = tmp_data)
  }
  
  # predict model
  tmp_data$mdcr_priv_rate_pred <- predict(mod, newdata = tmp_data, allow.new.levels = T, type = "response")
  
  tmp_data[mdcr_priv_rate_pred >1, mdcr_priv_rate_pred := 1]
  tmp_data[mdcr_priv_rate_pred <0, mdcr_priv_rate_pred := 0]
  
  # only plot for numbers divisible by 5
  if(plot & i%%5==0 & reg_map$sex_id==1){
    print(make_plot(tmp_data, "mdcr-private insurance rates", "mdcr_priv_rate", reg_map))
  }
  
  # use prediction where we don't have data, and smoothed where we do
  tmp_data[!is.na(mdcr_priv_smooth), mdcr_priv_rate_step1 := mdcr_priv_smooth]
  tmp_data[is.na(mdcr_priv_smooth), mdcr_priv_rate_step1 := mdcr_priv_rate_pred]
  
  return(tmp_data)
  
}

if(plot){
  pdf(paste0(save_dir, "/FILEPATH.pdf"), width = 12, height = 8)
  mdcr_pri_data <- rbindlist(lapply(1:nrow(mdcr_priv_reg_maps), mdcr_pri_reg, plot = T), fill = T)
  dev.off()
}else{
  mdcr_pri_data <- rbindlist(lapply(1:nrow(mdcr_priv_reg_maps), mdcr_pri_reg), fill =T)
}

mdcr_pri_data <- mdcr_pri_data[, .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, mdcr_priv_rate, mdcr_priv_rate_step1)]

write.csv(mdcr_pri_data, paste0(save_dir, "/FILEPATH.CSV"), row.names = F)


#
# uninsured --------
#

unins_reg_data <- prepped_data[!is.na(mcnty)]

# SAHIE and ACS have different age groups, so expand out to 5 year groups then merge
sahie_county <- expand_to_ages(sahie_county)
unins_reg_data <- merge(sahie_county[,.(age_group_name, mcnty, year_id, sex_id, state_name, uninsured_rate_sahie = uninsured_rate)], unins_reg_data, by = c("age_group_name", "mcnty", "year_id", "sex_id", "state_name"), all = T)

# just have sahie back to 2008
unins_reg_data <- unins_reg_data[year_id >= 2008 & year_id <= 2019]
unins_reg_maps <- unique(unins_reg_data[!is.na(sex_id),.(state_name, age_group_name, sex_id)])[order(state_name)]

uninsured_reg_sahie <- function(i, plot = F, variant = "linear"){
  
  print(i)
  reg_map <- unins_reg_maps[i,]
  
  tmp_data <- unins_reg_data[state_name == reg_map$state_name &
                               age_group_name == reg_map$age_group_name &
                               sex_id == reg_map$sex_id]
  
  # SAHIE stops at 65, so these age groups can't use SAHIE to predict
  if(reg_map$age_group_name %in% c("65 to 69","70 to 74", "75 to 79", "80 to 84", "85 plus")){
    dep_var <- "uninsured_rate_smooth"
    indep_vars <-  "unemployed + age65"
    plot <- F
  }else{
    dep_var <- "uninsured_rate_sahie"
    indep_vars <- "unemployed + age65 + uninsured_rate_smooth"
    plotdata <- T
  }
  
  # Only use MDCD expansion for states with it, since otherwise no variation 
  if(1 %in% tmp_data$mdcd_expanded){ indep_vars <- paste0(indep_vars, " + mdcd_expanded")}
  
  # fit model
  if(variant == "logit"){
    mod <- glm(formula = paste0(dep_var, " ~ ", indep_vars) , data = tmp_data, family = binomial())
  }else if(variant == "linear"){
    mod <- lm(formula = paste0(dep_var, " ~ ", indep_vars) , data = tmp_data)
  }
  
  tmp_data$uninsured_rate_sahie_pred <- predict(mod, newdata = tmp_data, type = "response")
  
  # only plot for numbers divisible by 5
  if(plot & i%%5==0 & reg_map$sex_id==1){
      print(make_plot(tmp_data, "Uninsured rates", "uninsured_rate_sahie", reg_map, datavar = "uninsured_rate_smooth", datalab = "ACS"))
  }
  
  
  # use prediction where we don't have data, and smoothed where we do
  tmp_data[!is.na(uninsured_rate_sahie), unins_rate_step1 := uninsured_rate_sahie]
  tmp_data[is.na(uninsured_rate_sahie) & is.na(uninsured_rate_smooth), unins_rate_step1 := uninsured_rate_sahie_pred]
  tmp_data[is.na(uninsured_rate_sahie) & !is.na(uninsured_rate_smooth), unins_rate_step1 := uninsured_rate_smooth]
  tmp_data[is.na(unins_rate_step1), unins_rate_step1 := uninsured_rate_smooth]
  
  return(tmp_data)
  
}


if(plot){
  pdf(paste0(save_dir, "/FILEPATH.pdf"), width = 12, height = 8)
  uninsured_data <- rbindlist(lapply(1:nrow(unins_reg_maps), uninsured_reg_sahie, plot = T), fill = T)
  dev.off()
}else{
  uninsured_data <- rbindlist(lapply(1:nrow(unins_reg_maps), uninsured_reg_sahie), fill = T)
}

uninsured_data <- uninsured_data[, .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, uninsured_rate, uninsured_rate_sahie_pred, unins_rate_step1)]

write.csv(uninsured_data, paste0(save_dir, "/FILEPATH.CSV"), row.names = F)


#
# mdcd --------
#

mdcd_reg_data <- prepped_data[!is.na(mcnty) & year_id %in% unique(acs_county$year_id)]
mdcd_reg_maps <- unique(mdcd_reg_data[!is.na(sex_id),.(state_name, age_group_name, sex_id)])

mdcd_reg <- function(i, plot = F, variant = "linear"){
  
  print(i)
  reg_map <- mdcd_reg_maps[i,]
  
  tmp_data <- mdcd_reg_data[state_name == reg_map$state_name &
                         age_group_name == reg_map$age_group_name &
                         sex_id == reg_map$sex_id]
  
  if(reg_map$state_name == "District of Columbia"){
    return(tmp_data[, mdcd_rate_step1 := mdcd_rate_smooth])
  }
  
  # Don't include dummy var if medicaid has not expanded
  if(1 %in% tmp_data$mdcd_expanded){
    indep_vars <-  "unemployed + poverty + age65 + mdcd_expanded"
  }else{
    indep_vars <-  "unemployed + poverty + age65"
  }
  
  if(variant == "logit"){
    mod <- glm(formula = paste0("mdcd_rate_smooth ~", indep_vars) , data = tmp_data, family = binomial())
  }else if(variant == "linear"){
    mod <- lm(formula = paste0("mdcd_rate_smooth ~", indep_vars) , data = tmp_data)
  }
  
  tmp_data$mdcd_rate_smooth_pred <- predict(mod, newdata = tmp_data, type = "response")
  
  #only plot for numbers divisible by 5
  if(plot & i%%5==0 & reg_map$sex_id==1){
    print(make_plot(tmp_data, "medicaid rates", "mdcd_rate_smooth", reg_map))
  }
  
  # use prediction where we don't have data, and smoothed where we do
  tmp_data[!is.na(mdcd_rate_smooth), mdcd_rate_step1 := mdcd_rate_smooth]
  tmp_data[is.na(mdcd_rate_smooth), mdcd_rate_step1 := mdcd_rate_smooth_pred]
  
  
  return(tmp_data)
  
}

if(plot){
  pdf(paste0(save_dir, "/FILEPATH.pdf"), width = 12, height = 8)
  mdcd_data <- rbindlist(lapply(1:nrow(mdcd_reg_maps), mdcd_reg, plot = T), fill = T)
  dev.off()
}else{
  mdcd_data <- rbindlist(lapply(1:nrow(mdcd_reg_maps), mdcd_reg), fill = T)
}

mdcd_data <- mdcd_data[, .(state_name, mcnty, year_id, sex_id, age_start, age_end, age_group_name, mdcd_rate, mdcd_rate_step1)]


write.csv(mdcd_data, paste0(save_dir, "/FILEPATH.CSV"), row.names = F)
