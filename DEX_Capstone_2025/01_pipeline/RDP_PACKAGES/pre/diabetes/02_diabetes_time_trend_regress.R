# #################
#
#    02_diabetes_time_trend_regress.R: Time trend
#
#
#    Author: Haley Lescinsky
#
# ################

#--
# Set up
#--

rm(list = ls())

Sys.setenv("RETICULATE_PYTHON" = 'FILEPATH/python')
library(configr, lib.loc = "FILEPATH/r_library/")
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, lme4, arrow)
library(lbd.loader, lib.loc = "FILEPATH")
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
source("/FILEPATH/get_outputs.R")
config <- get_config()

#--
# Arguments
#--

work_dir <- paste0(config$RDP$package_helper_dir, "/dex_package_helpers/diabetes/")

#--
# Data
#--
mscan <- fread(paste0(work_dir, "/diabetes_proportions_icd10_dex.csv"))[,.(year_id, sex_id, age_group_id, age_group_years_start, toc, prop_type1, prop_type2)]
gbd <- fread(paste0(work_dir, "/diabetes_proportions_gbd_prevalence.csv"))[,.(year_id, sex_id, age_group_id, age_group_name, prop_type1, prop_type2)]


# since IP and AM look similar, focus on IP for now
mscan <- mscan[toc =="IP"]

# Update names
setnames(mscan, c("prop_type1", "prop_type2"), c("mscan_prop_type1", "mscan_prop_type2"))
setnames(gbd, c("prop_type1", "prop_type2"), c("gbd_prop_type1", "gbd_prop_type2"))


gbd_predict <- gbd[age_group_id %in% mscan$age_group_id]
gbd_predict <- merge(gbd_predict, mscan, by = c("year_id", "sex_id", "age_group_id"), all.x=T)
regress_data <- merge(mscan, gbd, by = c("year_id", "sex_id", "age_group_id"))


#--
# regress 
#--

regress_data[, age_fac := as.factor(age_group_id)]
gbd_predict[, age_fac := as.factor(age_group_id)]


predict_time <- function(regress_data, gbd_predict, type, sex, random_slope = T){
  
  indep_var <- paste0("gbd_prop_type", type)
  dep_var <- paste0("mscan_prop_type", type)
  
  
  formula <- paste0(dep_var, " ~ ", indep_var, " + (1", ifelse(random_slope, paste0(" + ", indep_var), ""), " | age_fac )")
  
  
  reg <- lmer(formula, regress_data[sex_id==sex])
  summary(reg)
  
  gbd_predict[sex_id==sex, paste0("pred_", dep_var) := predict(reg, newdata = gbd_predict[sex_id==sex])]
  
  plot <- ggplot(gbd_predict[sex_id==sex], aes(x = year_id))+
    geom_line(aes(y = get(paste0("pred_", dep_var)), color = "predicted_mscan"))+
    geom_line(aes(y = get(dep_var), color = "orig mscan"))+
    geom_point(aes(y = get(dep_var), color = "orig mscan"))+
    geom_line(aes(y = get(indep_var), color = "gbd prev"))+
    facet_wrap(~age_group_name)+theme_bw() +labs(y = "pred", title = formula, subtitle = paste0("sex ", sex))
  print(plot)
  
  
  #setnames(gbd_predict, "pred", paste0("pred_", dep_var))
  
  return(gbd_predict)
  
}


pdf(paste0(work_dir, "/diabetes_time_trend_regression.pdf"), width = 11, height = 8)
gbd_predict <- predict_time(regress_data, gbd_predict, type = 1, sex = 1, random_slope = F)
gbd_predict <- predict_time(regress_data, gbd_predict, type = 1, sex = 2, random_slope = F)
gbd_predict <- predict_time(regress_data, gbd_predict, type = 2, sex = 1, random_slope = F)
gbd_predict <- predict_time(regress_data, gbd_predict, type = 2, sex = 2, random_slope = F)


#--
# Rake + force 0-1 and that type1 + type2 = 1
#--

# below 15 should all be type 1
gbd_predict[age_group_id %in% c(5,6,7) ,  `:=` (pred_mscan_prop_type1 = 1, pred_mscan_prop_type2 = 0)]


# truncation above and below 1 ... Not ideal, but we don't want to completely shift the time trend by scaling
gbd_predict[ pred_mscan_prop_type1 > 1, pred_mscan_prop_type1 := 1]
gbd_predict[ pred_mscan_prop_type1 < 0, pred_mscan_prop_type1 := 0]
gbd_predict[ pred_mscan_prop_type2 > 1, pred_mscan_prop_type2 := 1]
gbd_predict[ pred_mscan_prop_type2 < 0, pred_mscan_prop_type2 := 0]


gbd_predict[, tot_pred := pred_mscan_prop_type1 + pred_mscan_prop_type2]
gbd_predict[, pred_mscan_prop_type1 := pred_mscan_prop_type1 / tot_pred]
gbd_predict[, pred_mscan_prop_type2 := pred_mscan_prop_type2 / tot_pred]
gbd_predict[, tot_pred := pred_mscan_prop_type1 + pred_mscan_prop_type2]
table(gbd_predict$tot_pred)



plot <- ggplot(gbd_predict[sex_id==1], aes(x = year_id))+
  geom_line(aes(y = pred_mscan_prop_type1, color = "dex package proportions"))+
  geom_line(aes(y = mscan_prop_type1, color = "orig mscan"))+
  geom_point(aes(y = mscan_prop_type1, color = "orig mscan"))+
  geom_line(aes(y = gbd_prop_type1, color = "gbd prev"))+
  facet_wrap(~age_group_name)+theme_bw() +labs(y = "type 1 proportion", subtitle = "males", title = "DEX type 1 proportions time trend")
print(plot)


plot <- ggplot(gbd_predict[sex_id==2], aes(x = year_id))+
  geom_line(aes(y = pred_mscan_prop_type2, color = "predicted_mscan"))+
  geom_line(aes(y = mscan_prop_type2, color = "orig mscan"))+
  geom_point(aes(y = mscan_prop_type2, color = "orig mscan"))+
  geom_line(aes(y = gbd_prop_type2, color = "gbd prev"))+
  facet_wrap(~age_group_name)+theme_bw() +labs(y = "type 2 proportion", subtitle = "females", title = "DEX type 2 proportions time trend")
print(plot)
dev.off()


proportions <- gbd_predict[, .(year_id, sex_id, age_group_id, age_group_name, prop_type1 = pred_mscan_prop_type1, prop_type2 = pred_mscan_prop_type2)]

write.csv(proportions, paste0(work_dir, "diabetes_package_proportions_age_sex_year.csv"), row.names = F)


