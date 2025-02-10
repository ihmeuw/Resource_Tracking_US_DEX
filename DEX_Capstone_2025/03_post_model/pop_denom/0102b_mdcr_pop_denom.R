# ---------------------------------------------
#  Medicare pop denominators - extend time trend back
#                         
#
#  Author: Emily Johnson, edited by Haley Lescinsky
#
# ------------------------------------------------

# LOAD PACKAGES ----------------------------
pacman::p_load(arrow, RMySQL, data.table, lme4, parallel, tidyverse, boot)
library(lbd.loader, lib.loc = 'FILEPATH')
source('/FILEPATH/get_age_metadata.R')
load("/FILEPATH/states.RData")
load("/FILEPATH/merged_counties.RData")

if(interactive()){
  work_dir <- "/FILEPATH/"
  plot <- T
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  work_dir <- args[1]
  plot <- args[2]
}

dir.create(paste0(work_dir, "/FILEPATH"), recursive = T)

# Data prep ------------------------------------------------
county_names <- counties[current == 1,.(mcnty, cnty_name, state_name)]
county_names <- county_names[,.(cnty_name = min(cnty_name)), by=c("mcnty","state_name")]

mdcr_cms <- fread(paste0(work_dir, "/FILEPATH.CSV"))
setnames(mdcr_cms, "mdcr_rate", "mdcr_rate_rif")

mdcr_cms[, mdcr_rate_rif_logit := logit(mdcr_rate_rif)]
mdcr_cms[mdcr_rate_rif == 0, mdcr_rate_rif_logit := logit(0.0001)]
mdcr_cms[mdcr_rate_rif == 1, mdcr_rate_rif_logit := logit(0.9999)]

mdcr_cms <- merge(mdcr_cms, county_names, by="mcnty")

part_variables <- c("part_a_rate", "part_b_rate", "part_c_rate", "part_d_only_rate", 'part_cd_rate', "any_rate",
                    "dual_part_a_rate","dual_part_b_rate","dual_part_c_rate","dual_part_d_only_rate",'dual_part_cd_rate',"dual_any_rate")

# Smooth rate across time/sex/year *only* for time trend prediction  ------------------------
run_model_by_county <- function(eqn, df, model_type = "lm"){
  data <- lapply(unique(df$mcnty), function(mc){
    dt <- df[mcnty == mc]
    if(str_detect(eqn, "mdcr_rate_acs_logit") & any(is.na(dt$mdcr_rate_acs_logit))){
      eqn <- str_remove(eqn, "\\+ mdcr_rate_acs_logit")
    }

    if(model_type == "lm"){
      mod <- lm(eqn, data = dt)
    }else if(model_type == "loess"){
      mod <- loess(eqn, data = dt)
    }
    dt$pred <- predict(mod, newdata = dt)
    dt$residuals <- dt$pred - dt$mdcr_rate_rif_logit
    
    # y shift so 2008 matches
    shifts <- dt[year_id == 2008][, y_shift := mdcr_rate_rif - pred]
    dt <- merge(dt, shifts[, .(sex_id,  age_group_id, y_shift)], by = c("sex_id", "age_group_id"), all.x = T)
    dt[, pred := pred+y_shift]

    return(dt)
  }) %>% rbindlist()
  return(data)
}

d1_males <- rbindlist(lapply(part_variables, function(p){
  message(paste0("males - ", p))
  tmp_d1 <- run_model_by_county(eqn = "mdcr_rate_rif ~ year_id + age_group_name",
                      df = mdcr_cms[variable == p & year_id > 2000 & sex_id == 1])
  tmp_d1[pred > 1, pred := 1][pred < 0, pred := 0]
  return(tmp_d1[, variable := p])
}))

d1_females <- rbindlist(lapply(part_variables, function(p){
  message(paste0("females - ", p))
  tmp_d1 <- run_model_by_county(eqn = "mdcr_rate_rif ~ year_id + age_group_name",
                                df = mdcr_cms[variable == p & year_id > 2000 & sex_id == 2])
  tmp_d1[pred > 1, pred := 1][pred < 0, pred := 0]
  return(tmp_d1[, variable := p])
}))

d1 <- rbind(d1_males, d1_females)
rm(d1_males, d1_females)
sqrt(mean(d1$residuals^2))

# save final pre-scale denom rates
fwrite(d1, paste0(work_dir, "/FILEPATH/mdcr_intermediate_d1.csv"))
d1 <- fread(paste0(work_dir, "/FILEPATH/mdcr_intermediate_d1.csv"))

print("intermediates saved!")

# Extrapolation back to 2000 --------------------------------------------------------------------

full_df <- copy(d1)
full_df <- unique(full_df[,.(age_start, age_group_name, sex_id, mcnty, state_name, cnty_name, year_id,
                      variable, mdcr_rate_rif, mdcr_rate_smooth = pred)])

key <- unique(full_df[,.(age_start, mcnty, sex_id, variable)])

pred_years <- setdiff(seq(1999,2019), unique(d1$year_id))
new_years <- lapply(1:nrow(key), function(k){
  
  if(k%%5000 ==0){message(paste0(round(k/nrow(key) * 100, digits = 3), "%"))}
  keyt <- key[k,]
  dt <- full_df[age_start == keyt$age_start & mcnty == keyt$mcnty & sex_id == keyt$sex_id & variable == keyt$variable]
  
  extrap_mod <- lm(mdcr_rate_smooth ~ year_id, data = dt)
  extrap_mod_loess <- loess(mdcr_rate_rif ~ year_id, data = dt, span = 1)
  
  pred_df <- tidyr::expand(dt, nesting(age_start, age_group_name, sex_id, mcnty, state_name, 
                                       cnty_name, variable), year_id = pred_years)
  pred_df$mdcr_rate_smooth <- predict(extrap_mod, newdata = pred_df)
  pred_df$mdcr_rate_smooth_loess <- predict(extrap_mod_loess, newdata = pred_df)
  setDT(pred_df)
  return(pred_df)
}) %>% rbindlist()

full_df <- rbind(full_df, new_years, use.names = TRUE, fill = TRUE)

# use loess smoother for 2018, since now it is a bump relative to everything else!
full_df[year_id == 2018 & !is.na(mdcr_rate_smooth_loess), mdcr_rate_smooth := mdcr_rate_smooth_loess]

# Now use predicted (smooth) estimate where we don't have data, and the raw data where it does exist
full_df[mdcr_rate_smooth < 0.0001, mdcr_rate_smooth := 0][mdcr_rate_smooth > 0.9999, mdcr_rate_smooth := 1]
full_df[is.na(mdcr_rate_rif), mdcr_rate_rif := mdcr_rate_smooth]
full_df[, mdcr_rate_smooth_loess := NULL]
full_df[, mdcr_rate_smooth := NULL]

# save final pre-scale denom rates
fwrite(full_df, paste0(work_dir, "/FILEPATH.CSV"))
full_df <- fread(paste0(work_dir, "/FILEPATH.CSV"))

# Plotting ----------------------------------------------------------------
if(plot){
age <- readRDS("/FILEPATH")
age <- age[year == 2019, .(pop = sum(pop)), by=c("mcnty","state")]
age <- age[, index := .I]
age <- age[index%%10 == 0]
setorder(age, state)

counties_to_plot <-  filter(age, (index)%%10==0)$mcnty

plot_df <- full_df[mcnty %in% counties_to_plot]

plot_df[, col := ifelse(year_id %in% pred_years, "Predicted", "Data")]
plot_df[, county_label := paste0(cnty_name, ", ", state_name)]
setorder(plot_df, state_name)

pdf(paste0(work_dir, "/FILEPATH.pdf"), onefile = TRUE)
lapply(unique(plot_df$state_name), function(state){
  p <- ggplot(plot_df[state_name == state & variable == "any_rate"], aes(x = age_start, group = interaction(sex_id, year_id))) +
    geom_line(aes(y = mdcr_rate_rif, color = col, linetype = as.factor(sex_id)), alpha = 0.5) +
    facet_wrap(.~county_label) + 
    theme_bw()
  print(p)
})
dev.off()

pdf(paste0(work_dir, "/FILEPATH.pdf"), onefile = TRUE)
lapply(unique(plot_df$state_name), function(state){
  p <- ggplot(plot_df[state_name == state & age_start %in% c(15,50,75) & variable == "any_rate" & sex_id == 2], 
         aes(x = year_id, color = as.factor(age_start))) +
    geom_line(aes(y = mdcr_rate_rif)) +
    geom_vline(aes(xintercept = 2008)) +
    facet_wrap(.~county_label) + 
    # scale_y_log10() + 
    theme_bw()
  print(p)
})
dev.off()
}


