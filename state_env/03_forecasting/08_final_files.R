# File to compile final estimates for the state spending forecasts
# This script regenerates scaled draws, formats the data to include SHEA and our esimtates,
# and writes standardized draws/estimates.
# USERNAME, Sep 2021
rm(list = ls())
library(data.table)
library(tidyverse)
library(crosswalk002)
source('FILEPATH/envelope_functions.R')
load("FILEPATH/states.RData")
states$location_id <- as.character(states$location_id)

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

labels <- c(
  "mdcr" = "Medicare", 
  "mdcd" = "Medicaid", 
  "priv" = "Private", 
  "oop" = "OOP", 
  "dent" = "Dental", 
  "hh" = "Home health", 
  "hosp" = "Hospital", 
  "nf" = "Skilled nursing",
  "oprof" = "Other professional", 
  "other" = "Other", 
  "pharma" = "Pharmaceuticals", 
  "phys" = "Physician/clinical services",
  "agg" = "Aggregate"
)
labels <- data.table(model = names(labels), groups = labels)

nhea <- fread("FILEPATH/NHE60-28.csv", skip = 6)
nhea <- nhea[CATEGORY == "Personal Health Care",.(year = YEAR, agg = TOTAL, priv = PRIVATE_HEALTH_INSURANCE, 
                                                  mdcr = MEDICARE, mdcd = MEDICAID, oop = OUT_OF_POCKET + OTHER_HEALTH_INSURANCE + OTHER_THIRD_PARTY)]
nhea <- melt(nhea, id.vars = c("year"), variable.name = "model", value.name = "tot")
nhea[, year_id := as.integer(str_remove(year, "Y"))][, year := NULL]

nhea <- dex_currency_conversion(nhea, c("tot"))
nhea <- nhea[, tot_nhea := tot*1000000][, tot := NULL]

regenerate_draws <- function(df){
  df <- df[,.(location_id = as.character(location_id), year_id, model, data, mean, se = (upper - mean)/1.96)]
  tform <- crosswalk002::delta_transform(df$mean, df$se, "linear_to_logit")
  df[, mean_logit := tform[,1]][, se_logit := tform[,2]]
  draws <- rbindlist(lapply(c(1:1000), function(i){
    draws <- copy(df)[, fr := rnorm(1, mean_logit, se_logit), by=c("location_id","year_id","model")]
    draws[, draw := i]
    return(draws)
  }))
  draws[, fr := inv.logit(fr)][, mean_logit := NULL][, se_logit := NULL]
  
  return(draws)
}

submodel_format <- function(draws, payer = TRUE){
  # Merge on aggregate to get per capita
  draws <- merge(draws, agg_draws[,.(location_id, year_id, population, agg = pred*population, shea_agg = pc_spending*population, draw)], by=c("location_id","year_id","draw"))
  draws <- draws[,.(location_id, year_id, model, draw, population, data_tot = shea_agg*data, tot = fr*agg, agg, shea_agg)]
  
  # Rake to NHEA
  if(payer){
    draws <- merge(draws, nhea, by=c("year_id","model"))
    draws[, tot_us := sum(tot), by=c("year_id","model","draw")]
    draws[, ratio := tot_nhea/tot_us][, data_tot := ratio*data_tot][, tot := ratio*tot]
    draws[, ratio := NULL][, tot_nhea := NULL][, tot_us := NULL]
  }

  
  # Rake back to agg
  draws[, data_fr := data_tot/shea_agg][, fr := tot/agg]
  draws[, ratio := sum(fr), by=c("year_id","location_id","draw")][, data_ratio := sum(data_fr), by=c("year_id","location_id","draw")]
  draws[, data_fr := data_fr/ratio][, fr := fr/ratio]
  
  draws[, pc := fr*agg/population][, data_pc := data_fr*shea_agg/population]
  draws[,c("ratio", "data_ratio", "tot", "data_tot", "agg", "shea_agg") := NULL]
  
  draws[year_id < 2015, `:=`(fr = data_fr, pc = data_pc)]

  draws <- draws[,`:=`(fr_mean = mean(fr), fr_upper = quantile(fr, 0.975), fr_lower = quantile(fr, 0.025),
              pc_mean = mean(pc), pc_upper = quantile(pc, 0.975), pc_lower = quantile(pc, 0.025)),
           by=c("location_id","year_id","model")]
  
  df <- unique(copy(draws)[, fr := NULL][, pc := NULL][, draw := NULL][, data_pc := NULL][, data_fr := NULL])
  setnames(df,c("fr_mean","pc_mean"),c("fr","pc"))
  stopifnot(nrow(df[year_id %in% 2009:2013 & (data_fr > fr_upper | data_fr < fr_lower)]) == 0,
            nrow(df[year_id %in% 2009:2013 &(data_pc > pc_upper | data_pc < pc_lower)]) == 0)
  
  return(list(df = df, draws = draws))
}

# Aggregate ------
agg_model <- get_best_agg_model(bad_coefs = TRUE)
agg <- fread(paste0(agg_model$m_dir,"/model_",agg_model$metadata$model,"_uncertainty.csv"))
agg_draws <- arrow::read_feather(paste0(agg_model$m_dir,"/model_",agg_model$metadata$model,"_draws.feather")) %>%
  .[year_id < 2015, `:=`(pred = pc_spending, mean = pc_spending, median = pc_spending, lower = pc_spending, upper = pc_spending)] %>%
  arrow::write_feather("FILEPATH/aggregate_draws.feather")
setDT(agg_draws)

agg <- agg[,.(location_id, year_id, population, pc = mean, pc_lower = lower, pc_upper = upper, data_pc = pc_spending)]
agg[year_id < 2015, (c("pc","pc_upper","pc_lower")) := data_pc]
agg[, model := "Aggregate"]
agg[, model_set := "Aggregate"]

# Payer ------
payer_model <- get_best_disagg_model("payer")
payer <- fread(paste0(payer_model$m_dir,"/model_",payer_model$metadata$model,"_uncertainty.csv"))
payer$model <- payer$payer
payer <- payer %>% regenerate_draws() 
payer <- payer %>% submodel_format(payer = TRUE)
payer$draws[, draw := 1:.N, by=c("location_id","year_id","model")]
arrow::write_feather(payer$draws, "FILEPATH/payer_draws.feather")
payer$df[, model_set := "Payer"]

# Toc -------
toc_model <- get_best_disagg_model("toc")
toc <- fread(paste0(toc_model$m_dir,"/model_",toc_model$metadata$model,"_uncertainty.csv"))
toc$model <- toc$toc
toc <- toc %>% regenerate_draws() %>% submodel_format(payer = FALSE)
toc$draws[, draw := 1:.N, by=c("location_id","year_id","model")]
arrow::write_feather(toc$draws, "FILEPATH/toc_draws.feather")
toc$df[, model_set := "TOC"]

# Reformatting -----
submodels <- rbind(payer$df, toc$df)
submodels <- merge(submodels, labels, by="model")
submodels[, model := groups][, groups := NULL]

data <- rbind(agg, submodels, fill = TRUE)
data <- merge(states, data, by="location_id")
setcolorder(data, c("model_set", "model","year_id"))
data[, data_pc := NULL][, data_fr := NULL]
fwrite(data, "FILEPATH/final_estimates.csv")

# AROC -----
aroc <- copy(data)[year_id %in% c(2014,2019)]
setorder(aroc, "year_id")
aroc[, aroc := ((pc / data.table::shift(pc))**(1 / (year_id - data.table::shift(year_id))) - 1), by= .(location_id, model)]
aroc[, aroc_yrs := "2014-2019"]
aroc <- aroc[!is.na(aroc)]
fwrite(aroc, "FILEPATH/aroc.csv")
