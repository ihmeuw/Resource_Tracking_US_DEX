##---------------------------------------------------
#  Model the probability a particular dx is an injury cause by age/sex/year/toc
#       Save predictions and make diagnostic plots
#
# 
# Author: Haley Lescinsky
#
##---------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, ggplot2, mgcv)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

#---------------------------------------
config <- get_config()

inj_causes <- c("inj_NEC", "_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", "inj_suicide")
#-----------------------------------------

# Arguments
if(interactive()){
  
  params <- data.table(
    toc = "NF", code_system = "icd10", sex_id = 2
  )
  
  map_version <- "/FILEPATH/map_version_XX/"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1]
  params <- fread(task_map_path)[task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  
  map_version <- args[2]
}

#-----------------------------------------
# Part 1: Define functions
#------------------------------------------

calc_rss <- function(data, inj_cols = c("intent_agg", "unintent_agg", "inj_falls", "inj_mech", "inj_trans", "inj_suicide")){
  # Calculate RSS
  
  vec <- c()
  for(col in inj_cols){
    resid <- c(data[, get(col)] - data[, get(paste0(col, "_pred"))])^2
    vec <- c(vec, resid)
  }
  
  return(sum(vec, na.rm = T))
  
}

residual_adj <- function(data, inj_cols = c("intent_agg", "unintent_agg", "inj_falls", "inj_mech", "inj_trans", "inj_suicide")){
  
  # add mean residuals by age (across years) back to predictions
  df <- copy(data)
  
  for(col in inj_cols){
    
    setnames(df, paste0(col, "_pred"), c("pred"))
    df[, mean_resid := mean(get(col) - pred, na.rm = T), by = "age"]
    df[, pred := pred + mean_resid]
    setnames(df, "pred", paste0(col, "_pred"))
    df[, mean_resid := NULL]
  }
  
  return(df)
  
}

plot_splines <- function(data, y = "inj_trans", title, source, sex, age_plot = T, year_plot = T){
  # Plot diagnostic
  
  RSS <- calc_rss(data)
  
  if(age_plot){
    a <- ggplot(data, aes(x = get("age_actual")))+geom_bar(aes(y = get(y), fill = get("source")), stat = "identity", position = "dodge")+
      geom_point(aes(y = get(paste0(y, "_pred"))))+
      geom_line(aes(y = get(paste0(y, "_pred"))))+
      facet_wrap(~year_actual)+theme_bw()+labs(x = "age", y = y, fill = "source", title = title, subtitle = paste0(source, " & ", sex, 
                                                                         "\nVisualization of age spline (across years) in the ", gsub("_", " ", y), " model",
                                                                         "\nRSS = ", round(RSS, digits = 4)))
    print(a)
  }
  
  if(year_plot){
    b <- ggplot(data[age_actual %in% seq(10, 80, by = 10)], aes(x = get("year_actual")))+geom_bar(aes(y = get(y), fill = get("source")), stat = "identity", position = "dodge")+
      geom_point(aes(y = get(paste0(y, "_pred"))))+
      geom_line(aes(y = get(paste0(y, "_pred"))))+
      facet_wrap(~age_actual)+theme_bw()+labs(x = "year", y = y, title = title, fill = "source", subtitle = paste0(source, " & ", sex, "\nVisualization of year spline (across some ages) in the ", gsub("_", " ", y), " model",
                                                                                                                                          "\nRSS = ", round(RSS, digits = 4)))
    print(b)
  }
  
  
}

make_data_square <- function(data, code_sys){
  
  # determine min/max year
  ymin <- min(data$year)
  ymax <- max(data$year)
  amin <- min(data$age)
  amax <- max(data$age)
  
  # make square
  codes <- rbind(data.table(code_system = "icd9", year = seq(2000, 2015, by =1)),
                 data.table(code_system = "icd10", year = seq(2015, 2019, by = 1)))
  square_grid <- as.data.table(crossing(codes, data.table("age" = c(0, 1, seq(5, 95, by = 5))), 
                                        source = unique(data$source)))
  
  square_grid[, `:=` (toc = unique(data$toc), dx = unique(data$dx), sex_id = unique(data$sex_id))]
  square_grid <- square_grid[code_system == code_sys]
  
  data <- merge(data, square_grid[code_system == code_sys], by = c("code_system", "sex_id", "age", "year", "dx", "source", "toc"), all = T)
  
  # hold prediction trends constant with min/max year and min/max age
  data[, year_pred := year]
  data[year < ymin, year_pred := ymin]
  data[year > ymax, year_pred := year]
  data[, age_pred := age]
  data[age < amin, age_pred := amin]
  data[age > amax, age_pred := amax]
  
  
  # now "trick" the model by using age pred and year pred to predict instead of the actual
  setnames(data, c("age_pred", "age", "year_pred", "year"),
           c("age", "age_actual", "year", "year_actual"))
  
  
  data[, age_0 := ifelse(age==0,1,0)]
  
  return(data)
  
}

model_dx_by_sex <- function(all_data, code, s, sexid, numbers){
  
  # subset data to just that relevant for this dx + source + sex
  model_data <- all_data[dx == code & toc == s & sex_id == sexid]
  
  # pull out some variables to use for plotting/organizing
  sex <- ifelse(sexid==1, "males", "females")
  toc <- s
  code_sys <- unique(model_data$code_system)
  num <- mean(numbers[dx==code & toc ==s, N])
  title <- paste0(code, " (", code_sys, "): ", num, " obs")
  
  print(code)
  print(nrow(model_data))
  #-----------------------
  # FIT MODEL
  #-----------------------
      #tensor product between age and year
      #dummy on age 0
  
  if(nrow(model_data) < 50 ){
    knots <- 6
  }
  if(nrow(model_data) < 37){
    knots <- 5
  }
  if(nrow(model_data) < 26){
    knots <- 4
  }
  
  # most basic model, only age spline. 
  #  fewer than 4 years, and less than 50 age/year combinations
  if(length(unique(model_data$year)) < 4 | nrow(model_data) < 50){
    message("age spline only (no year)")
    intent_agg_metric <- gam(intent_agg ~ age_0 + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, weights = tot_dx, data=model_data, select = select)
    unintent_agg_metric <- gam(unintent_agg ~ age_0 + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, weights = tot_dx, data=model_data, select = select)
    falls_metric <- gam(inj_falls ~ age_0 + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
    mech_metric <- gam(inj_mech ~ age_0 + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
    trans_metric <- gam(inj_trans ~ age_0 + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
    suicide_metric <- gam(inj_suicide ~ age_0 + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
  
  # other model, age spline with linear year, but different splines
  }else{
    message("age spline w year")
    intent_agg_metric <- gam(intent_agg ~ age_0 + year + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, weights = tot_dx, data=model_data, select = select)
    unintent_agg_metric <- gam(unintent_agg ~ age_0 + year + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, weights = tot_dx, data=model_data, select = select)
    falls_metric <- gam(inj_falls ~ age_0 + year + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
    mech_metric <- gam(inj_mech ~ age_0 + year + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
    trans_metric <- gam(inj_trans ~ age_0 + year + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
    suicide_metric <- gam(inj_suicide ~ age_0 + year + s(age, bs=bs, k=knots, m=m, fx=fx), sp=sp, method=method, data=model_data, weights = tot_dx, select = select)
  
  }

  # make data square before predictions
  model_data <- make_data_square(model_data, code_sys)
  
  model_data[, intent_agg_pred := predict.gam(intent_agg_metric, model_data)]
  model_data[, unintent_agg_pred := predict.gam(unintent_agg_metric, model_data)]
  model_data[, inj_falls_pred := predict.gam(falls_metric, model_data)]
  model_data[, inj_mech_pred := predict.gam(mech_metric, model_data)]
  model_data[, inj_trans_pred := predict.gam(trans_metric, model_data)]
  model_data[, inj_suicide_pred := predict.gam(suicide_metric, model_data)]
  
  plot_splines(data = model_data, title = title, source = toc, sex = sex, year_plot = F)
  
  #-----------------------
  # RESIDUAL ADJUST
  #   - if high data coverage, subtract residuals from prediction to more closely follow raw data
  #-----------------------
  if(num > 15000){
  
    adj_model_data <- residual_adj(data = model_data)
    plot_splines(adj_model_data, title = title, source = toc, sex = sex, year_plot = F)
    
    # check for NA's post residual map
    introduce_nas <- nrow(adj_model_data[complete.cases(adj_model_data[,c(pred_vars), with = F])]) < nrow(adj_model_data[,c(pred_vars), with = F])
    
    # if RSS is lower than before, keep it
    if((calc_rss(adj_model_data) < calc_rss(model_data)) & !introduce_nas){
      model_data <- adj_model_data
    }else{
      message("NAs post residual map -> not doing residual adjust")
    }
  }
  
  plot_splines(model_data, title = title, source = toc, sex = sex, age_plot = F)
  
  
  #-----------------------
  # RAKE PREDICTIONS + VISUALIZE
  #-----------------------
  
  # make sure we are using the right age and year
  model_data[, `:=` (age = NULL, age_0 = NULL, year = NULL)]
  setnames(model_data, c("age_actual", "year_actual"), c("age", "year"))
  
  # Bound predictions between 0 and 1
  model_data[, (pred_vars) := lapply(.SD, function(x){
    x[x > 1] <- 1
    x[x < 0] <- 0
    return(x)
  }), .SDcols = pred_vars]
  
  # Rescale so proportions sum to 1
  model_data[, tot_prop := rowSums(model_data[,pred_vars, with = F])]
  model_data[, (pred_vars) := lapply(.SD, function(n) n/tot_prop), .SDcols = pred_vars]
  
  # Switch data long to make visualization easier
  data_long <- melt(model_data, measure.vars = c(pred_vars, init_vars), value.name = 'prop', variable.name = 'inj_acause')
  data_long[grepl("pred", inj_acause), type := "predicted"]
  data_long[!grepl("pred", inj_acause), type := paste0("observed-", source)]
  
  data_long[, inj_acause:=gsub("_pred", "", inj_acause)]
  
  # if multiple sources, predictions are duplicated so take unique
  data_long[type =="predicted", `:=` (source = NA, tot_dx = NA)]
  data_long <- unique(data_long)
  
  for(y in unique(data_long[!is.na(prop) & grepl("observed", type)]$year)){
  
    p <- ggplot(data_long[year == y], aes( x = as.factor(age), y = prop, fill = inj_acause))+
      facet_wrap(~type, scales = "free_y")+geom_bar(stat = "identity")+theme_bw()+
      labs(x = "age", y = "proportion", title = title, subtitle = paste0(toc, " & ", sex, "\nFinal predicted vs observed age pattern:", y))
    
    print(p)
  }
  
  # return data
  drop_vars <- c(init_vars, "tot_dx", "tot_prop", "source")
  model_data[, (drop_vars) := lapply(.SD, function(n) NULL), .SDcols = drop_vars]
  
  return(unique(model_data))

}

#-----------------------------------------
# Part 2: Actually run functions to smooth age year trend
#------------------------------------------
init_vars <-  setdiff(gsub("^_","",inj_causes), "inj_NEC")
pred_vars <- paste0(init_vars, "_pred")

numbers <- as.data.table(read.xlsx(paste0(map_version, "/intermediates/dxs_with_adjustment.xlsx")))
numbers <- numbers[toc == params$toc & code_system == params$code_system]
numbers <- numbers[rev(order(N))]

data <- fread(paste0(map_version, "/intermediates/prepped_for_modeling.csv"))
setnames(data, c("age_group_years_start", "year_id", "_intent_agg", "_unintent_agg"), c("age", "year", "intent_agg", "unintent_agg"))
data[, age_0 := ifelse(age==0,1,0)]

## Set GAM parameters as global variables
bs <- "bs"   # p-spline
m <- c(2,2) # m[1] specifies basis order, m[2] specifies penalty order.
fx <- FALSE  # a penalized regression spline.
method <- "GCV.Cp" # Generalized cross validation criterion
select <- FALSE  # gam can add an extra penalty to each term so that it can be penalized to zero.
## Calculate penalty (-1 means "let this be calculated automatically")
n <- -1
n1 <- -1
sp <- c(n,n,n1,n1) # penalty array
knots <- 7

# 
pdf(paste0(map_version, "/diagnostics/modeling_visualizations_", params$toc,"_", params$code_system,"_", params$sex_id, ".pdf"), width = 11, height = 8)
map <- rbindlist(lapply(unique(numbers$dx), function(dx){
  model_dx_by_sex(all_data = data, code = dx, s = params$toc, sexid = params$sex_id, numbers = numbers)
}))
dev.off()

arrow::write_feather(map, paste0(map_version, "/intermediates/modeled_", params$toc,"_", params$code_system,"_", params$sex_id, ".feather"))

