#---------------------------------------------------
#  COMORB: calculate AFs using LASSO regression
#          Heavily relies on functions defined in 'comorb_functions.R'
#
#
#  Author: Haley Lescinsky
#---------------------------------------------------

rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, openxlsx, RColorBrewer, ggplot2)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

set.seed(4567)

causelist <- fread("/FILEPATH/causelist.csv")
causelist <- causelist[!(acause == "_gc" | acause %like% "_NEC")]

family_names <- unique(causelist$family)
family_colormap <- colorRampPalette(brewer.pal(12, "Paired"))(length(family_names))
names(family_colormap) <- family_names
t0 <- Sys.time()


min_observations <- 1000
min_observations_for_comorb <- 100

source(paste0(code_path, "/helpers/comorb_functions.R"))

if(interactive()){
  
  params <- data.table(cause = "exp_well_person",
                       toc = "AM")
  
  map_version <- "/FILEPATH/map_version_XX/"
  
  cause_mod <- params$cause
  task_map_path <- "special"
  
  mem <- '80G'
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  task_map_path <- args[1] 
  map_version <- args[2]
  mem <- args[3]
  
  params <- fread(task_map_path)[mem_set == mem & task_id == Sys.getenv("SLURM_ARRAY_TASK_ID") ]
  print(params)
  cause_mod <- params$cause
  
  
}

# Initiate parallel processing 
if(mem == "80G"){
  registerDoParallel(6)
}else{
  registerDoParallel(15)
}

# need to read from filepath with pri_cause + source, etc
main_data <- arrow::open_dataset(paste0(map_version, "/prepped_data/toc=", params$toc))


dir.create(paste0(map_version, "/diagnostics/by_cause_toc/"), recursive = T)
dir.create(paste0(map_version, "/maps/"), recursive = T)

age_grid <- data.table(age_group_min = c(0,5,seq(10,70, by = 10), 85),
                       age_group_max = c(5,10,seq(20, 70, by = 10), 85, 125))

master_coef_df <- data.table()

# loop through ages
# Save output all the way at the end to enable across age-diagnostics
for(i in 1:nrow(age_grid)){

  age_group_min <- age_grid[i, age_group_min]
  age_group_max <- age_grid[i, age_group_max]
  
  label <- paste0(cause_mod, " | ", params$toc, " | ", age_group_min, "-", age_group_max)
  print(label)
  
  data <- main_data %>% 
    filter(pri_cause == params$cause &
             age_group_years_start >= age_group_min &
             age_group_years_start < age_group_max) 

  data <- data %>% collect() %>% as.data.table() %>% 
    mutate(toc = params$toc)
  print("1 - read in data ")
  
  data <- comorb_restrictions(data)
  
  if(nrow(data) < min_observations){
    print(paste0("There are fewer than ", min_observations, " encounters -> skipping to next age group!"))
    next
  }
  
  # add on dummies for sex - recode females as 0
  data[, sex_id := ifelse(sex_id==1, 1, 0)]
  
  # calculate tot_pay
  data[is.na(tot_pay_amt), tot_pay_amt := apply(.SD, 1, sum, na.rm = T), .SDcols = c("priv_pay_amt", "oop_pay_amt", "mdcd_pay_amt", "mdcr_pay_amt")]
  setnames(data, "tot_pay_amt", "spend")
  
  # drop primary cause from list of potential comorbs and subset to just those with non-zeros
  acause_list <- setdiff(intersect(colnames(data), causelist$acause), params$cause)
  num_comorbs <- data.table(comorb = acause_list, num = colSums(data[,c(acause_list), with = F]))
  causes_keep <- num_comorbs[num > min_observations_for_comorb]$comorb
  
  data[, spending_log := log(spend)]
  # tmp remove all observations with spend < 1
  data <- data[!(spend==0 | spend <1 | is.na(spending_log))]
 
  # keep only columns that are still relevant
  data <- data[, c(causes_keep, "spending_log", "sex_id", "year_id", "age_group_years_start", "pri_cause", 'source'), with = F]
  
  year_vec <- unique(data$year_id)
  
  if(length(year_vec) > 1){
    tmp <- lapply(year_vec, function(y){
      data[, paste0(y) := ifelse(year_id==y, 1, 0)]
    })
    print("2 - added year dummy ")
  }else{
    year_vec <- c()
  }
  
  # adding source dummy
  source_vec <- unique(data$source)

  if(length(source_vec) > 1){
    tmp <- lapply(source_vec, function(s){
      data[, paste0(s) := ifelse(source==s, 1, 0)]
    })
    print("2.5 - added source dummy ")
  }else{
    source_vec <- c()
  }

  # year_vec will contain year + source indicators
  year_vec <- c(year_vec, source_vec)
  
  n_encounters_used <- nrow(data)
  
  if(n_encounters_used < min_observations){
    print(paste0("After dropping encounters with implausible spending, there are fewer than ", min_observations, " encounters -> skipping to next age group!"))
    next
  }
  
  direction_colors <- c("lightgreen", "lightblue")
  names(direction_colors) <- c("positive", "negative")
  
  tuning <- tune_regression(cause_dt = data, causes_keep = causes_keep)
  tuning <- tuning[lambda_note=="1se"]$lambda

  
  print("3 - tuning done ")
  coefs <- get_coefs(cause_dt = data, causes_keep = causes_keep, alpha = 1, lambda = tuning)
  
  num_comorbs[rev(order(num)), rank:=1:.N]
  coefs <- merge(coefs, num_comorbs, by.x = "parameter", by.y = "comorb", all.x = T)
  
  print("4 - coefs done")
  if(nrow(coefs)==0){
    print("No covariates selected -> skipping to next age group!")
    print(Sys.time()-t0)
    next
  }
  
  coefs[order(abs(coef)), rank := .N:1]
  coefs[, parameters_ordered := factor(parameter, levels = rev(coefs[order(rank)]$parameter))]
  coefs[, direction := ifelse(coef > 0, "positive", "negative")]
  
  coef_plot <- ggplot(coefs, aes(x = parameters_ordered, y = abs(coef), fill = direction))+
    geom_bar(stat = "identity")+coord_flip()+
    scale_fill_manual(values = direction_colors)+
    theme_bw()+
    geom_text(aes(label = round(coef, digits = 2), hjust = quantile(abs(coefs$coef), 0.06)))+
    labs(title = label,
         subtitle = paste0("RR of excess spending w/ tuning parameters: lambda = ", round(tuning, digits = 4), " and alpha = ", 1, "    | ", length(coefs$parameter), " coefficients"), x = "", y = "estimated coefficient magnitude")+theme(legend.position = "bottom")
  
  afs <- calc_af(cause_dt = data, coefs = coefs)
  print("5 - AFs calculated")
  
  afs[order(abs(af)), rank := .N:1]
  afs[, parameters_ordered := factor(comorb, levels=rev(afs[order(rank)]$comorb) )]
  
  
  af_plot <- ggplot(afs, aes(x = parameters_ordered, y = abs(af), fill = direction))+
    geom_bar(stat = "identity")+coord_flip()+theme_bw()+
    scale_fill_manual(values = direction_colors)+
    geom_text(aes(label = round(af, digits = 4), hjust = quantile(abs(afs$af), 0.06)))+
    labs(title = label,
         subtitle = paste0("Attributable fractions (Sum of AFs = ", round(sum(afs$af), digits = 3), ")"),
         x = "", y = "estimated AF magnitude")+theme(legend.position = "bottom")
  
  
  master_coef_df <- rbind(master_coef_df, afs[, `:=` (age_group_min = age_group_min, age_group_max= age_group_max, n_encounters_used = n_encounters_used, sum_af = sum(afs$af), n_comorbs = .N)])
  
  print("-- Done with this age. ")
  print(Sys.time()-t0)
  
  rm(data)
  gc()
  
}

if(nrow(master_coef_df) > 0 ){
  
  # save map
  arrow::write_dataset(master_coef_df[,.(toc = params$toc, pri_cause = primary_cause, comorb, beta_coef = coef, rank, cond_prob, af, age_group_min, age_group_max, n_encounters_used)],
                       paste0(map_version, "/maps/"), partitioning = c("toc", "pri_cause"),
                       existing_data_behavior = "overwrite")
  
  
  # Now plot! 
  master_coef_df <- merge(master_coef_df, causelist[,.(comorb = acause, family)], by = "comorb")
  direction_colors <- c("black", "red")
  names(direction_colors) <- c("positive", "negative")
  master_coef_df[, direction := ifelse(af >=0, "positive", "negative")]
  
  
  # AF plot across ages
  pdf(paste0(map_version, "/diagnostics/by_cause_toc/", cause_mod, "_", params$toc, "_af_acrossage.pdf"), width = 12, height = 8)

  master_coef_df[, age := paste0("Age: ", age_group_min, "-", age_group_max, "\n n=", n_encounters_used, "\n ", n_comorbs, " comorbs  \n sum(AF)=", round(sum_af, digits = 3))]
  p1 <- ggplot(master_coef_df, aes(x = comorb, y = abs(af), fill = family, color = direction))+
    facet_grid(~age)+coord_flip()+
    scale_color_manual(values = direction_colors)+
    geom_bar(stat = "identity")+
    scale_fill_manual(values = family_colormap, guide = 'none')+
    theme(legend.position = "bottom")+labs(title = paste0(cause_mod, " | ", params$toc, " | AFs across ages, x-axis constant"))+guides(fill = FALSE)
  print(p1)
  dev.off()
  
}else{
  print("No comorbidities found for any age group!")
}

print("Done.")
print(Sys.time() - t0)
