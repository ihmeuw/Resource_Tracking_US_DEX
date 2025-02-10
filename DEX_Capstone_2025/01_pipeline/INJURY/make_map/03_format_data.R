##---------------------------------------------------
#  Post data pull processing
#   - apply truncation map
#   - apply threshold for dx inclusion with own map
#   - clean and save population-level distribution of inj causes to use as map for residual/excluded dxs
#   - make some preliminary plots

#  Author: Haley Lescinsky
# 
##---------------------------------------------------
rm(list = ls())
pacman::p_load(dplyr, openxlsx, RMySQL, data.table, ini, DBI, tidyr, ggplot2)
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)

#---------------------------------------
config <- get_config()

inj_causes <- c("inj_NEC", "_unintent_agg", "_intent_agg", "inj_trans", "inj_falls", "inj_mech", "inj_suicide")
by_cols <- c("age_group_years_start", "sex_id", "year_id", "code_system", "toc", "source")
#-----------------------------------------

# Arguments
if(interactive()){
  
  map_version <- "/FILEPATH/map_version_XX/"
  cm_map_version_id <- 'XX'
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  map_version <- args[1]
  cm_map_version_id <- args[2]
}

#---------------------------------------
# Set up paths and directories
#---------------------------------------
causemap_config <- parsed_config(config, key = "CAUSEMAP", map_version_id = as.integer(cm_map_version_id ))

dex_inj_code_list <- fread(causemap_config$injury_code_path)

dir.create(paste0(map_version, "/diagnostics/"))
dir.create(paste0(map_version, "/intermediates/"))
dir.create(paste0(map_version, "/maps/"))

trunc_map <- fread(paste0(map_version, "/inj_trunc_map.csv"))


#--------------------------------------------
# Apply truncation map 
#--------------------------------------------

data <- arrow::open_dataset(paste0(map_version, "/inputs/dx_distribution/")) %>% collect() %>% as.data.table
tmp_inj_causes <- setdiff(inj_causes, "inj_NEC")
data[, (tmp_inj_causes) := lapply(.SD, function(x) ifelse(is.na(x), 0, x)), .SDcols = tmp_inj_causes]

# apply trunc map to data
#---
data <- merge(data, trunc_map[,.(code_system, dx = icd_code, trunc_code)], by = c("dx", "code_system"), all.x = T)

# apply trunc map to dex_inj_code_list
dex_inj_code_list <- merge(dex_inj_code_list, trunc_map[,.(code_system, icd_code, trunc_code)], by = c("icd_code", "code_system"), all.x = T)

# merge on the inj code list to get names
data <- merge(data, dex_inj_code_list[,.(dx = icd_code, code_system, icd_name)], by = c("dx", "code_system"), all.x = T)

# we want to do adjustments for truncation codes, not raw dx
setnames(data, c("dx", "trunc_code"), c("orig_icd", "dx"))
setnames(dex_inj_code_list, c("trunc_code"), c("dx"))

data[, unique_row := 1:.N]

# save/preserve data for plotting all the way at the end
plot_data_long1 <- melt(data, measure.vars = setdiff(inj_causes, "inj_NEC"), value.name = 'num', variable.name = 'inj_acause')

# Filter to just relevant sources for each type of care
#---
data <- data[source %in% c("NIS", "NEDS", "KYTHERA", "MSCAN", "MDCR", "MDCD")]

# want to use just HCUP for IP and ED
data <- data[!(source %in% c("KYTHERA", "MSCAN", "MDCR", "MDCD") & toc %in% c("IP", "ED"))]
# don't use MDCD for AM, HH
data <- data[!(source %in% c("MDCD") & toc %in% c("AM", "HH"))]
# want to use just MDCR for 65+ only (except for NF)
data <- data[!(source %in% c("MDCR") & age_group_years_start < 65) | toc == "NF"]

counts_by_dx <- data[,.(N = sum(tot_dx)), by = c("dx", "source", "code_system", "toc")]
counts_by_dx <- counts_by_dx[rev(order(N))]

#--------------------------------------------
# Apply inclusion criteria by truncated dx - toc - source - code system 
#--------------------------------------------

# Apply across all age/sex/year/toc threshold. A dx needs at least 700 encounters to be included
#---
threshold <- 700
to_include <- counts_by_dx[N >= threshold]

# subset to just those rows that meet threshold
include_data <- merge(data, to_include[,.(dx, source, code_system, toc)], by = c("dx", "source", "code_system", "toc"))

# make sure it's summed up
include_data[, (tmp_inj_causes) := lapply(.SD, sum), .SDcols = tmp_inj_causes, by = c("dx", "source", "code_system", "toc", "year_id", "sex_id", "age_group_years_start")]
include_data[, `:=` (orig_icd = NULL, unique_row = NULL, icd_name = NULL, tot_dx=NULL)]
include_data <- unique(include_data)

# convert to proportion by injury cause
include_data[, tot_dx := rowSums(include_data[, c(tmp_inj_causes), with = F])]
include_data[, (tmp_inj_causes) := lapply(.SD, function(n) n/tot_dx), .SDcols = tmp_inj_causes]

# Apply age/sex/year threshold. An age/sex/year needs at least 5 encounters to be included
#---
include_data <- include_data[tot_dx >= 5]

# drop 2015 + ICD10 because small sample size
include_data <- include_data[!(code_system == "icd10" & year_id == 2015)]

# Apply final thresholds
#     - since we've just dropped some age/sex/years, make sure we still have 600 encounters
#     - also if a dx has very limited age/year combinations, drop it. (exception for MDCR which has weaker age coverage so gets a lower threshold)
#---
dx_map <- include_data[, .(.N, sum(tot_dx)), by = c("source", "dx", "sex_id", "code_system", "toc")]
drop <- dx_map[N < 17 & source != 'MDCR' , .(source, dx, code_system, toc)]
drop2 <- dx_map[N < 6 & source == 'MDCR' , .(source, dx, code_system, toc)]
drop <- rbind(drop, drop2)
drop <- unique(rbind(drop, dx_map[, .(sum(V2)), by = c("source", "dx", "code_system", "toc")][V1 < (threshold - 100), .(source, dx, code_system, toc)]))

include_data <- merge(include_data, drop[, drop := 1], by = c("source", 'dx', "code_system", "toc"), all = T)

include_data <- include_data[is.na(drop)]

# Save final data prepped for modeling
#---
write.csv(include_data[, drop:=NULL], paste0(map_version, "/intermediates/prepped_for_modeling.csv"), row.names = F)

print("Map data is prepped for modeling!")

#--------------------------------------------
# Properly track the dx's that met threshold vs didn't, we use these excels later
#--------------------------------------------

# make list of dxs that met threshold vs didn't
included <- unique(include_data[, .(dx, source, code_system, toc)])
included[, included := 1]

counts_by_dx <- merge(counts_by_dx, included, by = c("dx", "source", "code_system", "toc"), all = T)

table(counts_by_dx[source == "NEDS" & included == 1]$code_system)
table(counts_by_dx[source == "NIS"& included == 1]$code_system)
table(counts_by_dx[source == "KYTHERA" & included == 1]$code_system)
table(counts_by_dx[source == "MSCAN"& included == 1]$code_system)
table(counts_by_dx[source == "MDCR"& included == 1]$code_system)

write.xlsx(counts_by_dx[included == 1], paste0(map_version, "/intermediates/dxs_with_adjustment.xlsx"))

# now make sure the excluded is only by TOC - if one source is included, still want to use that instead of residual
counts_by_dx_excluded <- counts_by_dx[, .(included = sum(included, na.rm = T)), by = c("dx", "code_system", "toc")]
counts_by_dx_excluded[is.na(included), included := 0]
write.xlsx(counts_by_dx_excluded[included ==0, ][, included := NULL], paste0(map_version, "/intermediates/dxs_without_adjustment.xlsx"))

print("List of dxs with and without adjustment saved!")
#-------------------------------------------
# Load data for map (called 'residual distribution') for all the dx's that didn't get included and need an across-dx map
#-------------------------------------------

# Pull in the prepped data: counts of TOTAL non-NEC injury causes (across all dx's)
residual_dist <- arrow::open_dataset(paste0(map_version, "/inputs/residual_distribution/")) %>% collect() %>% as.data.table

# Some assumptions
#---
# only keep IP and ED for NIS/NEDS
residual_dist <- residual_dist[source %in% c("NIS", "NEDS", "KYTHERA", "MSCAN", 'MDCR', "MDCD")]
residual_dist <- residual_dist[!(source %in% c("KYTHERA", "MSCAN", "MDCR", "MDCD") & toc %in% c("IP", "ED"))]
# don't use mdcd for AM, HH
residual_dist <- residual_dist[!(source %in% c("MDCD") & toc %in% c("AM", "HH"))]
# 2015 & ICD10 looks terrible due to small sample size, so drop
residual_dist <- residual_dist[!(code_system == "icd10" & year_id==2015)]
residual_dist[, sum(N), by = c("toc", "source")]

# collapse on toc
residual_dist <- residual_dist[, .(N = sum(N)), by = c("acause", "sex_id", "code_system", "toc", "year_id", "age_group_years_start")]

# We treat NF differently because of lots of missingness by age/year
residual_dist_a <- residual_dist[toc != "NF"]
residual_dist_b <- residual_dist[toc == "NF"]

# For NF only - expand out to all age/years and remove year pattern (sum across years) 
combos <- unique(residual_dist[,.(acause, sex_id, code_system, year_id, age_group_years_start)])[, toc := "NF"]
residual_dist_b <- merge(residual_dist_b, combos, by = c("acause", "sex_id", "code_system", "toc", "age_group_years_start", "year_id"), all = T)
residual_dist_b[, N := sum(N, na.rm = T), by = c("acause", "sex_id", "code_system", "toc", "age_group_years_start")]

# combine again
residual_dist <- rbind(residual_dist_a, residual_dist_b)

# change names to match the pred
residual_dist[, acause:=paste0(gsub("^_","", acause), "_pred")]

pred_vars <- unique(residual_dist$acause)
# decast 6 injuries wide to get into matrix shape
residual_dist_wide <- dcast(residual_dist, ...~acause, value.var = "N", fill = 0)

# Rescale so proportions sum to 1
residual_dist_wide[, tot_prop := rowSums(residual_dist_wide[,pred_vars, with = F])]
residual_dist_wide[, (pred_vars) := lapply(.SD, function(n) n/tot_prop), .SDcols = pred_vars]

# plot it
residual_dist_long <- melt(residual_dist_wide, measure.vars = pred_vars, value.name = 'prop', variable.name = 'inj_acause')

#-------------------------------------------
# Now make the residual distribution map square. Lots of various assumptions to fill in the full time/age range
#   - year + age loop should be fairly automated now
#-------------------------------------------

what_to_fill <- tidyr::crossing(sex_id = c(1,2), year_id = seq(2000, 2019), age_group_years_start = unique(residual_dist_wide$age_group_years_start), toc = unique(residual_dist_wide$toc)) %>% as.data.table()
what_to_fill[, code_system := ifelse(year_id < 2015, "icd9", "icd10")]
what_to_fill <- rbind(what_to_fill, what_to_fill[year_id == 2015][, code_system := "icd9"])

what_to_fill_checked <- merge(what_to_fill, residual_dist_wide, by = c("sex_id", "year_id", "age_group_years_start", "toc", "code_system"), all = T)
fix <- what_to_fill_checked[is.na(tot_prop), .(toc, year_id, code_system, sex_id, age_group_years_start)]

# First fix years missing in entirety (ages*2sexes)
years_missing <- fix[, .N, by = c('toc', 'year_id', 'code_system')][N == length(unique(residual_dist_wide$age_group_years_start)) *2]

if(nrow(years_missing) > 0){
  for(i in 1:nrow(years_missing)){
    
    row <- years_missing[i,]
    
    # use the closest year in the same code_system
    ref_data <- residual_dist_wide[toc == row$toc & code_system == row$code_system]
    poss_years <- unique(ref_data$year_id)
    
    closest_year <- unique(ref_data$year_id)[which(poss_years - row$year_id == min(poss_years - row$year_id))]
    
    add_data <- ref_data[year_id == closest_year]
    add_data[, year_id := row$year_id]
    
    residual_dist_wide <<- rbind(residual_dist_wide, add_data)
  }
}

# Secondly fix ages that are still missing
what_to_fill_checked <- merge(what_to_fill, residual_dist_wide, by = c("sex_id", "year_id", "age_group_years_start", "toc", "code_system"), all = T)
fix <- what_to_fill_checked[is.na(tot_prop), .(toc, year_id, code_system, sex_id, age_group_years_start)]

ages_missing <- fix[, .N, by = c('toc', 'age_group_years_start', 'code_system')]

if(nrow(ages_missing) > 0){
  for(i in 1:nrow(ages_missing)){
    
    row <- ages_missing[i,]
    
    rows_to_add <- fix[toc == row$toc & code_system == row$code_system & age_group_years_start == row$age_group_years_start]
    
    # first check if that age exists in another year (same code system)
    # -----
    ref_data <- residual_dist_wide[toc == row$toc & code_system == row$code_system & age_group_years_start == row$age_group_years_start]
    setnames(ref_data, 'year_id', 'ref_year')
    
    add_one <- merge(rows_to_add, ref_data, by = c('sex_id', 'toc', 'code_system', 'age_group_years_start'), all.x = T, allow.cartesian = T)  
    add_one[, diff := abs(year_id - ref_year)]
    add_one[, closest_year := min(diff), by = c('sex_id', 'toc', 'code_system', 'age_group_years_start', 'year_id')]
    add_one[, rank := 1:.N, by = c('sex_id', 'toc', 'code_system', 'age_group_years_start', 'year_id', 'diff')]  # add rank in case multiple years are equally close
    
    add_one <- add_one[(rank == 1 & closest_year == diff) | is.na(ref_year)][, `:=` (ref_year = NULL, diff = NULL, closest_year = NULL, rank = NULL)]
  
    still_to_add <- add_one[is.na(tot_prop), .(sex_id, toc, code_system, age_group_years_start, year_id)]
    add_one <- add_one[!is.na(tot_prop)]
    
    if(nrow(still_to_add) == 0){
      add_two <- data.table()
    }else{
    
      # if not, use closest age in the same year
      #------
      ref_data <- residual_dist_wide[toc == row$toc & code_system == row$code_system & year_id %in% still_to_add$year_id]
      setnames(ref_data, 'age_group_years_start', 'ref_age')
      
      add_two <- merge(still_to_add, ref_data, by = c('sex_id', 'toc', 'code_system', 'year_id'), all.x = T, allow.cartesian = T)  
      add_two[, diff := abs(age_group_years_start - ref_age)]
      add_two[, closest_age := min(diff), by = c('sex_id', 'toc', 'code_system', 'year_id', 'age_group_years_start')]
      add_two[, rank := 1:.N, by = c('sex_id', 'toc', 'code_system', 'age_group_years_start', 'year_id', 'diff')]  # add rank in case multiple years are equally close
      
      add_two <- add_two[(rank == 1 & closest_age == diff)][, `:=` (ref_age = NULL, diff = NULL, closest_age = NULL, rank = NULL)]
    }
    
    full_add <- rbind(add_one, add_two)
    
    if(nrow(full_add) != nrow(rows_to_add)){
      stop("Still missing some backfilling of ages after the fill in")
    }
    
    residual_dist_wide <<- rbind(residual_dist_wide, full_add)
  }
}

#  Final check we have square residual map!
test <- residual_dist_wide[,.N, by = c("toc", "code_system")][order(N)]
if(length(unique(test$N)) > 2){
  print(test)
  
  what_to_fill_checked <- merge(what_to_fill, residual_dist_wide, by = c("sex_id", "year_id", "age_group_years_start", "toc", "code_system"), all = T)
  print(what_to_fill_checked[is.na(tot_prop), .(toc, year_id, code_system, sex_id, age_group_years_start)])
  
  stop("Don't have the right number of age/year combinations for residual patterns!")
}

# Save final residual map
#----
write.csv(residual_dist_wide, paste0(map_version, "/intermediates//residual_for_package.csv"), row.names = F)

print('Saved residual map for all excluded dxs!')

#-----------------------------------------------------------------------------------------------------------------------------------------------
#    Validation / additional diagnostics here on out!
#-----------------------------------------------------------------------------------------------------------------------------------------------

print("Moving onto various diagnostics for validation purposes")

#--------------------------------------------
# Plot final residual distribution
#--------------------------------------------
pdf(paste0(map_version, "/diagnostics/residual_distribution.pdf"), width = 11, height = 8)
for(t in unique(residual_dist_long$toc)){
  for(s in c(1,2)){
    
    title <- paste0(t, " ", ifelse(s == 1, "males", "females"))
    
    if(t == "NF"){
      plot <- ggplot(residual_dist_long[sex_id == s & toc == t & year_id %in% c(2010, 2019)], aes(x = as.factor(age_group_years_start), y = prop, fill = gsub("_pred", "", inj_acause)))+
        geom_bar(stat = "identity")+
        facet_wrap(~code_system, ncol = 1)+
        labs(title = title, y = "proportion", x = "age", fill = "")+theme_bw()
    }else{
      plot <- ggplot(residual_dist_long[sex_id == s & toc == t], aes(x = as.factor(age_group_years_start), y = prop, fill = gsub("_pred", "", inj_acause)))+
        geom_bar(stat = "identity")+
        facet_wrap(~year_id+code_system)+
        labs(title = title, y = "proportion", x = "age", fill = "")+theme_bw()
    }
    
    
    print(plot)
    
  }
}
dev.off()

#--------------------------------------------
# Track fraction of N code encounters that will get a specific map (vs residual map)
#--------------------------------------------

counts_orig <- arrow::open_dataset(paste0(map_version, "/inputs/validation/count_inj_nec/")) %>% collect() %>% as.data.table
counts <- merge(counts_orig, trunc_map[,.(code_system, dx = icd_code, trunc_code)], by = c("dx", "code_system"), all.x = T)
counts <- counts[, (N = sum(N)), by = c("trunc_code", "source", "code_system", "toc")]
setnames(counts, "trunc_code", "dx")

counts2 <- merge(counts, counts_by_dx_excluded, by = c("dx", "toc", "code_system"), all.x = T)
counts2[included > 1, included := 1]
counts2[is.na(included), included := 0]

check1 <- counts2[, sum(V1), by = c("included", "toc", "source")]
check1[, prop := V1 / sum(V1), by = c("toc", "source")]
check1[, prop := prop*100]

pdf(paste0(map_version, "/diagnostics/residual_threshold.pdf"), width = 11, height = 8)
ggplot(check1[included == 1], aes(x = source, y = prop, label = round(prop, 2)))+geom_bar(stat = "identity", fill = "lightblue")+
  geom_text()+
  facet_grid(~toc, scales = "free_x")+
  labs(title = "Percent of all inj_NEC codes covered by the inj codes with sufficient observations",
       subtitle = paste0("With a threshold of ", threshold, " observations across all ages/sex/years"))+theme_bw()
dev.off()

#--------------------------------------------
# Plot the raw distribution for all sources
#--------------------------------------------

# calculate proportion of codes that get redistributed to that injury cause by dx and age (across year + sex)
plot_data <- plot_data_long1[, .(prop = sum(num)/sum(tot_dx), num = sum(tot_dx)), by = c("dx", "inj_acause", "age_group_years_start", "source", "toc", "code_system")]

plot_data <- merge(plot_data, counts_by_dx, by = c("dx", "source", "code_system", "toc"), all = T)

plot_data <- plot_data[rev(order(N))]
plot_data[is.na(included), included := 0]

pdf(paste0(map_version, "/diagnostics/dx_raw_distribution_allcodes_toc.pdf"), width = 11, height = 8)

for(d in unique(plot_data[!is.na(N)]$dx)){
  
  tmp_plot_data <- plot_data[dx == d]
  tmp_plot_data[, facet_label := paste(code_system, source, toc)]
  tmp_plot_data[, facet_label := paste(code_system, source, toc, "(n=", N, " ->", ifelse(included==1, " included)", " residual)"))]
  
  for(t in unique(tmp_plot_data$toc)){
    
    title <- paste0(d, ": ", dex_inj_code_list[icd_code == d, icd_name], " (", unique(tmp_plot_data$code_system), ") - ", t)
    
    p <- ggplot(tmp_plot_data[dx==d & toc == t], aes( x = as.factor(age_group_years_start), y = prop, fill = inj_acause))+
      facet_wrap(~facet_label, scales = "free_y")+geom_bar(stat = "identity")+theme_bw()+
      labs(x = "age", y = "proportion", title = title)
    
    print(p)
    
  }

  
  
}
dev.off()


print('done!')


