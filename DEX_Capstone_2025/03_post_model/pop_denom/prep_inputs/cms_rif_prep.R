# ------------------------------  
#
#    Use in house enrollment data to quantify population coverage for Medicare
#     - bring in population to get rates
#
#    Author: Emily Johnson
#
# ------------------------------  


source('/FILEPATH/get_population.R')
source('/FILEPATH/get_age_metadata.R')
load("/FILEPATH/states.RData")
load("/FILEPATH/merged_counties.RData")
pacman::p_load(arrow, dplyr, openxlsx, ggplot2, data.table, tidyr, tidyverse)
library(lbd.loader, lib.loc = 'FILEPATH')
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

t0 <- Sys.time()


if(interactive()){
  pop_denom_loc <- "/FILEPATH/"
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  pop_denom_loc <- args[1]
}


age <- readRDS("/FILEPATH")
age_metadata <- get_age_metadata(age_group_set_id = 27, gbd_round_id = 7) %>% setDT()
age_metadata <- age_metadata[age_group_years_start <= 85]
age_metadata[age_group_id == 31, `:=`(age_group_id = 160, age_group_name = "85 plus")]
age <- merge(age, age_metadata[,.(age_group_name, age_group_id, age = age_group_years_start)], by = "age")
age <- age[year >= 2000]

#----------
# MDCR 
#----------

# load in sample denom dataset
config <- get_config()
sample_denom_loc <- config$SAMPLE_DENOM$data_output_dir$MDCR

mdcr_sample_denoms <- open_dataset(sample_denom_loc) %>% 
  collect() %>% as.data.table() 

# subset to full for denominators
mdcr_sample_denoms <- mdcr_sample_denoms[sample == "full" & any_coverage!=0]

# add column for d only, no c
mdcr_sample_denoms[, part_d_only := ifelse(part_d == 1 & part_c == 0, 1, 0)]

# group 85+ to county population
mdcr_sample_denoms[age_group_id %in% c(31,32,235), `:=`(age_group_id = 160, age_start = 85, age_group_name = "85 plus")]
mdcr_sample_denoms <- mdcr_sample_denoms[,.(n = sum(n, na.rm = TRUE)), 
                                         by=c("age_start","mcnty","state_name","year_id","age_group_id","sex_id","part_a","part_b","part_c",
                                              "part_d_only","part_ab", 'part_cd',"any_coverage","dual_enrol")]

# reshape and filter to populations of interest - part_ab, part_c, and any coverage for all MDCR and for duals
part_vars <- c("part_a","part_b", "part_ab","part_c", "part_d_only", 'part_cd', "any_coverage")

mdcr_dual_sample <- copy(mdcr_sample_denoms)[dual_enrol == 1]
mdcr_dual_sample <- mdcr_dual_sample[,lapply(.SD, function(x) sum(x*n, na.rm = TRUE)), 
                                     by = c("age_start","mcnty","state_name","year_id","age_group_id","sex_id"),
                                     .SDcols = part_vars]
setnames(mdcr_dual_sample, part_vars, paste0("dual_", part_vars))

mdcr_sample_denoms <- mdcr_sample_denoms[,lapply(.SD, function(x) sum(x*n, na.rm = TRUE)), 
                                         by = c("age_start","mcnty","state_name","year_id","age_group_id","sex_id"),
                                         .SDcols = part_vars]

# basically a dcast wide, want to keep all columns
mdcr_sample_denoms <- merge(mdcr_sample_denoms, mdcr_dual_sample, by=c("age_start","mcnty","state_name","year_id","age_group_id","sex_id"), all = T)

# merge on county population to calculate rates
mdcr_sample_denoms <- merge(mdcr_sample_denoms, 
                            age[year %in% unique(mdcr_sample_denoms$year_id), .(mcnty, year_id = year, sex_id = sex, pop, age_group_id)], 
                            by = c("mcnty", "year_id", "sex_id", "age_group_id"), all = T)

# fix some oddities in population
mdcr_sample_denoms[pop != 0 & pop < 1, pop := 1]
setnafill(mdcr_sample_denoms, type = "const", fill = 0, cols = c(part_vars, paste0("dual_", part_vars)))

# calculate rates
tot_part_vars <- c(part_vars, paste0("dual_", part_vars))
for(c in tot_part_vars){
  mdcr_sample_denoms[, paste0(c, "_rate") := get(c) / pop]
}

# subset to just those we are interested in
mdcr_sample_denoms <- mdcr_sample_denoms[, c("year_id", "sex_id", "age_group_id", "mcnty", 
                                             tot_part_vars,
                                             paste0(tot_part_vars,  "_rate"),
                                             "pop"), with = F]
mdcr_sample_denoms <- merge(mdcr_sample_denoms, age_metadata[, .(age_group_id, age_group_name, age_start = age_group_years_start)], 
                            by = "age_group_id")

# now melt
mdcr_sample_denoms_long <- melt(mdcr_sample_denoms, 
                                id.vars = c("year_id", "sex_id", "age_group_id", "mcnty","age_group_name","age_start", "pop"), 
                                measure.vars = c(paste0(tot_part_vars, "_rate")), value.name = "mdcr_rate")
mdcr_sample_denoms_long[pop == 0 & is.nan(mdcr_rate), mdcr_rate := 0]
mdcr_sample_denoms_long <- mdcr_sample_denoms_long[!is.na(mdcr_rate)]
# Cap rate at 1
mdcr_sample_denoms_long[mdcr_rate > 1]
mdcr_sample_denoms_long[mdcr_rate > 1, mdcr_rate := 1]

mdcr_sample_denoms_long[variable %like% "any_coverage", variable := gsub("_coverage", "", variable)]

fwrite(mdcr_sample_denoms_long, paste0(pop_denom_loc, "/inputs/mdcr_rif_rates.csv"))

print("Done!")
print(Sys.time() - t0)
