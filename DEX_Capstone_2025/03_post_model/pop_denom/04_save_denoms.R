# ---------------------------------------------
#  Population denominators; make mutually exclusive and ensure sum to total population
#              We save two sets of population denominators here. One for compile (mutually exclusive), the other for figures/analysis (NOT mutually exclusive)
# 
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

# SCRIPT SETTINGS
#--------------------------
if(interactive()){
  work_dir <- "/FILEPATH/"
  plot <- F
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  work_dir <- args[1]
  plot <- args[2]
}


compile_save_dir <- paste0(work_dir, "/denoms_for_compile")
denoms_true_save_dir <- paste0(work_dir, "/denoms_true")

if(!dir.exists(compile_save_dir)){dir.create(compile_save_dir)}
if(!dir.exists(denoms_true_save_dir)){dir.create(denoms_true_save_dir)}

age_metadata <- fread(paste0(work_dir, "/FILEPATH")) 

denoms <- read_parquet(paste0(work_dir, "/FILEPATH")) %>% as.data.table()
mdcd_denoms <- read_parquet(paste0(work_dir, "/FILEPATH")) %>% as.data.table()
mdcr_denoms <- read_parquet(paste0(work_dir, "/FILEPATH")) %>% as.data.table()
mdcr_priv_rates <- read_parquet(paste0(work_dir, "/FILEPATH/")) %>% as.data.table()

# here we expand out to TOC since mdcr has TOC
denoms <- merge(denoms, mdcr_denoms, by = c("state_name","mcnty", "year_id" ,"age_group_id", "sex_id", "age_group_name", "pop", "cnty_name"))
denoms <- merge(denoms, mdcd_denoms, by = c("state_name","mcnty", "year_id" ,"age_group_id", "sex_id", "age_group_name", "pop", "cnty_name"))


#--------
# FIRST WE CALCULATE PERFECTLY MUTUALLY EXCLUSIVE BINS 
#--------

# Trust CMS MDCR RIF most, which informs mdcr and duals 
denoms[mdcr_mdcd_denom > mdcr_denom, mdcr_mdcd_denom := mdcr_denom]
denoms[mdcr_mdcd_denom > mdcd_denom, mdcd_denom := mdcr_mdcd_denom]

# want mdcd to be mutually exclusive!
denoms[, mdcd_denom := mdcd_denom - mdcr_mdcd_denom]

# apply the mdcr_priv rate
denoms <- merge(denoms, mdcr_priv_rates, by = c("sex_id", "age_group_name", "mcnty", "year_id", "state_name", "age_group_id"), all = T)
denoms[, mdcr_priv_denom := mdcr_priv_rate*mdcr_denom]
denoms[mdcr_priv_denom > mdcr_ffs_denom, mdcr_priv_denom := mdcr_ffs_denom]

# private denom should not include mdcr (mdcr_priv)
denoms[mdcr_priv_denom > priv_denom, mdcr_priv_denom := priv_denom]
denoms[, priv_denom := priv_denom - mdcr_priv_denom]

# private is almost always way too high, even after subtracting the duals with medicare as we did above.
#    compare to population and where private is too high, prioritize medicare and lower private
denoms[mdcr_denom + priv_denom > pop, priv_denom := pop - mdcr_denom][priv_denom <0, priv_denom := 0]
denoms[mdcr_priv_denom + priv_denom > pop, priv_denom := pop - mdcr_priv_denom][priv_denom <0, priv_denom := 0]

# we've changed mdcr_mdcd denom above, need to make sure ffs and mc still sum
denoms[, `:=` (comp_mdcr_mdcd = mdcr_mdcd_ffs_denom + mdcr_mdcd_mc_denom)]
denoms[, `:=` (mdcr_mdcd_ffs_denom = ifelse(comp_mdcr_mdcd==0, 0, (mdcr_mdcd_ffs_denom / comp_mdcr_mdcd )*mdcr_mdcd_denom),
               mdcr_mdcd_mc_denom = ifelse(comp_mdcr_mdcd==0, 0, (mdcr_mdcd_mc_denom / comp_mdcr_mdcd )*mdcr_mdcd_denom))]

# Back out medicare only as medicare denominator
denoms[, mdcr_ffs_denom := mdcr_ffs_denom - mdcr_mdcd_ffs_denom - mdcr_priv_denom]
denoms[, mdcr_mc_denom := mdcr_mc_denom - mdcr_mdcd_mc_denom]
denoms[, mdcr_denom := mdcr_denom - mdcr_mdcd_denom - mdcr_priv_denom]

denoms[mdcr_ffs_denom < 0 , mdcr_ffs_denom := 0]
denoms[mdcr_mc_denom < 0 , mdcr_mc_denom := 0]
denoms[mdcr_denom < 0 , mdcr_denom := 0]

# we've changed mdcr denom above, need to make sure ffs and mc still sum
denoms[, `:=` (comp_mdcr = mdcr_ffs_denom + mdcr_mc_denom)]
denoms[, `:=` (mdcr_ffs_denom = ifelse(comp_mdcr==0, 0, (mdcr_ffs_denom / comp_mdcr )*mdcr_denom),
               mdcr_mc_denom = ifelse(comp_mdcr==0, 0, (mdcr_mc_denom / comp_mdcr )*mdcr_denom))]

# CHECK
denoms[, `:=` (comp_mdcr = mdcr_ffs_denom + mdcr_mc_denom,
               comp_mdcr_mdcd = mdcr_mdcd_ffs_denom + mdcr_mdcd_mc_denom)]
denoms[abs(comp_mdcr - mdcr_denom) > 0.1]
denoms[abs(comp_mdcr_mdcd - mdcr_mdcd_denom) > 0.1]

#--------
# NOW COMPARE TO POPULATION AND SCALE SO IT SUMS TO POP
#--------

# compare to population - mutually exclusive should not be greater than pop!
denoms[, comp_sum := mdcr_denom + mdcd_denom + priv_denom + mdcr_priv_denom + mdcr_mdcd_denom + unins_denom]

# IF RX, assign any unaccounted to uninsured!
denoms[toc == 'RX' & pop - comp_sum > 0, unins_denom := unins_denom + (pop - comp_sum)]
denoms[, comp_sum := mdcr_denom + mdcd_denom + priv_denom + mdcr_priv_denom + mdcr_mdcd_denom + unins_denom]

# SCALE
denoms[, ratio_to_pop :=  pop / comp_sum]
denoms[is.na(ratio_to_pop) | is.infinite(ratio_to_pop) | is.nan(ratio_to_pop), ratio_to_pop := 1]

denoms[, `:=` (
  mdcd_denom = mdcd_denom * ratio_to_pop,
  mdcr_denom = mdcr_denom * ratio_to_pop,
  mdcr_ffs_denom = mdcr_ffs_denom * ratio_to_pop,
  mdcr_mc_denom = mdcr_mc_denom * ratio_to_pop,
  mdcr_mdcd_denom = mdcr_mdcd_denom * ratio_to_pop,
  mdcr_mdcd_ffs_denom = mdcr_mdcd_ffs_denom * ratio_to_pop,
  mdcr_mdcd_mc_denom = mdcr_mdcd_mc_denom * ratio_to_pop,
  unins_denom = unins_denom * ratio_to_pop,
  priv_denom = priv_denom * ratio_to_pop,
  mdcr_priv_denom = mdcr_priv_denom * ratio_to_pop
)]

# need to split mdcd out after the adjusting above
denoms[, mdcd_mc_denom := mdcd_denom *percent_mdcd_mco]
denoms[, mdcd_ffs_denom := mdcd_denom *(1-percent_mdcd_mco)]


# now calculate any mdcr in case we want this in the future 
denoms[, any_mdcr_denom := mdcr_denom + mdcr_mdcd_denom + mdcr_priv_denom]
denoms[, any_mdcr_ffs_denom := mdcr_ffs_denom + mdcr_mdcd_ffs_denom + mdcr_priv_denom]
denoms[, any_mdcr_mc_denom := mdcr_mc_denom + mdcr_mdcd_mc_denom]

#--------
# CLEAN UP FOR SAVING - STATE + NATIONAL AGGREGATES
#--------

# make state and national aggregates 
save_denom_vars <- c(colnames(denoms)[grepl("_denom", colnames(denoms))], 'pop')

id_vars <- c("state_name", "mcnty", "year_id", "age_group_name", "age_group_id", "sex_id", "toc")

denoms_save <- denoms[,c(id_vars, save_denom_vars), with = F]

state_denoms <- denoms_save[, lapply(.SD, sum), by = setdiff(id_vars, "mcnty"), .SDcols = save_denom_vars]
nat_denoms <- denoms_save[, lapply(.SD, sum), by = setdiff(id_vars, c("mcnty", "state_name")), .SDcols = save_denom_vars]

# combine all together
all_denoms <- rbindlist(list(denoms_save[, geo := 'county'], 
                             state_denoms[, geo := 'state'], 
                             nat_denoms[, geo := 'national']), fill = T)

# add on helper columns
all_denoms <- merge(all_denoms, age_metadata[,.(age_group_years_start, age_group_id)], by = 'age_group_id')
all_denoms <- merge(all_denoms, states[,.(state_name, location = abbreviation, state = abbreviation)], by = "state_name", all.x = T)
all_denoms[!is.na(mcnty), location := mcnty]
all_denoms[geo == "national", `:=` (location = 'USA', state = 'USA')]
all_denoms[, `:=` (state_name = NULL, age_group_id = NULL, mcnty = NULL)]

#--------
# SAVE - DENOMS FOR COMPILE
#--------

compile_save_dir

arrow::write_dataset(dataset = all_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type = 'total', pop, denom = unins_denom, pri_payer = "oop")], 
                     path = paste0(compile_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))    


arrow::write_dataset(dataset = all_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type = 'total', pop, denom = priv_denom, pri_payer = "priv")], 
                     path = paste0(compile_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))    


arrow::write_dataset(dataset = all_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type = 'total', pop, denom = mdcr_priv_denom, pri_payer = "mdcr_priv")], 
                     path = paste0(compile_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))    

# reshape medicare
mdcr_denoms <- melt(all_denoms, id.vars = c("year_id", "age_group_name", "sex_id", "toc", "pop", "geo", "age_group_years_start", "location", "state"), 
                    measure.vars = c("mdcr_denom", "mdcr_ffs_denom", "mdcr_mc_denom","any_mdcr_denom", "any_mdcr_ffs_denom", "any_mdcr_mc_denom", "mdcr_mdcd_denom", "mdcr_mdcd_ffs_denom", "mdcr_mdcd_mc_denom"), 
                    value.name = "denom", variable.name = "type")
mdcr_denoms[, pri_payer := "mdcr"]
mdcr_denoms[str_detect(type, "mdcd"), pri_payer := "mdcr_mdcd"]
mdcr_denoms[str_detect(type, "any"), pri_payer := "any-mdcr"]
mdcr_denoms[, type := str_remove(type, "mdcd_")]
mdcr_denoms[, type := str_remove(type, "any_")]

mdcr_denoms[type == "mdcr_denom", type := "total"]
mdcr_denoms[type == "mdcr_ffs_denom", type := "ffs"]
mdcr_denoms[type == "mdcr_mc_denom", type := "mc"]

# save all three medicare payers at once bc parquet!
arrow::write_dataset(dataset = mdcr_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type, pop, denom, pri_payer)], 
                     path = paste0(compile_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))    


# reshape medicaid
mdcd_denoms <- melt(all_denoms, id.vars = c("year_id", "age_group_name", "sex_id", "toc", "pop", "geo", "age_group_years_start", "location", "state"), 
                    measure.vars = c("mdcd_denom", "mdcd_ffs_denom", "mdcd_mc_denom"), 
                    value.name = "denom", variable.name = "type")
mdcd_denoms[, pri_payer := "mdcd"]
mdcd_denoms[type == "mdcd_denom", type := "total"]
mdcd_denoms[type == "mdcd_ffs_denom", type := "ffs"]
mdcd_denoms[type == "mdcd_mc_denom", type := "mc"]
mdcd_denoms[, type := as.character(type)]

# save all three medicare payers at once bc parquet!
arrow::write_dataset(dataset = mdcd_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type, pop, denom, pri_payer)], 
                     path = paste0(compile_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))    



#--------
# CONVERT TO DENOMS TRUE (mdcr = any medicare, priv = any private, mdcd = any mdcd) AND SAVE
#--------

# SAVE AS PARQUET ONLY, including duals

denoms_true_save_dir
print("Onto saving for denoms_true!")

# save mdcr and mdcr_mdcd
mdcr_denoms_true <- mdcr_denoms[pri_payer %in% c('any-mdcr', 'mdcr_mdcd')]
mdcr_denoms_true[pri_payer == 'any-mdcr', pri_payer := 'mdcr']

arrow::write_dataset(dataset = mdcr_denoms_true[pri_payer %in% c('mdcr', 'mdcr_mdcd'),.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type, pop, denom, pri_payer)], 
                     path = paste0(denoms_true_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))    

# save mdcr_priv and oop
arrow::write_dataset(dataset = all_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type = 'total', pop, denom = mdcr_priv_denom, pri_payer = "mdcr_priv")],
                     path = paste0(denoms_true_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))
arrow::write_dataset(dataset = all_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type = 'total', pop, denom = unins_denom, pri_payer = "oop")],
                     path = paste0(denoms_true_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))  

# calculate (and save) medicaid
mdcd_percent_mco <- fread(paste0(work_dir, "/FILEPATH.CSV"))

all_denoms[, percent_mco := mdcd_mc_denom / (mdcd_denom)]
all_denoms[is.na(percent_mco), percent_mco := 0] # put 100% of duals in ffs
all_denoms[, mdcd_any := mdcd_denom + mdcr_mdcd_denom]
all_denoms[, mdcd_ffs_any := mdcd_any * (1 - percent_mco)]
all_denoms[, mdcd_mc_any := mdcd_any * percent_mco]

mdcd_denoms <- melt(all_denoms, id.vars = c("year_id", "age_group_name", "sex_id", "toc", "pop", "geo", "age_group_years_start", "location", "state"), 
                    measure.vars = c("mdcd_any", "mdcd_ffs_any", "mdcd_mc_any"), 
                    value.name = "denom", variable.name = "type")
mdcd_denoms[, pri_payer := "mdcd"]
mdcd_denoms[type == "mdcd_any", type := "total"]
mdcd_denoms[type == "mdcd_ffs_any", type := "ffs"]
mdcd_denoms[type == "mdcd_mc_any", type := "mc"]
mdcd_denoms[, type := as.character(type)]

arrow::write_dataset(dataset = mdcd_denoms[pri_payer == "mdcd"],
                     path = paste0(denoms_true_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))  

# calculate (and save) private
all_denoms[, priv_any := priv_denom + mdcr_priv_denom]

arrow::write_dataset(dataset = all_denoms[,.(geo, toc, year_id, state, location, age_group_years_start, sex_id, type = 'total', pop, denom = priv_any, pri_payer = "priv")],
                     path = paste0(denoms_true_save_dir,'/data/'),
                     partitioning = c("geo","toc","pri_payer"))  





