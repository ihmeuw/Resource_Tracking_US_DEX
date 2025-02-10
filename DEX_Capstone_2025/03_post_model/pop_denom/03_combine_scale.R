# ---------------------------------------------
#  Population denominators; ensure square, convert rates to counts, and scale to state envelope
#                        at this point all population denominators are NOT mutually exclusive 
#                       (ex: now 'mdcd' represents the # of people with mdcd, regardless of other insurances)
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
source('/FILEPATHget_age_metadata.R')
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

save_dir <- paste0(work_dir, "/FILEPATH/")
mdcd_save_dir <-  paste0(work_dir, "/FILEPATH/")
if(!dir.exists(mdcd_save_dir)){dir.create(mdcd_save_dir)}

#--------
# LOAD IN DATA - rate space
#--------

mdcd_data <- fread(paste0(save_dir, "FILEPATH.CSV"))
private_insurance_data <- fread(paste0(save_dir, "/FILEPATH.CSV"))
uninsured_data <- fread(paste0(save_dir, "/FILEPATH.CSV"))
mdcr_data <- fread(paste0(work_dir, "/FILEPATH.CSV")) 
mdcr_priv_data <- fread(paste0(save_dir, "/FILEPATH.CSV")) 

state_rates <- fread(paste0(work_dir, "/FILEPATH.CSV"))
state_rates <- dcast(state_rates, year_id+state_name+sex_id+age_group_name~payer, value.var = "scaled_rate")
# reshape wide
setnames(state_rates, c("mdcd", "mdcr", "priv", "uninsured"),
         c("mdcd_rate", "mdcr_rate", "pri_rate", "uninsured_rate"))
age_metadata <- fread(paste0(work_dir, "FILEPATH.CSV")) # reference this with custom age group 85+
county_names <- counties[current == 1,.(mcnty, cnty_name, state_name)]
county_names <- county_names[,.(cnty_name = min(cnty_name)), by=c("mcnty","state_name")]

#--------
# RENAME VARIABLES, ADD ON AGE
#--------

private_insurance_data <- unique(private_insurance_data[,.(state_name, year_id, age_start, age_end, sex_id, mcnty, priv_rate = priv_rate_step2)])
private_insurance_data <- merge(private_insurance_data, age_metadata[,.(age_start = age_group_years_start, age_group_name, age_group_id)], by = "age_start")

mdcd_data <- unique(mdcd_data[,.(state_name, year_id, age_start, age_end, sex_id, mcnty, mdcd_rate = mdcd_rate_step2)])
mdcd_data <- merge(mdcd_data, age_metadata[,.(age_start = age_group_years_start, age_group_name, age_group_id)], by = "age_start")

uninsured_data <- unique(uninsured_data[,.(state_name, year_id, age_group_name, sex_id, mcnty, unins_rate = unins_rate_step2)])
uninsured_data <- merge(uninsured_data, age_metadata[,.(age_group_id, age_group_name)], by = c("age_group_name"))

mdcr_priv_data <- unique(mdcr_priv_data[,.(state_name, year_id, age_group_name, sex_id, mcnty, mdcr_priv_rate = mdcr_priv_rate_step2)])
mdcr_priv_data <- merge(mdcr_priv_data, age_metadata[,.(age_group_id, age_group_name)], by = c("age_group_name"))
write_parquet(mdcr_priv_data, paste0(work_dir, "/FILEPATH.parquet"))

#--------
# MDCR SPECIAL TREATMENT - use different variables for different TOCs
#--------
mdcr_data <- unique(mdcr_data[,.(state_name, year_id, age_group_name, sex_id, mcnty, mdcr_rate = mdcr_rate_rif, variable)]) 

# add toc depending on variable
mdcr_data[ variable %like% "dual", name := "mdcr_mdcd_rate"]
mdcr_data[ ! variable %like% "dual", name := "mdcr_rate"]
mdcr_data[ variable %like% "part_c", name := paste0(name, "_mc")]
mdcr_data[ ! variable %like% "part_c" & ! variable %like% "any", name := paste0(name, "_ffs")]

unique(mdcr_data[,.(variable, name)])[order(name)]

mdcr_data_by_toc <- rbindlist(lapply(c("part_a", "part_b", "part_c", "part_d_only", "part_cd", "any"), function(v){
  
  message(v)
  if(v == 'part_a'){tocs <- c("IP", "HH", "NF")
  }else if(v == 'part_b'){ tocs <- c("AM", "ED", "DV")
  }else if(v == 'part_c'){ tocs <- c("IP", "HH", "NF", "AM", "ED", "DV",'all')
  }else if(v == 'part_d_only'){ tocs <- 'RX'
  }else if(v == 'part_cd'){ tocs <- 'RX'
  }else if(v == 'any'){ tocs <- 'all'}
  
  mdcr_data_sub <- mdcr_data[variable %like% paste0(v, "_rate")]
  print(unique(mdcr_data_sub$variable))
  mdcr_data_sub <- tidyr::crossing(mdcr_data_sub, data.table('toc' = tocs)) %>% as.data.table()

  return(mdcr_data_sub)

}))
mdcr_data_by_toc[, variable := NULL]

# now reshape wide
mdcr_data <- dcast(mdcr_data_by_toc, ... ~ name, value.var = "mdcr_rate")

mdcr_data <- merge(mdcr_data, age_metadata[,.(age_group_id, age_group_name)], by = c("age_group_name"))

# make sure square
get_square_grid <- function(){
  
  county_list <- unique(counties[current == 1, .(mcnty, state_name)])
  sex_id <- c(1,2)
  age_group_id <- age_metadata$age_group_id
  year_id <- 1999:2019
  
  square_grid <- tidyr::crossing(county_list, sex_id, age_group_id, year_id) %>% as.data.table()
  square_grid <- merge(square_grid, age_metadata[,.(age_group_id, age_group_name)], by = "age_group_id")
  
  return(square_grid) 
}
square_grid <- get_square_grid()
mdcr_data <- merge(mdcr_data, square_grid, by = c("mcnty", "state_name", "sex_id", "age_group_id", "age_group_name" , "year_id"), all = T)

to_save <- mdcr_data[is.na(mdcr_rate_ffs) | is.na(mdcr_rate_mc) | is.na(mdcr_mdcd_rate_ffs) | is.na(mdcr_mdcd_rate_mc)]
mdcr_data[toc!="all" & is.na(mdcr_rate_ffs), mdcr_rate_ffs := 0]
mdcr_data[toc!="all" & is.na(mdcr_rate_mc), mdcr_rate_mc := 0]
mdcr_data[toc!="all" & is.na(mdcr_mdcd_rate_ffs), mdcr_mdcd_rate_ffs := 0]
mdcr_data[toc!="all" & is.na(mdcr_mdcd_rate_mc), mdcr_mdcd_rate_mc := 0]

# make sure rates don't exceed 1
mdcr_data[ mdcr_rate_ffs + mdcr_rate_mc > 1] # lots
mdcr_data[ mdcr_mdcd_rate_ffs + mdcr_mdcd_rate_mc > 1] # fewer

mdcr_data[ mdcr_rate_ffs + mdcr_rate_mc > 1, `:=` (mdcr_rate_ffs = mdcr_rate_ffs / (mdcr_rate_ffs + mdcr_rate_mc),
                                                   mdcr_rate_mc = mdcr_rate_mc / (mdcr_rate_ffs + mdcr_rate_mc))]

mdcr_data[mdcr_mdcd_rate_ffs + mdcr_mdcd_rate_mc > 1, `:=` (mdcr_mdcd_rate_ffs = mdcr_mdcd_rate_ffs / (mdcr_mdcd_rate_ffs + mdcr_mdcd_rate_mc),
                                                              mdcr_mdcd_rate_mc = mdcr_mdcd_rate_mc / (mdcr_mdcd_rate_ffs + mdcr_mdcd_rate_mc))]

# add total rates
mdcr_data[toc!='all', mdcr_rate := mdcr_rate_ffs + mdcr_rate_mc]
mdcr_data[toc!='all', mdcr_mdcd_rate := mdcr_mdcd_rate_ffs + mdcr_mdcd_rate_mc]

mdcr_data[mdcr_mdcd_rate_mc > mdcr_mdcd_rate, mdcr_mdcd_rate_mc := mdcr_mdcd_rate]
mdcr_data[mdcr_rate_mc > mdcr_rate, mdcr_rate_mc := mdcr_rate]

mdcr_data[toc=='all', `:=` (mdcr_mdcd_rate_ffs = mdcr_mdcd_rate - mdcr_mdcd_rate_mc,
                            mdcr_rate_ffs = mdcr_rate - mdcr_rate_mc)]

mdcr_data <- mdcr_data[year_id <= 2019]

#--------
# ENSURE DATA SQUARE
#--------

expected_rows <- 3110*19*2*length(1999:2019)
mdcr_expected_rows <- expected_rows * (length(unique(mdcr_data$toc)))

if(!nrow(private_insurance_data) == expected_rows){stop()}
if(!nrow(mdcd_data) == expected_rows){stop()}
if(!nrow(mdcr_data) == mdcr_expected_rows){stop()}
if(!nrow(uninsured_data) == expected_rows){stop()}
if(!nrow(mdcr_priv_data) == expected_rows){stop()}

combined_data <- merge(uninsured_data, private_insurance_data, 
                       by = c("state_name","mcnty", "year_id" ,"age_group_id", "sex_id", 'age_group_name'))
combined_data <- merge(combined_data, mdcd_data, 
                       by = c("state_name","mcnty", "year_id" ,"age_group_id", "sex_id", "age_group_name", "age_start", "age_end"))

# now we have
#  - combined_data
#  - mdcr_data (long by TOC)

#--------
# CONVERT RATES TO COUNTS
#--------

ages <- fread(paste0(work_dir, "/FILEPATH.CSV"))

combined_data <- merge(combined_data, ages, by = c("mcnty", "sex_id","year_id", "age_group_id", "age_group_name"), all.x = T)
combined_data[is.na(pop)]

mdcr_data <- merge(mdcr_data, ages, by = c("mcnty", "sex_id","year_id", "age_group_id", "age_group_name"), all.x = T)
mdcr_data[is.na(pop)]

combined_data[, mdcd_denom := mdcd_rate*pop]
combined_data[, unins_denom := unins_rate*pop]
combined_data[, priv_denom := priv_rate*pop]
mdcr_data[, mdcr_denom := mdcr_rate*pop]
mdcr_data[, mdcr_ffs_denom := mdcr_rate_ffs*pop]
mdcr_data[, mdcr_mc_denom := mdcr_rate_mc*pop]
mdcr_data[, mdcr_mdcd_denom := mdcr_mdcd_rate*pop]
mdcr_data[, mdcr_mdcd_ffs_denom := mdcr_mdcd_rate_ffs*pop]
mdcr_data[, mdcr_mdcd_mc_denom := mdcr_mdcd_rate_mc*pop]

county_names <- counties[current == 1,.(mcnty, cnty_name, state_name)]
county_names <- county_names[,.(cnty_name = min(cnty_name)), by=c("mcnty","state_name")]

combined_data <- merge(combined_data, county_names, by = c("mcnty", "state_name"))
mdcr_data <- merge(mdcr_data, county_names, by = c("mcnty", "state_name"))


expected_rows <- 3110*19*2*length(1999:2019)
mdcr_expected_rows <- expected_rows * (length(unique(mdcr_data$toc)))


if(!nrow(combined_data)== expected_rows){stop()}
if(!nrow(mdcr_data)== mdcr_expected_rows){stop()}

setcolorder(combined_data, c("state_name", "mcnty", "cnty_name", "year_id", "age_group_name", "age_group_id", "sex_id"))
setcolorder(mdcr_data, c("state_name", "mcnty", "cnty_name", "year_id", "age_group_name", "age_group_id", "sex_id"))

# save MDCR here, to be used in 04_save_denoms
arrow::write_parquet(mdcr_data, paste0(work_dir, "/FILEPATH/mdcr_payer_pop_denoms_prescale.parquet"))


#--------
# SCALE COUNTS TO STATE
#   all but medicare
#--------

ages <- fread(paste0(work_dir, "/FILEPATH/county_population_age_sex.csv"))
ages <- merge(ages, unique(counties[,.(state_name, mcnty)]), by = "mcnty")
state_pop <- ages[,.(population = sum(pop)), by = c("year_id", "sex_id", "age_group_name", "state_name")]

state_rates <- merge(state_rates, state_pop, by = c("year_id", "sex_id", 'age_group_name', "state_name"))

state_rates[, `:=` (
  mdcd_state = mdcd_rate*population,
  uninsured_state = uninsured_rate*population,
  pri_state = pri_rate*population
)]

denoms <- copy(combined_data)

denoms_state <- denoms[, .(mdcd_tot = sum(mdcd_denom),
                           unins_tot = sum(unins_denom),
                           priv_tot = sum(priv_denom)), by = c("state_name", "year_id", "sex_id", "age_group_name")]

denoms_state <- merge(denoms_state, state_rates, by = c("year_id", "state_name", "sex_id", "age_group_name"))

denoms_state[, `:=` (
  mdcd_ratio = mdcd_tot/mdcd_state,
  unins_ratio = unins_tot/uninsured_state,
  pri_ratio = priv_tot/pri_state
)]

denoms_state[mdcd_state == 0 | is.nan(mdcd_ratio), mdcd_ratio := 1]
denoms_state[pri_state == 0 | is.nan(pri_ratio), pri_ratio := 1]
denoms_state[uninsured_state == 0 | is.nan(unins_ratio), unins_ratio := 1]

denoms <- merge(denoms, denoms_state[,.(year_id, state_name, sex_id, age_group_name, mdcd_ratio, unins_ratio, pri_ratio)], 
                by = c("year_id", "state_name", "sex_id", "age_group_name"))

# actually scale! (all but mdcr)
denoms[, `:=` (
  mdcd_denom = mdcd_denom/mdcd_ratio,
  unins_denom = unins_denom/unins_ratio,
  priv_denom = priv_denom/pri_ratio
)]


denoms[, `:=` (mdcd_ratio = NULL,
               unins_ratio  = NULL,
               pri_ratio = NULL)]

# make sure max denom is the pop
denoms[ mdcd_denom > pop, mdcd_denom := pop]
denoms[ priv_denom > pop, priv_denom := pop]
denoms[ unins_denom > pop, unins_denom := pop]


mdcd_denoms <- denoms[, .(year_id, state_name, sex_id, age_group_name, mcnty, cnty_name, age_group_id, age_start, age_end, mdcd_rate, pop, mdcd_denom)]
priv_unins_denoms <- denoms[, .(year_id, state_name, sex_id, age_group_name, mcnty, cnty_name, age_group_id, age_start, age_end, pop, unins_rate, unins_denom, priv_rate, priv_denom)]

# save priv + unins denoms here, to be used in 04_save_denoms
arrow::write_parquet(priv_unins_denoms, paste0(work_dir, "/FILEPATH.parquet"))


#--------
# EXTRA SCALING FOR MDCD 
#   - scale recent years based off of KFF numbers since we trust that most
#--------

mdcd_total_enroll <- read.csv("/FILEPATH.CSV", skip = 2) %>% as.data.table()
mdcd_total_enroll <- mdcd_total_enroll[Location %in% states$state_name]
mdcd_total_enroll <- melt(mdcd_total_enroll, id.vars = "Location")
mdcd_total_enroll <- mdcd_total_enroll[variable!="Footnotes"]
mdcd_total_enroll[, month := gsub("\\..*$", "", variable)]
mdcd_total_enroll[, year_id := as.numeric(gsub("^.*\\.", "", gsub("__.*$", "", variable)))]
mdcd_total_enroll <- mdcd_total_enroll[,.(state_name = Location, month, year_id, mdcd_enroll = value)]
mdcd_total_enroll <- mdcd_total_enroll[, .(mdcd_enroll = mean(as.numeric(mdcd_enroll), na.rm = T)), by = c("state_name", "year_id")]

mdcd_denom_ref <- mdcd_denoms[, .(compare_mdcd_enroll = sum(mdcd_denom)), by = c("state_name", "year_id")]
ratios <- merge(mdcd_denom_ref, mdcd_total_enroll, by = c("state_name", "year_id"), all.x = T)
ratios[, ratio := mdcd_enroll / compare_mdcd_enroll]
shift_by_state <- ratios[year_id == 2014, .(shift = mdcd_enroll - compare_mdcd_enroll, state_name)]
ratios <- merge(ratios, shift_by_state, by = c("state_name"))
ratios[is.na(ratio), ratio := (compare_mdcd_enroll + shift)/compare_mdcd_enroll]

mdcd_denoms <- merge(mdcd_denoms, ratios[,.(state_name, year_id, ratio)], by = c("state_name", "year_id"))

# intercept shift ACS prior to 2014 to intersect KFF trend
# scale the 2014+ to equal KFF
mdcd_denoms[!is.na(ratio), mdcd_denom := mdcd_denom*ratio]
mdcd_denoms[, ratio := NULL]


if(plot){
  ratios[, compare_mdcd_enroll_shifted := compare_mdcd_enroll + shift ]
  ratios[is.na(mdcd_enroll), mdcd_enroll := compare_mdcd_enroll_shifted]
  
  pdf(paste0(mdcd_save_dir, "/state_total_mdcd_enrollment.pdf"), width = 11, height = 8)
  for(s in unique(ratios$state_name)){
    print(ggplot(ratios[state_name == s], aes(x = year_id))+
            geom_line(aes(y = compare_mdcd_enroll_shifted, color = "ACS denom full time series, shifted"))+
            geom_line(aes(y = compare_mdcd_enroll, color = "ACS denom full time series"))+
            geom_line(aes(y = mdcd_enroll, color = "ACS denom 1991-2013, KFF/CMS 2014-2019"))+
            labs(title = s, y = "medicaid enrollment (#)")+theme_bw())
  }
  dev.off()
}


#--------
# SPLIT MDCD into FFS and MC
#   Step 1: prep proportion of mdcd enrollment that is mco
#--------

# states with no MCO
states_no <- read.csv("/FILEPATH.CSV", skip = 2) %>% as.data.table()
states_no <- states_no[Location %in% states$state_name]
states_no[Total.Medicaid.MCOs == "N/A", value := 0]
states_no_mco <- states_no[value == 0, Location]


# main data source = KFF/CMS % enrollment that is MCO, but they only have % post 2015 and just MCO enrollment prior
prop_mco <- read.csv("/FILEPATH.CSV", skip = 2) %>% as.data.table()

prop_mco <- prop_mco[Location %in% states$state_name]
prop_mco <- melt(prop_mco, id.vars = "Location")
prop_mco <- prop_mco[variable!="Footnotes"]
prop_mco[, metric := ifelse(grepl("Percent", variable), "prop_mco", "mdcd_mco_enroll" )]
prop_mco[, year_id := as.numeric(gsub("^X", "", gsub("__.*$", "", variable)))]
prop_mco[value == "N/A", value := NA]
prop_mco[grepl(",", value), value := gsub(",", "", value)]
prop_mco <- dcast(prop_mco, Location + year_id ~ metric, value.var = "value")
prop_mco[is.na(mdcd_mco_enroll), `:=` (mdcd_mco_enroll = 0, prop_mco = 0)] # documentation says missings mean no MCO, so set to 0
prop_mco[, mdcd_mco_enroll := as.numeric(mdcd_mco_enroll)]
prop_mco[, prop_mco := as.numeric(prop_mco)]
setnames(prop_mco, c("Location"), "state_name")

# 2012 is missing everywhere for the MDCD MCO enrollment variable, so replace with mean of 2011 and 2013
add_2012 <- prop_mco[year_id %in% c(2011, 2013)][,.(mdcd_mco_enroll_add = mean(mdcd_mco_enroll)), by = c("state_name")][, year_id := 2012]
prop_mco <- merge(prop_mco, add_2012, by = c("state_name", "year_id"), all = T)
prop_mco[year_id == 2012 & is.na(mdcd_mco_enroll ),mdcd_mco_enroll := mdcd_mco_enroll_add][, mdcd_mco_enroll_add := NULL]

mdcd_denom_ref <- mdcd_denoms[, .(tot_mdcd_enroll = sum(mdcd_denom)), by = c("state_name", "year_id")]
prop_mco <- merge(prop_mco, mdcd_denom_ref, by = c("state_name", "year_id"))

# only care about states with some MCOS
prop_mco <- prop_mco[!state_name %in% states_no_mco]

# now calculate proportion based off of our data
prop_mco[, calc_prop_mco := mdcd_mco_enroll / tot_mdcd_enroll]
prop_mco[ calc_prop_mco > 1, calc_prop_mco := 1]

# intercept shift calc to line up with obs at 2016
shift_by_state <- prop_mco[year_id == 2016, .(shift = prop_mco - calc_prop_mco, state_name)]
prop_mco <- merge(prop_mco, shift_by_state, by = c("state_name"))
prop_mco[, calc_prop_mco_shifted := calc_prop_mco + shift]

# if the shift bumps up into 0 or away from 0 where we weren't already there, don't use it
prop_mco[(calc_prop_mco_shifted <= 0 & calc_prop_mco > 0) | (calc_prop_mco_shifted > 0 & calc_prop_mco <= 0), calc_prop_mco_shifted := calc_prop_mco]
prop_mco[(calc_prop_mco_shifted <= 1 & calc_prop_mco > 1) | (calc_prop_mco_shifted > 1 & calc_prop_mco <= 1),  calc_prop_mco_shifted := calc_prop_mco]

prop_mco[ calc_prop_mco_shifted > 1 | calc_prop_mco_shifted <0, calc_prop_mco_shifted := ifelse(calc_prop_mco_shifted > 1, 1, 0)]

if(plot){
  ratios[is.na(mdcd_enroll), mdcd_enroll := compare_mdcd_enroll]
  pdf(paste0(mdcd_save_dir, "/FILEPATH"), width = 11, height = 8)
  for(s in unique(prop_mco$state_name)){
    print(ggplot(prop_mco[state_name == s], aes(x = year_id))+
            geom_line(aes(y = prop_mco, color = "obs"))+
            geom_line(aes(y = calc_prop_mco, color = "calc"))+
            geom_line(aes(y = calc_prop_mco_shifted, color = "calc_shifted"))+
            ylim(0,1)+
            labs(title = s, y = "prop mdcd that is mco")+theme_bw())
  }
  dev.off()
}

# now use obs for 2016-2019, shifted calculed for 2003-2015, and hold constant between 1999 and 2002
prop_mco[, source := "obs (KFF)"]
prop_mco[is.na(prop_mco), `:=` (prop_mco = calc_prop_mco_shifted, source = "calc + intercept shift")]
if(nrow(prop_mco[is.na(prop_mco) & year_id > 2002]) > 0){
  stop("don't have enough data to fill in 2003-2019!")
}

data2003 <- prop_mco[year_id == 2003][, `:=` (year_id = NULL, source = "held constant")]
data2003 <- tidyr::crossing(data2003, year_id = seq(1999, 2002)) %>% as.data.table()

prop_mco <- rbind(prop_mco[year_id > 2002],
                  data2003)

prop_mco[ prop_mco > 1 | prop_mco <0, prop_mco := ifelse(prop_mco > 1, 1, 0)]

# add on for states with no mco
data_other_states <- tidyr::crossing(state_name = states_no_mco,
                                     year_id = seq(1999, 2019),
                                     prop_mco = 0, 
                                     toc = unique(ratios$toc),
                                     source = "no mcos") %>% as.data.table()

prop_mco <- rbind(prop_mco,data_other_states, fill = T)
final_proportions <- prop_mco[, .(state_name, year_id, percent_mdcd_mco = prop_mco, source)]
final_proportions <- final_proportions[order(state_name, year_id)]

check <- final_proportions[,.N, by = c('state_name')]
if(length(unique(check$N)) > 1){
  stop("mdcd fractions aren't perfectly square!")
}


write.csv(final_proportions, paste0(mdcd_save_dir, "/FILEPATH"), row.names = F)

#--------
# SPLIT MDCD into FFS and MC
#   Step 2: apply proportion to mdcd enrollment to split data; we actually do this in 04_save denoms, but add on the percent_mdcd_mco here!
#--------

mdcd_denoms <- merge(mdcd_denoms, final_proportions[,.(state_name, year_id, percent_mdcd_mco)], by= c("state_name", "year_id"), all.x = T)

# save mdcd denoms here, to be used in 04_save_denoms
arrow::write_parquet(mdcd_denoms, paste0(work_dir, "/FILEPATH.parquet"))
