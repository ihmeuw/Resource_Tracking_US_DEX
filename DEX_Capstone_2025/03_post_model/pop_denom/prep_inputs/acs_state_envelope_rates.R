# -------------------------------
#   Use ACS data - the age-sex specific rates 2010-2019 and the all-age, both-sex rates (we call cdhi) 1999-2019, 
#        to make age-sex specific rates for 1999-2019; these are all by payer too! (medicaid, medicare, uninsured, private)
#   
#   Author: Haley Lescinsky
# -------------------------------


# Set paths
#---------------------
pacman::p_load(arrow, dplyr, openxlsx, ggplot2, data.table, tidyr, tidyverse)
t0 <- Sys.time()

source('/FILEPATH/get_population.R')
load("/FILEPATH/states.RData")
load("/FILEPATH/merged_counties.RData")

if(interactive()){
  work_dir <- "/FILEPATH/"
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  work_dir <- args[1]
}

ages <- fread(paste0(work_dir, "/inputs/age_metadata.csv")) # reference this with custom age group 85+
viz_dir <- paste0(work_dir, "/intermediates/acs_data/")

#---------------------
# Load in datasets and get into right format
#---------------------

# Load in age-sex specific ACS rates: 2010-2019
#   Note - even though we don't use the smoothed rates here, use the smoothed files to get the DEX age bins
as_state_rates <- fread(paste0(work_dir, "/inputs/acs_state_and_county_smoothed.csv"))
as_state_rates <- as_state_rates[is.na(cnty) & year_id <= 2019]
as_state_rates[, group := NULL]
as_state_rates <- melt(as_state_rates, id.vars = c("cnty", "cnty_name" ,"year_id", "state_name", "age_start", "age_end", "age_group_name", "sex_id", "mcnty"), value.name = "as_rate", variable.name = "payer")
as_state_rates[, payer := as.character(payer)]
as_state_rates <- as_state_rates[payer %in% c("mdcd_rate", "mdcr_rate", "priv_rate", "uninsured_rate")]
as_state_rates[, payer := as.character(gsub("_rate", "", payer))]


# load in all-age both-sex ACS rates (CDHI): 1999-2019 
tot_state_rates <- fread("/FILEPATH/cdhi_rates.csv")
setnames(tot_state_rates, c("ins_mdcd", "ins_mdcr", "ins_private", "uninsured"),
         c("mdcd", "mdcr", "priv",  "uninsured"))
tot_state_rates <- melt(tot_state_rates, id.vars = c("state_name", "year_id", "location_id", "state", "abbreviation", "nid"), value.name = "cdhi_rate", variable.name = "payer")
tot_state_rates[, payer := as.character(payer)]



# load in populations  #31,32,235 = 85 plus
pops <- get_population(age_group_id = c(unique(ages$age_group_id), 31,32,235), 
                       sex_id = c(1,2), 
                       year_id = seq(1999, 2019), 
                       location_id = c(states$location_id), 
                       release_id = 7)
pops <- unique(pops[age_group_id %in% c(31,32,235), `:=` (population = sum(population), age_group_id = 160), by = c("location_id", "year_id", "sex_id")])
pops <- merge(pops, ages[,.(age_group_id, age_group_name)], by = "age_group_id")
pops <- merge(pops, states[,.(location_id, state_name)], by = "location_id")

agg_pops <- pops[, .(population = sum(population)), by = c("state_name", "year_id")]


#---------------------
# Begin scale the age-sex specific rates to the all-age both-sex rates using populations
#---------------------


#  Hold payer rates constant from 2010 to 1999
as_state_rates_2010 <- as_state_rates[year_id == 2010]
as_state_rates_add_on <- tidyr::crossing(as_state_rates_2010[, year_id := NULL], "year_id"= seq(1999, 2009)) %>% as.data.table()
as_state_rates <- rbind(as_state_rates, as_state_rates_add_on)

# Add on populations to convert as rates to num
as_state_rates <- merge(as_state_rates, pops, by = c("year_id", "state_name", "sex_id", "age_group_name"))
as_state_rates[, num := as_rate*population]
compare_as_state_rates <- as_state_rates[, .(num_from_as = sum(num), tot_pop = sum(population)), by = c("year_id", "state_name", "payer")]

#---------------------
# Break to fix MDCR tot state rate - oddly low, seems to be missing some age groups
#---------------------
mdcr_rescale <- compare_as_state_rates[payer == "mdcr", .(tot_rate_true = num_from_as / tot_pop, year_id, state_name)]
mdcr_rescale <- merge(mdcr_rescale, tot_state_rates[payer == "mdcr"], by = c("state_name", "year_id"))
mdcr_rescale[year_id < 2010, tot_rate_true := NA]


new_mdcr_rescale <- rbindlist(lapply(unique(mdcr_rescale$state_name), function(s){
  
  sub_data <- mdcr_rescale[state_name == s]
  reg <- lm(tot_rate_true ~ cdhi_rate, data = sub_data)
  sub_data$rescale_rate_true <- predict(reg, newdata = sub_data)
  
  return(sub_data)
  
}))
new_mdcr_rescale[, orig_cdhi := cdhi_rate]
new_mdcr_rescale[, cdhi_rate := ifelse(is.na(tot_rate_true), rescale_rate_true, tot_rate_true)]

# replace mdcr in tot state rates
tot_state_rates <- rbind(tot_state_rates[payer!="mdcr"],
                         new_mdcr_rescale[,.(state_name, year_id, location_id, state, abbreviation, nid, payer, cdhi_rate)])

#---------------------
# Resume scale the age-sex specific rates to the all-age both-sex rates using populations
#---------------------

# Add on populations to convert tot rates to num
tot_state_rates <- merge(tot_state_rates, agg_pops, by = c("year_id", "state_name"))
tot_state_rates[, num := cdhi_rate*population]
compare_tot_state_rates <- tot_state_rates[, .(num_from_tot = sum(num)), by = c("year_id", "state_name", "payer")]

# merge together to create scalar (tot / age-sex specific)
scalar <- merge(compare_as_state_rates, compare_tot_state_rates, by = c("year_id", "state_name", "payer"))
scalar[, ratio := num_from_tot/num_from_as]

# now scale the state age/sex specific nums by the scalar, and recalculate rate
as_state_rates <- merge(as_state_rates, scalar, by = c("year_id", "state_name", "payer"))
as_state_rates[, scaled_num := num * ratio]   
as_state_rates[, scaled_rate := scaled_num / population]

as_state_rates[scaled_rate >1, scaled_rate := 1]

as_state_rates <- as_state_rates[,.(year_id, state_name, payer, sex_id, age_group_name, as_rate, population, ratio, scaled_rate)]


# for plotting
df_plot <- rbind(as_state_rates, 
                 tot_state_rates[payer %in% as_state_rates$payer, .(year_id, state_name, payer, scaled_rate = cdhi_rate, age_group_name = "_cdhi reference (all-age, both-sex)")], fill = T)
df_plot[ year_id > 2009, obs := as_rate]

pdf(paste0(viz_dir, "/acs_state_envelope_by_as_payer.pdf"), width = 11, height = 8)
for(s in sort(unique(df_plot$state_name))){
  
  print(ggplot(df_plot[state_name == s & (sex_id == 1 | is.na(sex_id))], 
         aes(x = year_id, color = payer)) + 
    geom_point(aes(y = obs))+
    geom_line(aes(y = scaled_rate))+facet_wrap(~age_group_name)+labs(title = paste0(s, "-males"))+
    theme(legend.position = "bottom"))
  
}
dev.off()


# Save !
#---------------------

write.csv(as_state_rates, paste0(work_dir, "/inputs/acs_state_envelope_by_as_payer.csv"), row.names = F)


print("Done!")
print(Sys.time() - t0)
