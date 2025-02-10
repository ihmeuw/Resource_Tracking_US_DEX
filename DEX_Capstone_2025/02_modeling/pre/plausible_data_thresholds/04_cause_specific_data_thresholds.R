# -------------------------------------------------
#    Bring in all data collected on the amount of data that exceeds the default thresholds
#           Makes cause specific data thresholds for causes where more than 5% of the data points for a given acause/geo/age/dataset are over the threshold
#
#    Author: Haley Lescinsky
#   
#    TO be run interactively.
# -------------------------------------------------

library(plyr)
library(openxlsx)


#####
run_id <- 'XX'
archive_and_resave <- F # update the thresholds?
#####

work_dir <- paste0("/FILEPATH/run_", run_id, "/compare_data_to_thresholds/")

data <- open_dataset(paste0(work_dir, "/data/")) %>%
  collect() %>% as.data.table()

unique(data$grouping)

# we focus on the most granular grouping - age,toc,acause,metric,geo,dataset
summary <- data[grouping == "age_group_years_start,toc,acause,metric,geo,dataset", .(count = sum(count), total_rows = sum(total_rows)), by = c("toc", "metric", "variable")]
summary[, percent := (count / total_rows)*100]
summary[variable == "raw_val_above"][rev(order(percent))]

# look within each toc & metric
all_groupings <- unique(data[variable == 'raw_val_above', .(toc, metric, variable)])

summarize_list_by_acause <- function(group,  data){
  hm <- data[grouping == "age_group_years_start,toc,acause,metric,geo,dataset" & toc ==  group$toc & metric==  group$metric & variable == group$variable]
  hm <- hm[rev(order(prop_over))][, .(toc, metric, geo, acause, dataset, age_group_years_start, total_rows, prop_over, lower_quant, upper_quant, median, median_se, threshold )]
  return(hm)
}

#####---------- UPDATE THRESHOLDS -----------------------------------------

# ENCOUNTERS
#--------------

cause_specific_thresholds <- data.table()

for(toc in c('ED', 'AM', 'IP', 'HH', 'NF', 'RX', 'DV')){
  
  examine <- summarize_list_by_acause(group = data.table(toc = toc, metric = 'encounters_per_person', variable = 'raw_val_above'), data)
  
  # if more than 5% of the data points exceed the threshold, we fix
  #     for sake of small numbers, we force rows > 1 (sometimes encounters have duplicates across payers, so same thing to confirm median!=lower!=upper)
  examine2 <- examine[prop_over > 0.05 & total_rows > 1 & (upper_quant != lower_quant)]
  
  # Set cause threshold as the 90th percentile of the median values, OR if > 70% of the data is over the threshold, use the max median value from those rows
  examine2[, threshold := quantile(as.numeric(median), 0.90), by = 'acause']
  examine2[prop_over > 0.7, threshold := median]
  examine2 <- examine2[, .(threshold = ceiling(max(threshold))), by = 'acause']

  cause_specific_thresholds <- rbind(cause_specific_thresholds, 
                                     examine2[, toc  := toc])
}

# ROUND
cause_specific_thresholds[ threshold > 20, threshold := plyr::round_any(threshold, 5, f = ceiling)]
cause_specific_thresholds[ threshold > 100, threshold := plyr::round_any(threshold, 25, f = ceiling)]
cause_specific_thresholds <- cause_specific_thresholds[, .(toc, metric = 'encounters_per_person', threshold, acause)]


# SPEND
#--------------

cause_specific_thresholds_spend <- data.table()

for(toc in c('ED', 'AM', 'IP', 'HH', 'NF', 'RX', 'DV')){
  
  examine <- summarize_list_by_acause(group = data.table(toc = toc, metric = 'spend_per_encounter',variable = 'raw_val_above'), data)
  
  # if more than 5% of the data points exceed the threshold, we fix
  #     for sake of small numbers, we force rows > 1 (sometimes encounters have duplicates across payers, so same thing to confirm median!=lower!=upper)
  examine2 <- examine[prop_over > 0.05 & total_rows > 1 & (upper_quant != lower_quant)]
  
  # Set cause threshold as the 90th percentile of the median values, OR if > 70% of the data is over the threshold, use the max median value from those rows
  examine2[, threshold := quantile(as.numeric(median), 0.90), by = 'acause']
  examine2[prop_over > 0.7, threshold := median]
  examine2 <- examine2[, .(threshold = ceiling(max(threshold))), by = 'acause']
  
  cause_specific_thresholds_spend <- rbind(cause_specific_thresholds_spend, 
                                     examine2[, toc  := toc])
}

# ROUND
cause_specific_thresholds_spend[threshold > 1000 & threshold < 10000, threshold := plyr::round_any(threshold, 500, f = ceiling)]
cause_specific_thresholds_spend[threshold > 10000 & threshold < 100000, threshold := plyr::round_any(threshold, 5000, f = ceiling)]
cause_specific_thresholds_spend[threshold > 100000, threshold := plyr::round_any(threshold, 50000, f = ceiling)]

cause_specific_thresholds_spend <- cause_specific_thresholds_spend[, .(toc, metric = 'spend_per_encounter', threshold, acause)]

# COMBINE METRICS
add_threshold <- rbind(cause_specific_thresholds_spend, cause_specific_thresholds)

# SET MANUAL THRESHOLDS
print('setting manual thresholds!')
add_threshold[acause == 'exp_family_planning' & toc == 'RX' & metric == 'spend_per_encounter',  threshold := 1000]
add_threshold[acause == 'exp_well_person' & toc == 'IP' & metric == 'encounters_per_person',  threshold := 8]


# MAKE UPDATED THRESHOLD LIST (pulled from prepped across cause defaults)
new_thresholds <- fread(paste0(work_dir, "../data_magnitude/across_cause_thresholds.csv"))
new_thresholds <- rbind(new_thresholds, add_threshold, fill = T)

# ARCHIVE OLD
if(archive_and_resave == T){
  
  check <- readline(paste0('Are you sure you want to archive old thresholds and save new ones?', " \n type 'yes' to proceed"))
  if (check == 'yes'){
    
    threshold_path <- '/FILEPATH/estimate_thresholds.csv'
    thresholds_dt <- fread(threshold_path)
    write.csv(thresholds_dt, paste0('/FILEPATH/estimate_thresholds_', gsub('-',"_", Sys.Date()), ".csv"), row.names = F)
    
    # SAVE NEW
    write.csv(new_thresholds, threshold_path, row.names = F)
    print("new thresholds saved!")
  
  }else{
    print("Check not equal to 'yes', not saving")
  }
  
}


#####---------- COMPARE THRESHOLDS TO PREVIOUS BEST -----------------------------------------

thresholds_prev <- fread(paste0('/FILEPATH/estimate_thresholds_', gsub('-',"_", Sys.Date()), ".csv"))

default_thresholds <- thresholds_prev[is.na(acause)]
thresholds_prev <- thresholds_prev[!is.na(acause)]
new_default_thresholds <- new_thresholds[is.na(acause)]

setnames(thresholds_prev, "threshold", "prev_threshold")
setnames(default_thresholds, "threshold", "prev_default_threshold")
setnames(new_default_thresholds, "threshold", "new_default_threshold")

compare_thresholds <- merge(thresholds_prev, new_thresholds, by = c('acause', 'toc', 'metric'), all = T)

# Expand out to all causes
causes_by_toc <- unique(data[,.(acause, toc, metric)])
compare_thresholds <- merge(compare_thresholds, causes_by_toc, by = c('toc', 'metric', 'acause'), all = T)

# Add on defaults
compare_thresholds <- merge(compare_thresholds,default_thresholds[, acause := NULL], by = c('toc', 'metric'), all = T)
compare_thresholds <- merge(compare_thresholds,new_default_thresholds[, acause := NULL], by = c('toc', 'metric'), all = T)

compare_thresholds[is.na(prev_threshold), `:=` (prev_threshold = prev_default_threshold, default_prev = 1)]
compare_thresholds[is.na(threshold), `:=` (threshold = new_default_threshold, default_new = 1)]
compare_thresholds <- compare_thresholds[!is.na(acause),.(toc, metric, acause, threshold, default_new, prev_threshold, default_prev)]
compare_thresholds[threshold > prev_threshold, change := 'Increased']
compare_thresholds[threshold < prev_threshold, change := 'Decreased']
compare_thresholds[, difference := threshold - prev_threshold]
compare_thresholds[default_prev == 1 & default_new == 1, change := 'Default changed']
compare_thresholds[threshold == prev_threshold, change := 'No change']

# Save excel file for vetting
write.xlsx(list('spend' = compare_thresholds[metric == 'spend_per_encounter'],
                "encounters" = compare_thresholds[metric == 'encounters_per_person'],
                "rx_supply_days" = compare_thresholds[metric %in% c('days_per_encounter', 'spend_per_day')]), paste0(work_dir, "/compare_new_to_old_thresholds.xlsx"))
