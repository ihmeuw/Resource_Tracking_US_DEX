# -------------------------------------------------
#    Bring in all data collected on the magnitude of input data.
#         The default (across-cause) threshold is calculated as the 97.5% percentile (by toc & metric) of the 97.5% percentile of input data (by acause & dataset & age & geo)
#
#    Author: Haley Lescinsky
#   
#    TO be run interactively.
# -------------------------------------------------


Sys.umask(mode = 002)
pacman::p_load(dplyr, openxlsx, RMySQL, rjson, data.table, ini, DBI, tidyr, arrow)

#####
run_id <- 'XX'
#####

work_dir <- paste0("FILEPATH/run_", run_id, "/data_magnitude/")

# pull in all prepped data  (prepped in compare_data_to_thresholds_launcher_worker.R)
data <- open_dataset(paste0(work_dir, "/data/")) %>%
  collect() %>% as.data.table()

unique(data$grouping)

# we focus on the most granular grouping - age,toc,acause,metric,geo,dataset
data <- data[grouping == "age_group_years_start,toc,acause,metric,geo,dataset", ]

# reference thresholds
ref_thresholds <- fread("/FILPEATH/estimate_thresholds.csv")[is.na(acause)]

setnames(ref_thresholds, "threshold", "ref_threshold")


# Restrict to data combos with more than 1 observation and take some summaries by toc and metric (so across cause-dataset-age-geo)
across_cause <- data[N > 1, .(max = max(max_val), quant_975_max = max(quant.975), quant_975_975 = quantile(quant.975, .975),
         quant_975_80 = quantile(quant.975, .8), quant_975_med = median(quant.975)), by = c('toc', 'metric')]

across_cause <- merge(across_cause, ref_thresholds, by = c('toc', 'metric'), all.x = TRUE)

across_cause <- across_cause[order(metric)]
across_cause_clean <- copy(across_cause)

round_threshold <- function(dt, col){
  
  setnames(dt, col, "column_thresh")
  
  # make whole number (ceiling)
  dt[, column_thresh := ceiling(column_thresh)]
  
  # if encounters
  dt[ metric == 'encounters_per_person' & column_thresh > 20, column_thresh := plyr::round_any(column_thresh, 5, f = ceiling)]
  
  # if spend
  dt[ metric == 'spend_per_encounter' & column_thresh > 100000, column_thresh := plyr::round_any(column_thresh, 50000, f = ceiling)]
  dt[ metric == 'spend_per_encounter' & column_thresh > 10000 & column_thresh < 100000, column_thresh := plyr::round_any(column_thresh, 5000, f = ceiling)]
  dt[ metric == 'spend_per_encounter' & column_thresh > 1000 & column_thresh < 10000, column_thresh := plyr::round_any(column_thresh, 500, f = ceiling)]
  
  # if days supply metrics
  dt[ metric == 'spend_per_day' & column_thresh < 1000, column_thresh := plyr::round_any(column_thresh, 50, f = ceiling)]
  dt[ metric == 'spend_per_day' & column_thresh > 1000, column_thresh := plyr::round_any(column_thresh, 100, f = ceiling)]
  dt[ metric == 'days_per_encounter', column_thresh := plyr::round_any(column_thresh, 10, f = ceiling)]
  
  
  setnames(dt, "column_thresh", col)
  
  return(dt)
  
}


across_cause_clean <- round_threshold(across_cause_clean, "max")
across_cause_clean <- round_threshold(across_cause_clean, "quant_975_max")
across_cause_clean <- round_threshold(across_cause_clean, "quant_975_975")
across_cause_clean <- round_threshold(across_cause_clean, "quant_975_80")
across_cause_clean <- round_threshold(across_cause_clean, "quant_975_med")


write.xlsx(list("rounded" = across_cause_clean, 
                "raw" = across_cause), paste0(work_dir, "/decide_across_cause_thresholds.xlsx"))

#
#  Choose 975_975
#

across_cause_clean <- across_cause_clean[,.(toc, metric, threshold = quant_975_975)]

write.csv(across_cause_clean, paste0(work_dir, "/across_cause_thresholds.csv"), row.names = FALSE)


