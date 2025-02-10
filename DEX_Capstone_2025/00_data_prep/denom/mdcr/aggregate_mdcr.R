## Puropose: sum across the various jobs
# AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
rm(list = ls())
library(data.table)
library(tidyverse)
library(arrow)
library(parallel)

timestamp <- "2024-11-14"
#specify if running for chia data
chia <- 1

## ----------------------
## AGGREGATE
## ----------------------
#save out two versions, one with race, one without
if(chia != 1){
  years<-c(2000,2008:2017,2019)
  dataset <- "MDCR"
} else {
  years <- c(2015,2016, 2017, 2018, 2019, 2020, 2021, 2022) #
  dataset <- "CHIA_MDCR"
}

for (yr in years){
  print(yr)
  denom <- data.table(collect(arrow::open_dataset(paste0('FILEPATH'))))%>%
    filter(sex_id %in% c(1,2)) %>%
    mutate(part_cd = ifelse(part_c == 1 & part_d == 1,1,0))
  denom$year_id<-yr
  setDT(denom)

  # #version w/o race
  agg <- denom[,.(n= sum(n)), by = .(age_start,age_group_id,sex_id,year_id,mcnty,state_name,sample,
                                                        part_a, part_b, part_c, part_d, part_ab, part_cd, any_coverage, dual_enrol)]
  #version w/ race
  agg_race <- denom[,.(n= sum(n)), by = .(age_start,age_group_id,sex_id,year_id,state_name,race_cd_imp,sample,
                                     part_a, part_b, part_c, part_d, part_ab, part_cd, any_coverage, dual_enrol)]
  setnames(agg_race, 'race_cd_imp', 'race_cd')
  agg_race_raw <- denom[,.(n= sum(n)), by = .(age_start,age_group_id,sex_id,year_id,state_name,race_cd_raw,sample,
                                          part_a, part_b, part_c, part_d, part_ab, part_cd, any_coverage, dual_enrol)]
  setnames(agg_race_raw, 'race_cd_raw', 'race_cd')
  write_dataset(agg,
                  path = paste0('FILEPATH'),
                  format = "parquet",
                  partitioning = c("year_id", "state_name"),
                  basename_template = paste0(yr,"_part-{i}."),
                  existing_data_behavior = "overwrite")
  write_dataset(agg_race,
                  path = paste0('FILEPATH'),
                  format = "parquet", 
                  partitioning = c("year_id", "state_name"),
                  basename_template = paste0(yr,"_part-{i}."),
                  existing_data_behavior = "overwrite")
  write_dataset(agg_race_raw,
                path = paste0('FILEPATH'),
                format = "parquet", 
                partitioning = c("year_id", "state_name"),
                basename_template = paste0(yr,"_part-{i}."),
                existing_data_behavior = "overwrite")
}

## ----------------------
## SYMLINK
## ----------------------
c_t <- paste0('FILEPATH')
c_b <- paste0('FILEPATH', "_best")
system(paste("rm -r", c_b)) #remove old best
system(paste("ln -sf", c_t, c_b))
