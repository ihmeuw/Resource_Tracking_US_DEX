##' ***************************************************************************
##' Title: hop_ed.R
##' Purpose: Identify from hospital outpatient line level claims, which are ED visits and write out intermediate file.
##' Author: Azalea Thomson
##' ***************************************************************************
library(data.table)
library(dplyr)
library(tidyverse)
library(arrow)
library(parallel)
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

options(arrow.skip_nul = TRUE)

t0 <- Sys.time()


args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
chia <- args[2]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
year <- tasks$year
part_c <- tasks$part_c
  

outdir <- paste0("FILEPATH/hop_ed/")
if(chia == 1){
  indir <- paste0("FILEPATH", year)
  outdir <- paste0("FILEPATH")
} else {
  indir <- paste0("FILEPATH",year)
}



print(paste0('Starting hop ed',year))

indir_files <- list.dirs(path=indir)
if (year %in% c(2016,2019) & part_c == 1){
  base_file <- grep("op_base", indir_files, value = TRUE)
  rev_file <- grep("op_revenue", indir_files, value = TRUE)
}else{
  base_file <- grep("outpatient_base", indir_files, value = TRUE)
  rev_file <- grep("outpatient_revenue", indir_files, value = TRUE)
}


if (part_c==1){
  rev <- open_dataset(rev_file) %>%
    select(BENE_ID, ENC_JOIN_KEY, REV_CNTR) %>%
    filter(REV_CNTR %in% c('0450','0451','0452','0453','0454','0455','0456','0457','0458','0459', '0981')) %>%
    collect() %>%
    setDT()
  rev$REV_CNTR <- as.double(rev$REV_CNTR)

  print('Opened rev')

}else if (year == 2019 & part_c==0){
  rev <- open_dataset(rev_file) %>%
    select(BENE_ID, CLM_ID, REV_CNTR) %>%
    filter(REV_CNTR %in% c('0450','0451','0452','0453','0454','0455','0456','0457','0458','0459', '0981')) %>%
    collect() %>%
    setDT()
}else{
    
    rev <- open_dataset(rev_file) %>%
      select(BENE_ID, CLM_ID, REV_CNTR) %>%
      filter(REV_CNTR %in% c(0450:0459, 0981)) %>%
      collect() %>%
      setDT()
    
    print('Opened rev')
}






##anything with at least one ER line is ER
ed_visits <- unique(rev)
ed_visits$year <- year
ed_visits$part_c <- part_c

ed_visits %>% write_dataset(path = paste0(outdir),
                            basename_template = paste0("year_",year,"-{i}.parquet"),
                            partitioning = c("year","part_c"),
                            existing_data_behavior = c("overwrite"))



print(nrow(ed_visits))

print(Sys.time() - t0)

print(paste0(year," done"))







