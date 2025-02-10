##----------------------------------------------------------------
## Title: mdcd_fac_prof.R
## Purpose: 
## Institutional:
## >1 unique REV_CNTR_CD
## --OR--
## BILL_TYPE_CD exists but POS_CD is missing/invalid
## 
## 
## Professional:
## POS_CD exists & unique REV_CNTR_CD are all missing/invalid
## --OR--
## POS_CD missing/invalid & BILL_TYPE_CD missing/invalid & REV_CNTR_CD missing/invalid & unique LINE_PRCDR_CD always valid
##See Table 5 here: https://resdac.org/sites/datadocumentation.resdac.org/files/2021-08/TAF_TechGuide_Claims_Files.pdf 
## also: https://www.medicaid.gov/dq-atlas/downloads/supplemental/5241-Federally-Assigned-Service-Category.pdf
##
## Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)
username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
timestamp <- Sys.Date()

## --------------------
## Setup
## --------------------

args <- commandArgs(trailingOnly = TRUE)
metadata_filepath <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
metadata <- fread(metadata_filepath)
metadata <- metadata[task_id,]
part <- metadata$part
state <- metadata$state
outdir <- metadata$outdir
year <- metadata$year



source(paste0(here,'/fac_prof_dictionary.R'))
BLG_PRVDR_TYPE_CD_dt$fac_prof_ind <- as.character(BLG_PRVDR_TYPE_CD_dt$fac_prof_ind)
root_dir <- paste0('FILEPATH/stage_1/MDCD/',year,'/')
if (year == 2019){
  taf_base_indir <- paste0(root_dir,'FILEPATH/')
  taf_line_indir <- paste0(root_dir,'FILEPATH')
}else{
  taf_base_indir <- paste0(root_dir, 'FILEPATH',year,'/')
  taf_line_indir <- paste0(root_dir,'FILEPATH',year,'.parquet/')
}



if (!is.null(part)){
  line <- open_dataset(paste0(taf_line_indir,'/',part)) %>% 
    filter(STATE_CD == state) %>%
    select(BENE_ID, REV_CNTR_CD, CLM_ID, LINE_NUM, LINE_PRCDR_CD, STATE_CD) %>%
    collect() %>%
    setDT()
}else{
  line <- open_dataset(taf_line_indir) %>% 
    filter(STATE_CD == state) %>%
    select(BENE_ID, REV_CNTR_CD, CLM_ID, LINE_NUM, LINE_PRCDR_CD, STATE_CD) %>%
    collect() %>%
    setDT()
}


print('Got line data')
print(nrow(line))

if (nrow(line)>0){
  clms <- unique(line$CLM_ID)
  
  base <- open_dataset(taf_base_indir) %>%
    filter(STATE_CD == state) %>%
    filter(CLM_ID %in% clms) %>%
    select(BENE_ID, CLM_ID, BILL_TYPE_CD, POS_CD, BLG_PRVDR_TYPE_CD) %>%
    collect() %>%
    setDT()
  
  print('Got base data')
  print(nrow(base))
}else{
  print('No data for this state partition')
}



if (nrow(line)>0){
  

  ## If line has a valid REV_CNTR_CD, flag as 1
  line[, valid_rev := ifelse(REV_CNTR_CD == '' | is.na(REV_CNTR_CD) | is.null(REV_CNTR_CD), 0, 1) ]
  
  ## If line has a valid procedure code, flag as 1
  line[, valid_proc := ifelse(LINE_PRCDR_CD == '' | is.na(LINE_PRCDR_CD) | is.null(LINE_PRCDR_CD), 0, 1) ]
  
  ## If there is at least one valid REV_CNTR_CD, classify as facility
  line[, line_f := ifelse(sum(valid_rev)>0, 1, 0), by = 'CLM_ID']
  
  ## If all lines per claim have a valid procedure code, flag as always valid procedure
  line[, n_line := .N, by = 'CLM_ID']
  line[, always_valid_PRCDR := ifelse(sum(valid_proc) == n_line, 1, 0), by = 'CLM_ID']
  line$n_line <- NULL
  
  print('Done with line level logic')
  ## If there is a valid place of service code on header record, flag as 1
  base[, POS_CD_exists := ifelse(POS_CD == '' | is.na(POS_CD) | is.null(POS_CD), 0, 1)]
  ## If there is a valid bill type code on header record, flag as 1
  base[, BILL_TYPE_CD_exists := ifelse(BILL_TYPE_CD == '' | is.na(BILL_TYPE_CD) | is.null(BILL_TYPE_CD), 0, 1)]
  ## If the claim (header records are at claim level) has a valid bill type code but invalid place of service code, classify as facility
  base[, base_f := ifelse(BILL_TYPE_CD_exists == 1 & POS_CD_exists == 0, 1, 0)]
  
  print('Done with base level logic')
  
  ## --------------------
  ## Merge line and base to align final logic for fac/prof assignment
  ## --------------------
  merged <- merge(line,base, by = c('CLM_ID','BENE_ID'), all = T)
  merged[, has_line:= ifelse(is.na(line_f), 0, 1)]
  merged[, has_base := ifelse(is.na(base_f), 0, 1)]
  print('Merged line and base')
  
  ## If either line or base has facility designation, assign facility
  merged[, n := .N, by = .(CLM_ID, BENE_ID)]
  merged[, fac := ifelse(line_f == 1 | base_f == 1, 1,0)]
  
  ## If place of service code exists and line isn't facility (ie it had at least 1 valid revenue center code), assign professional
  merged[, prof := ifelse( (POS_CD_exists == 1 & line_f == 0) | (POS_CD_exists == 1 & has_line == 0) , 1, 0)]
  
  ## If place of service code doesn't exist and bill type code doesn't exist and it had at least 1 valid rev center code and always valid procedure code, assign professional
  merged[prof==0 | is.na(prof), prof := ifelse( POS_CD_exists == 0 & BILL_TYPE_CD_exists == 0 & line_f == 0 & always_valid_PRCDR == 1, 1,0)]
  
  ## If its professional, assign P, otherwise assign F
  has_line <- merged[has_line==1][, fac_prof_ind := ifelse(prof == 1,'P','F')]
  print('Assigned fac_prof_ind to claims with line')
  
  ## If base claim doesn't have a line, merge on the billing provider type code map and assign based on that
  no_line <- merge(merged[has_line == 0], BLG_PRVDR_TYPE_CD_dt, by = 'BLG_PRVDR_TYPE_CD')
  print('Assigned fac_prof_ind to claims without line')
  
  final <- rbind(has_line,no_line)
  ## Check that each claim has a unique FP
  final[, unique_fp:= length(unique(fac_prof_ind)), by = .(CLM_ID, BENE_ID)]
  
  if (length(unique(final$unique_fp)) != 1){
    message('Warning, you have more than one F/P assignment per claim')
    stop()
  }
  
  final <- final[is.na(fac_prof_ind), fac_prof_ind:= 'U']
  ## --------------------
  ## Write out summary numbers 
  ## --------------------
  result <- data.table(yr = year,
                       st = state,
                       total = nrow(final),
                       prof = nrow(final[fac_prof_ind == 'P']),
                       fac = nrow(final[fac_prof_ind == 'F']),
                       percent_wout_line = nrow(final[has_line ==0])/nrow(final) *100,
                       percent_wout_base = nrow(final[has_base ==0])/nrow(final) *100,
                       percent_unclassified_wline = nrow(final[fac_prof_ind== 'U' & has_line ==1])/nrow(final) *100,
                       percent_unclassified_woutline = nrow(final[fac_prof_ind== 'U' & has_line ==0])/nrow(final) *100,
                       percent_unclassified_woutbase = nrow(final[fac_prof_ind== 'U' & has_base ==0])/nrow(final) *100,
                       percent_unclassified_overall = nrow(final[fac_prof_ind== 'U'])/nrow(final) *100,
                       unclass_woutbase_clm_id = unique(final[fac_prof_ind== 'U' & has_base ==0]$CLM_ID)
                       )
  
  r_path<-paste0('FILEPATH/',timestamp,'/')
  result_path <- paste0(r_path,state,'_',year,'_',part,'.csv')
  if (!dir.exists(r_path)){
    dir.create(r_path)
    print('folders created')
  }else{
    print("dir exists")
  fwrite(result, result_path)
  }
  
  print(paste0('Saved summary numbers here: ', result_path))
  
  
  ## --------------------
  ## Write out data
  ## --------------------
  
  final <- final[,.(BENE_ID, CLM_ID, fac_prof_ind)]
  final$state <- state
  final$year <- year

  
  arrow::write_dataset(final, path =outdir,
                       existing_data_behavior = c('overwrite'),
                       basename_template = paste0("partition_",part,"-{i}.parquet"),
                       partitioning = c("state","year"))
  
}
print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------



