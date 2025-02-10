##' ***************************************************************************
##' Title: carrier_toc.R
##' Purpose: Map CMS carrier data types of care
##' Authors: Emily Johnson, Azalea Thomson
##' Notes:
##' 2016&2019 - CLM_TYPE_CD in carrier data is exclusively 4700 which is the "professional" claim type
##' all NCH_CLM_TYPE_CD are "OTH"
##' ***************************************************************************

library(data.table)
library(tidyverse)
library(arrow)
library(parallel)
t0 <- Sys.time()
options(arrow.skip_nul = TRUE)
'%ni%' <- Negate('%in%')
'%notlike%' <- Negate('%like%')


args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
is_chia <- args[2]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
year <- tasks$year
part_c <- tasks$part_c
  
print(year)
print(part_c)

if(is_chia == 1){
  indir <- "FILEPATH"
  outdir <- 'FILEPATH'
} else {
  outdir <- 'FILEPATH'
}

if(is_chia == 1){
  base_file <- 'FILEPATH'
  line_file <- 'FILEPATH'
} else {
  if (year %in% c(2016, 2019) & part_c == 1){
    base_file <- 'FILEPATH'
    line_file <- 'FILEPATH'
  }else if (year ==2019 & part_c == 0){
    base_file <- 'FILEPATH'
    line_file <- 'FILEPATH'
  }else{
    base_file <- 'FILEPATH'
    line_file <- 'FILEPATH'
  }
}

print(base_file)
print(line_file)
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
source(paste0(here, '/carrier_toc_maps.R'))
place_codes_dt$line_cms_place_srvc_cd <- as.numeric(place_codes_dt$line_cms_place_srvc_cd)
place_codes_dt$toc <- as.character(place_codes_dt$toc)

if (year %in% c(2016, 2019) & part_c == 1){
  line <- open_dataset(line_file) %>%
    select(BENE_ID, CLM_LINE_NUM, CLM_TYPE_CD, LINE_PLACE_OF_SRVC_CD,ENC_JOIN_KEY) %>%
    collect() %>%
    setDT()
  setnames(line, c('CLM_LINE_NUM','CLM_TYPE_CD','ENC_JOIN_KEY'),c('LINE_NUM','NCH_CLM_TYPE_CD','CLM_ID'))
}else{
  line <- open_dataset(line_file) %>%
    select(BENE_ID, CLM_ID, LINE_NUM, NCH_CLM_TYPE_CD, LINE_PLACE_OF_SRVC_CD,
           LINE_NCH_PMT_AMT, LINE_BENE_PRMRY_PYR_PD_AMT, LINE_BENE_PRMRY_PYR_CD) %>%
    collect() %>%
    setDT()
}
line$LINE_PLACE_OF_SRVC_CD <- as.numeric(line$LINE_PLACE_OF_SRVC_CD)
line[is.na(LINE_PLACE_OF_SRVC_CD),LINE_PLACE_OF_SRVC_CD:= 28] ## assigning any missing values to map to "Other" ToC category


#Count unique rows of type of submitted claim (NCH_CLM_TYPE_CD) and place of service (LINE_CMS_TYPE_SRVC_CD) and number of rows, per BENE_CLAIM
line[, `:=`(clm_type_num = uniqueN(NCH_CLM_TYPE_CD),
            plc_srvc_num = uniqueN(LINE_PLACE_OF_SRVC_CD),
            num_lines = .N),
     by=c("BENE_ID","CLM_ID")]

line_no_conflicts <- line[plc_srvc_num == 1]
setnames(place_codes_dt, 'line_cms_place_srvc_cd', 'LINE_PLACE_OF_SRVC_CD')
line_no_conflicts <- merge(line_no_conflicts, place_codes_dt, all.x=T)
line_no_conflicts[, dex_toc := toc]
line_no_conflicts$toc <- NULL



##' ***************************************************************************
##' Make sure each encounter doesn't have conflicts in mapping
##' ***************************************************************************
line_conflicts <- line[plc_srvc_num >1] 
line_conflicts <- merge(line_conflicts, place_codes_dt, all.x=T)
line_conflicts[, toc_num :=uniqueN(toc), by=c("BENE_ID","CLM_ID")]
line_conflicts <- line_conflicts[toc_num >1]

## If toc is OTH, replace with first instance of non OTH toc for that claim
line_conflicts <- line_conflicts[, toc:= ifelse(toc=='OTH', unique(toc[toc != 'OTH'])[1], toc), by=c("BENE_ID","CLM_ID")]

line_conflicts[, toc_num :=uniqueN(toc), by=c("BENE_ID","CLM_ID")]
line_oth_remapped <- line_conflicts[toc_num == 1]

line_conflicts <- line_conflicts[toc_num >1] 

## If there are still multiple ToCs per claim, assign either using spend (if not 2016, 2019) or logic
if (year %in% c(2016, 2019) & part_c == 1){
  line_conflicts <- line_conflicts[, toc:= ifelse(toc=='ED' & unique(toc[toc != 'ED'])[1] == 'IP', 'IP', toc), by=c("BENE_ID","CLM_ID")]
  line_conflicts[, toc_num :=uniqueN(toc), by=c("BENE_ID","CLM_ID")]
  full <- rbind(line_oth_remapped, line_conflicts)  
  full[, dex_toc := ifelse(toc_num==1, toc, 'UNKNOWN')] #At this point, if more than one TOC, cannot determine what is true TOC so set to unknown
  full$toc <- NULL
}else{
  line_conflicts[is.na(LINE_NCH_PMT_AMT), LINE_NCH_PMT_AMT := LINE_BENE_PRMRY_PYR_PD_AMT]
  
  line_conflicts[, toc_pmt := sum(LINE_NCH_PMT_AMT), by = c('toc','CLM_ID','BENE_ID')]
  line_conflicts[, max_pmt := max(toc_pmt), by = c('CLM_ID','BENE_ID')]
  line_conflicts[, DEX_TOC := ifelse(toc_pmt == max_pmt, 1,0)]
  
  ## Subset to only the unique BENE/CLM ID with the max payment because we believe this is the true TOC
  max_pay <- unique(line_conflicts[DEX_TOC == 1, .(BENE_ID,
                                                   CLM_ID,
                                                   LINE_BENE_PRMRY_PYR_CD,
                                                   toc,
                                                   num_lines)])
  max_pay[,n:= .N, by = 'CLM_ID']
  ## If there is more than one TOC with the max payment, and one of them is OTHER, drop the OTHER TOC
  max_pay <- max_pay[!(n>1 & toc == 'OTH')]
  max_pay[,n:= .N, by = 'CLM_ID']
  max_pay_toc <- max_pay[n==1]
  ## If there is more than one TOC with the max payment, and neither are OTHER, then count the number of rows
  ## and set TOC to be the type with the most rows.
  most_pay_clms <- max_pay[n>1]$CLM_ID
  most_pay <- line_conflicts[CLM_ID %in% most_pay_clms]
  most_pay[, toc_count := .N, by = c('toc','CLM_ID','BENE_ID')]
  most_pay[, max_toc_count := max(toc_count), by = c('CLM_ID','BENE_ID')]
  most_pay[, DEX_TOC := ifelse(max_toc_count == toc_count, 1,0)]
  most_pay_toc <- unique(most_pay[DEX_TOC == 1, .(BENE_ID,
                                                  CLM_ID,
                                                  LINE_BENE_PRMRY_PYR_CD,
                                                  toc,
                                                  num_lines)])
  most_pay_toc[,n:= .N, by = 'CLM_ID']
  
  ## Combine the claims determined by max payment and those determine my most payments
  full <- rbind(max_pay_toc, most_pay_toc)
  
  ## if there is still more than one TOC, set TOC to unknown
  full[, dex_toc := ifelse(n==1, toc, 'UNKNOWN')] #At this point, if more than one TOC, cannot determine what is true TOC so set to unknown
  full$toc <- NULL
}  



full$n <- NULL


full <- unique(full[,.(BENE_ID,CLM_ID,num_lines,dex_toc)])


single_toc <- unique(line_no_conflicts[,.(BENE_ID,CLM_ID,num_lines,dex_toc)])


final <- rbind(full, single_toc)
toc_table <- as.data.table(table(final$dex_toc))
fwrite(toc_table, paste0('FILEPATH/carrier_toc_freq_',year,'_partc_',part_c,'.csv'))
#######
print('Finished maps, now writing out new base file with toc')

if (year %in% c(2016, 2019) & part_c == 1){
  base <- open_dataset(base_file) %>% 
    select(BENE_ID, ENC_JOIN_KEY) %>%
    collect() %>%
    setDT()
  setnames(base,'ENC_JOIN_KEY','CLM_ID')
}else{
  base <- open_dataset(base_file) %>% 
    select(BENE_ID, CLM_ID) %>%
    collect() %>%
    setDT()
}


setnames(final, 'dex_toc','toc')
final$num_lines <- NULL
new_base <- merge(base, final, by = c('BENE_ID','CLM_ID'))

new_base$year <- year
if (year %in% c(2016, 2019) & part_c == 1){
  ## set clm_id back to enc_join_key
  setnames(new_base,'CLM_ID','ENC_JOIN_KEY')
}

new_base$year <- year
new_base$part_c <- part_c
new_base %>% write_dataset(path = paste0(outdir),
                           basename_template = paste0("year_",year,"-{i}.parquet"),
                           partitioning = c("year","part_c"),
                           existing_data_behavior = c("overwrite"))



print(paste0('Done, data saved here: ', outdir))
print(Sys.time() - t0)

