##----------------------------------------------------------------
## Title: make_toc_split_scalars.R
## Purpose: Use sources with facility/prof distinction to create scalars
##          for splitting SHEA envelope into DEX types of care
## Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)

Sys.setenv("RETICULATE_PYTHON" = '/FILEPATH/python')
library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))

username <- Sys.getenv('USER')
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())


args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
st <- tasks$state
source_metapath <- tasks$source_metapath
outdir <- tasks$outdir
test <- F

## --------------------
## Sum source spending for ED, IP, and EM and calculate fractions by year
## --------------------
source_meta <- fread(source_metapath)
sources <- unique(source_meta$source)


for(s in sources){
  print(s)
  years <- unique(source_meta[source == s]$year)
  
  all_years <- data.table()
  for (yr in years){
    print(yr)
    tocs <- unique(source_meta[source == s & year == yr]$toc)
    
    for (care in tocs){
      print(care)

      indir <- unique(paste0(source_meta[source == s & year == yr & toc==care]$indir, 'toc=',care,'/year_id=',yr,'/')) 
      
      payers <- c(unique(source_meta[source == s & year == yr]$payer),'tot')
      
      schema <- update_nulls_schema(indir)
      dt <- arrow::open_dataset(indir, schema = schema)
        
      if (s == 'MDCR'){
        select_cols <- dt %>% select(contains('_pay_')|contains('_chg_')| encounter_id | acause | mc_ind ) %>% names()
        mypipe <-. %>%filter(st_resi == st) %>%
          filter(primary_cause == 1) %>%
          filter(mc_ind != 1) %>% #If CMS, drop where managed care is 1
          filter(ENHANCED_FIVE_PERCENT_FLAG == 'Y') ## only want 5% sample, since we can only use years where we have all 3 tocs present anyway

      }else if (s == 'MDCD'){
        select_cols <- dt %>% select(contains('_pay_')|contains('_chg_')| encounter_id | acause | mc_ind ) %>% names()
        
        mypipe <-. %>%filter(st_resi == st) %>%
          filter(dx_level == 1) %>%
          filter(mc_ind != 1) #If CMS, drop where managed care is 1
      }else{ ## CHIA_MDCR and MSCAN which are pulled from C2E because they dont need F2T
        select_cols <- dt %>% select(contains('_pay_')| encounter_id | acause ) %>% names() ##MSCAN doesn't have charge info, only pay
        mypipe <- .%>%filter(st_resi == st) %>%
          filter(dx_level == 1) ## filtering on dx_level rather than primary_cause because this is C2E data not post primary cause data
      }
      
      
      ## Pull in data
      if (test == T){
        filtered <- dt %>% mypipe %>% head(10000L)%>% select(all_of(select_cols))%>% as.data.table()
      }else{
        filtered <- dt %>% mypipe %>% select(all_of(select_cols))%>% as.data.table()
      }
      
      
      if (filtered[,.N]>0){
        facility_pay_cols <- filtered %>% select(contains('_pay_') & contains('facility')) %>% select(-contains('tot')) %>% names()
  
        ## If tot_pay_amt is missing, sum across the payer specific pay columns
        filtered[is.na(tot_pay_amt_facility),tot_pay_amt_facility:= rowSums(.SD, na.rm = T), .SDcols = facility_pay_cols]
        
  
        
        for (pay in payers){
          tot_col <- paste0(pay,'_pay_amt')
          fac_col <- paste0(pay,'_pay_amt_facility')
          
          filtered[is.na(get(tot_col)) & !is.na(get(fac_col)), (tot_col):= get(fac_col)]
          filtered[is.na(get(fac_col)), (fac_col):=0] 
          
          summary_dt <- filtered %>%
            rename(tot_spend = get('tot_col',envir=.GlobalEnv),
                   fac_spend = get('fac_col',envir=.GlobalEnv)) %>%
            mutate(prof_spend = tot_spend - fac_spend) %>%
            summarise(prof = sum(prof_spend, na.rm = T),
                      fac = sum(fac_spend, na.rm = T),
                      total = sum(tot_spend, na.rm = T),
                      n = n()) %>%
            as.data.table()
          
          summary_dt$payer <- pay
          summary_dt$source <- s
          summary_dt$toc <- care
          summary_dt$year <- yr
    
          
          all_years <- rbind(all_years, summary_dt)
        }
          
      }
      print('Got payers')
    }
  }
  
  all_years$state <- st
  if (!interactive()){
    fwrite(all_years, paste0(outdir,st,'_',s,'.csv'))
  }

}


print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------

