##----------------------------------------------------------------
## Title: make_ratios.R
## Purpose: Take the ratio of the dex estimates to the SHEA envelope (state/toc/year) - draw level
## Authors: Haley Lescinsky, Azalea Thomson
##----------------------------------------------------------------

t0 <- Sys.time()

pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
library(lbd.loader,
        lib.loc = sprintf("FILEPATH",R.version$major))

library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())

code_path <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(code_path)


## ARGS
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
care <- tasks$toc
agg_dir <- tasks$agg_dir
shea_path <- tasks$shea_path
ratio_path <- tasks$ratio_path
max_model_year <- args[2]



#
# Load in DEX results (aggregated to toc/state/year )
#


dex <- arrow::open_dataset(paste0(agg_dir,'/')) 

dex <- dex %>% 
  filter(toc==care)%>%
  collect() %>% as.data.table()

message('Got dex aggregated data')

#
# Load in SHEA results (toc/state/year)
#

shea <- fread(shea_path)[payer!='ALL'][payer!='OTH']
shea <- shea[,payer := tolower(payer)]
setnames(shea, "year", "year_id")
setnames(shea,'spend','shea_spend')
shea <- shea[state %in% unique(dex$state)]

#
# Merge datasets on toc/state/year
#

message('Got SHEA data')
if (shea[shea_spend<0, .N]>0){
  shea[shea_spend <0, shea_spend := 0]
}

both <- merge(dex, shea, by = c("state", "year_id", "toc","payer"), all.x = T)

if(nrow(both[is.na(spend)]) >0){
  stop("there are some state/year/toc combinations we don't have a shea spend value for")
}


#
# COMPUTE RATIO: DEX / SHEA
#
both <- both[,ratio:=spend/shea_spend]
both[is.nan(ratio), ratio:=1] 

## Check!
if (both[ratio<0, .N]>0){
  stop('Uh oh you have negative ratios')
}
# now subset to just those relevant columns
both <- both[, .(state,year_id,payer,toc,ratio,draw)]

both <- both[with(both, order(year_id,draw)), ]


both <- both[year_id<=max_model_year]

arrow::write_dataset(both, path = ratio_path, 
                     partitioning = c("toc","state"))

print(Sys.time() - t0)
print('Done')
##----------------------------------------------------------------