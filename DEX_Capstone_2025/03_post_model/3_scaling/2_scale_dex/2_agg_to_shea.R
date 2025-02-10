##----------------------------------------------------------------
## Title: agg_to_shea.R
## Purpose: Take the dex state level estimates (state/toc/year/pri_payer/payer/sex/age) 
##          and aggregate to SHEA envelope (state/toc/year) -- draw level
## Authors: Haley Lescinsky, Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()

'%ni%' <- Negate('%in%')

library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)


## ARGS
args <- commandArgs(trailingOnly = TRUE)
task_path <- args[1]
task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
tasks <- fread(task_path)
tasks <- tasks[task_id,]
draw_num <- tasks$draw
geog <- tasks$geog
care <- tasks$toc
compile_dir <- tasks$compile_dir
agg_dir <- tasks$agg_dir



## Read in DEX estimates for all estimates within the bigger SHEA envelope
message(paste0(compile_dir,'geo=',geog,'/toc=',care,'/'))
dt <- arrow::open_dataset(paste0(compile_dir,'geo=',geog,'/toc=',care,'/'))


data <- dt %>% filter(draw == draw_num) %>% 
  group_by(year_id, state, payer) %>%
  summarise(spend = sum(spend/1000000, na.rm = T), ## Divide spending by million to be in same space as SHEA
            vol = sum(vol, na.rm = T),
            n = n()) %>%
  as.data.table()


if (nrow(data[is.na(spend) | spend == 'Inf'])>0){
  message('Warning, you have infintes or NA spend vals for this combo')
  stop()
}
message('Got data')
data[,toc:= care]
data[,draw:=as.integer(draw_num)]

data <- data[with(data, order(year_id)), ]

arrow::write_dataset(data, path = agg_dir, 
                     partitioning = c("toc"),
                     basename_template = paste0(draw_num,"-{i}.parquet"), 
                     existing_data_behavior = "overwrite", )



message('Done')