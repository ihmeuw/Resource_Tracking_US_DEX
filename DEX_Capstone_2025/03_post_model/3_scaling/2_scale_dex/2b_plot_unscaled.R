##----------------------------------------------------------------
## Title: 2b_plot_unscaled.R
## Purpose: Plot time trend of aggregated state and national spend compared to SHEA envelopes
## Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr)
options(arrow.skip_nul = TRUE)
## --------------------
## Setup
## --------------------


here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())

## ARGS
args <- commandArgs(trailingOnly = TRUE)
sv <- args[1]
agg_dir <- args[2]
shea_path <- args[3]



outdir <- 'FILEPATH'
if(!(dir.exists(outdir))){
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
}

dex <- arrow::open_dataset(paste0(agg_dir,'/')) #, schema = schema)

dex <- dex %>% 
  collect() %>% as.data.table()

message('Got dex aggregated data')
dex$source <- 'DEX_unscaled'
dex <- dex[draw==0]
dex$payer <- toupper(dex$payer)  

shea <- fread(shea_path)
shea[, spend:=spend*1e6] ## otherwise its in millions of $
setnames(shea, "year", "year_id")
shea$source <- 'SHEA'

states <- unique(dex$state)
shea <- shea[state%in% states]


tocs_of_interest <- c('AM','IP','ED','HH','RX','DV','NF')
all <- rbind(shea, dex,fill = T)
all <- all[toc %in% tocs_of_interest]
all[, total:= sum(spend), by = c('year_id','source','state')]
all[, toc_frac := spend/total]

all$source <-  factor(all$source, levels=c('DEX_unscaled','SHEA'))


states <- unique(all$state)

cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

library(ggpubr)
pdf(file = paste0(outdir,"spend_over_time.pdf"), width = 20, height = 11)
for (st in states){
  print(st)
  
  plot <- all[state == st] %>%
    ggplot(aes(x = year_id, y = spend, color = toc ))+
    geom_line()+
    scale_colour_manual(values=cbPalette) +
    theme_bw() +
    labs(title = paste0('Spend over time in USD, state: ',st),
         x = "Year",
         y = "Spend ",
         color = 'TOC') +
    scale_y_log10()+
    facet_grid(source~payer, scales = 'free')
  

  
  plot_frac <- all[state == st] %>%
    ggplot(aes(x = year_id, y = toc_frac, color = toc ))+
    geom_line()+    
    scale_colour_manual(values=cbPalette) +
    theme_bw() +
    labs(title = paste0('ToC fraction of total spending over time, state: ',st),
         x = "Year",
         y = "Spend ",
         color = 'TOC') +
    facet_grid(source~payer, scales = 'fixed')
  
  print(ggarrange(plot,plot_frac, nrow = 2))
  
}
dev.off()


