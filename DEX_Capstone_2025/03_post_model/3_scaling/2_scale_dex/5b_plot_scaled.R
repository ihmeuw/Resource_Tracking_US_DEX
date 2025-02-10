##----------------------------------------------------------------
## Title: 5b_plot_scaled.R
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
final_dir <- args[4]
run_only_national <- args[5]


library(RColorBrewer)
mycolors <- brewer.pal(10, 'Set3')
mycolors <- rev(mycolors)
toc_colors <- c('AM'=mycolors[10],
                'IP'=mycolors[9],
                'RX'=mycolors[8],
                'oth_pers'=mycolors[7],
                'NF'=mycolors[6],
                'DV'=mycolors[5],
                'ED'=mycolors[4],
                'oth_non_dur'=mycolors[3],
                'HH'=mycolors[2],
                'dur'=mycolors[1])
payer_colors <- c('OOP' = mycolors[5],
                  'PRIV' = mycolors[4],
                  'MDCR' = mycolors[3],
                  'MDCD' = mycolors[2],
                  'OTH' = mycolors[1])

final_dir <- paste0(final_dir,'collapsed/data/')
outdir <- 'FILEPATH'
if(!(dir.exists(outdir))){
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
}

dex <- arrow::open_dataset(paste0(agg_dir,'/')) 

dex <- dex %>% 
  collect() %>% as.data.table()

message('Got dex aggregated data')
dex$source <- 'DEX_unscaled'
dex <- dex[draw==0]


dex_scaled_nat <- arrow::open_dataset(paste0(final_dir, 'geo=national/')) %>%
  group_by(payer, year_id, toc, state) %>%
  summarise(spend = sum(mean_spend)) %>% collect() %>% as.data.table()


if (run_only_national == F){
  dex_scaled_st <- arrow::open_dataset(paste0(final_dir,'geo=state/')) %>%
    group_by(payer, year_id, toc, state) %>%
    summarise(spend = sum(mean_spend)) %>% collect() %>% as.data.table()
  dex_scaled <- rbind(dex_scaled_st, dex_scaled_nat)
}else{
  dex_scaled <- dex_scaled_nat
}



message('Got dex scaled data')
dex_scaled$source <- 'DEX_scaled'


shea <- fread(shea_path)
shea[, spend:=spend*1e6] ## otherwise its in millions of $
shea[,payer:=tolower(payer)]
setnames(shea, "year", "year_id")
shea$source <- 'SHEA'
shea <- shea[!(payer =='all')]

states <- unique(dex$state)
shea <- shea[state%in% states]

tocs_of_interest <- c('AM','IP','ED','HH','NF', 'DV')
all <- rbind(shea, dex,dex_scaled, fill = T)
all <- all[toc %in% tocs_of_interest]
all[, total_toc:= sum(spend), by = c('payer','year_id','source','state')]
all[, toc_frac := spend/total_toc]
all[, total_payer:= sum(spend), by = c('toc','year_id','source','state')]
all[, pay_frac := spend/total_payer]

all$source <-  factor(all$source, levels=c('DEX_unscaled','SHEA','DEX_scaled'))
all[,payer:=toupper(payer)]

states <- unique(all$state)
cbPalette <- c("#999999", "#E69F00", "#56B4E9", "#009E73", "#F0E442", "#0072B2", "#D55E00", "#CC79A7")
min_year <- min(dex$year_id)
max_year <- max(dex$year_id)
all <- all[year_id >=min_year & year_id<= max_year]
all$toc <- reorder(all$toc, all$toc_frac)

library(ggpubr)
pdf(file = paste0(outdir,"spend_over_time.pdf"), width = 20, height = 11)
for (st in states){
  print(st)



  toc_scaled_stacked_bar <- all[state == st] %>%
    ggplot(aes(x = as.factor(year_id), y = toc_frac, fill = toc))+
    geom_bar(position='stack',stat='identity')+
    scale_fill_manual(values = toc_colors)+
    labs(x = "Year", y = "Spending fraction") +
    ggtitle(paste0("Type of care spend fraction: ", st)) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black"))+
    facet_grid(payer~source)
  print(toc_scaled_stacked_bar)

  payer_scaled_stacked_bar <- all[state == st] %>%
    ggplot(aes(x = as.factor(year_id), y = pay_frac, fill = payer))+
    geom_bar(position='stack',stat='identity')+
    scale_fill_manual(values = payer_colors)+
    labs(x = "Year", y = "Spending fraction") +
    ggtitle(paste0("Payer spend fraction: ", st)) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black"))+
    facet_grid(toc~source)
  print(payer_scaled_stacked_bar)
  }
dev.off()


