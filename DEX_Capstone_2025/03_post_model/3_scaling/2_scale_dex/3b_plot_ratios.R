##----------------------------------------------------------------
## Title: 3b_plot_ratios.R
## Purpose: Plot time trend of scaling ratios
## Authors: Azalea Thomson
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


args <- commandArgs(trailingOnly = TRUE)
sv <- args[1]
ratio_path <- args[2]



outdir <- paste0('FILEPATH/scaled_version_',sv,'/diagnostics/ratios/')

if(!(dir.exists(outdir))){
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
}


all_ratios_mean <- arrow::open_dataset(paste0(ratio_path,'/')) %>% #, schema = schema)
  filter(ratio!= Inf) %>%
  group_by(year_id, state, toc) %>% 
  summarise(mean = mean(ratio)) %>%
  as.data.table()
all_ratios_mean$payer <- 'all_payer_mean'
all_ratios_payer <- arrow::open_dataset(paste0(ratio_path,'/')) %>% #, schema = schema)
  group_by(year_id, state, toc, payer) %>% 
  summarise(mean = mean(ratio)) %>%
  as.data.table()
all_ratios <- rbind(all_ratios_mean, all_ratios_payer)

payers <- unique(all_ratios$payer)

pdf(file = paste0(outdir,"ratios_time_trend.pdf"), width = 20, height = 11)

for (i in payers){
  print(i)
  plot <- all_ratios[payer == i] %>%
    ggplot(aes(x = year_id, y = mean, color = toc ))+
    geom_line()+
    labs(title = paste0('Mean scaling ratio over time, (geo=state), payer: ',i),
         x = "Year",
         y = "mean ratio",
         color = 'TOC') +
    {if(i == 'all_payer_mean')labs(subtitle = 'Removed any Inf ratios that scale DEX to zero')}+
    facet_wrap(~state, scales = 'free')

  print(plot)
}

dev.off()



ratios_2019 <- all_ratios[year_id==2019]

ratios_2019[,payer := plyr::revalue(
  payer,
  c(
    "mdcd" = "Medicaid", 
    "mdcr" = "Medicare", 
    "priv" = "Private",
    "oop" = "Out-of-pocket"
  )
)]


ratios_2019[,toc := plyr::revalue(
  toc,
  c("AM" = "Ambulatory", 
    "DV" = "Dental",
    "ED" = "Emergency Department",
    "HH" = "Home Health",
    "IP" = "Inpatient",
    "NF" = "Nursing Facility",
    "RX" = "Pharmaceuticals"
  )
)]


p <-  ggplot(data=ratios_2019[payer!= 'all_payer_mean' & mean <1000], aes(x=mean)) +
  geom_histogram(binwidth = 1)+
  labs(title = paste0('SHEA scaling factors by type of care and payer, year 2019'), 
       x="Mean scalar (across 50 draws)", 
       y="Count of states") + 
  facet_grid(payer~toc, scales = 'free')+
  theme_bw()

pdf(file = paste0(outdir,"ratios_2019.pdf"), width = 13, height = 6)
print(p)
dev.off()
