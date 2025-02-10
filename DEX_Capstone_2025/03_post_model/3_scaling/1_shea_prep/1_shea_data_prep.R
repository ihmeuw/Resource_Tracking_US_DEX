#############################################################
## Scaling: 1_shea_data_prep.R
## Purpose: Pull SHEA data and currency convert
## Authors: Meera Beauchamp, Azalea Thomson, Sawyer Crosby
#############################################################

## ------------------------
## Setup
## ------------------------
rm(list = ls())

## load packages
library(tidyr)
library(
  lbd.loader, 
  lib.loc = sprintf(
    "FILEPATH", 
    R.version$major, 
    strsplit(R.version$minor, '.', fixed = TRUE)[[1]][[1]]
  )
)
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())

## source functions
source(paste0(here, '/1a_functions.R'))

## set constants
year_end <- 2022 
shea_dir <- 'FILEPATH'
nhea_dir <- 'FILEPATH'
save_dir <- 'FILEPATH'
plot_outdir <- 'FILEPATH'
if (!(dir.exists(plot_outdir))) dir.create(plot_outdir)
library(RColorBrewer)
mycolors <- brewer.pal(10, 'Set3')
mycolors <- rev(mycolors)
toc_colors <- c('hosp'=mycolors[10],
                'phys_clin'=mycolors[9],
                'rx'=mycolors[8],
                'oth_pers'=mycolors[7],
                'nf'=mycolors[6],
                'dental'=mycolors[5],
                'oth_prof'=mycolors[4],
                'oth_non_dur'=mycolors[3],
                'hh'=mycolors[2],
                'dur'=mycolors[1])

payer_colors <- c('OOP' = mycolors[5],
                  'PRIV' = mycolors[4],
                  'MDCR' = mycolors[3],
                  'MDCD' = mycolors[2],
                  'OTH' = mycolors[1])

## --------------------
## 1. Get SHEA data
## --------------------

SHEA_mdcd <- read.csv('FILEPATH')%>% as.data.table()
SHEA_mdcr <- read.csv('FILEPATH')%>% as.data.table()
SHEA_priv <- read.csv('FILEPATH')%>% as.data.table()
SHEA_all <- read.csv('FILEPATH')%>% as.data.table()

## Clean SHEA data
SHEA_mdcd <- prep_shea(SHEA_mdcd, payer = 'MDCD', year_end)[,payer:='MDCD'] #%>% filter(item !='Medicaid/Durable Medical Products (Millions of Dollars)')
SHEA_mdcr <- prep_shea(SHEA_mdcr, payer = 'MDCR', year_end)[,payer:='MDCR'] #%>% filter(item !='Medicare/Durable Medical Products (Millions of Dollars)')
SHEA_priv <- prep_shea(SHEA_priv, payer = 'PRIV', year_end)[,payer:='PRIV'] #%>% filter(item !='Private Health Insurance/Durable Medical Products (Millions of Dollars)')
SHEA_all <- prep_shea(SHEA_all, payer = 'All', year_end)[,payer:='All'] #%>% filter(item !='Durable Medical Products (Millions of Dollars)')
SHEA_nopriv <- rbind(SHEA_mdcd, SHEA_mdcr, SHEA_all)
rm(SHEA_mdcd, SHEA_mdcr, SHEA_all)

CHECK1 <- SHEA_nopriv[payer == 'All' & item != "total", .(sum_items = sum(spend)), by = .(state, year)]
CHECK2 <- SHEA_nopriv[payer == 'All' & item == "total", .(state, year, item_tot = spend)]
comp <- merge(CHECK1, CHECK2)
comp[,diff := abs(sum_items - item_tot)]
comp[order(-diff)]

## Append modeled shea
if (year_end == 2022){
  ## set model version
  date <- "2024-08-15"
  in_dir <- 'FILEPATH'
  appended <- append_modeled_shea(
    in_dir = 'FILEPATH', 
    nopriv_DT = SHEA_nopriv, 
    priv_DT = SHEA_priv, 
    year_start = max(SHEA_nopriv$year), ## overlaps by one year with year_end
    year_end, 
    modeled_shea_currency_year = 2022
  )
  SHEA_priv <- appended$priv_DT
  SHEA_nopriv <- appended$nopriv_DT
  rm(appended)
}

## copy for later checking
SHEA_nopriv_RAW <- copy(SHEA_nopriv)
SHEA_priv_RAW <- copy(SHEA_priv)

## --------------------
## 2. Get NHEA data
## --------------------

NHEA <- fread('FILEPATH', skip = 2, header = T)
setnames(NHEA, c("Source of Funds", "Type of Service"), c("payer", "item"))
NHEA <- NHEA[payer %in% c(
  "Total Expenditures",
  "Medicare",
  "Medicaid (Title XIX)",
  "Private Health Insurance",
  "Out of pocket", "Other Health Insurance", "Other Third Party Payers and Programs"
)]
NHEA <- NHEA[item %in% c(
  "Personal Health Care",
  "Hospital Expenditures",
  "Physician and Clinical Expenditures",
  "Dental Services Expenditures",
  "Other Professional Services Expenditures",
  "Home Health Care Expenditures",
  "Nursing Care Facilities and Continuing Care Retirement Communities",
  "Durable Medical Equipment Expenditures",
  "Prescription Drug Expenditures", "Other Non-Durable Medical Products Expenditures",
  "Other Health, Residential, and Personal Care Expenditures", "Durable Medical Equipment Expenditures"
)]

NHEA[,item := plyr::revalue(
  item,
  c(
    "Personal Health Care" = "total",
    "Hospital Expenditures" = "hosp",
    "Physician and Clinical Expenditures" = "phys_clin",
    "Dental Services Expenditures" = "dental",
    "Other Professional Services Expenditures" = "oth_prof",
    "Home Health Care Expenditures" = "hh",
    "Nursing Care Facilities and Continuing Care Retirement Communities" = "nf",
    "Prescription Drug Expenditures" = "rx",
    "Other Non-Durable Medical Products Expenditures" = "oth_non_dur",
    "Other Health, Residential, and Personal Care Expenditures" = "oth_pers",
    "Durable Medical Equipment Expenditures" = "dur"
  )
)]
NHEA[,payer := plyr::revalue(
  payer, 
  c(
    "Total Expenditures" = "All",
    "Private Health Insurance" = "PRIV", 
    "Medicare" = "MDCR", 
    "Medicaid (Title XIX)" = "MDCD",
    "Out of pocket" = "OOP",
    "Other Health Insurance" = "OTH",
    "Other Third Party Payers and Programs" = "OTH"
  )
)]
NHEA <- melt(NHEA, id.vars = c('item','payer'))
NHEA[,year:= as.numeric(str_remove(variable, 'Year_'))]
NHEA <- NHEA[year >= 2000 & year <= year_end]
NHEA$variable <- NULL
setnames(NHEA,c('value'), c('spend'))
NHEA[,spend := as.numeric(spend)]

## aggregate
NHEA <- NHEA[,.(spend = sum(spend)), by = .(year, item, payer)]

## Deflate NHEA
NHEA <- deflate(
  data = NHEA,
  val_columns = "spend",
  old_year = "year",
  new_year = 2019
)


## Pull out PRIV total spend from NHEA and append to SHEA because we don't have SHEA PRIV for 2000
nhea_priv_2000 <- NHEA[item=='total' & payer == 'PRIV' & year == 2000]$spend
## > apply 2001 SHEA state proportions to 2000 NHEA total
priv_state_2001_scalars <- copy(SHEA_priv)[year==2001][,scalar:= spend/(SHEA_priv[year==2001 & state == 'USA']$spend)][,spend:=NULL]
shea_priv_2000 <- priv_state_2001_scalars[,spend:= scalar*nhea_priv_2000][,scalar:=NULL][,year:=2000]
SHEA_priv <- rbind(shea_priv_2000, SHEA_priv)
rm(nhea_priv_2000, priv_state_2001_scalars, shea_priv_2000)

## Rescale NHEA numbers to summed SHEA using totals
usa_mdcd_shea <- SHEA_nopriv[payer == "MDCD" & state=='USA' & item =='total'][,shea_summed_spend:=spend][,spend:=NULL]
usa_mdcr_shea <- SHEA_nopriv[payer == "MDCR" & state=='USA' & item =='total'][,shea_summed_spend:=spend][,spend:=NULL]
usa_priv_shea <- SHEA_priv[state=='USA' & item =='total'][,shea_summed_spend:=spend][,spend:=NULL]
usa_shea <- rbind(usa_mdcd_shea,usa_mdcr_shea,usa_priv_shea) #,usa_all_shea)
rm(usa_mdcd_shea, usa_mdcr_shea, usa_priv_shea)

usa_scalar <- merge(NHEA[item=='total',.(year,item,spend,payer)], usa_shea, by = c('item','payer','year'))
usa_scalar <- usa_scalar[,scalar:=shea_summed_spend/spend][,.(payer,year,scalar)]
rm(usa_shea)

nhea_new <- merge(copy(NHEA)[payer!='All'], usa_scalar, by = c('year','payer'),all.x=T)
nhea_new[is.na(scalar),scalar:=1] ## we dont have OOP or OTH totals in SHEA, so the scalar is just 1 for these payers
nhea_new[,spend_new:=spend*scalar]
nhea_new$scalar <- NULL
rm(usa_scalar)

## Sum the scaled payers to get the correct total
nhea_all_payer_old <- copy(nhea_new)[,lapply(.SD, sum), .SDcols = 'spend', by = c('year','item')][,payer:='All']
nhea_all_payer_new <- copy(nhea_new)[,lapply(.SD, sum), .SDcols = 'spend_new', by = c('year','item')][,payer:='All']
nhea_all_payer <- merge(nhea_all_payer_old,nhea_all_payer_new)
rm(nhea_all_payer_old, nhea_all_payer_new)

## And append in the total
## Use in place of raw NHEA
rm(NHEA)
NHEA <- rbind(nhea_new,nhea_all_payer)
rm(nhea_new, nhea_all_payer)
NHEA$spend <- NULL
setnames(NHEA,'spend_new','spend')

## ------------------------------------------------------------
## 3. Split out 'rx' and 'oth_non_nondur' from SHEA's 'rx_non_dur' using NHEA fractions
## ------------------------------------------------------------

## Create proportion of Other non durable + RX that is RX because we only have the combined category in SHEA
nhea_rx_plus_nondur <- NHEA[item %in% c('oth_non_dur','rx')]
nhea_rx_plus_nondur[, both := sum(spend), by = .(year,payer)]
nhea_rx_plus_nondur[item == 'rx', rx_percent:= spend/both ]
nhea_rx_plus_nondur <- nhea_rx_plus_nondur[!(is.na(rx_percent)), .(rx_percent, year, payer, both)]
nhea_rx_plus_nondur$item<-"rx_non_dur"
nhea_rx_plus_nondur$both <- NULL

## Pull out RX amount from SHEA (for all payer, MDCR, and MDCD only)
nhea_rx <- nhea_rx_plus_nondur[payer %in% c('All','MDCD','MDCR'),.(year,rx_percent,payer)]
shea_rx_nondur <- merge(SHEA_nopriv[item == 'rx_non_dur'], nhea_rx, by = c('year','payer'))
shea_rx <- copy(shea_rx_nondur)[, spend:=spend*rx_percent][,item:='rx'][,rx_percent:=NULL]
shea_nondur <- copy(shea_rx_nondur)[, spend:=spend*(1-rx_percent)][,item:='oth_non_dur'][,rx_percent:=NULL]
rm(nhea_rx, shea_rx_nondur, nhea_rx_plus_nondur)

SHEA_nopriv <- SHEA_nopriv[!(item=='rx_non_dur')]
SHEA_nopriv <- rbind(SHEA_nopriv, shea_rx, shea_nondur)
rm(shea_rx, shea_nondur)

## ----------------------------------------------------------------------------------------------------
## 4. Estimate item-specific private spending in SHEA using NHEA fractions
## ----------------------------------------------------------------------------------------------------

## ***********
## (First split)
## > Given item-specific priv+oop+oth from SHEA
## > Split into priv / oop_oth using NHEA fractions
## ***********

## Get SHEA totals
## > PRIV+OTH+OOP by item
SHEA_all <- SHEA_nopriv[payer == 'All' & item != "total"]
setnames(SHEA_all, 'spend','all_payer_spend')
SHEA_mdcr_mdcd <- SHEA_nopriv[
  i = payer %in% c('MDCR','MDCD') & item != "total",
  j = .(mdcr_mdcd_spend = sum(spend)),
  by = .(year, state, item)
]
SHEA_x1 <- merge(SHEA_all[,.(year, state, item, all_payer_spend)], SHEA_mdcr_mdcd, by =c('year','state','item'))
SHEA_x1[, priv_oth_oop_spend := round(all_payer_spend-mdcr_mdcd_spend, digits=3)]
SHEA_x1[,c("all_payer_spend", "mdcr_mdcd_spend") := NULL]

## Get NHEA fractions
## > PRIV / (PRIV+OOP+OTH) by item
## > OTH_OOP / (PRIV+OOP+OTH) by item
NHEA_x1 <- NHEA[payer %in% c('PRIV','OTH','OOP') & item != "total"]
NHEA_x1[payer %in% c("OTH", "OOP"), payer := "OTH_OOP"]
NHEA_x1 <- NHEA_x1[,.(spend = sum(spend)), by = .(year, payer, item)]
NHEA_x1[, priv_oth_oop_spend := sum(spend), by = .(year, item)]
NHEA_x1 <- NHEA_x1[,.(payer, year, item, frac = spend/priv_oth_oop_spend)]
NHEA_x1 <- dcast(NHEA_x1, year + item ~ paste0("NHEA_frac_", tolower(payer)), value.var = 'frac')

## Apply NHEA fractions to SHEA totals
SHEA_x1_NHEA <- merge(SHEA_x1, NHEA_x1, by = c('item','year'), all = T)
SHEA_x1_NHEA[,PRIV := priv_oth_oop_spend*NHEA_frac_priv]
SHEA_x1_NHEA[,OTH_OOP := priv_oth_oop_spend*NHEA_frac_oth_oop]
SHEA_x1_NHEA[,c("priv_oth_oop_spend", "NHEA_frac_priv", "NHEA_frac_oth_oop") := NULL]
rm(SHEA_x1, NHEA_x1)

## melt again
SHEA_x1_NHEA <- melt(SHEA_x1_NHEA, id.vars = c("state", "year", "item"), variable.name = "payer", value.name = "item_split_into_payers")

## ***********
## (Second split)
## > Given non-item-specific priv / oop_oth spending from SHEA
## > Split into items using NHEA fractions
## ***********

## Get SHEA totals
## > PRIV and OOP_OTH (not by item)
SHEA_all <- SHEA_nopriv[payer == 'All' & item == "total", .(year, state, all_payer_spend = spend)]
SHEA_mdcr_mdcd <- SHEA_nopriv[
  i = payer %in% c('MDCR','MDCD') & item == "total",
  j = .(mdcr_mdcd_spend = sum(spend)),
  by = .(year, state)
]
SHEA_x2 <- merge(SHEA_all, SHEA_mdcr_mdcd, by =c('year','state'))
SHEA_x2 <- merge(SHEA_x2, SHEA_priv[,.(year, state, PRIV = spend)], by = c("year", "state"))
SHEA_x2[, OTH_OOP := round(all_payer_spend-mdcr_mdcd_spend-PRIV, digits=3)]
SHEA_x2[,c("all_payer_spend", "mdcr_mdcd_spend") := NULL]
SHEA_x2 <- melt(SHEA_x2, measure.vars = c("PRIV", "OTH_OOP"), variable.name = "payer", value.name = "all_item_spend")

## Get NHEA fractions
## > (item_x / item_total) by payer
NHEA_x2 <- NHEA[payer %in% c('PRIV', 'OTH', 'OOP') & item != "total"]
NHEA_x2[payer %in% c("OTH", "OOP"), payer := "OTH_OOP"]
NHEA_x2 <- NHEA_x2[,.(spend = sum(spend)), by = .(year, payer, item)]
NHEA_x2[, total_item_spend := sum(spend), by = .(year, payer)]
NHEA_x2 <- NHEA_x2[,.(payer, year, item, frac = spend/total_item_spend)]
NHEA_x2 <- dcast(NHEA_x2, year + payer ~ paste0("NHEA_frac_", item), value.var = 'frac')

## Apply NHEA fractions to SHEA totals
SHEA_x2_NHEA <- merge(SHEA_x2, NHEA_x2, by = c('year', "payer"), all = T)
colnames(SHEA_x2_NHEA)[colnames(SHEA_x2_NHEA) %like% "NHEA"] %>% str_remove("NHEA_frac_") %>% data.table()
SHEA_x2_NHEA[, dental := all_item_spend*NHEA_frac_dental]
SHEA_x2_NHEA[, dur := all_item_spend*NHEA_frac_dur]
SHEA_x2_NHEA[, hh := all_item_spend*NHEA_frac_hh]
SHEA_x2_NHEA[, hosp := all_item_spend*NHEA_frac_hosp]
SHEA_x2_NHEA[, nf := all_item_spend*NHEA_frac_nf]
SHEA_x2_NHEA[, oth_non_dur := all_item_spend*NHEA_frac_oth_non_dur]
SHEA_x2_NHEA[, oth_pers := all_item_spend*NHEA_frac_oth_pers]
SHEA_x2_NHEA[, oth_prof := all_item_spend*NHEA_frac_oth_prof]
SHEA_x2_NHEA[, phys_clin := all_item_spend*NHEA_frac_phys_clin]
SHEA_x2_NHEA[, rx := all_item_spend*NHEA_frac_rx]
SHEA_x2_NHEA[,all_item_spend := NULL]
drop_cols <- names(SHEA_x2_NHEA)[names(SHEA_x2_NHEA) %like% "NHEA_frac"]
SHEA_x2_NHEA[,(drop_cols) := NULL]
rm(SHEA_x2, NHEA_x2)

## melt again
SHEA_x2_NHEA <- melt(SHEA_x2_NHEA, id.vars = c("state", "year", "payer"), variable.name = "item", value.name = "payer_split_into_items")

## ***********
## Cross rake the two splits to sum to one another
## > Given item-specific priv+oop+oth from SHEA
## > Split into priv / oop_oth using NHEA fractions
## ***********

## merge the two splits
SHEA_x_NHEA <- merge(SHEA_x1_NHEA, SHEA_x2_NHEA, by = c("state", "year", "payer", "item"), all = T)
rm(SHEA_x1_NHEA, SHEA_x2_NHEA)

## double check the all-payer, all item total are the same
SHEA_x_NHEA[,grand_total_A := sum(payer_split_into_items), by = .(state, year)]
SHEA_x_NHEA[,grand_total_B := sum(item_split_into_payers), by = .(state, year)]
SHEA_x_NHEA[, max(abs(grand_total_A - grand_total_B))] ## I guess $6 is ok?
SHEA_x_NHEA[,c("grand_total_A", "grand_total_B") := NULL]

## regenerate original totals
SHEA_x_NHEA[,true_item := sum(item_split_into_payers), by = .(state, year, item)]
SHEA_x_NHEA[,true_payer := sum(payer_split_into_items), by = .(state, year, payer)]

## Function to perform one iteration of raking
two_way_rake <- function(dt, step) {
  if(step == "out"){
    dt <- rake(dt, "item_split_into_payers", "true_payer", c("state", "year", "payer"))
    dt <- rake(dt, "payer_split_into_items", "true_item", c("state", "year", "item")) 
  }
  if(step == "back"){
    dt <- rake(dt, "item_split_into_payers", "true_item", c("state", "year", "item"))
    dt <- rake(dt, "payer_split_into_items", "true_payer", c("state", "year", "payer")) 
  }
  return(dt)
}

## Now, all we have to do is go back and forth until they converge
RAKED <- copy(SHEA_x_NHEA)
tolerance <- 5
max_iter <- 1000
iter <- 0
converged <- FALSE
while (!converged && iter < max_iter) {
  
  ## Rake "out" and "back"
  previos_iteration <- RAKED[, .(item_split_into_payers, payer_split_into_items)]
  RAKED <- two_way_rake(RAKED, "out")
  diff_out <- max(
    abs(RAKED$item_split_into_payers - previos_iteration$item_split_into_payers),
    abs(RAKED$payer_split_into_items - previos_iteration$payer_split_into_items) 
  )
  previos_iteration <- RAKED[, .(item_split_into_payers, payer_split_into_items)]
  RAKED <- two_way_rake(RAKED, "back")
  diff_back <- max(
    abs(RAKED$item_split_into_payers - previos_iteration$item_split_into_payers),
    abs(RAKED$payer_split_into_items - previos_iteration$payer_split_into_items) 
  )
  
  max_diff <- max(diff_out, diff_back)
  
  # Check for convergence
  converged <- max_diff < tolerance
  iter <- iter + 1
  cat(paste0("iteration : ", iter, "\n"))
  cat(paste0("  > max_diff : ", max_diff, "\n"))
}
rm(previos_iteration)

## arbitrarily pick one, and rake one last time to the true payer
RAKED <- RAKED[,.(state, year, payer, item, true_payer, spend = payer_split_into_items)]
RAKED <- rake(RAKED, "spend", "true_payer", c("state", "year", "payer"))
RAKED <- RAKED[,.(state, year, payer, item, spend)]
rm(SHEA_x_NHEA)

## drop some other things
rm(SHEA_all, SHEA_mdcr_mdcd, SHEA_priv)

## split out OTH_OOP
## > get amounts from SHEA
RAKED_othoop <- RAKED[payer == "OTH_OOP"]
RAKED_priv <- RAKED[payer == "PRIV"]
## > get fractions from NHEA
NHEA_oop_oth <- NHEA[payer %in% c("OOP", "OTH") & item != "total"]
NHEA_oop_oth[,total := sum(spend), by = .(year, item)]
NHEA_oop_oth <- NHEA_oop_oth[,.(year, item, payer, frac = spend/total)]
NHEA_oop_oth <- dcast(NHEA_oop_oth, year + item ~ payer, value.var = "frac")
## > apply them
RAKED_othoop <- merge(RAKED_othoop, NHEA_oop_oth, by = c("year", "item"), all = T)
RAKED_othoop[,OOP := OOP*spend]
RAKED_othoop[,OTH := OTH*spend]
RAKED_othoop[,c("spend", "payer") := NULL]
RAKED_othoop <- melt(RAKED_othoop, measure.vars = c("OOP", "OTH"), variable.name = "payer", value.name = "spend")
## > recombine
RAKED <- rbind(RAKED_othoop, RAKED_priv)
rm(RAKED_othoop, RAKED_priv, NHEA_oop_oth)

## create item == "total" for the raked/split data
RAKED_all_item <- RAKED[,.(spend = sum(spend), item = "total"), by = .(year, state, payer)]
RAKED <- rbind(RAKED, RAKED_all_item)
rm(RAKED_all_item)

## Recombine with mdcr/mdcr
SHEA <- rbind(SHEA_nopriv, RAKED)
rm(SHEA_nopriv, RAKED)

## --------------------
## 5. Double check no raw values changed
## --------------------

## compare each to raw values
setnames(SHEA_nopriv_RAW, "spend", "raw_nopriv")
COMP <- merge(SHEA, SHEA_nopriv_RAW, by = c("year", "state", "item", "payer"), all = T)
COMP[,max(abs(spend-raw_nopriv), na.rm = T)] ## new is exactly the same
setnames(SHEA_priv_RAW, "spend", "raw_priv")
COMP <- merge(COMP, SHEA_priv_RAW, by = c("year", "state", "item", "payer"), all = T)
COMP[,max(abs(spend-raw_priv), na.rm = T)] ## new is (almost) exactly the same
rm(COMP, SHEA_nopriv_RAW, SHEA_priv_RAW)

## --------------------
## 6. Save
## --------------------
fwrite(SHEA, 'FILEPATH')

