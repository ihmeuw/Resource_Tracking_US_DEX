##----------------------------------------------------------------
## Title: shea_split.R
## Purpose: Produce SHEA estimates at the DEX toc-payer levels.
## 1. Calculate fractions of facility and professional care for each payer/toc
## 2. Do a lm to get the smoothed fractions over time
## 2. For each data source, multiply the smoothed fractions by payer spend across IP, ED, and AM to get a "total hospital envelope" and "total facility envelope"
## 3. Estimate the proportion of each envelope that is IP, ED, and AM.
#
## - SHEA categories to split:
##      - Hospital care (hosp)
##        - This is the facility portion of IP, OP and ER in DEX tocs
##      - Physician and clinical services (phys_clin)
##        - This is the professional (doctor) portion of IP, OP and ER
## Author: Azalea Thomson
##----------------------------------------------------------------

Sys.umask(mode = 002)
t0 <- Sys.time()
pacman::p_load(data.table, tidyverse, arrow, openxlsx, dplyr, stringr, ggrepel)
options(arrow.skip_nul = TRUE)
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


run_v <- get_phase_run_id(status=list("best"))$phase_run_id %>% as.character() # run v of data used to make the scalars

shea_root_dir <- paste0('FILEPATH/')
if (!dir.exists(shea_root_dir)){dir.create(shea_root_dir)}
year_end <- 2022
shea_data_path <- paste0('FILEPATH')
raw_scalar_indir <- paste0('FILEPATH/')

smooth_scalar_outdir <- paste0(shea_root_dir,'toc_smoothed_scalars/')
if (!(dir.exists(smooth_scalar_outdir))){
  dir.create(smooth_scalar_outdir)
}
plot_outpath <- paste0(shea_root_dir, 'diagnostics/')
dir.create(plot_outpath, recursive = TRUE, showWarnings = FALSE)
source(paste0(here, '/1a_functions.R')) 


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
## --------------------
## 1. Read in DEX (MDCR,MSCAN,MDCD TAF) data used to create TOC fractions
## --------------------

files <- list.files(raw_scalar_indir)
all_states <- data.table()
for (i in unique(files)){
  st <- fread(paste0(raw_scalar_indir,i))
  if (ncol(st)!=1){
    all_states <- rbind(all_states,st)
  }
}


all_states <- all_states[!(source=='MDCR' & year == 2014)] ## drop MDCR 2014 since we dont have ED we cant reliably know other IP and AM
all_states <- all_states[,`:=`(frac_p = NULL, frac_f = NULL, total_p = NULL, total_f = NULL)]
spend_cols <- c('prof','fac','total')
national <- all_states[, lapply(.SD, sum, na.rm=TRUE), by=c('toc','year','source','payer'), .SDcols=spend_cols ]
national$state <- "USA"
all_states$n <- NULL
all_states <- rbind(all_states, national)

all_states[, total_p := sum(prof), by = c('year','payer','source','state')]   
all_states[, total_f := sum(fac), by = c('year','payer','source','state')]
all_states[, frac_p := prof/total_p]    
all_states[, frac_f := fac/total_f]  

## drop any states where one of the TOCs for a year-payer-source is zero. if facility is zero then we cant trust professional and vice versa.
all_states[, f_zero:=ifelse(min(frac_f)==0,1,0), by = c('state','year','payer','source')]
all_states[, p_zero:=ifelse(min(frac_p)==0,1,0), by = c('state','year','payer','source')]
all_states <- all_states[!(f_zero == 1 | p_zero == 1)]
all_states[,`:=`(f_zero=NULL,p_zero=NULL)]

all_states[,none:=ifelse(max(total)==0,1,0), by = c('state','year','payer','source')]
all_states <- all_states[,.(frac_f,frac_p,toc, state,year,payer,source)]
## --------------------
## 2. Run model
## --------------------
payers <- c('mdcr','oop','tot','priv','mdcd')

types_of_care <- c('ED','IP','AM')

full <- expand.grid(state = as.character(unique(all_states$state)),
                       year = 2000:2022,
                       payer = payers,
                       toc = types_of_care) %>% as.data.table()

full <- merge(full, all_states[payer!='tot'], by = c('state','year','toc','payer'), all.x = T)

save <- copy(full)

## 2. Pooled states, linear fit with state fixed effect, MDCR only
pay_mdcr_preds <- data.table()
for (care in types_of_care){
  print(care)
  model_dt <- full[toc==care & payer == 'mdcr']
  model_dt <- melt(model_dt, measure.vars = c('frac_f','frac_p'))
  print("Running a linear regression")

  fit_f <- glm(value~ year + factor(state)-1, data= model_dt[variable=='frac_f'],family=gaussian(link='log'))
  fit_p <- glm(value ~ year + factor(state)-1, data= model_dt[variable == 'frac_p'],family=gaussian(link='log'))
 
  pred <- predict(fit_f,newdata=model_dt[variable=='frac_f',.(year,state,value,payer)]) %>% exp()
  model_dt_f <- cbind(model_dt[variable=='frac_f'],pred)
  pred <- predict(fit_p,newdata=model_dt[variable == 'frac_p',.(year,state,value,payer)]) %>% exp()
  model_dt_p <- cbind(model_dt[variable=='frac_p'],pred)
  
  mod_full <- rbind(model_dt_f,model_dt_p)

  pay_mdcr_preds <- rbind(mod_full,pay_mdcr_preds)
}

pay_mdcr_preds$state <- factor( as.character(pay_mdcr_preds$state), levels=c('USA', unique(pay_mdcr_preds[state!='USA']$state)) )
pay_mdcr_preds <- pay_mdcr_preds[order(pay_mdcr_preds$state),]


## 3. Add MDCR preds as a predictor for other payers
other_payers <-  melt(full[!(payer %in% c('mdcr','tot'))], measure.vars = c('frac_f','frac_p'))
setnames(pay_mdcr_preds,'pred','pred_mdcr')
other_payers <- merge(other_payers, pay_mdcr_preds[,.(state,year,toc,variable,pred_mdcr)], by = c('state','year','toc','variable'))

other_payer_preds <- data.table()
for (care in types_of_care){
  print(care)
  model_dt <- other_payers[toc==care]
  print("Running a linear regression")


  if (length(unique(model_dt[variable == 'frac_f' & !is.na(value) & !is.nan(value)]$payer))>1){
    print('Fitting with payer as a predictor')
    fit_f <- glm(value~ payer+ pred_mdcr + factor(state)-1, data= model_dt[variable=='frac_f'],family=gaussian(link='log'))
  }else{
    print('Fitting without payer as a predictor')
    fit_f <- glm(value~ pred_mdcr + factor(state)-1, data= model_dt[variable=='frac_f'],family=gaussian(link='log'))
  }

  if (length(unique(model_dt[variable == 'frac_p' & !is.na(value)]$payer))>1){
    print('Fitting with payer as a predictor')
    fit_p <- glm(value ~ payer+pred_mdcr+ factor(state)-1, data= model_dt[variable == 'frac_p'],family=gaussian(link='log'))
  }else{
    print('Fitting without payer as a predictor')
    fit_p <- glm(value ~ pred_mdcr+ factor(state)-1, data= model_dt[variable == 'frac_p'],family=gaussian(link='log'))
  }



  pred <- predict(fit_f,newdata=model_dt[variable=='frac_f',.(pred_mdcr,value,payer,state,year)]) %>% exp()
  model_dt_f <- cbind(model_dt[variable=='frac_f'],pred)

  pred <- predict(fit_p,newdata=model_dt[variable == 'frac_p',.(pred_mdcr,value,payer,state,year)]) %>% exp()
  model_dt_p <- cbind(model_dt[variable=='frac_p'],pred)

  mod_full <- rbind(model_dt_f,model_dt_p)

  other_payer_preds <- rbind(mod_full,other_payer_preds)
}

setnames(pay_mdcr_preds,'pred_mdcr','pred')
other_payer_preds$pred_mdcr <- NULL
full_preds <- rbind(other_payer_preds, pay_mdcr_preds)
fwrite(full_preds, paste0(smooth_scalar_outdir, 'preds.csv'))


full_preds$state <- factor( full_preds$state, levels=c('USA', as.character(unique(full_preds[state!='USA']$state))) )
full_preds <- full_preds[order(full_preds$state),]
## --------------------
## 3. Read in SHEA
## --------------------
payers <- c('ALL','MDCD','MDCR','PRIV','OOP','OTH')

payer_dt <- fread(paste0(shea_data_path,'payer_specific.csv'))
payer_dt$payer <- toupper(payer_dt$payer)
shea <- payer_dt[payer %in% payers]
shea <- shea[,.(year,item,state,spend,payer)]

setnames(shea,'state','state_name')
shea[state_name == 'USA', state_name := "United States"]


## Merge on state abbreviations
library(datasets)
state_dt <- data.table(state = state.abb, state_name = state.name)
state_dt <- rbind(state_dt, data.table(state ='USA', state_name='United States'))
state_dt <- rbind(state_dt, data.table(state ='DC', state_name='District of Columbia'))
shea <- merge(shea, state_dt, by = c('state_name'))

setnames(shea,'item','toc')
## --------------------
## 4. Roehrig adjustment
## --------------------
# 0.50% from Hospital to Other Professional
# 1.00% from Hospital to Home Health
# 0.70% from Hospital to Prescriptions
# 0.30% from Hospital to Durable Medical Equipment
# 2.30% from Hospital to Nursing Home
# 1.10% from Hospital to Other Personal
# 
# 12.0% from Physician & Clinical to Other Professional
# 0.90% from Physician & Clinical to Home Health
# 1.30% from Physician & Clinical to Prescription
# 1.00% from Physician & Clinical to Durable Medical Equipment
# 0.50% from Physician & Clinical to Nursing Home
# 1.00% from Physician & Clinical to Other Personal
# 
# 1.10% from Dental to Other Professional
# 1.90% from Other Professional to Durable Medical Equipment
# 1.10% from Prescription Drugs to Other Personal
# 8.30% from Durable Medical Equipment to Other Personal
# 13.40% from Other Personal Health Care to Durable Medical Equipment

shea[toc == 'hosp',out_oth_prof:= .005]
shea[toc == 'hosp',out_hh:= .01]
shea[toc == 'hosp',out_rx:= .007]
shea[toc == 'hosp',out_dur:= .003]
shea[toc == 'hosp',out_nf:= .023]
shea[toc == 'hosp',out_oth_pers:= .011]

shea[toc == 'phys_clin',out_oth_prof:= .12]
shea[toc == 'phys_clin',out_hh:= .009]
shea[toc == 'phys_clin',out_rx:= .013]
shea[toc == 'phys_clin',out_dur:= .01]
shea[toc == 'phys_clin',out_nf:= .005]
shea[toc == 'phys_clin',out_oth_pers:= .01]

shea[toc == 'dental', out_oth_prof := .011]
shea[toc == 'oth_prof', out_dur := .019]
shea[toc == 'rx', out_oth_pers := .011]
shea[toc == 'dur', out_oth_pers := .083]
shea[toc == 'oth_pers', out_dur := .134]

out_cols <- shea %>% select(contains('out')) %>% names()
shea$total_out <- rowSums(shea[,..out_cols], na.rm = T)
shea[ , (out_cols):=lapply(.SD, function(x) x * spend), .SDcols = out_cols] 
shea$total_out <- rowSums(shea[,..out_cols], na.rm = T)

## for sankey
sankey_dt <- copy(shea)[,total_out:= spend -total_out]
sankey_flows <-melt(sankey_dt,id.vars = c('state','state_name','year','payer','toc','spend'))
no_toc_change <- sankey_flows[variable == 'total_out']
no_toc_change[,variable:= toc]
sankey_flows <- sankey_flows[!(variable == 'total_out' | is.na(value))]
sankey_flows <- rbind(sankey_flows, no_toc_change)
sankey_flows[, variable:= gsub('out_','',variable)]
setnames(sankey_flows,c('spend','variable','value'), c('pre_adjust','toc_adjusted','post_adjust'))
fwrite(sankey_flows, paste0(shea_root_dir,'sankey_data.csv'))

## back to shea env
total_in <- shea[ , lapply(.SD, sum, na.rm = T), .SDcols = out_cols, by = c('state','year','payer')] 
total_in[,`:=`(dental = 0, hosp = 0, total = 0, phys_clin = 0, oth_non_dur = 0)] ## These types of care don't get adjusted (plus total)
total_in <- melt(total_in,id.vars = c('state','year','payer'),variable.name = 'toc',value.name = 'total_in')
total_in[, toc:= gsub('out_','',toc)]
shea[,(out_cols):=NULL]
shea <- merge(shea,total_in, by=c('state','year','payer','toc'))
shea[,spend_adj:=spend-total_out+total_in]


## Confirm that all tocs sum to total
if (as.integer(shea[toc == 'total' & state == 'USA' & year == 2000 & payer == 'ALL']$spend) != as.integer(sum(shea[toc !='total' & state == 'USA' & year == 2000 & payer == 'ALL']$spend_adj)) ){
  message('Warning, all tocs do not sum to total post Roehrig')
  stop()
}


shea[, total_spend := sum(spend[toc!='total']),by=c('payer','state','year')]
shea[,frac:=spend/total_spend]
shea$toc <- reorder(shea$toc, shea$frac)


cols_to_rm <- shea %>% select(contains('total_')) %>% names()
shea$spend <- NULL
shea$frac <- NULL
setnames(shea,'spend_adj','spend')
shea[,(cols_to_rm) := NULL]



## --------------------
## 5. Calculate new SHEA envelopes adjusted for DEX types of care)
## --------------------
full_preds <- fread(paste0(smooth_scalar_outdir,'preds.csv'))
unique(full_preds$payer)

## Duplicate MDCR payer data and use for non oop and priv payers: ALL, OTH
all_payer <- copy(full_preds[payer=='mdcr'])[,payer:='ALL']
oth_payer <- copy(full_preds[payer=='mdcr'])[,payer:='OTH']
## Append them together
full_preds <- rbind(all_payer,oth_payer,full_preds)

# ## Subsetting just to MDCR right now
full_preds[,value := NULL]
full_preds[,source := NULL]
full_preds <- unique(full_preds) 

## Cast MDCR toc smoothed scalars wide on TOC so we have one row per state/year with the ED, IP and AM fractions
sm_scalar <- dcast(full_preds, ... ~ variable + toc, value.var = c('pred'))

## check that Ed, AM and IP sum to 1
sm_scalar <- sm_scalar[, tot_f:= frac_f_AM + frac_f_ED + frac_f_IP]
sm_scalar <- sm_scalar[, tot_p:= frac_p_AM + frac_p_ED + frac_p_IP]
if (unique(round(sm_scalar$tot_f),digits=2) != 1 & unique(round(sm_scalar$tot_p),digits=2) != 1){
  message('Warning, fractions do not sum to 1')
  stop()
}

sm_scalar[,c('tot_f','tot_p'):= NULL]
sm_scalar$payer <- toupper(sm_scalar$payer)
shea <- shea[year %in% c(2000:year_end)]


## split out just phys clin and hospital which are the categories we want to adjust
shea_fine <- shea[!(toc %in% c('hosp','phys_clin','oth_prof'))]
shea_other_prof <- shea[toc =='oth_prof']
shea_to_split <- shea[toc %in% c('hosp','phys_clin')]

## merge shea and toc scalars
merged <- merge(shea_to_split, sm_scalar, by = c('state','year','payer')) 

merged[toc == 'hosp', ed_f := spend*frac_f_ED]
merged[toc == 'hosp', ip_f := spend*frac_f_IP]
merged[toc == 'hosp', am_f := spend*frac_f_AM]

merged[toc == 'phys_clin', ed_p := spend*frac_p_ED]
merged[toc == 'phys_clin', ip_p := spend*frac_p_IP]
merged[toc == 'phys_clin', am_p := spend*frac_p_AM]

merged_clean <- merged[,.(state,year,payer, ed_f, ed_p, ip_f, ip_p, am_f, am_p)]
merged_clean <- melt(merged_clean, id.vars = c('state','year','payer'))
merged_clean <- merged_clean[!(is.na(value))]
merged_cast <- dcast(merged_clean, state + year + payer~ ...)
merged_cast[, ED := ed_p + ed_f]
merged_cast[, IP := ip_p + ip_f]
merged_cast[, AM := am_p + am_f]
merged_cast[,c('ed_p','ed_f','ip_p','ip_f','am_p','am_f'):=NULL]

shea_adjusted <- melt(merged_cast, id.vars = c('state','year','payer'))
setnames(shea_adjusted, c('variable','value'), c('toc','spend'))


## Add other prof to AM
shea_other_prof <- shea_other_prof[,.(state,toc,year,spend,payer)]
shea_other_prof[,toc:= 'AM']
shea_adj_other_prof <- rbind(shea_adjusted, shea_other_prof)


shea_adj_other_prof[, spend:=sum(spend), by = c('toc','year','state','payer')]
shea_adj_other_prof <- unique(shea_adj_other_prof)

## --------------------
## 6. Save
## --------------------

shea_adjusted_final <- rbind(shea_adj_other_prof, shea_fine[,.(state,toc,year,spend,payer)])
shea_adjusted_final[toc =='hh',toc:='HH']
shea_adjusted_final[toc =='rx',toc:='RX']
shea_adjusted_final[toc =='dental',toc:='DV']
shea_adjusted_final[toc =='nf',toc:='NF']
fwrite(shea_adjusted_final, paste0(shea_root_dir, 'shea_adjusted_envelopes_allpayer.csv'))
