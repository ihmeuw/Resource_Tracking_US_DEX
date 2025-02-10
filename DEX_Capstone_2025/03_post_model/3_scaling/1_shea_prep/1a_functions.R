#############################################################
## Scaling: 1a_functions.R
## Purpose: Functions used in preparing SHEA data for final DEX scaling
## Authors: Sawyer Crosby, Azalea Thomson, 
#############################################################

item_toc_dict_shea <- data.table(
  Item_with_dollars= c("Personal Health Care (Millions of Dollars)",
                       "Hospital Care (Millions of Dollars)",
                       "Physician & Clinical Services (Millions of Dollars)",
                       "Other Professional Services (Millions of Dollars)",
                       "Dental Services (Millions of Dollars)",
                       "Home Health Care (Millions of Dollars)",
                       "Prescription Drugs and Other Non-durable Medical Products (Millions of Dollars)",
                       "Durable Medical Products (Millions of Dollars)",
                       "Nursing Home Care (Millions of Dollars)",
                       "Other Health, Residential, and Personal Care (Millions of Dollars)",
                       "Other Non-Durable Medical Products Expenditures"),
  Item= c("Personal Health Care",
          "Hospital Care",
          "Physician & Clinical Services",
          "Other Professional Services",
          "Dental Services",
          "Home Health Care",
          "Prescription Drugs and Other Non-durable Medical Products",
          "Durable Medical Products",
          "Nursing Home Care",
          "Other Health, Residential, and Personal Care",
          "Other Non-Durable Medical Products Expenditures"),
  item_short = c('total',
                 'hosp',
                 'phys_clin',
                 'oth_prof',
                 'dental',
                 'hh',
                 'rx_non_dur',
                 'dur',
                 'nf',
                 'oth_pers',
                 'oth_non_dur')
)

## Raking function
rake <- function(dt, val_col, tot_col, by_cols){
  tmp <- copy(dt)
  ## rake
  tmp[,SUM := sum(get(val_col)), by = by_cols]
  tmp[,SCALAR := get(tot_col)/SUM]
  tmp[,new := get(val_col)*SCALAR]
  ## check
  tmp[,SUM := sum(new), by = by_cols]
  tmp[,SCALAR := get(tot_col)/SUM]
  stopifnot(tmp[,max(abs(SCALAR-1))] < 10^-9)
  tmp[,c(val_col, "SUM", "SCALAR") := NULL]
  setnames(tmp, "new", val_col)
  ## return
  return(tmp)
}

## Function to prep SHEA data
prep_shea <- function(data_df, payer, year_end, dict = item_toc_dict_shea) {
  
  data_df <- data_df[Group %in% c('United States','State')]
  data_df[Group == 'United States', State_Name := 'USA']
  #Remove prefix from values in Item column
  data_df$Item<- gsub(".*/", "", data_df$Item)
  
  # Merge on short item names
  setnames(data_df,'Item','Item_with_dollars')
  data_df <- merge(data_df, item_toc_dict_shea, by ='Item_with_dollars')
  data_df$Item_with_dollars <- NULL
  data_df$Item <- NULL
  setnames(data_df,'item_short','item')
  
  #Select all the year columns
  year_cols <- data_df %>% select(starts_with('Y')) %>% colnames()
  keep_cols <- c(year_cols, c('State_Name','item'))
  data_df <- data_df[,..keep_cols]
  
  # Melt long
  data_df <- melt(data_df, id.vars = c('State_Name','item'))
  data_df[,year:= as.numeric(str_remove(variable, 'Y'))]
  data_df$variable <- NULL
  setnames(data_df,c('value','State_Name'), c('spend','state'))
  
  # Subset to 2000-max_year
  data_df <- data_df[year >= 2000 & year <= year_end]
  
  # #Apply deflate function
  data_df<- deflate(data = data_df,
                    val_columns = "spend",
                    old_year = "year",
                    new_year = 2019) ## fine to convert to 2019 dollars even if we are extending to 2022 
  
  return(data_df)
}

## function to append modeled SHEA to raw SHEA
append_modeled_shea <- function(in_dir, nopriv_DT, priv_DT, year_start, year_end, modeled_shea_currency_year){
  
  ## get modeled estimates
  toc <- data.table(arrow::read_feather(paste0(in_dir, "/toc_draws.feather")))
  payer <- data.table(arrow::read_feather(paste0(in_dir, "/payer_draws.feather")))

  ## get means
  toc <- toc[year_id %in% year_start:year_end,.(val = mean(val)), by = .(year_id, state, model)]
  payer <- payer[year_id %in% year_start:year_end,.(val = mean(val)), by = .(year_id, state, model)]

  ## deflate to 2019 (need to change old_year manually)
  toc <- deflate(toc, val_columns = "val", old_year = modeled_shea_currency_year, new_year = 2019)
  payer <- deflate(payer, val_columns = "val", old_year = modeled_shea_currency_year, new_year = 2019)

  ## ensure equal
  stopifnot(all.equal(toc[,sum(val), by = .(year_id, state)], payer[,sum(val), by = .(year_id, state)]))

  ## recode values to align with raw SHEA data
  toc_recode <- c(
    "dent" = "dental",
    "other_dme" = "other_dme",
    "hh" = "hh",
    "hosp" = "hosp",
    "nf" = "nf",
    "pharma_nondur" = "rx_non_dur",
    "oprof" = "oth_prof",
    "phys" = "phys_clin"
  )
  toc[,item := toc_recode[model]][,model := NULL]
  payer_recode <- c(
    "mdcd" = "MDCD",
    "mdcr" = "MDCR",
    "priv" = "PRIV_OOP_OTH",
    "oop_oth" = "PRIV_OOP_OTH"
  )
  payer_recode2 <- c(
    "mdcd" = "MDCD",
    "mdcr" = "MDCR",
    # "priv" = "PRIV_OOP_OTH",
    "priv" = "PRIV",
    # "oop_oth" = "PRIV_OOP_OTH"
    "oop_oth" = "OOP_OTH"
  )
  payer_orig <- copy(payer)[,payer := payer_recode2[model]][,model := NULL]
  payer_orig <- payer_orig[,.(val = sum(val)), by = .(year_id, state, payer)]

  payer[,payer := payer_recode[model]][,model := NULL]
  payer <- payer[,.(val = sum(val)), by = .(year_id, state, payer)]

  ## ensure equal
  stopifnot(all.equal(toc[,sum(val), by = .(year_id, state)], payer[,sum(val), by = .(year_id, state)]))
  stopifnot(all.equal(toc[,sum(val), by = .(year_id, state)], payer_orig[,sum(val), by = .(year_id, state)]))

  ## split other_dme out using ratios from SHEA -- this only touches toc, not payer!
  shea_other_dme <- copy(nopriv_DT)[year == year_start & payer == "All" & item %in% c("oth_pers", "dur"), .(state, item, spend)]
  shea_other_dme <- dcast(shea_other_dme, state ~ item, value.var = "spend")
  shea_other_dme[,frac_dur := dur/(oth_pers + dur)]
  shea_other_dme[,c("dur", "oth_pers") := NULL]
  toc_other_dme <- toc[item == "other_dme"]
  toc <- toc[item != "other_dme"]
  toc_dur <- copy(toc_other_dme)[,item := "dur"]
  toc_other <- copy(toc_other_dme)[,item := "oth_pers"]
  toc_dur <- merge(toc_dur, shea_other_dme, by = c("state"))
  toc_other <- merge(toc_other, shea_other_dme, by = c("state"))
  toc_dur[,val := val * (frac_dur)][,frac_dur := NULL]
  toc_other[,val := val * (1-frac_dur)][,frac_dur := NULL]
  toc <- rbindlist(list(toc, toc_dur, toc_other), use.names = T)

  ## ensure equal
  stopifnot(all.equal(toc[,sum(val), by = .(year_id, state)], payer[,sum(val), by = .(year_id, state)]))
  stopifnot(all.equal(toc[,sum(val), by = .(year_id, state)], payer_orig[,sum(val), by = .(year_id, state)]))

  ## recode nopriv_DT a bit
  shea_fracs <- dcast(nopriv_DT, year + state + item ~ payer, value.var = "spend")
  shea_fracs <- shea_fracs[item != "total"]
  shea_fracs[,PRIV_OOP_OTH := All - (MDCD + MDCR)]

  shea_fracs[,All := NULL]
  shea_fracs <- melt(shea_fracs, id.vars = c("year", "state", "item"), variable.name = "payer", value.name = "spend")

  ## create two splits
  ## (a) toc split into payers
  ## (b) payers split into tocs

  ## (A) split toc into payers
  shea_toc_payer_ratios <-  shea_fracs[year == year_start]
  shea_toc_payer_ratios[,frac_spend := spend/sum(spend), by = .(year, state, item)]
  shea_toc_payer_ratios <- shea_toc_payer_ratios[,.(state, item, payer, frac_spend)]
  toc_split <- merge(toc, shea_toc_payer_ratios, by = c("state", "item"), allow.cartesian = T)
  toc_split[,spend := val * frac_spend]
  toc_split <- toc_split[,.(year = year_id, state, item, payer, t_split = spend, t_total = val)]

  ## (B) split payers into toc
  shea_payer_toc_ratios <-  shea_fracs[year == year_start]
  shea_payer_toc_ratios[,frac_spend := spend/sum(spend), by = .(year, state, payer)]
  shea_payer_toc_ratios <- shea_toc_payer_ratios[,.(state, item, payer, frac_spend)]
  payer_split <- merge(payer, shea_payer_toc_ratios, by = c("state", "payer"), allow.cartesian = T)
  payer_split[,spend := val * frac_spend]
  payer_split <- payer_split[,.(year = year_id, state, item, payer, p_split = spend, p_total = val)]

  ## compare the two splits
  splits <- merge(toc_split, payer_split, by = c("year", "state", "item", "payer"), all = T)

  ## make sure they sum to the same totals
  t_total2 <- splits[,.(year, state, item, t_total)] %>% unique()
  p_total2 <- splits[,.(year, state, payer, p_total)] %>% unique()
  t_total2 <- t_total2[,.(val = sum(t_total)), by = .(year, state)]
  p_total2 <- p_total2[,.(val = sum(p_total)), by = .(year, state)]
  stopifnot(all.equal(t_total2, p_total2))

  
  ## two-way-rake until the totals add up to one another
  ## Function to perform one iteration of raking
  two_way_rake <- function(dt, step) {

    if(step == "out"){
      ## Rake pay_split to toc_total
      dt <- rake(dt, "p_split", "t_total", c("state", "year", "item"))
      ## Rake toc_split to pay_total
      dt <- rake(dt, "t_split", "p_total", c("state", "year", "payer"))
    }
    if(step == "back"){
      ## Rake pay_split back to pay_total
      dt <- rake(dt, "p_split", "p_total", c("state", "year", "payer"))
      ## Rake toc_split back to toc_total
      dt <- rake(dt, "t_split", "t_total", c("state", "year", "item"))
    }

    return(dt)

  }

  ## Initialize
  RAKED <- copy(splits)  # Replace with your actual data.table
  tolerance <- 1e-3  # Convergence tolerance (1/10 of a cent)
  max_iter <- 1000   # Maximum number of iterations
  iter <- 0
  converged <- FALSE
  while (!converged && iter < max_iter) {

    ## Rake "out" and "back"
    previos_iteration <- RAKED[, .(p_split, t_split)]
    RAKED <- two_way_rake(RAKED, "out")
    diff_out <- max(abs(RAKED$p_split - previos_iteration$p_split), abs(RAKED$t_split - previos_iteration$t_split))
    previos_iteration <- RAKED[, .(p_split, t_split)]
    RAKED <- two_way_rake(RAKED, "back")
    diff_back <- max(abs(RAKED$p_split - previos_iteration$p_split), abs(RAKED$t_split - previos_iteration$t_split))

    max_diff <- max(diff_out, diff_back)

    # Check for convergence
    converged <- max_diff < tolerance
    iter <- iter + 1
    cat(paste0("iteration : ", iter, "\n"))
    cat(paste0("  > max_diff : ", max_diff, "\n"))
  }


  RAKED[, max(abs(t_split - p_split))] ## splits are the same
  RAKED[,.(sum(t_split)), by = .(year, state, item, t_total)][,max(abs(t_total-V1))] ## toc split sums to toc total
  RAKED[,.(sum(t_split)), by = .(year, state, payer, p_total)][,max(abs(p_total-V1))] ## toc split sums to pay total
  RAKED[,.(sum(p_split)), by = .(year, state, payer, p_total)][,max(abs(p_total-V1))] ## pay split sums to pay total
  RAKED[,.(sum(p_split)), by = .(year, state, item, t_total)][,max(abs(t_total-V1))] ## pay split sums to toc total

  ## simplify
  RAKED <- RAKED[,.(year, state, item, payer, spend = p_split)]

  ## convert to millions
  RAKED[,spend := spend/10^6]

  ## adjust level to match year_start
  RAKED_19 <- RAKED[year == year_start]
  SHEA_19 <- shea_fracs[year == year_start]
  setnames(SHEA_19, "spend", "raw_spend")
  diff <- merge(RAKED_19, SHEA_19, by = c("year", "state", "item", "payer"), all = T)
  diff[,shift := raw_spend - spend]
  diff <- diff[,.(state, item, payer, shift)]
  RAKED <- merge(RAKED, diff, by = c("state", "item", "payer"))
  RAKED[,mean(abs(shift/spend), na.rm = T)] ## average 3% difference
  RAKED[,spend := spend+shift]
  RAKED <- RAKED[year > year_start, -"shift"]

  ## remake "ALl" and "total"
  all_payer <- RAKED[,.(spend = sum(spend), payer = "All"), by = .(year, state, item)]
  RAKED <- rbind(RAKED, all_payer, use.names = T)
  all_toc <- RAKED[,.(spend = sum(spend), item = "total"), by = .(year, state, payer)]
  RAKED <- rbind(RAKED, all_toc, use.names = T)

  ## make USA too
  all_state <- RAKED[,.(spend = sum(spend), state = "USA"), by = .(year, item, payer)]
  RAKED <- rbind(RAKED, all_state, use.names = T)


  ## Get RAKED PRIV_OOP_OTH so we can merge with payer_orig to get the new PRIV
  RAKED_PRIV_OOP_OTH <- RAKED[payer == "PRIV_OOP_OTH" & item == 'total']
  setnames(RAKED_PRIV_OOP_OTH,'year','year_id')
  RAKED_PRIV_OOP_OTH$item <- NULL
  RAKED_PRIV_OOP_OTH$payer <- NULL
  setnames(RAKED_PRIV_OOP_OTH, 'spend','raked_summed_spend')
  
  ## Get orginal estimated payer values for years >year_start (but only the OOP_OTH and PRIV payers)
  priv_oop_oth_orig <- copy(payer_orig)[payer %in% c('OOP_OTH','PRIV') & year_id >year_start]
  ## make USA too
  usa_orig <- priv_oop_oth_orig[,.(val = sum(val), state = "USA"), by = .(year_id, payer)]
  priv_oop_oth_orig <- rbind(priv_oop_oth_orig, usa_orig, use.names = T)
  ## Divide to get into same space as raked
  priv_oop_oth_orig[,val:=val/10^6]
  priv_oop_oth_orig[,summed_spend:=sum(val), by = c('state','year_id')]

  adjust <- merge(priv_oop_oth_orig, RAKED_PRIV_OOP_OTH, by = c('state','year_id'))
  adjust[,diff:=raked_summed_spend-summed_spend]
  adjust[,weight:=val/summed_spend]
  adjust[,new_val:=val+(diff*weight)]
  adjust[,check:=sum(new_val), by = c('state','year_id')]
  new_priv <- adjust[payer== 'PRIV',.(state,year=year_id,spend = new_val)][,`:=`(item='total',payer='PRIV')]
  priv_DT_new <- rbind(new_priv,priv_DT)

  ## append
  RAKED <- RAKED[payer != "PRIV_OOP_OTH"]
  nopriv_DT_new <- rbind(nopriv_DT, RAKED, use.names = T)
  
  return(list("priv_DT" = priv_DT_new, "nopriv_DT" = nopriv_DT_new))
}