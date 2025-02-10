#---------------------------------------------------
#  HCCI format data, make plots, and save maps
#
#
#  Author: Haley Lescinsky
#---------------------------------------------------

rm(list = ls())
library(lbd.loader, lib.loc = "FILEPATH")
if("dex.dbr"%in% (.packages())) detach("package:dex.dbr", unload=TRUE)
library(dex.dbr, lib.loc = lbd.loader::pkg_loc("dex.dbr"))
suppressMessages(lbd.loader::load.containing.package())
here <- dirname(if(interactive()) rstudioapi::getSourceEditorContext()$path else rprojroot::thisfile())
setwd(here)
t0 <- Sys.time()

#--------------------------------
# STEP 0 - paths
#--------------------------------
config <- get_config()
causelist_path <- config$CAUSEMAP$causelist_path

if(interactive()){
  
  working_dir <- "/FILEPATH/INJURY_RDP/"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  print(args)
  
  working_dir <- args[1]
}

dir.create(paste0(working_dir, "/diagnostics/"))


#--------------------------------
# STEP 1 - Plot Raw
#--------------------------------
causelist <- fread(causelist_path)
causes_to_use <- causelist[acause %like% "NEC" | acause == "_gc"]$acause

prepped_data <- arrow::open_dataset(paste0(working_dir, "/inputs/data/")) %>% collect() %>% as.data.table()

# first by year (age on X)
prepped_data <- prepped_data[!(year_id == 2015)]

# summarize across year
prepped_data2 <- prepped_data[, .(N = sum(N)), by = c("age_group_years_start", "source", "nec", "acause" ,"sex_id", "toc")]
prepped_data2[, tot := sum(N), by = c("age_group_years_start", "source", "nec", "sex_id", "toc")]
prepped_data2[, prop := N/tot]

# summarize across year + source
prepped_data3 <- prepped_data[, .(N = sum(N)), by = c("age_group_years_start", "nec", "acause" ,"sex_id", "toc")]
prepped_data3[, tot := sum(N), by = c("age_group_years_start", "nec", "sex_id", "toc")]
prepped_data3[, prop := N/tot]
prepped_data3[, source := "_ COMBINED"]

prepped_data2 <- rbind(prepped_data2, prepped_data3)

prepped_data2[, plot_source := paste0(source, " (", prettyNum(sum(N), big.mark = ",") ,")"), by = c("source", "toc", "nec", "sex_id")]

# Plot
s <- 1
sex_lab <- ifelse(s == 1, "males", "females")

pdf(paste0(working_dir, "/diagnostics/vetting_plots_", sex_lab, ".pdf"), width = 11, height = 8)

for(cause in  causes_to_use){

  for(t in c("AM", "ED", "HH", "IP", "NF")){

    plot_data <- prepped_data2[nec == cause & toc == t & sex_id == s]
    if(nrow(plot_data) ==0){
      next
    }

    # order causes smallest to largest, pull out any that are top 10 within a combination
    cause_order <- plot_data[, .(both_prop = sum(prop)), by = c("acause")][rev(order(both_prop)), acause]

    plot_causes <- unique(plot_data[rev(order(prop)), .(rank = 1:.N, acause = acause), by = c("age_group_years_start", "sex_id", "source")][rank <= 10, acause])


    plot_data[, plot_cause := ifelse(acause %in% plot_causes, acause, NA)]
    plot_data[, plot_cause := factor(plot_cause, levels = cause_order)]

    p <- ggplot(plot_data,aes(x = as.factor(age_group_years_start),
                                          y=prop,
                                          fill = plot_cause))+
      geom_bar(stat = "identity")+facet_wrap(~plot_source)+
      labs(title = paste0(cause, '-', t, "-", sex_lab),
           x = "",
           fill = "",
           subtitle = "top 10 ranked causes for each age/sex/source pulled out, others NA")+
      theme_bw()+
      theme(legend.position = "bottom",
            legend.key.size = unit(0.3, 'cm'))

    print(p)


  }
}

dev.off()



#--------------------------------
# STEP 2 - Modifications 
#--------------------------------

prepped_data3[, source := NULL]

# A - Pull injury from injury map
inj_maps <- get_map_metadata(maps = list("INJURY"))[status == "Best", map_version_id]
inj_config <- parsed_config(config, "INJURY", map_version = as.character(inj_maps))

inj_props <- fread(paste0(inj_config$map_output_dir, "/intermediates/residual_for_package.csv"))

inj_props <- melt(inj_props, id.vars = c("sex_id", "code_system", "toc", "year_id", "age_group_years_start", "tot_prop"), variable.name = "acause")
inj_props[, N := value*tot_prop]

inj_props <- inj_props[, .(N = sum(N)), by = c("sex_id", "toc", "age_group_years_start", "acause")]
inj_props[, tot := sum(N), by = c("sex_id", "toc", "age_group_years_start") ]
inj_props[, prop := N/tot]
inj_props[, acause := gsub("_pred", "", acause)]
inj_props[acause == "unintent_agg", acause := "_unintent_agg"]
inj_props[acause == "intent_agg", acause := "_intent_agg"]
inj_props[, nec := "inj_NEC"]

prepped_data3 <- rbind(prepped_data3, inj_props)
prepped_data3[, id := paste0(nec, "_", toc, "_", sex_id)]


# B - Deal with necs that have small numbers
#  A little confusing, but just pulling the nec cause pattern from different places in small numbers. using 'id' to facilitate merging/look up across multiple columns
#  Get pattern by nec across age/sex
nec_sums <- prepped_data3[, .(N = sum(N)), by = c("nec", "toc", "acause")]
check_complete <- tidyr::crossing(nec = causes_to_use, toc = unique(prepped_data3$toc)) %>% as.data.table()
nec_sums_check <- merge(unique(nec_sums[, .(nec, toc, present = 1)]), check_complete, by = c("nec", "toc"), all = T)
to_fix_across_toc <- nec_sums_check[is.na(present)]

nec_sums[, tot := sum(N), by = c("nec", "toc") ]
nec_sums[, prop := N/tot]

# expand out to sexes and ages
nec_sums <- tidyr::crossing(nec_sums, "sex_id" = c(1,2), "age_group_years_start" = unique(prepped_data3$age_group_years_start)) %>% as.data.table()
nec_sums[, id := paste0(nec, "_", toc, "_", sex_id)]

# If the full category doesn't have enough observations (1000) to be age specific, replace with across age/sex pattern
nums_across_age <- prepped_data3[, sum(N), by = c("nec", "toc", "sex_id", "id")]
to_fix <- nums_across_age[V1 < 1000]

prepped_data3 <- rbind(prepped_data3[!(id %in% to_fix$id)],
                       nec_sums[id %in% to_fix$id])

# If certain ages don't have enough observations (15), replace with across age/sex pattern 
#      want id to be age specific now
prepped_data3[, id := paste0(nec, "_", toc, "_", sex_id, "_", age_group_years_start)]
nec_sums[, id := paste0(nec, "_", toc, "_", sex_id, "_", age_group_years_start)]

nums_w_age <- prepped_data3[, sum(N), by = c("nec", "toc", "sex_id", "age_group_years_start", "id")] # make sure we have all age groups here
grid_w_age <- tidyr::crossing(nec = unique(prepped_data3$nec), toc = unique(prepped_data3$toc), "sex_id" = c(1,2), "age_group_years_start" = unique(prepped_data3$age_group_years_start)) %>% as.data.table()
nums_w_age <- merge(nums_w_age, grid_w_age, by = c("nec", "toc", "sex_id", "age_group_years_start"), all = T)
nums_w_age[, id := paste0(nec, "_", toc, "_", sex_id, "_", age_group_years_start)]
nums_w_age[is.na(V1), V1 := 0]
to_fix <- nums_w_age[V1 < 15]

prepped_data3 <- rbind(prepped_data3[!(id %in% to_fix$id)],
                       nec_sums[id %in% to_fix$id])


prepped_data3[, id := NULL]

# add on places where there were no observations within that TOC
for(n in unique(to_fix_across_toc$nec)){
  
  nums_across_tocs <- prepped_data3[nec == n]
  need_tocs <- to_fix_across_toc[nec == n, toc]
  
  
  if(nrow(nums_across_tocs) > 0){
    nums_across_tocs <- nums_across_tocs[, .(N = sum(N), tot = sum(tot)), by = c("age_group_years_start", "nec", "acause", "sex_id")]
    nums_across_tocs[, prop := N/tot]
    add <- rbindlist(lapply(need_tocs, function(t){
      return(nums_across_tocs[, .(age_group_years_start, nec, acause, sex_id, N, tot, prop, toc = t)])
    }))
    
  }else{

    add <- prepped_data3[nec == "_gc" & toc %in% need_tocs][, nec := n]
    
  }

  prepped_data3 <- rbind(prepped_data3, add)
}


# C - Implement cause restrictions
restrictions <- fread("/FILEPATH/toc_cause_age_sex.csv")[gc_nec == 0 & toc %in% prepped_data3$toc]
restrictions[age_end == 85, age_end := 95]

# format the restrictions
restrictions[, restrictions := paste0("all_causes[i,]$age_group_years_start >= ", age_start, " & all_causes[i,]$age_group_years_start <= ", age_end)]
restrictions[male == 0, restrictions := paste0(restrictions, " & all_causes[i,]$sex_id==2")]
restrictions[female == 0, restrictions := paste0(restrictions, " & all_causes[i,]$sex_id==1")]

# evaluate each row based on that acause's restrictions for age/sex
all_causes <- unique(prepped_data3[,.(acause, toc, sex_id, age_group_years_start)])
all_causes <- merge(all_causes, restrictions[,.(acause, toc, restrictions)], by = c("acause", "toc"), all.x = T)
all_causes[is.na(restrictions), restrictions := T]

# do evaluation
eval_col <- sapply(c(1:nrow(all_causes)), function(i){
  
  result <- eval(parse(text = all_causes[i, restrictions]))
  return(result)
})

all_causes$restrictions_met <- eval_col

# merge back onto full dataset
# only keep rows where restrictions are met
prepped_data3 <- merge(prepped_data3, all_causes[,.(acause, toc, sex_id, age_group_years_start, restrictions_met)], by = c("acause", "toc", "sex_id", "age_group_years_start"))
prepped_data3 <- prepped_data3[restrictions_met==TRUE, ][, `:=` (restrictions_met = NULL, id = NULL, N = NULL, tot = NULL)]


# D - rescale all probabilities to ensure they sum to 1 (some rows dropped during cause restriction)
prepped_data3[, sum_prop := sum(prop), by = c("age_group_years_start", "nec", "sex_id", "toc")]
prepped_data3[, prop := prop/sum_prop][, sum_prop := NULL]

#--------------------------------
# STEP 4 - Check & save
#--------------------------------


grid <- unique(prepped_data3[, .(nec, toc, age_group_years_start, sex_id)])

grid <- grid[toc != "NF"]

testa <- grid[, .N, by = "nec"]
testa_check <- testa[nec != 'maternal_NEC']
testb <- grid[nec != 'maternal_NEC', .N, by = "toc"]
testc <- setdiff(prepped_data3$nec, causes_to_use)

if(length(unique(testa$N)) > 1){
  print("Reminder - due to cause restricions, we only have maternal_NEC maps for females 10+")
}

if(length(unique(testa_check$N)) > 1 | length(unique(testb$N)) > 1 | length(testc) > 0){
  
  print(testa_check)
  print(testb)
  print(testc)
  stop("Missing an age/sex/nec/toc somewhere!")
  
}else{
  
  #  save map!
  setcolorder(prepped_data3, c("toc", "sex_id", "age_group_years_start", "nec", "acause", "prop"))
  write_dataset(prepped_data3, path = paste0(working_dir, "/maps/"), partitioning = c("toc"))
  
  
}




pdf(paste0(working_dir, "/diagnostics/final_maps.pdf"), width = 13, height = 6)
for(cause in sort(unique(prepped_data3$nec))){
  
  plot_data <- prepped_data3[nec == cause]

  # order causes smallest to largest, pull out any that are top 10 within a combination
  cause_order <- plot_data[, .(both_prop = sum(prop)), by = c("acause")][rev(order(both_prop)), acause]
  
  plot_causes <- unique(plot_data[rev(order(prop)), .(rank = 1:.N, acause = acause), by = c("age_group_years_start", "sex_id", "toc")][rank <= 10, acause])
  
  plot_data[, plot_cause := ifelse(acause %in% plot_causes, acause, NA)]
  plot_data[, plot_cause := factor(plot_cause, levels = cause_order)]
  plot_data[, plot_toc := paste0(ifelse(sex_id==1, "males", "females"), " - ", toc)]
  
  if(cause == "inj_NEC"){
    # change cause order so it matches what we're used to seeing in the injury plots
    plot_data[, plot_cause := NULL]
    plot_data[, plot_cause := gsub("^_", "", acause)]
  }
  
  p <- ggplot(plot_data,aes(x = as.factor(age_group_years_start),
                            y=prop,
                            fill = plot_cause))+
    geom_bar(stat = "identity")+facet_wrap(~plot_toc, ncol = 5)+
    labs(title = paste0(cause),
         x = "", fill = "", subtitle = "top 10 ranked causes for each age/sex/toc pulled out, others NA")+
    theme_bw()+
    theme(legend.position = "bottom",
          legend.key.size = unit(0.3, 'cm'))
  print(p)

}
dev.off()



print("Done! ")
print(Sys.time() - t0)








