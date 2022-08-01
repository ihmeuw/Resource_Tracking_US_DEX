library(data.table)
library(tidyverse)
library(readxl)
library(unpivotr)
library(DataCombine)

library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package()) # load shared functions

dir <- "FILEPATH"
states <- fread("FILEPATH")
nids <- fread("FILEPATH")

# Format older years ----
df1 <- read_excel(paste0(dir,"USA_CENSUS_DEMO_HEALTH_INSURANCE_1999_2009_HIA_4_Y2021M06D24.XLS"), skip = 2) 
df1 <- df1 %>%
  as_cells() %>%
  behead("up-left","payer") %>%
  behead("up-left","source") %>%
  behead("up","measure") %>%
  behead_if(str_detect(chr,":$"), direction = "left-up", name = "state") %>%
  behead("left","year_id") %>%
  mutate(payer = paste0(payer," ",source)) %>%
  select(state, year_id, payer, measure, cps = chr)

setDT(df1)
df1$payer <- plyr::revalue(df1$payer, c("Private Health Insurance Total" = "ins_private",
                                        "Private Health Insurance Employment-based" = "ins_private_employer",
                                        "Private Health Insurance Direct Purchase" = "ins_private_direct_purchase",
                                        "Government Health Insurance Total" = "ins_public_total",
                                        "Government Health Insurance Medicaid" = "ins_mdcd",
                                        "Government Health Insurance Medicare" = "ins_mdcr",
                                        "Government Health Insurance Military Health Care (1)" = "ins_military",
                                        "All People NA" = "population",
                                        "Not Covered NA" = "uninsured",
                                        "Covered by Private or Government Health Insurance NA" = "insured"))

df1[, state := str_remove(state, ":")][, year_id := as.integer(str_sub(year_id, start = 1L, end = 4L))]
df1 <- df1[measure == "Percent"][!is.na(year_id)][, cps := str_replace(cps,",",".")][, cps := as.numeric(cps)/100]

# Format new years ----
df2 <- read_excel(paste0(dir,"USA_CENSUS_DEMO_HEALTH_INSURANCE_2008_2019_HIC_6_ACS_Y2021M06D24.XLSX"), skip = 2)
df2 <- df2 %>% 
  as_cells() %>%
  behead("up-left","year_id") %>%
  behead("up","measure") %>%
  behead("left-up","state") %>%
  behead("left","payer") %>%
  select(state, year_id, payer, measure, acs = chr)

setDT(df2)
df2[, acs := as.numeric(str_replace(acs,"Z","0"))/100][, year_id := as.numeric(year_id)]
df2 <- df2[!is.na(payer) & measure == "Percent" & payer != "Total"][, payer := tolower(payer)]
df2$payer <- plyr::revalue(df2$payer, c("any coverage" = "insured",
                                        "..employer-based" = "ins_private_employer",
                                        "..direct-purchase" = "ins_private_direct_purchase",
                                        "..tricare" = "ins_private_tricare",
                                        "public" = "ins_public_total",
                                        "private" = "ins_private",
                                        "..medicaid" = "ins_mdcd",
                                        "..medicare" = "ins_mdcr",
                                        "..va care" = "ins_military"))


## Taking percent change from ACS to apply to CPS
# Apply percent difference to prior year's rate
rowwise_percent_change <- function(df){
  for(y in 2007:1999){
    df[year_id == y]$rate <- df[year_id == y+1]$rate*df[year_id == y]$pct_diff
  }
  return(df)
}

df <- merge(df1, df2, all = TRUE)
df <- df %>%
  group_by(state, measure, payer) %>% 
  arrange(state, measure, payer, -year_id) %>%
  mutate(rate = acs, pct_diff = 1 + (cps/lag(cps) - 1)) %>%
  ungroup() %>%
  as.data.table()

df <- split(df, by=c("state","measure","payer"))
df <- rbindlist(lapply(df, rowwise_percent_change))

df <- dcast(df, state + year_id ~ payer, value.var = "rate")

setnames(df, "state", "state_name")
df <- merge(df, states, by="state_name")

# Validation
expected_state_years <- expand.grid(year_id = c(1999:2019), location_id = unique(states$location_id))
state_years <- unique(df[,.(year_id, location_id)])
stopifnot(nrow(diff(expected_state_years, state_years)) == 0)

## add NIDs
lapply(list(df, df1, df2), setDT)
df1[,nid := nids[year_id == 1999, nid]]
df2[,nid := nids[year_id == 2019, nid]]
df[,nid := nids[,unique(dex_nid)]]

# Write combined df, along with the cps and acs rates separately
fwrite(df,"FILEPATH")
fwrite(df1,"FILEPATH")
fwrite(df2,"FILEPATH")
