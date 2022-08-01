################################################
#' @description format CMS64 from cleaned data
################################################
library(openxlsx)
library(data.table)
library(tidyverse)
library(parallel)
library(arrow)
rm(list=ls())

#Directory
data_dir<-"FILEPATH"
outdir<-"FILEPATH"
source("FILEPATH")
library(lbd.loader, lib.loc = sprintf("FILEPATH", R.version$major))
suppressMessages(lbd.loader::load.containing.package())

#location names -  adding the location id for all datasets
locs <- fread("FILEPATH")
locs[, state := str_pad(as.character(state), 5, side = "left", pad = "0")]

# reading cleaned data from j drive
files<-list.files(data_dir, full.names = T, pattern = "MAP")

cl <- makeCluster(10, type="FORK")
dfs <- parLapply(cl,
                 files,
                 fread)
stopCluster(cl)
all_data <- rbindlist(dfs, use.names = T, fill = T)
rm(dfs)


#renaming and keeping key variables
all_data<-all_data[,.(year_id, 
                      nid,
                      state_name = str_to_title(state), 
                      service_cat = Service.Category, 
                      total = Total.Computable,
                      federal_share = Federal.Share, 
                      state_share = State.Share)]
all_data[state_name == "Dist. Of Col.", state_name := "District of Columbia"]
# all_data[state == "N. Mariana Islands", location_name := "Northern Mariana Islands"]
# all_data[state == "Amer. Samoa", location_name := "American Samoa"]
# all_data[state == "Virgin Islands", location_name := "United States Virgin Islands"]
split_service_type<-merge(all_data,locs)%>% 
  filter(!service_cat %like% "^C-" & !service_cat %like% "^T-" &!is.na(total)) %>% 
  filter(!service_cat %like% "Total VIII Group" & !service_cat %like% "Total Newly Eligible" & !service_cat %like% "Total Not Newly") %>% 
  select(nid, year_id, location_id,state, state_name, abbreviation, service_cat,total)

#assigning types based on the service categories
### we decided to re-calculate the total based on the types we have assigned, so drop the balance, collections and total
# split_service_type[service_cat %like% "Balance", type := "Balance"]
# split_service_type[service_cat %like% "Collections", type := "Collections"]
split_service_type[service_cat %like% "Total Net Expenditures", type := "Total"]
split_service_type[service_cat %like% "Inpatient", type := "IP"]
split_service_type[service_cat %like% "Mental", type := "IP"]
split_service_type[service_cat %like% "Nursing", type := "NF"]
split_service_type[service_cat %like% "Intermediate Care", type := "NF"]
split_service_type[service_cat %like% "Drug", type := "RX"]
split_service_type[service_cat %like% "Emergency", type := "ER"]
split_service_type[service_cat %like% "Outpatient", type := "AM"]
split_service_type[service_cat %like% "Physician", type := "AM"]
split_service_type[service_cat %like% "Practicioners", type := "AM"]
split_service_type[service_cat %like% "Clinic", type := "AM"]
split_service_type[service_cat %like% "Sterilizations", type := "AM"]
split_service_type[service_cat %like% "Abortions", type := "AM"]
split_service_type[service_cat %like% "Laboratory", type := "AM"]
split_service_type[service_cat %like% "Screening", type := "AM"]
split_service_type[service_cat %like% "Home", type := "HH"]
split_service_type[service_cat %like% "Dental", type := "DV"]
split_service_type[service_cat %like% "Prosthetic Devices", type := "DME"]
split_service_type[service_cat %like% "Rehabilitative Services", type := "AM"]
split_service_type[service_cat %like% "Freestanding Birth Center", type := "AM"]
split_service_type[service_cat %like% "Health Home w Chronic Conditions", type := "AM"]
split_service_type[service_cat %like% "Tobacco Cessation for Preg", type := "AM"]

#aggregate by location, year, type
split_service_type <- split_service_type[!is.na(type),sum(total), by = list(nid,year_id,location_id,state,state_name,abbreviation,type)]
setnames(split_service_type,old = "V1", new = "total")

#saving out files
expected_state_years <- expand.grid(year_id = 1997:2019, location_id = unique(locs$location_id))
state_years <- unique(df[,.(location_id, year_id)])
stopifnot(nrow(diff(state_years, expected_state_years))==0)
write_feather(split_service_type, paste0(outdir,"CMS_64.feather"))
