# Generating datatable of healthcare occupation covariates for use in updated model

# Each covariate is represented as occupation of interest per capita (10000 pop)
# Occupations of interest include pharmacists, physicians, and all healthcare 
# Workers (Healthcare Practitioners and Technical Occupations and Healthcare
# Support Occupations)

# USERNAME
# 10/20/2021


## -----------------------------------
## Set-up
## -----------------------------------

library(data.table)
library(tidyverse)
library(readxl)
library(dplyr)

# Loading in state table and get_population function
load("FILEPATH/states.RData")
source("FILEPATH/get_population.R")

# Setting up metadata
gbd_round <- 7
gbd_decomp <- 'iterative'

# Function to sum and ignore NA values, unless all values to sum are NA
sum.na <- function(df){
  if (all(is.na(df))){
    suma <- NA
  }  
  else {    
    suma <- sum(df, na.rm = T)
  }
  return(suma)
}


## -----------------------------------
## File set-up
## -----------------------------------

# Setting up directory and files (ignoring years 98-00, formatted badly)
dir <- "FILEPATH"
file_list <- list.files(path=dir, pattern= "dl.xls", recursive=TRUE)
file_list <- file_list[startsWith(file_list,'oes')]

# Establishing OCC Codes to look for in data
# Codes for different physicians
phys_codes <- c(lapply(c(1060:1069, 1210:1229, 1240:1249), function(x) paste0('29-',x)))
names(phys_codes) = rep('phys', length(phys_codes))

# Code for pharmacists
pharm_codes <- c('29-1051')
names(pharm_codes) = rep('pharm', length(pharm_codes))

# Codes for two categories of healthcare workers
hwork_codes <- c('29-0000', '31-0000')
names(hwork_codes) = rep('hwork', length(hwork_codes))

# Codes to avoid (veterinarians and vet techs)
bad_codes <- c('29-1131', '29-2056')
names(bad_codes) = rep('bad', length(bad_codes))

# Merging all into one list (with names)
occ_code_list <- c(phys_codes, pharm_codes, hwork_codes, bad_codes)



## -----------------------------------
## File read-in
## -----------------------------------

# Initializing primary data frame for storing all information
all_data <- data.frame()

# Processing all filenames
for (filename in file_list) {
  df <- data.frame(readxl::read_excel(paste0(dir,filename)))
  
  # Adding column for year (taken from filename)
  df$year_id <- str_extract(filename, '\\d{4}')
  
  # Only adding select columns and rows to primary dataframe
  names(df) <-tolower(names(df))
  data <- df[df$occ_code %in% occ_code_list, 
             c("area", 'occ_code', "occ_title", "tot_emp", "year_id")]
  all_data <- rbind(all_data,data)
}

# Casting total employee numbers to numeric type
all_data$tot_emp <- as.numeric(all_data$tot_emp)


## -----------------------------------
## Pre-processing and merging data
## -----------------------------------

# Creating processing dataframe
pproc_data <- all_data %>% 
  rowwise() %>%
  
  # Adding name of variable for each occupation group and formatting FIPS code
  mutate(var=names(occ_code_list[occ_code_list == occ_code]),
         state=paste0('000',area)) %>%
  
  # Grouping and summing all employees for every state-year and occupation group
  group_by(year_id, state, var) %>% 
  summarise(tot_emp=sum(tot_emp, na.rm = TRUE))

# Merging with states data to get GBD location ID
pproc_data <- merge(pproc_data, states[, c('location_id', 'state')], by='state')

# Getting population information from GBD for all locations
population <- get_population(age_group_id = 22, 
                             location_id = states$location_id, 
                             year_id = 1990:2020, 
                             sex_id = 3, 
                             gbd_round_id = gbd_round,
                             decomp_step = gbd_decomp,
                             status = 'best') 
population <- dcast(population, location_id + year_id ~ sex_id + age_group_id, 
                    value.var = "population")
pop <- population[,.(location_id, year_id, population = `3_22`)]

# Merging population information from GBD
pproc_data <- merge(pproc_data, pop, by=c('location_id', 'year_id'))

## -----------------------------------
## Processing and adjusting for bad occ codes
## -----------------------------------


# Creating a dataframe with totals for each occ group
data_totals <- pproc_data %>%
  
  # Creating columns for each occ group total employee count
  mutate(phys_total=case_when(var=='phys' ~ tot_emp),
         pharm_total=case_when(var=='pharm' ~ tot_emp),
         hwork_total=case_when(var=='hwork' ~ tot_emp),
         bad_total=case_when(var=='bad' ~ tot_emp)) %>%
  
  # Grouping by state-year
  group_by(year_id, location_id, population) %>%
  summarize(phys_total=sum.na(phys_total), 
            pharm_total=sum.na(pharm_total), 
            hwork_total=sum.na(hwork_total),
            bad_total=sum.na(bad_total))

# Adjusting hwork occ group for bad occ codes (vets and vet techs)
adj_data_totals <- data_totals %>%
  
  # Creating adjusted column and dropping old ones
  mutate(adj_hwork_total=hwork_total-bad_total) %>%
  select(-hwork_total, -bad_total)


## -----------------------------------
## Finalizing data and outputting
## -----------------------------------

# Creating final dataframe from processed data
hocc_data <- adj_data_totals %>%
  
  # Calculating variable columns based in occ group count per 10000 pop
  mutate(phys_per_cap=(phys_total/(population/10000)),
         pharm_per_cap=(pharm_total/(population/10000)),
         hwork_per_cap=(adj_hwork_total/(population/10000))) %>%
  select(year_id, location_id, phys_per_cap, pharm_per_cap, hwork_per_cap)
  

fwrite(hocc_data,"FILEPATH/healthcare_workers_pc_var.csv")
