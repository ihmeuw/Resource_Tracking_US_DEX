# KFF Marketplace Data
# Sep 28, 2021

# Format KFF marketplace data 
# We calculate spending as average premiums * enrollees (all from KFF)
# For missing average premium data, we use benchmark premiums to predict

# Setup ----------------------------------------------------------------------------------------------

rm(list=ls())

library(dplyr)
library(tidyr)
library(stringr)
library(arrow)
library(data.table)

load("FILEPATH/states.RData")

year_id <- 2001:2019
df <- crossing(states[,c("state_name", "abbreviation", "location_id")], year_id) %>% 
  rename(state_name = 1) 
mergeCols <- c("state_name", "year_id")

in_dir <- "FILEPATH"
out_dir <- "FILEPATH"

# KFF Average premiums ---------------------------------------------------------------------------------

years_kff <- 2017:2019

kff_df <- data.frame(state_name = as.character(),
                     year_id = as.integer(),
                     average_premium = as.numeric())

for (y in years_kff) {

  kff <- read.csv(paste0(in_dir, "average_premiums/",y,"_kff_marketplace_avg.csv"), header = T)[,1:2] 
  kff$average_premium <- as.numeric(str_sub(as.character(kff$Average.Premium, 2, 1e6)))
  kff$state_name <- as.character(kff$Location)
  kff <- kff[ , c(4,3)]
  kff$year_id <- y
  
  kff_df <- rbind(kff_df, kff)
}  

# Merge

df <- df %>% left_join(kff_df, by = mergeCols)  

# Benchmark premiums (used to estimate missing premium data) ---------------------------------------------

kff_benchmark <- read.csv(paste0(in_dir, "benchmark_premiums/kff_benchmark_premiums.csv")) %>% 
  rename(state_name = Location) %>% 
  reshape2::melt(id = "state_name", value.name = "benchmark_premium") %>% 
  mutate(year_id = as.integer(substr(as.character(variable), 2, 1e6))) %>% 
  select(state_name, year_id, benchmark_premium)

df <- df %>% left_join(kff_benchmark, by = mergeCols) 

# RUN FIXED EFFECTS REGRESSION to predict average premiums
# Ln(average premium_sy) = alpha_s + beta*ln(benchmark_sy) +  e_sy ; where s = state and y= year

m1 <- lm(log(average_premium) ~ as.factor(state_name) + log(benchmark_premium), data = df)
df$predicted_premium <- exp(predict.lm(m1, df))
  
# Use observed data where we have it, predicted data where we don't

df <- df %>% 
  mutate(average_premium_final = ifelse(year_id %in% 2001:2013, 0, average_premium), # 0's for 2001-2013
         average_premium_final = ifelse(is.na(average_premium_final), predicted_premium, average_premium_final))

# Enrollees ------------------------------------------------------------------------------------------------
  
years_enroll <- 2014:2019

enroll_df <- data.frame(state_name = as.character(),
                     year_id = as.integer(),
                     average_premium = as.numeric())

for (y in years_enroll) {

  enrollees <- read.csv(paste0(in_dir, "enrollees/",y,"_kff_enrollees.csv"), header = T) %>% 
    rename(state_name = Location,
           enrollees = 2) %>% 
    filter(state_name!="United States") %>% 
    mutate(year_id = y)

  enrollees <- enrollees[ , c(1,3,2)]
  
  enroll_df <- rbind(enroll_df, enrollees) 

}

# Merge; Enrollees are 0 if 2001 - 2013

df <- df %>% left_join(enroll_df, by =  mergeCols) %>% 
  mutate(enrollees = ifelse(year_id %in% 2001:2013, 0, enrollees)) 

# Calculate spending ---------------------------------------------------------------------------------

df$spending = df$average_premium_final * df$enrollees

# SAVE! -----------------------------------------------------------------------------------------------

arrow::write_feather(df, paste0(out_dir,"marketplace_spending.feather"))
write.csv(df, paste0(out_dir,"marketplace_spending.csv"))
