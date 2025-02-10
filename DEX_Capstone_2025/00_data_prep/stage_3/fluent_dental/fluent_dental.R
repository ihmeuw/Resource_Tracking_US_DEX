#
# Preparing Fluent dental data for spend modeling
# * We were provided total private and total oop spending on dental procedures and total dental treatments by:
#   - state, procedure type, age (2016-2019)
#   - county, procedure type (2016 - 2019)
#
# * Procedure types are:
#   - prevention
#   - treatment
#
# 1. Fit loess curves for each state/year/procedure/variable
#   - to get age curve for per capita rate of oop spending, private spending, and treatments
#   - the age groups we received aren't 1-1 matches to DEX age groups
#   - we use the curve to predict values for the midpoints of our age groups
# 2. Apply state/age specific rates from the curve to county population
#   - split totals by county sex proportions to make county/age/sex/specific
#   - rake values to provided Fluent county totals
# 3. Model spending values to estimate spending in years before 2016
#   - fit linear model of spend proportion ~ population proportion
#    - for each state/sex/age/procedure
#   - apply predicted ratios to SHEA state totals
# 4. Model spend per treatment to estimate treatments in years before 2016
#   - random effects model on state/age/cause/year
#   - apply treatments per encounter from MEPS to estimate total encounters
#
# Drew DeJarnatt

# load packages
library(data.table)
library(tidyverse)
library(arrow)
library(readxl)
library(ggpubr)


# Read in Fluent data
fluent_dental_path <- "FILEPATH"


#different sheets in the excel document vary by geo level, procedure (acause), and year
#calculate oop column from total paid amount and private paid amount columns
ps16 <- readxl::read_excel(fluent_path, 1) %>% mutate(procedure = "prevention", year_id = 2016, paid_oop = allowed_dollars - paid_dollars)
ts16 <- readxl::read_excel(fluent_path, 2) %>% mutate(procedure = "treatment", year_id = 2016, paid_oop = allowed_dollars - paid_dollars)
pc16 <- readxl::read_excel(fluent_path, 3) %>% mutate(procedure = "prevention", year_id = 2016, paid_oop = allowed_dollars - paid_dollars)
tc16 <- readxl::read_excel(fluent_path, 4) %>% mutate(procedure = "treatment", year_id = 2016, paid_oop = allowed_dollars - paid_dollars)
ps17 <- readxl::read_excel(fluent_path, 5) %>% mutate(procedure = "prevention", year_id = 2017, paid_oop = allowed_dollars - paid_dollars)
ts17 <- readxl::read_excel(fluent_path, 6) %>% mutate(procedure = "treatment", year_id = 2017, paid_oop = allowed_dollars - paid_dollars)
pc17 <- readxl::read_excel(fluent_path, 7) %>% mutate(procedure = "prevention", year_id = 2017, paid_oop = allowed_dollars - paid_dollars)
tc17 <- readxl::read_excel(fluent_path, 8) %>% mutate(procedure = "treatment", year_id = 2017, paid_oop = allowed_dollars - paid_dollars)
ps18 <- readxl::read_excel(fluent_path, 9) %>% mutate(procedure = "prevention", year_id = 2018, paid_oop = allowed_dollars - paid_dollars)
ts18 <- readxl::read_excel(fluent_path, 10) %>% mutate(procedure = "treatment", year_id = 2018, paid_oop = allowed_dollars - paid_dollars)
pc18 <- readxl::read_excel(fluent_path, 11) %>% mutate(procedure = "prevention", year_id = 2018, paid_oop = allowed_dollars - paid_dollars)
tc18 <- readxl::read_excel(fluent_path, 12) %>% mutate(procedure = "treatment", year_id = 2018, paid_oop = allowed_dollars - paid_dollars)
ps19 <- readxl::read_excel(fluent_path, 13) %>% mutate(procedure = "prevention", year_id = 2019, paid_oop = allowed_dollars - paid_dollars)
ts19 <- readxl::read_excel(fluent_path, 14) %>% mutate(procedure = "treatment", year_id = 2019, paid_oop = allowed_dollars - paid_dollars)
pc19 <- readxl::read_excel(fluent_path, 15) %>% mutate(procedure = "prevention", year_id = 2019, paid_oop = allowed_dollars - paid_dollars)
tc19 <- readxl::read_excel(fluent_path, 16) %>% mutate(procedure = "treatment", year_id = 2019, paid_oop = allowed_dollars - paid_dollars)


#Read in population and ages
pop_temp <- fread("FILEPATH/county_population_age_sex.csv")

# Read in county names
county_names <- fread("FILEPATH/merged_counties.csv")[, .(mcnty, state = state_name, cnty_name)]

# Merge on county name 
# Clean our county names to match the county names in Fluent
# Fluent does not have any of these following the county name
to_remove <- c(" County", " Census Area", " Borough", " Municipality", " Parish", " City and","'")

# Remove the above designators (e.g., 'County,' 'Parish,' 'Borough') that follow the name of the county 
county_names <- county_names %>%
  mutate(cnty_name = str_remove_all(cnty_name, paste(to_remove, collapse = "|")),
         cnty_name = toupper(cnty_name),
         duplicated = ifelse(duplicated(mcnty), 1, 0)) 
# Fix formatting for others (mostly spacing ex. LASALLE becoming LA SALLE to match Fluent formatting) 
county_names <- county_names %>%
  mutate(cnty_name = case_when(
    mcnty == 24 ~ "DE KALB",
    mcnty == 75 ~ "MATANUSKA SUSITNA",
    cnty_name == "PRINCE OF WALES-HYDER" ~ str_replace(cnty_name, "-", " "),
    mcnty == 598 ~ "DEWITT",
    cnty_name == "LASALLE" ~ "LA SALLE",
    mcnty == 726 ~ "LA PORTE",
    mcnty == 2566 ~ "DE WITT",
    mcnty %in% c(751, 1144) ~ str_replace(cnty_name, "SAINT", "ST"),
    mcnty %in% c(2899, 2902) ~ str_remove(cnty_name, " CITY"),
    TRUE ~ cnty_name
  ))


# Get state abbreviations and merge to population
policy_path <- "FILEPATH"
postal_code <-  fread(paste0(policy_path,"FILEPATH")) %>%
  mutate(state_abr=as.character(state),
         state=as.character(location_name)) %>% 
  select(-location_name)
pop <- left_join(county_names, pop_temp, by = "mcnty")
pop <- pop[,age_group_name:=NULL]
pop <- merge(pop, postal_code, by = c("state"), all.x = TRUE)


# Match age group labels to Fluent age group labels and aggregate on state, age_grp, year
state_pop <- pop %>%
  #dropping duplicated mcntys 
  filter(duplicated == 0, year_id %in% c(2016, 2017, 2018, 2019)) %>%
  mutate(age_grp = case_when(
    age_group_years_start %in% c(0, 1) ~ "<5",
    age_group_years_start == 5 ~ "5-9",
    age_group_years_start == 10 ~ "10-14",
    age_group_years_start == 15 ~ "15-19",
    age_group_years_start == 20 ~ "20-24",
    age_group_years_start %in% c(25, 30) ~ "25-34",
    age_group_years_start %in% c(35, 40) ~ "35-44",
    age_group_years_start %in% c(45, 50) ~ "45-54",
    age_group_years_start %in% c(55, 60) ~ "55-64",
    age_group_years_start %in% c(65, 70) ~ "65-74",
    age_group_years_start >= 75 ~ "75+",
  )) %>% 
  # set age to be midpoint of age range
  mutate(age = case_when(
    age_grp == "<5" ~ 2.5,
    age_grp == "5-9" ~ 7,
    age_grp == "10-14" ~ 12,
    age_grp == "15-19" ~ 17,
    age_grp == "20-24" ~ 22,
    age_grp == "25-34" ~ 29.5,
    age_grp == "35-44" ~ 39.5,
    age_grp == "45-54" ~ 49.5,
    age_grp == "55-64" ~ 59.5,
    age_grp == "65-74" ~ 69.5,
    age_grp == "75+" ~ 81 )
  ) %>%
  group_by(state_abr, year_id, age_grp, age) %>%
  summarize(pop = sum(pop)) %>%
  as.data.table()


# join Fluent state files together
# set negative OOP amounts to 0 (only appear a handful of times in the 2019 data)
pr_state <- rbind(ps16, ts16, ps17, ts17, ps18, ts18, ps19, ts19) %>%
  mutate(paid_oop = ifelse(paid_oop < 0, 0, paid_oop)) 

# join to population data and calculate per capita rates for utilization, private spending, and out-of-pocket spending
pr_loess <- pr_state %>%
  left_join(state_pop, by = c("State" = "state_abr", "age_grp", "year_id")) %>%
  mutate(vol_rate = treatments/pop,
         priv_rate = paid_dollars/pop,
         oop_rate = paid_oop/pop)



# LOESS
#----------------
## Fit state, year, and cause specific loess curves to ages for utilization, private spending, and oop spending

# dataframe of loess results used for plotting the full curve
df_loess <- data.frame() 
# dataframe of loess results with constants for ages <2.5 and 81+
df_constants <- data.frame() # used for estimates
states <- unique(pr_state$State)
for(s in states){
  for(year in c(2016, 2017, 2018, 2019)){
    for(care in c("prevention", "treatment")){
      
      df <- pr_loess %>%
        filter(procedure == care, year_id == year, State == s)
      
      # loess and prediction for utilization
      loess_vol <- loess(vol_rate ~ age, df, span = 0.2)
      pred_vol_rate <- predict(loess_vol, df)
      
      # loess and prediction for private spending
      loess_priv <- loess(priv_rate ~ age, df, span = 0.2)
      pred_priv_rate <- predict(loess_priv, df)
      
      # loess and prediction for out-of-pocket spending
      loess_oop <- loess(oop_rate ~ age, df, span = 0.2)
      pred_oop_rate <- predict(loess_oop, df)
      
      # bind predicted per capita rates and multiply by pop to go back to count space
      df2 <- cbind(df, pred_vol_rate, pred_priv_rate, pred_oop_rate) %>%
        mutate(pred_vol = pop * pred_vol_rate,
               pred_priv = pop * pred_priv_rate,
               pred_oop = pop * pred_oop_rate)
      
      # data frame of all predictions (used for plotting)
      df_loess <- rbind(df_loess, df2)
      
      #Predict on every age 
      df_constant <- data.frame(age = c(0, 1, 2.5, seq(7, 79.5, 2.5), 81, 82, 87), State = s, year_id = year, procedure = care)
      
      pred_vol_rate <- predict(loess_vol, df_constant)
      pred_priv_rate <- predict(loess_priv, df_constant)
      pred_oop_rate <- predict(loess_oop, df_constant)
      
      df_constant <- cbind(df_constant, pred_vol_rate, pred_priv_rate, pred_oop_rate)
      
      # set constants to use for below 2.5 and above 81
      vol_constant_25 <- df_constant %>% filter(age == 2.5) %>% pull(pred_vol_rate)
      priv_constant_25 <- df_constant %>% filter(age == 2.5) %>% pull(pred_priv_rate)
      oop_constant_25 <- df_constant %>% filter(age == 2.5) %>% pull(pred_oop_rate)
      
      vol_constant_81 <- df_constant %>% filter(age == 81) %>% pull(pred_vol_rate)
      priv_constant_81 <- df_constant %>% filter(age == 81) %>% pull(pred_priv_rate)
      oop_constant_81 <- df_constant %>% filter(age == 81) %>% pull(pred_oop_rate)
      
      # replace fitted values with constants
      df_constant <- df_constant %>%
        mutate(pred_vol_rate = case_when(
          age == 0 ~ 0,
          age == 1 ~ vol_constant_25,
          age > 81 ~ vol_constant_81,
          TRUE ~ pred_vol_rate
        ),
        pred_priv_rate = case_when(
          age == 0 ~ 0,
          age == 1 ~ priv_constant_25,
          age > 81 ~ priv_constant_81,
          TRUE ~ pred_priv_rate
        ),
        pred_oop_rate = case_when(
          age == 0 ~ 0,
          age == 1 ~ oop_constant_25,
          age > 81 ~ oop_constant_81,
          TRUE ~ pred_oop_rate
        ))
      
      df_constants <- rbind(df_constants, df_constant)
      
    }
  }
}

# Diagnostic plot to assess the loess fit 
plot_list <- list()
states <- unique(df_loess$State)

dex_ages <- c(0, 2.5, seq(7, 87, 5))

for(s in states){
  print(s)
  
  loess <- df_constants %>%
    filter(State == s)
  
  dex_age <- loess %>%
    filter(age %in% dex_ages)
  
  vol_curve <- df_loess %>%
    filter(State == s) %>%
    ggplot() + 
    geom_point(aes(x = age, y = vol_rate), size = 2) +
    geom_line(data = loess, aes(x = age, y = pred_vol_rate)) +
    geom_point(data = dex_age, aes(x = age, y = pred_vol_rate), color = "red", size = 2.2, shape = 1)+
    scale_x_continuous(breaks = seq(0, 90, 10)) +
    labs(title = paste(s, "Loess Curves for Utilization Rate (Span = 0.2)"),
         x = "Age",
         y = "Encounters/Person") +
    facet_wrap(~procedure + year_id, scales = "free_y") +
    theme_light()
  
  
  priv_curve <- df_loess %>%
    filter(State == s) %>%
    ggplot() + 
    geom_point(aes(x = age, y = priv_rate), size = 2) +
    geom_line(data = loess, aes(x = age, y = pred_priv_rate)) +
    geom_point(data = dex_age, aes(x = age, y = pred_priv_rate), color = "red", size = 2.2, shape = 1)+
    scale_x_continuous(breaks = seq(0, 90, 10)) +
    labs(title = paste(s, "Loess Curves for Private Spend Rate (Span = 0.2)"),
         x = "Age",
         y = "Private Spend/Person") +
    facet_wrap(~procedure + year_id, scales = "free_y") +
    theme_light()
  
  plot_list[[length(plot_list) + 1]] <- ggarrange(vol_curve, priv_curve, nrow = 1, ncol = 2)
}

pdf(file = "FILEPATH", width = 15, height = 8)
ggarrange(plotlist = plot_list, nrow = 1, ncol = 1)
dev.off()


# Apply state and age specific rates from above to county totals 
#------------------------------------------------------------------

# Join all Fluent county data together
pr_county <- rbind(pc16, tc16, pc17, tc17, pc18, tc18, pc19, tc19) %>%
  #some negative oop counties - set them to 0 for now because there are so few
  mutate(paid_oop = ifelse(paid_oop < 0, 0, paid_oop))

# county population and set midpoint age for age groups
county_pop <- pop %>%
  filter(year_id %in% c(2016:2019)) %>%
  group_by(state_abr, cnty_name, mcnty, year_id, age_group_years_start) %>%
  summarize(pop = sum(pop)) %>%
  mutate(age = case_when(
    age_group_years_start == 0 ~ 0,
    age_group_years_start == 1 ~ 2.5,
    TRUE ~ age_group_years_start + 2
  ))

# get county sex distribution to eventually split total county spending by sex
county_sex_dist <- pop %>%
  filter(year_id %in% c(2016, 2017, 2018, 2019)) %>%
  group_by(state_abr, cnty_name, year_id, age_group_years_start) %>%
  mutate(county_pop = sum(pop)) %>%
  ungroup() %>%
  mutate(prop_male = pop/county_pop) %>%
  filter(sex_id == 1) %>%
  select(state_abr, cnty_name, year_id, age_group_years_start, prop_male)


# Join county population info to PR county data 
pr_county2 <- left_join(pr_county, county_pop, by = c("State" = "state_abr", "County" = "cnty_name", "year_id")) %>%
  select(-StateFIPS, -CountyFIPS) 
# Add county sex proportions 
pr_county2 <- left_join(pr_county2, county_sex_dist, by = c("State" = "state_abr", "County" = "cnty_name", "year_id", "age_group_years_start"))


# Join state age and procedure specific per capita rates to county level data
pr_pred <- left_join(pr_county2, df_constants, by = c("State", "year_id", "age", "procedure")) 

# Apply state/age specific rates to county level data
pr_pred2 <- pr_pred %>%
  mutate(pred_vol = pred_vol_rate * pop,
         pred_priv = pred_priv_rate * pop,
         pred_oop = pred_oop_rate * pop)

# Add column of county totals to calculate proportion of county totals that goes to each age
pr_pred2 <- pr_pred2 %>%
  group_by(State, County, mcnty, procedure, year_id) %>%
  # county totals
  mutate(est_county_vol = sum(pred_vol, na.rm = TRUE),
         est_county_priv = sum(pred_priv, na.rm = TRUE),
         est_county_oop = sum(pred_oop, na.rm = TRUE)) %>%
  # proportion of county totals * county totals from Fluent
  mutate(prop_vol = pred_vol/est_county_vol,
         prop_priv = pred_priv/est_county_priv,
         prop_oop = pred_oop/est_county_oop,
         scaled_vol = prop_vol * treatments,
         scaled_priv = prop_priv * paid_dollars,
         scaled_oop = prop_oop * paid_oop)

# split by sex
pr_pred2 <- pr_pred2 %>%
  select(State, County, mcnty, procedure, year_id, age_group_years_start, contains("scaled"), prop_male) %>%
  mutate(vol_M = scaled_vol * prop_male,
         prv_M = scaled_priv * prop_male,
         oop_M = scaled_oop * prop_male,
         vol_F = scaled_vol * (1-prop_male),
         prv_F = scaled_priv * (1-prop_male),
         oop_F = scaled_oop * (1-prop_male))

# wide to long on sex/rate combos
id_vars <- c("State", "County", "mcnty","procedure", "age_group_years_start", "year_id")
measure_vars <- c("vol_M", "vol_F", "prv_M", "prv_F", "oop_M", 'oop_F')
fluent <- pr_pred2 %>%
  reshape2::melt(id.vars = id_vars, measure.vars = measure_vars, variable.name = "sex", value.name = "value") %>%
  mutate(sex_id = str_sub(sex, -1, -1),
         type = str_sub(sex, 1, 3))
# leave sex long but make utilization, private spending, and out-of-pocket wide
fluent <- reshape2::dcast(fluent, State + County + mcnty + procedure + age_group_years_start + sex_id + year_id ~ type, value.var = "value") %>%
  mutate(sex_id = ifelse(sex_id == "F", 2, 1)) %>%
  rename(treatments = vol, private_spending = prv, oop_spending = oop) %>%
  mutate(acause = ifelse(procedure == "treatment", "_oral", "exp_well_dental")) %>%
  select(-procedure)

# sum counties to mcnty
fluent <- fluent %>%
  group_by(State, mcnty, age_group_years_start, sex_id, year_id, acause) %>%
  summarize(oop_spending = sum(oop_spending),
            private_spending = sum(private_spending),
            treatments = sum(treatments))



#--------------------------------
# Modeling spend
#--------------------------------

# Model Data
#--------------------------------------
# Aggregate to get state spending per year and calculate the private spend proportion for each county/age/sex/cause
# proportion equal to the demographics spending over all state spending that year
pr_model <- fluent %>%
  filter(age_group_years_start > 0) %>%
  mutate(State = ifelse(mcnty == 304, "MD", State)) %>%
  group_by(State, year_id) %>%
  summarize(state_priv_spending = sum(private_spending),
            state_oop_spending = sum(oop_spending),
            across()) %>% 
  mutate(priv_spend_ratio = private_spending/state_priv_spending,
         oop_spend_ratio = oop_spending/state_oop_spending) %>%
  rename("state_abr" = "State")


# Get population ratio for every county, age, sex, and year
# Use this to join on SHEA
pop_ratio <- pop %>%
  filter(between(year_id, 2000, 2019), duplicated == 0) %>%
  mutate(state_abr = ifelse(mcnty == 304, "MD", state_abr),
         state = ifelse(mcnty == 304, "Maryland", state)) %>%
  group_by(state, year_id) %>%
  summarize(state_pop = sum(pop), across()) %>%
  ungroup() %>%
  mutate(pop_ratio = pop/state_pop)

# Filter to years available in Fluent to join on model data
# Use this to join pop proportions to spend proportions
model_pop <- pop_ratio %>%
  filter(between(year_id, 2016, 2019), age_group_years_start > 0) %>%
  select(-cnty_name)

# Joining PR data with spending proportions to data with population proportions
model_data <- left_join(pr_model, model_pop, by = c("state_abr", "mcnty","year_id", "sex_id","age_group_years_start")) 



# Prediction Data 
#-----------------------
# Read in SHEA data and filter for private and out of pocket spending for dental 
SHEA <- fread("FILEPATH") %>%
  filter(payer == "PRIV" | payer == "OOP", item == "dental", between(year, 2000, 2019)) 


# Add state abbreviations for joining and get spending units out of millions
SHEA <- left_join(SHEA, postal_code, by = 'state') %>%
  select(year, state_abr, spend, payer) %>%
  mutate(spend = spend * 1e6)

# Go from long to wide on payer
SHEA <- dcast(SHEA, year + state_abr ~ payer, value.var = "spend")
colnames(SHEA) <- c("year", "state_abr", "SHEA_priv_spend", "SHEA_oop_spend")
# Drop national estimates
SHEA <- SHEA %>% filter(!is.na(state_abr))

# Aggregate DC and MD for prediction
SHEA <- SHEA %>%
  mutate(state_abr = ifelse(state_abr == "DC", "MD", state_abr)) %>%
  group_by(year, state_abr) %>%
  summarize(SHEA_priv_spend = sum(SHEA_priv_spend),
            SHEA_oop_spend = sum(SHEA_oop_spend)) %>%
  ungroup()

#Add population proportions to SHEA and filter out unused data
SHEA_pop <- left_join(pop_ratio, SHEA, by = c("state_abr", "year_id" = "year")) %>%
  select(-duplicated) %>%
  filter(age_group_years_start > 0, pop > 0)

# Adding acause to SHEA data
# Duplicating data so each year, county, age, and sex has an oral observation and exp_well_dental observation

# adding "_oral" acause
oral_col <- data.frame(rep("_oral", nrow(SHEA_pop)))
colnames(oral_col) <- "acause"
SHEA_oral <- cbind(SHEA_pop, oral_col)

# adding "exp_well_dental" acause
exp_col <- data.frame(rep("exp_well_dental", nrow(SHEA_pop)))
colnames(exp_col) <- "acause"
SHEA_exp <- cbind(SHEA_pop, exp_col)

# binding data so prediction df has all appropriate columns
# one row for every county/age/sex/year/cause
SHEA_final <- bind_rows(SHEA_oral, SHEA_exp) %>%
  ungroup() %>%
  select(-state)


# Model
#------------------------------
states <- unique(model_data$state_abr)
ages <- unique(model_data$age_group_years_start)
sex_id <- c(1, 2)
pred_df_final <- data.frame(matrix(ncol = ncol(SHEA_final) + 2))
colnames(pred_df_final) <- c(colnames(SHEA_final), "pred_priv_sp_rate", "pred_oop_sp_rate")
for(s in states){
  for(sex in sex_id){
    for(age in ages){
      
      print(s)
      print(sex)
      print(age)
      
      
      df <- model_data %>%
        filter(state_abr == s, sex_id == sex, age_group_years_start == age, oop_spend_ratio > 0)
      
      
      df$pop_ratio <- log(df$pop_ratio)
      df$priv_spend_ratio <- log(df$priv_spend_ratio)
      df$oop_spend_ratio <- log(df$oop_spend_ratio)
      
      
      priv_model <- lm(formula = priv_spend_ratio ~ pop_ratio + factor(acause), data = df)
      oop_model<- lm(formula = oop_spend_ratio ~ pop_ratio + factor(acause), data = df)
      
      
      pred_df <- SHEA_final %>%
        filter(state_abr == s, sex_id == sex, age_group_years_start == age) %>%
        mutate(pop_ratio = log(pop_ratio))
      
      pred_df$pred_priv_sp_rate <- predict(priv_model, pred_df)
      pred_df$pred_oop_sp_rate <- predict(oop_model, pred_df)
      
      pred_df_final <- bind_rows(pred_df_final, pred_df)
      
    }
  }
}

# Predict
#----------------
# calculate ratios to scale to SHEA
pred_df_final <- pred_df_final %>% 
  filter(!is.na(year_id)) %>%
  #get out of log space
  mutate(pred_priv_sp_rate_exp = exp(pred_priv_sp_rate),
         pred_oop_sp_rate_exp = exp(pred_oop_sp_rate)) %>%
  # state/year sums of predicted proportions
  group_by(year_id, state_abr) %>%
  mutate(sum_priv_prop = sum(pred_priv_sp_rate_exp),
         sum_oop_prop = sum(pred_oop_sp_rate_exp)) %>%
  ungroup() %>%
  # calculate ratios
  mutate(scaler_priv = pred_priv_sp_rate_exp/sum_priv_prop,
         scaler_oop = pred_oop_sp_rate_exp/sum_oop_prop,
         scaled_priv_spend_prop = pred_priv_sp_rate_exp * scaler_priv,
         scaled_oop_spend_prop = pred_oop_sp_rate_exp * scaler_oop) %>%
  select(-SHEA_priv_spend, -SHEA_oop_spend) %>%
  data.table()

# add DC back
pred_df_final <- pred_df_final  %>%
  mutate(state_abr = ifelse(mcnty == 304, "DC", state_abr)) %>%
  left_join(SHEA, by = c("year_id" = "year", "state_abr")) %>%
  mutate(pred_priv_spend = scaler_priv * SHEA_priv_spend,
         pred_oop_spend = scaler_oop * SHEA_oop_spend) %>%
  filter(!is.na(year_id)) 


county_names <- as.data.table(read.csv("FILEPATH")) %>%
  filter(current == 1)

pred_df_final <- left_join(pred_df_final, county_names, by = "mcnty")




#----------------------------------------------------------------------
# Modeling spend per treatment
# - model state/age/year spend per treatment
# - apply estimated spend per treatment to county spending estimates
# - use treatments per encounter from MEPS to estimate total encounters
#-----------------------------------------------------------------------
library(lme4)
library(lmerTest)

# state/age/cause/year specific spend per treatment
spend_per_trtmnt <- fluent %>%
  group_by(age_group_years_start, State, year_id, acause) %>%
  summarize(state_priv_spending = sum(private_spending),
            state_oop_spending = sum(oop_spending),
            state_visits = sum(treatments)) %>%
  mutate(priv_spend_per_visit = state_priv_spending/state_visits,
         oop_spend_per_visit = state_oop_spending/state_visits) %>% 
  filter(age_group_years_start > 0) %>%
  rename("state_abr" = "State")

# Prepare data for modeling
#---------------------------
model_df <- spend_per_trtmnt %>%
  select(year_id, state_abr, acause, age_group_years_start, priv_spend_per_visit, oop_spend_per_visit) 
# Reshape data to long on cause, age, and payer
model_df <- reshape2::melt(model_df, 
                           id.vars = c("year_id", "state_abr", "acause", "age_group_years_start"),
                           measure.vars = c("priv_spend_per_visit", "oop_spend_per_visit"),
                           variable.name = "payer",
                           value.name = "spend_per_visit") %>%
  mutate(payer = ifelse(str_detect(payer, "oop"), "oop", "priv"))

# Outlier extreme values with modified z-score
model_df <- model_df %>%
  group_by(acause, age_group_years_start, payer) %>%
  mutate(med_val = median(spend_per_visit),
         mad_val = stats::mad(spend_per_visit)) %>%
  ungroup() %>%
  mutate(mod_z = (0.6745*(spend_per_visit - med_val))/mad_val) %>%
  filter(between(mod_z, -5, 5))

# put spend_per_treatment in log scale
model_df_log <- model_df %>%
  mutate(log_ppv = log(spend_per_trtmnt))

# Model
#------------------------
# Random effect model
# random intercept for payer, cause and state
# random intercept and slope for year varying by age, payer, cause, and state
model_vol <- lmer(log_ppv ~ year_id + (1|payer) + (1|acause) + (1|state_abr) + (1+year_id|age_group_years_start:payer:acause:state_abr), data = model_df_log)

# Predict
#------------------------
# Make spending estimates long on cause, age, and sex 
pred_df_trtmnt <- pred_df_final %>%
  select(year_id, state_abr, acause, age_group_years_start, sex_id, mcnty, cnty_name, pred_priv_spend, pred_oop_spend)
pred_df_trtmnt <- reshape2::melt(pred_df_trtmnt, 
                                 id.vars = c("year_id", "state_abr", "acause", "age_group_years_start", "sex_id", "mcnty", "cnty_name"),
                                 measure.vars = c("pred_priv_spend", "pred_oop_spend"),
                                 variable.name = "payer",
                                 value.name = "spend") %>%
  mutate(payer = ifelse(str_detect(payer, "oop"), "oop", "priv"))

# add predictions of spend per treatment to spending estimates
pred_df_trtmnt$pred_spv <- predict(model_vol, pred_df_trtmnt)
# get out of log space
pred_df_trtmnt <- pred_df_trtmnt %>%
  mutate(pred_spv_exp = exp(pred_spv))
# Back out total treatment estimates by applying predicted rate to estimated spending
pred_df_trtmnt2 <- pred_df_trtmnt %>%
  mutate(treatments = spend/pred_spv_exp)
# add DEX formatted columns
pred_df_trtmnt <- pred_df_trtmnt %>%
  mutate(pri_payer = "priv",
         geo = "county",
         toc = "DV",
         state = state_abr) %>%
  select(year_id, geo, toc, state, mcnty, age_group_years_start, sex_id, acause, pri_payer, payer, spend, treatments) %>% data.table()

# Estimate total encounters
#----------------------------
# Use treatments per encounter map estimated from MEPS dental data
proc_per_enc <- fread("FILEPATH/meps_proc_per_enc.csv")

# join by year, age, sex, cause and divide vol by procedures per encounter
pr_final <- left_join(pred_df_trtmnt, proc_per_enc, by = c("year_id", "age_group_years_start", "sex_id", "acause"))
pr_final <- pr_final %>%
  mutate(vol = vol/proc_per_enc) %>%
  select(year_id, geo, toc, state, mcnty, age_group_years_start, sex_id, acause, pri_payer, payer, spend, vol)

write_parquet(pr_final, "FILEPATH")

