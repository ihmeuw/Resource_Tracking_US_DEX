# MEPS-IC Formatting

# Purpose: Format and save MEPS-IC data following webscraping

# NOTE -- NHEA uses: State enrollment and premium data, average per policy premiums by state

# Setup --------------------------------------------------------------------------------------------

rm(list=ls())

library(tidyverse)
library(data.table)
library(stringr)
library(readxl)
library(arrow)

source("FILEPATH/currency_conversion.R") 

# Directories / Maps

state_years <- Sys.glob("FILEPATH/*/USA_MEPS_*_INSURANCE_COMPONENT_STATE_TABLES_BY_STATE_*_TABLEII_Y*.CSV")
exceptions <- Sys.glob("FILEPATH/MEDICAL_EXPENDITURE_PANEL_SURVEY/2001/USA_MEPS_*_INSURANCE_COMPONENT_STATE_TABLES_BY_STATE_*_Y*.XLS")  # These 3 files are different!
state_years <- c(state_years, exceptions)
out_dir <- "FILEPATH"
state_map <- read.csv("FILEPATH/states.csv", stringsAsFactors = F)
state_map$raw_state <- str_to_upper(str_remove_all(state_map$state_name," "))

nids <- data.table(year_id = as.character(c(1996:2006,2008:2019)), 
                   nids = c(53265,53441,53663,53836,54014,54196,54393,54580,54753,54910,55093,55480,55659,55809,111483,111484,148258,204628,
                            251384,324130,408316,408317,448401))

# Set up empty DF -------------------------------------------------------------------------------------

col_names <- read.csv(paste0(state_years[1]), header = F,  # column names
              stringsAsFactors = F, encoding = "latin1")[ -c(1:2), ][[1]]
col_names <- lapply(col_names, function (y) gsub(pattern = "[[:punct:]]", replacement = "", y) ) %>% unlist() # remove periods, parentheses
col_names <- lapply(col_names, function (y) gsub(pattern = "[II]", replacement = "", y) )  %>% unlist()  # remove initial "II"
col_names <- toupper(col_names)

col_names_se <- paste0(col_names, "_SE")  # std. error columns 

finalDF <- data.frame(matrix(ncol = 2 + 2 * length(col_names), nrow = 0))

colnames(finalDF) <- c("raw_state", "year_id", c(col_names, col_names_se))  # add maps

# LOOP THROUGH EACH STATE & YEAR and bind --------------------------------------------------------------

data_vars <- c("Total", "Std. Err. Total")
state <- state_map[ , "state_name"]

# NOTE that the 2018 & 2019 file paths contained spaces - manually removed

for (s in state_years) {
  
    if (! s %in% exceptions) {  # Most files
      
      # Read file and clean headings
  
      x <- read.csv(s, header = F, stringsAsFactors = F, encoding = "latin1")[ -1, ]
  
      colnames(x) <- str_trim(as.character(x[ 1,]))
      
      # Select/format columns
      
      x <- x[ -1, ] %>%  
        dplyr::select("Table No.", "Table Description", data_vars) # Keep name, total and total s.e. columns 
      
      # Remove invalid characters, asterisks, periods, parentheses, & commas
      setDT(x)
      
      ## invalid character
      x[] <- lapply(x, function (y) iconv(y, from="UTF-8", to="UTF-8", sub="") )
      
      ## replace all punctiation except periods in the numbers
      x[,c("Total", "Std. Err. Total") := lapply(.SD, function (y) gsub(pattern = "[^\\w.-]", replacement = "", perl=TRUE, y)), .SDcols = c("Total", "Std. Err. Total")]
      
      ## replace all punctuation in the other columns
      x[,c("Table No.", "Table Description") := lapply(.SD, function (y) gsub(pattern = "[[:punct:]]", replacement = "", y)), .SDcols = c("Table No.", "Table Description")]
      
      ## replace "II"
      x[] <- lapply(x, function (y) gsub(pattern = "[II]", replacement = "", y) ) # initial "II"
        
      x <- as.data.frame(x)
      
    } else {  # 3 exceptions: Kansas 2001, New Hampshire 2001, South Dakota 2001
      
      # Read file and clean headings
      
      x <- read_xls(s) %>% 
        dplyr::select(c(1,2,3,13))  # HARD CODED: Keep name, total and total s.e. columns 
      
      # Select/format columns
      
      x <- x[-c(1:4), ] %>% 
        rename(`Table No.` = 1,
               `Table Description` = 2,
               `Total` = 3,
               `Std. Err. Total` = 4)
      
      # Remove extra characters
      
      x[] <- lapply(x, function (y) gsub(pattern = "[II]", replacement = "", y) ) # initial "II"
      
    }  
    
    # Replace with NA if value == "suppressed"
    
    x[ , data_vars][x[, data_vars]=="suppressed"] <- NA
    
    # Convert to numeric, converting percentages to proportions 
    
    x$Total <- ifelse(grepl("%", x$Total), 
                          as.numeric(sub("%", "", x$Total))/100, 
                          as.numeric(x$Total)) 
    
    x$`Std. Err. Total` <- ifelse(grepl("%", x$`Std. Err. Total`), 
                                  as.numeric(sub("%", "", x$`Std. Err. Total`))/100,
                                  as.numeric(x$`Std. Err. Total`)) 
    
    # Capitalize names
    
    x$`Table No.` <- toupper(x$`Table No.`)

    # Reshape, add variable label headings
    
    x1 <- x %>% dplyr::select(`Table No.`, Total) %>% rename(variable = 1, value = 2)
    x2 <- x %>% dplyr::select(`Table No.`, `Std. Err. Total`) %>% 
      mutate(variable = paste0(`Table No.`, "_SE")) %>% 
      rename(value = 2) %>% dplyr::select(variable, value)
      
    x <- as.data.frame( rbind(x1, x2) %>% t() )[-1, ]
    colnames(x) <- c(x1$variable, x2$variable)
    rownames(x) <- NULL
    
    # Convert to numeric
    
    x[] <- lapply(x, function (y) as.numeric(as.character(y)) )
    
    # Add Year and State Name (without spaces)
    x <- x %>%  
      mutate(raw_state = str_sub(str_split(s,"_")[[1]][13],1,-5),
             year_id = str_sub(str_split(s,"_")[[1]][13],-4,-1)) %>%
      dplyr::select(raw_state, year_id, everything())
    
    # Bind
    
    finalDF <- plyr::rbind.fill(finalDF, x)
    
    print(s)
}

rm(x, x1, x2)


# Add Maps --------------------------------------------------------------------------------------------

# First, add spaces to state names

finalDF <- finalDF %>%
# Merge state maps
  inner_join(state_map, by = "raw_state") %>% 
  inner_join(nids, by="year_id") %>%
  mutate(year_id = as.integer(year_id)) %>% 
  dplyr::select(state_name, location_id, state, abbreviation, year_id, everything(), -raw_state)

# Save codebook ---------------------------------------------------------------------------------------

# Loop through all dataframes to make sure we don't miss any vars

codebook <- read.csv(paste0(state_years[1]), header = F,  
                     stringsAsFactors = F, encoding = "latin1")[ -1, ]

colnames(codebook) <- str_trim(as.character(codebook[ 1,]))

codebook <- codebook[ -1, ] %>%  
  dplyr::select("Table No.", "Table Description") # Keep name columns

codebook[] <- lapply(codebook, function (y) gsub(pattern = "[[:punct:]]", replacement = "", y) ) # asterisks, periods, commas, parenthesis
codebook[] <- lapply(codebook, function (y) gsub(pattern = "[II]", replacement = "", y) ) # initial "II"

codebook$`Table No.` <- toupper(codebook$`Table No.`) # Capitalize names

for (s in state_years) {
  
  if (! s %in% exceptions) {  # Most files 
    
    # Read file and clean headings
    
    x <- read.csv(s, header = F,
                  stringsAsFactors = F, encoding = "latin1")[ -1, ]
    
    colnames(x) <- str_trim(as.character(x[ 1,]))
    
    # Select/format columns
    
    x <- x[ -1, ] %>%  
      dplyr::select("Table No.", "Table Description") # Keep name, total and total s.e. columns 
    
    # Remove invalid characters, asterisks, periods, parentheses, & commas
    
    x[] <- lapply(x, function (y) iconv(y, from="UTF-8", to="UTF-8", sub="") ) # invalid characters
    x[] <- lapply(x, function (y) gsub(pattern = "[[:punct:]]", replacement = "", y) ) # asterisks, periods, commas, parenthesis
    x[] <- lapply(x, function (y) gsub(pattern = "[II]", replacement = "", y) ) # initial "II"
    
  }
  
  else {  # 3 exceptions: Kansas 2001, New Hampshire 2001, South Dakota 2001
    
    # Read file and clean headings
    
    x <- read_xls(s) %>% 
      dplyr::select(c(1,2))  # HARD CODED: Keep name columns 
    
    # Select/format columns
    
    x <- x[-c(1:4), ] %>% 
      rename(`Table No.` = 1,
             `Table Description` = 2)
    
    # Remove extra characters
    
    x[] <- lapply(x, function (y) gsub(pattern = "[II]", replacement = "", y) ) # initial "II"
    
  }  
  
  # Capitalize names
  
  x$`Table No.` <- toupper(x$`Table No.`)
  
  # Bind
  
  codebook <- rbind(codebook, x)
  
  print(s)
}

# Keep only the unique rows

codebook2 <- distinct(codebook) 

# Data validation ------------------------------------------------------------------------------------------------
expected_state_years <- expand.grid(location_id = state_map$location_id, year_id = unique(finalDF$year_id))
missing <- setdiff(expected_state_years, select(finalDF, location_id, year_id))

# Currency conversion ------------------------------------------------------------------------------------------------
setDT(finalDF)
setDT(codebook2)

## get spending vars
vars <- codebook2[`Table Description` %like% "dollars", unique(`Table No.`)]

## reshape long
long <- melt(finalDF, id.vars = c("state_name", "location_id", "state", "abbreviation", "year_id", "nids"))

## split by spending and not
long[,dollar := ifelse(variable %in% vars, "dollar", "no")]
long <- split(long, long$dollar)

## currency convert
long$dollar[, iso3 := "USA"][, year := year_id]
long$dollar <- currency_conversion(
  data = long$dollar, 
  col.loc = "iso3",
  col.value = "value",
  currency = "usd",
  col.currency.year = "year",
  base.year = 2020
)
long$dollar[,iso3 := NULL]

## bind back together
long <- rbindlist(long, use.names = T, fill = T)
long[,dollar := NULL]

## dcast again 
finalDF <- dcast(long, state_name + location_id + state + abbreviation + year_id + nids ~ variable, value.var = "value")

## label vars as currency converted
codebook2[`Table No.` %in% vars, currency_converted := TRUE]

# Save ------------------------------------------------------------------------------------------------
arrow::write_feather(finalDF, paste0(out_dir, "meps_ic.feather"))
arrow::write_feather(codebook2, paste0(out_dir, "meps_ic_codebook.feather"))