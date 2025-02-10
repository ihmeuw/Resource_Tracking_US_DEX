## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to run Shapley decomposition.
## ==================================================

rm(list = ls())
pacman::p_load(tidyverse, data.table, arrow, glue, argparse)
library(shapley, lib.loc = "FILEPATH") 

if(interactive()){
  scale_ver <- "93_all_yrs"
  draw <- -1
  year <- 2019
  weighted <- 1
  filter_col <- "payer"
  filter_val <- 'mdcr'
}else{
  # Create a parser
  parser <- ArgumentParser()
  
  # Add command line arguments
  parser$add_argument("--scale_ver", help="Scaled version of prepped data", type="character", required=TRUE)
  parser$add_argument("--draw", help="Draw number to read", type="integer", required=TRUE)
  parser$add_argument("--year", help="Year to read", type="integer", required=TRUE)
  parser$add_argument("--weighted", help="Whether to use weighting or not", type="integer", required=TRUE)
  parser$add_argument("--filter_col", help="Column to filter", type="character", required=TRUE)
  parser$add_argument("--filter_val", help="Value to use for column filter", type="character", required=TRUE)
  
  # Parse the command line arguments
  args <- parser$parse_args()
  scale_ver <- args$scale_ver
  draw <- args$draw
  year <- args$year
  weighted <- as.logical(args$weighted)
  filter_col <- args$filter_col
  filter_val <- args$filter_val
}

# Defining regression varaibles for Shapeley decomposition
x_vars <- c("pop_frac_ln_adj","prev_per_pop_ln_adj","enc_per_prev_ln_adj","spend_per_enc_ln_adj")
y_var <- "spend_per_pop_ln_adj"

# Defining regression function for Shapley decomposition, weighted or not
if (weighted){
  reg <- function(x_vars, y_var, fixed_effects, data) {
    if (length(x_vars) == 0) {
      return(0)
    }
    formula <- paste(y_var, "~", paste(x_vars, collapse = " + "))
    m <- lm(formula=formula, data=data, weights=data$weight)
    summary(m)$r.squared
  }
} else {
  reg <- function(x_vars, y_var, fixed_effects, data) {
    if (length(x_vars) == 0) {
      return(0)
    }
    formula <- paste(y_var, "~", paste(x_vars, collapse = " + "))
    m <- lm(formula=formula, data=data)
    summary(m)$r.squared
  }
}

# Loading in dataset
ds <- arrow::open_dataset(glue("FILEPATH"))
df <- ds %>%
  as.tibble()

if (filter_col == 'None'){
  # Running decomp on full dataset
  decomp_res <- shapley(reg,
                        x_vars,
                        y_var=y_var,
                        data=df)
  
  # Renaming result
  decomp_res <- decomp_res %>%
    rename(!!filter_val := value)
  
} else {
  
  # Filtering data using specified column and specified value
  decomp_df <- df %>%
    filter(get(!!filter_col)==filter_val)
  
  # Quitting if no data found
  if (decomp_df %>% nrow() == 0){
    quit("No data for specified filter")
  }
  
  # Running decomp
  decomp_res <- shapley(reg,
                        x_vars,
                        y_var=y_var,
                        data=decomp_df)
  
  # Renaming result value column
  decomp_res <- decomp_res %>%
    rename(!!filter_val := value)
  
}

# Pivoting from long to wide
final_res <- decomp_res %>%
  pivot_wider(names_from="factor", values_from=!!filter_val) %>%
  mutate(draw=draw, year_id=year, facet=filter_col, val=filter_val) 

# Writing to file
write_dataset(final_res,
              glue("FILEPATH"),
              partitioning=c('draw', 'year_id'),
              basename_template=glue("{filter_col}_{filter_val}_{{i}}"))
