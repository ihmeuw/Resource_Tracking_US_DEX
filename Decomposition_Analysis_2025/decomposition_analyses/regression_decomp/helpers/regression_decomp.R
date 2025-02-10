## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to run regression decomposition.
## ==================================================

rm(list = ls())
pacman::p_load(tidyverse, data.table, arrow, glue, argparse, MASS, sandwich, corpcor)
library(shapley, lib.loc = "FILEPATH") 

if(interactive()){
  scale_ver <- "93_draws"
  draw <- 1
  weighted <- 1
  filter_col <- "toc"
  filter_val <- 'NF'
}else{
  # Create a parser
  parser <- ArgumentParser()
  
  # Add command line arguments
  parser$add_argument("--scale_ver", help="Scaled version of prepped data", type="character", required=TRUE)
  parser$add_argument("--draw", help="Draw number to read", type="integer", required=TRUE)
  parser$add_argument("--covariate", help="Covariate to regress", type="character", required=TRUE)
  parser$add_argument("--weighted", help="Whether to use weighting or not", type="integer", required=TRUE)
  parser$add_argument("--filter_col", help="Column to filter", type="character", required=TRUE)
  parser$add_argument("--filter_val", help="Value to use for column filter", type="character", required=TRUE)
  
  # Parse the command line arguments
  args <- parser$parse_args()
  scale_ver <- args$scale_ver
  draw <- args$draw
  weighted <- as.logical(args$weighted)
  cov <- args$covariate
  filter_col <- args$filter_col
  filter_val <- args$filter_val
}

x_vars <- c("ahrf_mds_pc_adj", "urban_adj", "income_median_adj", 
            "insured_pct_adj", "obesity_prev_stndz_adj")

# Loading in dataset
ds <- arrow::open_dataset(glue("FILEPATH"))
reg_df <- ds %>%
  filter(get(!!filter_col)==filter_val) %>%
  as_tibble()

#######################
# RUNNING REGRESSIONS #
#######################

# Ensuring all INF/NA values are of proper type
reg_df <- reg_df %>% 
  mutate_if(is.numeric, ~ replace_na(., NA) %>% replace(., is.infinite(.), NA))

# Quitting if no data found
if (reg_df %>% nrow() == 0){
    quit("No data for specified filter")
}

# Running encounter regression
enc_formula <- paste("enc_per_prev_ln_adj", "~", cov)
if (weighted){
  enc_lm <- lm(formula=enc_formula, data=reg_df, weights=reg_df$weight)
}else{
  enc_lm <- lm(formula=enc_formula, data=reg_df)
}

# Extract coefficients and covariance matrix
beta_hat <- coef(enc_lm)
cov_matrix <- vcovCL(enc_lm, cluster = ~ acause + sex_id + age_group_years_start)

# Sample from the multivariate normal distribution
betas <- mvrnorm(1, mu = beta_hat, Sigma = make.positive.definite(cov_matrix))[2]
beta_var <- diag(cov_matrix)[2]
p_val <- summary(enc_lm)$coefficients[,4][2]

# Creating dataframe from sampled betas
enc_beta_df <- cbind(data.frame(betas), data.frame(beta_var), data.frame(p_val)) %>%
  rownames_to_column('covariate') %>%
  mutate(draw=draw, metric="enc_per_prev", facet=filter_col, val=filter_val)

# Running spend regression
spend_formula <- paste("spend_per_enc_ln_adj", "~", paste(cov, collapse = " + "))
if (weighted){
  spend_lm <- lm(formula=spend_formula, data=reg_df, weights=reg_df$weight)
}else{
  spend_lm <- lm(formula=spend_formula, data=reg_df)
}

# Extract coefficients and covariance matrix
beta_hat <- coef(spend_lm)
cov_matrix <- vcovCL(spend_lm, cluster = ~ acause + sex_id + age_group_years_start)

# Sample from the multivariate normal distribution
betas <- mvrnorm(1, mu = beta_hat, Sigma = make.positive.definite(cov_matrix))[2]
beta_var <- diag(cov_matrix)[2]
p_val <- summary(spend_lm)$coefficients[,4][2]

# Creating dataframe from sampled betas
spend_beta_df <- cbind(data.frame(betas), data.frame(beta_var), data.frame(p_val)) %>%
  rownames_to_column('covariate') %>%
  mutate(draw=draw, metric="spend_per_enc", facet=filter_col, val=filter_val)

# Writing out results for spending and encounter regressions
final_res <- rbind(enc_beta_df, spend_beta_df)
write_dataset(final_res,
              glue("FILEPATH"),
              partitioning=c('draw'),
              basename_template=glue("{cov}_{filter_col}_{filter_val}_{{i}}"))
