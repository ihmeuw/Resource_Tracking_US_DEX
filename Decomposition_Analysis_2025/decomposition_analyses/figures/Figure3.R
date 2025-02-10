## ==================================================
## Author(s): Drew DeJarnatt, Max Weil
## Purpose: This script is used to generate the figure for the regression decomposition.
## ==================================================
rm(list = ls())
pacman::p_load(tidyverse, data.table, arrow, glue)

scale_ver = '102'

# maps for long labels
metric_map = c(
  'enc_per_prev'='Service Utilization',
  'spend_per_enc'='Service Price and Intensity'
)

toc_map = c(
  'AM'="Ambulatory",
  'ED'="Emergency\nDepartment",
  'HH'="Home Health",
  'IP'='Inpatient',
  'NF'='Nursing\nFacility',
  'RX'='Pharmaceutical\nClaims'
)

covariate_map = c(
  "insured_pct_adj" = "Insured rate",
  "income_median_adj" = "Median household income",
  "urban_adj" = "Proportion urban households",
  "ahrf_mds_pc_adj" = "MDs per capita",
  'edu_ba_adj' = "Bachelor's degree+ rate",
  'percent_insured_w_priv_aa_adj' = 'Fraction of insured that are privately insured',
  'primarycare_phys_nonfed_ratio_to_mds_adj' = 'Fraction of MDs that are Primary Care Providers',
  'prop_medicare_mc_adj' = 'Fraction of Medicare that is Medicare Advantage'
)

# Read in data
reg_df <- open_dataset(glue("FILEPATH")) %>%
  collect() %>%
  filter(covariate %in% names(covariate_map)) %>%
  setDT()

# Bonferroni num is number of tests which is # TOCS * # covariates * # metrics
bonferroni_num <- 6*5*2

# Calculate new alpha
alpha_bonf <- 0.05/bonferroni_num

# Calculate new standard deviation to cover alpha value
std_size <- qnorm(1-alpha_bonf/2, mean=0, sd=1)

# Calculate within and between variances
reg_df <- reg_df[
  j = .(
    beta = mean(betas),
    within_var = mean(beta_var),
    between_var = var(betas),
    p_val = mean(p_val)
  ),
  by = .(covariate, metric, val, facet)
]

# Calculate total variance 
reg_df <- reg_df %>%
  mutate(total_var = within_var+(1+(1/50))*between_var) %>%
  mutate(stdev = sqrt(total_var)) %>%
  mutate(lower_ci = beta-std_size*stdev, upper_ci = beta+std_size*stdev) %>%
  mutate(signif = ifelse((lower_ci < 0 & upper_ci < 0) | (lower_ci > 0 & upper_ci > 0), 1, 0))

# Ignore intercept for plotting
plot_df <- reg_df[covariate != "(Intercept)"]

# Replace short labels with long labels
plot_df[, ':='(toc = toc_map[val], covariate = covariate_map[covariate], metric = metric_map[metric])]

# Order toc, metric, and covariate
plot_df[,toc := factor(toc, levels = unname(toc_map))]
plot_df[,metric := factor(metric, levels = unname(metric_map))]
plot_df[,covariate := factor(covariate, levels = unname(covariate_map))]

# Create plot
plot <- ggplot(plot_df, aes(y = rev(toc), x = beta, color = toc, alpha=signif))+
  geom_vline(aes(xintercept = 0), color = 'blue')+
  geom_point(size = 3)+
  facet_grid(covariate ~ metric, switch = "y")+
  geom_errorbarh(aes(xmin = lower_ci, xmax = upper_ci), size = 1)+
  scale_color_brewer(palette="Dark2")+
  theme_bw()+
  scale_alpha_continuous(range = c(0.2, 1), guide='none')+
  labs(y = "",
       x = "Relative change associated with a standardized increase in each variable")+
  theme(legend.title = element_blank(),
        legend.text = element_text(size = 12),
        axis.text.x =element_text(size=12),
        axis.ticks.y = element_blank(),
        plot.title = element_text(size = 16, face = 'bold'),
        strip.text.y.left = element_text(angle = 0, size = 12),
        axis.text.y = element_blank(),
        strip.text.x = element_text(angle = 0, size = 12),
        axis.title=element_text(size=14))

plot

# Saving figure
ggsave(glue("FILEPATH"),
            plot,
            width = 11,
            height = 8.5,
            units = "in")

# Saving table of beta values
plot_df %>%
  dplyr::select(c('metric', 'toc', 'covariate', 'beta', 'lower_ci', 'upper_ci')) %>%
  arrange(metric, toc, covariate) %>%
  write.csv(glue("FILEPATH"),
                 row.names=FALSE)
  