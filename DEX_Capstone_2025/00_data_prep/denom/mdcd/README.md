How to produce the MDCD sample denominator: 
  0. Make sure claim_demographic tables have been created to impute the sample denom with 
  1. Run 1_launch.R - this launches 1_MDCD.R and 1_MDCD_TAF.R by state and yr this takes a few hours to run (Make sure nothing errored by following the comments at the end of this script)
  2. Aggregate over state (and race for one version) by running 2_aggregate_mdcr.R
