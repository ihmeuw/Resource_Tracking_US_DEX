How to produce the MDCR sample denominator:
0. Make sure claim_demographic tables have been created to impute the sample denom 
1. Run launch.R - this launches MDCR.R by state and yr this takes a few hours to run
    (Make sure nothing errored by following the comments at the end of this script)
2. Final step is to aggregate over state (and race for one version) by running aggregate_mdcr.R