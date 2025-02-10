
## Facility to Total (F2T) and Charge to Payment (C2P) 


Charge to Payment approach:
Calculate the c2p ratios as the payer-specific paid amount/ total charge amount of an encounter.
For each pri-payer/toc/cause family combination, fit a weighted linear regression to these c2p ratios
lm.fit <- lm(c2p_ratio ~ payer + payer_year -1)
There is a payer and payer_year effect for each payer that has >= 200 non-NA observations. There is a payer variable only for each payer that has >1 but <200 non-NA observations.

Facility to Total approach:
Calculate the f2t ratios as the total charge/facility charge for each encounter
For each toc/cause family combination, fit a robust linear regression to these f2t ratios. 
lm.fit <- rlm(f2t_ratio ~ year_id + MEPS_source)
There is a dummy variable for any rows coming from the MEPS dataset.
If there are <= 90 non-NA observations and > 0 observations, we take the median across the f2t ratios.
If there are zero observations we set F2T to 1.

