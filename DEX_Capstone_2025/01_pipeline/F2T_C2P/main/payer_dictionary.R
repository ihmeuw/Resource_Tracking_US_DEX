payer_nums <- list(
  'mdcr' = 1,
  'priv' = 2,
  'mdcd' = 3,
  'oop' = 4,
  'oth_not_priv'=	5,
  'oth_not_mdcr'=	6,
  'oth_not_mdcd'=	7,
  'oth_not_oop'=	8,
  'oth_not_mdcd_mdcr'=	9,
  'oth_not_mdcd_oop'=	10,
  'oth_not_mdcd_priv'=	11,
  'oth_not_mdcr_oop'=	12,
  'oth_not_mdcr_priv'=	13,
  'oth_not_oop_priv'=	14,
  'OTH' =  15,
  'oth_not_mdcd_mdcr_priv'=	16,
  'oth_not_mdcd_oop_priv'=	17,
  'oth_not_mdcr_oop_priv'=	18,
  'oth_not_mdcd_mdcr_oop_priv'=	19,
  'NC' = 20,
  'UNK' = 21) 



payer_nums <-  data.table(cbind(payer_nums), keep.rownames = TRUE)
colnames(payer_nums) <- c("payer", "payer_num")
payer_nums$payer_num <- as.numeric(payer_nums$payer_num)
payer_nums$payer <- as.character(payer_nums$payer)

payer_names <- list(
  'Medicare' = 'MDCR',
  'Private Insurance' = 'PRIV',
  'Medicaid' = 'MDCD',
  'Out of Pocket' = 'OOP',
  'Other – not Medicare, Medicaid, OOP (could be private, could be other)' = 'OTH',
  'No Charge' = 'NC',
  'Unknown' = 'UNK')
payer_names <-  data.table(cbind(payer_names), keep.rownames = TRUE)

colnames(payer_names) <- c("payer_name", "payer")

