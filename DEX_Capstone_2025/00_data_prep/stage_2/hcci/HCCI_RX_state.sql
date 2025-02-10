/* 
This code is written to run in HCCI's Enclave.
This was last run on 07/2023 by Max Weil.
*/	

/*
 * TEMP TABLES FOR VARIOUS MAPS
*/
-- Creating temp SQL table for DEX sex-specific NDC map
CREATE LOCAL TEMP TABLE ndc_sex_map
(idx int, ndc varchar, acause varchar, probability float, n int, sex_id int)
ON COMMIT PRESERVE ROWS;

-- Copying DEX sex-specific  NDC map from csv file to SQL table
copy ndc_sex_map from local
'FILEPATH' parser
fcsvparser(header='true') abort on error;

-- Creating temp SQL table for DEX NDC map
CREATE LOCAL TEMP TABLE ndc_map
(idx int, ndc varchar, acause varchar, probability float, n int)
ON COMMIT PRESERVE ROWS;

-- Copying DEX NDC map from csv file to SQL table
copy ndc_map from local
'FILEPATH' parser
fcsvparser(header='true') abort on error;

/*
 * Data Processing, the main goals here are to:
 * 1. Create new columns that we use in our later steps
 * 2. Rename existing columns to DEX standard
 * 3. Group the data up to claim level (from line-level)
 */
WITH /*+ENABLE_WITH_CLAUSE_MATERIALIZATION */ stage_3 AS
(
SELECT
	MIN(rs.yr) AS year_id,
	MIN(rs.mnth) AS mnth_id,
	rs.z_patid AS bene_id,
	rs.z_clmid AS claim_id,
	1 AS n_claim,
	CASE
		WHEN MIN(rs.hnpi) IS NOT NULL THEN MIN(rs.hnpi)
		ELSE MIN(rs.hnpi_be)
	END AS prov_id,
	MIN(rs.fill_dt) AS service_date,
	'RX' as toc,
	MIN(rs.ndc) AS ndc,
	'priv' as pri_payer,
	SUM(rs.calc_allwd) AS tot_pay_amt,
	SUM(rs.amt_net_paid) AS priv_pay_amt,
	SUM(rs.tot_mem_cs) AS oop_pay_amt
FROM HCCI_2.RX_SDDV2 rs
GROUP BY bene_id, claim_id
HAVING year_id = 2021
),

/*
 * Beneficiary Table, which contains demographic info for each beneficary.
 * Some bene_ids have duplicated information, so only using the first rows for each
 * Combination of z_patid, month, year
 */
bene_tbl AS
(
SELECT
	mr.z_patid AS bene_id,
	mr.mnth,
	mr.yr,
	CASE
		WHEN mr.sex='M' THEN 1
		ELSE 2
	END AS sex_id,
	mr.age_band_cd AS age_id,
	mr.mbr_state AS st_resi
FROM (
	SELECT
		ms.z_patid,
		ms.mnth,
		ms.yr,
		ms.sex,
		ms.age_band_cd,
		ms.mbr_state,
		ROW_NUMBER() OVER(PARTITION BY ms.z_patid, ms.mnth, ms.yr) AS n_row
	FROM HCCI_2.MBR_SDDV2 ms
	) mr
WHERE n_row = 1
),

/*
 * Pre-Collapsed Data, has bene demographic info joined on, adds a few constant columns,
 * And converts data to be long on payers (priv/mdcr and oop).
 */
pre_collapse AS
(
SELECT
	stg3.bene_id,
	stg3.n_claim,
	stg3.prov_id,
	stg3.toc,
	stg3.ndc,
	bt.sex_id,
	stg3.pri_payer AS pri_payer,
	'priv' AS payer,
	bt.age_id,
	stg3.year_id,
	'state' AS geo,
	bt.st_resi AS state,
	'HCCI' AS dataset,
	stg3.priv_pay_amt AS pay_amt
FROM stage_3 stg3
	LEFT JOIN bene_tbl bt ON stg3.bene_id=bt.bene_id AND stg3.mnth_id=bt.mnth AND stg3.year_id=bt.yr
WHERE
	bt.sex_id IS NOT NULL AND 
	bt.age_id IS NOT NULL AND 
	state IS NOT NULL
UNION ALL
SELECT
	stg3.bene_id,
	stg3.n_claim,
	stg3.prov_id,
	stg3.toc,
	stg3.ndc,
	bt.sex_id,
	stg3.pri_payer AS pri_payer,
	'oop' AS payer,
	bt.age_id,
	stg3.year_id,
	'state' AS geo,
	bt.st_resi AS state,
	'HCCI' AS dataset,
	stg3.oop_pay_amt AS pay_amt
FROM stage_3 stg3
	LEFT JOIN bene_tbl bt ON stg3.bene_id=bt.bene_id AND stg3.mnth_id=bt.mnth AND stg3.year_id=bt.yr
WHERE
	bt.sex_id IS NOT NULL AND 
	bt.age_id IS NOT NULL AND 
	state IS NOT NULL
),

/* 
 * First part of cause mapping, NDC codes are proportionally assigned to DEX causes using a
 * previously generated map that includes sex_id.
 */
cause_mapped_1 AS
(
SELECT 
	pc.bene_id,
	pc.n_claim,
	pc.prov_id,
	pc.toc,
	pc.sex_id,
	pc.pri_payer,
	pc.payer,
	pc.age_id,
	pc.year_id,
	pc.geo,
	pc.state,
	pc.dataset,
	nsm.acause,
	nsm.probability,
	CASE
		WHEN nsm.acause IS NOT NULL THEN NULL
		ELSE pc.ndc
	END as ndc,
	pc.pay_amt
FROM pre_collapse pc
LEFT JOIN ndc_sex_map nsm ON pc.ndc=nsm.ndc AND pc.sex_id=nsm.sex_id
),

/* 
 * Second part of cause mapping, NDC codes not previously assigned are now assigned
 * to DEX causes using a map that is diaggregated by sex.
 */
cause_mapped_2 AS
(
SELECT 
	cm1.bene_id,
	cm1.n_claim,
	cm1.prov_id,
	cm1.toc,
	cm1.sex_id,
	cm1.pri_payer,
	cm1.payer,
	cm1.age_id,
	cm1.year_id,
	cm1.geo,
	cm1.state,
	cm1.dataset,
	COALESCE(cm1.acause, nm.acause) AS acause_final,
	COALESCE(cm1.probability, nm.probability) AS probability,
	cm1.pay_amt
FROM cause_mapped_1 cm1
LEFT JOIN ndc_map nm ON cm1.ndc=nm.ndc
WHERE acause_final IS NOT NULL
),


/*
 * Pre-Collapsed Data 2, redistributes pay_amt and n based on ndc mapping, drop missing locations
 */
pre_collapse_2 AS
(
SELECT
	cm2.bene_id,
	cm2.n_claim,
	cm2.prov_id,
	cm2.toc,
	cm2.acause_final AS acause,
	cm2.sex_id,
	cm2.pri_payer,
	cm2.payer,
	cm2.age_id,
	cm2.year_id,
	cm2.geo,
	cm2.state AS state,
	cm2.dataset,
	cm2.probability*cm2.pay_amt AS pay_amt,
	cm2.probability AS n
FROM cause_mapped_2 cm2
),

/*
 * Counting beneficiaries per year, month, sex, age, and location.
 * This is used for masking and as a denominator in later metrics.
 */
denom_bene AS
(
SELECT
	COUNT(DISTINCT(pc2.bene_id)) AS n_bene,
	pc2.year_id,
	pc2.sex_id,
	pc2.age_id,
	pc2.state
FROM
	pre_collapse_2 pc2
GROUP BY pc2.year_id, pc2.sex_id, pc2.age_id, pc2.state
HAVING
	pc2.year_id IS NOT NULL
	AND pc2.sex_id IS NOT NULL
	AND pc2.age_id IS NOT NULL
	AND pc2.state IS NOT NULL
),

/*
 * Counting providers per year and location.
 * This is used for masking.
 */
denom_prov AS
(
SELECT 
	COUNT(DISTINCT(pc2.prov_id)) AS n_prov,
	pc2.year_id,
	pc2.state
FROM
	pre_collapse_2 pc2
GROUP BY pc2.year_id, pc2.state
HAVING
	pc2.year_id IS NOT NULL
	AND pc2.state IS NOT NULL
),

/*
 * Collapse, data is grouped into collapse categories and metrics are calculated.
 * Masking values are also calculated in this step, using information from above.
 */
collapse AS
(
SELECT
	pc2.toc,
	pc2.acause,
	pc2.sex_id,
	pc2.pri_payer,
	pc2.payer,
	pc2.age_id,
	pc2.year_id,
	pc2.geo,
	pc2.state,
	pc2.dataset,
	AVG(pc2.pay_amt) AS spend_per_encounter,
	CASE
		WHEN SUM(pc2.n)<=1 THEN 2.6*AVG(pc2.pay_amt)
		ELSE STDDEV(pc2.pay_amt)/SQRT(SUM(pc2.n))
	END AS se_spend_per_encounter,
	SUM(pc2.n)/MIN(db.n_bene) AS encounters_per_person,
	SUM(pc2.n) AS n,
	MIN(db.n_bene) AS n_bene,
	SUM(SUM(pc2.n_claim)) OVER (PARTITION BY pc2.toc, pc2.sex_id, pc2.acause, pc2.age_id, pc2.year_id, pc2.state) AS n_claim,
	MIN(dp.n_prov) AS n_prov
FROM pre_collapse_2 pc2
LEFT JOIN denom_bene db ON pc2.year_id=db.year_id AND pc2.sex_id=db.sex_id AND pc2.age_id=db.age_id AND pc2.state=db.state
LEFT JOIN denom_prov dp ON pc2.year_id=dp.year_id AND pc2.state=dp.state
GROUP BY pc2.toc, pc2.sex_id, pc2.acause, pc2.pri_payer, pc2.payer, pc2.age_id, pc2.year_id, pc2.geo, pc2.state, pc2.dataset
)

SELECT * FROM collapse
