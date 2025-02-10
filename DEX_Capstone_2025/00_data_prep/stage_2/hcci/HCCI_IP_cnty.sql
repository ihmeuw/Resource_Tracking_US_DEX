/* 
This code is written to run in HCCI's Enclave.
This was last run on 07/2023 by Max Weil.
*/

/*
 * TEMP TABLES FOR VARIOUS MAPS
*/
-- Creating temp SQL table for DEX causemap
CREATE LOCAL TEMP TABLE cause_map
(idx int, code_system varchar, icd_code varchar, icd_name varchar, acause varchar, wrong_code_system varchar)
ON COMMIT PRESERVE ROWS;

-- Copying DEX causemap from csv file to SQL table
copy cause_map from local
'FILEPATH' parser
fcsvparser(header='true') abort on error;

-- Creating temp SQL table for DEX zipmap
CREATE LOCAL TEMP TABLE zip_map
(idx int, zip5_resi varchar, mcnty_resi varchar, residence_wt float)
ON COMMIT PRESERVE ROWS;

-- Copying DEX zipmap from csv file to SQL table
copy zip_map from local
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
	MIN(is2.yr) AS year_id,
	MIN(is2.mnth) AS mnth_id,
	is2.z_patid AS bene_id,
	is2.z_clmid AS claim_id,
	CASE
		WHEN MIN(is2.hnpi) IS NOT NULL THEN MIN(is2.hnpi)
		ELSE MIN(is2.hnpi_be)
	END AS prov_id,
	MIN(is2.prov_zip_5_cd) AS zip_5_serv,
	MIN(is2.prov_state) AS st_serv,
	MIN(is2.fst_admtdt) AS service_date,
	MAX(is2.lst_dischdt) AS discharge_date,
	'IP' as toc,
	CASE
		WHEN (MIN(is2.diag_icd9_cm1) IS NOT NULL AND MIN(is2.diag_icd10_cm1) IS NULL) OR (MIN(is2.diag_icd9_cm1) IS NOT NULL AND MAX(is2.lst_dischdt) < '2015-10-1') THEN 'icd9'
		WHEN (MIN(is2.diag_icd9_cm1) IS NULL AND MIN(is2.diag_icd10_cm1) IS NOT NULL) OR (MIN(is2.diag_icd10_cm1) IS NOT NULL AND MAX(is2.lst_dischdt) >= '2015-10-1') THEN 'icd10'
		ELSE NULL
	END AS code_system,
	MIN(is2.diag_icd9_cm1) AS diag_icd9_cm1,
	MIN(is2.diag_icd9_cm2) AS diag_icd9_cm2,
	MIN(is2.diag_icd9_cm3) AS diag_icd9_cm3,
	MIN(is2.diag_icd10_cm1) AS diag_icd10_cm1,
	MIN(is2.diag_icd10_cm2) AS diag_icd10_cm2,
	MIN(is2.diag_icd10_cm3) AS diag_icd10_cm3,
	MIN(is2.diag_icd10_cm4) AS diag_icd10_cm4,
	MIN(is2.diag_icd10_cm5) AS diag_icd10_cm5,
	MIN(is2.diag_icd10_cm6) AS diag_icd10_cm6,
	MIN(is2.diag_icd10_cm7) AS diag_icd10_cm7,
	MIN(is2.diag_icd10_cm8) AS diag_icd10_cm8,
	MIN(is2.diag_icd10_cm9) AS diag_icd10_cm9,
	MIN(is2.diag_icd10_cm10) AS diag_icd10_cm10,
	CASE
		WHEN MAX(is2.primary_cvg_ind) = 1 THEN 'priv'
		ELSE 'unk'
	END AS pri_payer,
	SUM(is2.calc_allwd) AS tot_pay_amt,
	SUM(is2.amt_net_paid) AS priv_pay_amt,
	SUM(is2.tot_mem_cs) AS oop_pay_amt
FROM HCCI_2.INP_SDDV2 is2
GROUP BY bene_id, claim_id
),

/* 
 * Cause Mapping, DEX causes are mapped to each ICD code in the data
 */
cause_mapped AS
(
SELECT 
	stg3.year_id,
	stg3.mnth_id,
	stg3.bene_id,
	stg3.claim_id,
	stg3.prov_id,
	stg3.zip_5_serv,
	stg3.st_serv,
	stg3.service_date,
	stg3.discharge_date,
	stg3.toc,
	stg3.code_system,
	CASE
		WHEN icd10_cm1.acause IS NOT NULL THEN icd10_cm1.acause
		WHEN icd9_cm1.acause IS NOT NULL THEN icd9_cm1.acause
		ELSE NULL
	END AS dx_1,
	CASE
		WHEN icd10_cm2.acause IS NOT NULL THEN icd10_cm2.acause
		WHEN icd9_cm2.acause IS NOT NULL THEN icd9_cm2.acause
		ELSE NULL
	END AS dx_2,
	CASE
		WHEN icd10_cm3.acause IS NOT NULL THEN icd10_cm3.acause
		WHEN icd9_cm3.acause IS NOT NULL THEN icd9_cm3.acause
		ELSE NULL
	END AS dx_3,
	icd10_cm4.acause AS dx_4,
	icd10_cm5.acause AS dx_5,
	icd10_cm6.acause AS dx_6,
	icd10_cm7.acause AS dx_7,
	icd10_cm8.acause AS dx_8,
	icd10_cm9.acause AS dx_9,
	icd10_cm10.acause AS dx_10,
	stg3.pri_payer,
	stg3.tot_pay_amt,
	stg3.priv_pay_amt,
	stg3.oop_pay_amt
FROM stage_3 stg3
LEFT JOIN cause_map icd9_cm1 ON stg3.diag_icd9_cm1=icd9_cm1.icd_code AND stg3.code_system=icd9_cm1.code_system
LEFT JOIN cause_map icd9_cm2 ON stg3.diag_icd9_cm2=icd9_cm2.icd_code AND stg3.code_system=icd9_cm2.code_system
LEFT JOIN cause_map icd9_cm3 ON stg3.diag_icd9_cm3=icd9_cm3.icd_code AND stg3.code_system=icd9_cm3.code_system
LEFT JOIN cause_map icd10_cm1 ON stg3.diag_icd10_cm1=icd10_cm1.icd_code AND stg3.code_system=icd10_cm1.code_system
LEFT JOIN cause_map icd10_cm2 ON stg3.diag_icd10_cm2=icd10_cm2.icd_code AND stg3.code_system=icd10_cm2.code_system
LEFT JOIN cause_map icd10_cm3 ON stg3.diag_icd10_cm3=icd10_cm3.icd_code AND stg3.code_system=icd10_cm3.code_system
LEFT JOIN cause_map icd10_cm4 ON stg3.diag_icd10_cm4=icd10_cm4.icd_code AND stg3.code_system=icd10_cm4.code_system
LEFT JOIN cause_map icd10_cm5 ON stg3.diag_icd10_cm5=icd10_cm5.icd_code AND stg3.code_system=icd10_cm5.code_system
LEFT JOIN cause_map icd10_cm6 ON stg3.diag_icd10_cm6=icd10_cm6.icd_code AND stg3.code_system=icd10_cm6.code_system
LEFT JOIN cause_map icd10_cm7 ON stg3.diag_icd10_cm7=icd10_cm7.icd_code AND stg3.code_system=icd10_cm7.code_system
LEFT JOIN cause_map icd10_cm8 ON stg3.diag_icd10_cm8=icd10_cm8.icd_code AND stg3.code_system=icd10_cm8.code_system
LEFT JOIN cause_map icd10_cm9 ON stg3.diag_icd10_cm9=icd10_cm9.icd_code AND stg3.code_system=icd10_cm9.code_system
LEFT JOIN cause_map icd10_cm10 ON stg3.diag_icd10_cm10=icd10_cm10.icd_code AND stg3.code_system=icd10_cm10.code_system
),

/*
 * Claims to Encounters (C2E), claims are aggregated up to the DEX encounter level
 * DEX encounters are defined by claims with the same date of service (or admission date),
 * primary DEX cause, and beneficiary
 */
c2e AS
(
SELECT
	MIN(cm.year_id) AS year_id,
	MIN(cm.mnth_id) AS mnth_id,
	cm.bene_id,
	COUNT(DISTINCT(cm.claim_id)) AS n_claim,
	MIN(cm.prov_id) AS prov_id,
	MIN(cm.zip_5_serv) AS zip_5_serv,
	MIN(cm.st_serv) AS st_serv,
	cm.service_date,
	MIN(cm.toc) AS toc,
	CASE 
		WHEN MAX(cm.discharge_date)-cm.service_date<=0 THEN 1
		ELSE MAX(cm.discharge_date)-cm.service_date
	END AS los,
	MIN(cm.code_system) AS code_system,
	cm.dx_1,
	MIN(cm.dx_2) AS dx_2,
	MIN(cm.dx_3) AS dx_3,
	MIN(cm.dx_4) AS dx_4,
	MIN(cm.dx_5) AS dx_5,
	MIN(cm.dx_6) AS dx_6,
	MIN(cm.dx_7) AS dx_7,
	MIN(cm.dx_8) AS dx_8,
	MIN(cm.dx_9) AS dx_9,
	MIN(cm.dx_10) AS dx_10,
	MIN(cm.pri_payer) AS pri_payer,
	SUM(cm.tot_pay_amt) AS tot_pay_amt,
	SUM(cm.priv_pay_amt) AS priv_pay_amt,
	SUM(cm.oop_pay_amt) AS oop_pay_amt
FROM cause_mapped cm
GROUP BY cm.bene_id, cm.service_date, cm.dx_1
),

/*
 * Primary Cause Mapping, a primary cause is chosen for each encounter. The following logic is used:
 * 1. Is DX_1 null, _gc (garbage code), _NEC (not elsewhere classified) or other (grouped category)?
 * 		If yes, proceed to step 2. If no, assign as primary cause.
 * 2. Repeat step 1 for each DX code. If no DX is assigne as primary, proceed to step 3.
 * 3. Assign the first non-_gc, non-null DX code as primary cause.
 * 		If no DX codes meet this requirement, assign _gc as primary cause.
 */
pri_cause AS
(
SELECT
	*,
	CASE
		WHEN dx_1 != '_gc' AND dx_1 NOT LIKE '%NEC%' AND dx_1 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_1 IS NOT NULL THEN dx_1
		WHEN dx_2 != '_gc' AND dx_2 NOT LIKE '%NEC%' AND dx_2 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_2 IS NOT NULL THEN dx_2
		WHEN dx_3 != '_gc' AND dx_3 NOT LIKE '%NEC%' AND dx_3 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_3 IS NOT NULL THEN dx_3
		WHEN dx_4 != '_gc' AND dx_4 NOT LIKE '%NEC%' AND dx_4 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_4 IS NOT NULL THEN dx_4
		WHEN dx_5 != '_gc' AND dx_5 NOT LIKE '%NEC%' AND dx_5 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_5 IS NOT NULL THEN dx_5
		WHEN dx_6 != '_gc' AND dx_6 NOT LIKE '%NEC%' AND dx_6 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_6 IS NOT NULL THEN dx_6
		WHEN dx_7 != '_gc' AND dx_7 NOT LIKE '%NEC%' AND dx_7 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_7 IS NOT NULL THEN dx_7
		WHEN dx_8 != '_gc' AND dx_8 NOT LIKE '%NEC%' AND dx_8 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_8 IS NOT NULL THEN dx_8
		WHEN dx_9 != '_gc' AND dx_9 NOT LIKE '%NEC%' AND dx_9 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_9 IS NOT NULL THEN dx_9
		WHEN dx_10 != '_gc' AND dx_10 NOT LIKE '%NEC%' AND dx_10 NOT IN ('cvd_other', 'digest_other', 'maternal_other', 'msk_other', 'neo_other_cancer', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other') AND dx_10 IS NOT NULL THEN dx_10
		WHEN dx_1 != '_gc' AND dx_1 IS NOT NULL THEN dx_1
		WHEN dx_2 != '_gc' AND dx_2 IS NOT NULL THEN dx_2
		WHEN dx_3 != '_gc' AND dx_3 IS NOT NULL THEN dx_3
		WHEN dx_4 != '_gc' AND dx_4 IS NOT NULL THEN dx_4
		WHEN dx_5 != '_gc' AND dx_5 IS NOT NULL THEN dx_5
		WHEN dx_6 != '_gc' AND dx_6 IS NOT NULL THEN dx_6
		WHEN dx_7 != '_gc' AND dx_7 IS NOT NULL THEN dx_7
		WHEN dx_8 != '_gc' AND dx_8 IS NOT NULL THEN dx_8
		WHEN dx_9 != '_gc' AND dx_9 IS NOT NULL THEN dx_9
		WHEN dx_10 != '_gc' AND dx_10 IS NOT NULL THEN dx_10
		ELSE '_gc'
	END AS primary_dx
FROM c2e
),

/*
 * Beneficiary Table, which contains demographic info for each beneficary.
 * Some bene_ids have duplicated information, so only using the first rows for each
 * Combination of z_patid, month, year, sex, age_band, zip_5, and state.
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
	mr.mbr_zip_5_cd AS zip_5_resi,
	mr.mbr_state AS st_resi
FROM (
	SELECT
		ms.z_patid,
		ms.mnth,
		ms.yr,
		ms.sex,
		ms.age_band_cd,
		ms.mbr_zip_5_cd,
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
	pri.bene_id,
	pri.n_claim,
	pri.prov_id,
	pri.toc,
	bt.sex_id,
	pri.primary_dx AS acause,
	CASE
		WHEN pri.pri_payer='unk' AND bt.age_id='07' THEN 'mdcr'
		ELSE pri.pri_payer
	END AS pri_payer,
	'priv' AS payer,
	bt.age_id,
	pri.year_id,
	'county' AS geo,
	CASE
		WHEN bt.zip_5_resi IS NULL THEN pri.zip_5_serv
		ELSE bt.zip_5_resi
	END AS zip5,
	CASE
		WHEN bt.st_resi IS NULL THEN pri.st_serv
		ELSE bt.st_resi
	END AS state,
	'HCCI' AS dataset,
	pri.los,
	pri.priv_pay_amt AS pay_amt
FROM pri_cause pri
	LEFT JOIN bene_tbl bt ON pri.bene_id=bt.bene_id AND pri.mnth_id=bt.mnth AND pri.year_id=bt.yr
WHERE
	bt.sex_id IS NOT NULL AND 
	bt.age_id IS NOT NULL AND 
	zip5 IS NOT NULL
UNION ALL
SELECT
	pri.bene_id,
	pri.n_claim,
	pri.prov_id,
	pri.toc,
	bt.sex_id,
	pri.primary_dx AS acause,
	CASE
		WHEN pri.pri_payer='unk' AND bt.age_id='07' THEN 'mdcr'
		ELSE pri.pri_payer
	END AS pri_payer,
	'oop' AS payer,
	bt.age_id,
	pri.year_id,
	'county' AS geo,
	CASE
		WHEN bt.zip_5_resi IS NULL THEN pri.zip_5_serv
		ELSE bt.zip_5_resi
	END AS zip5,
	CASE
		WHEN bt.st_resi IS NULL THEN pri.st_serv
		ELSE bt.st_resi
	END AS state,
	'HCCI' AS dataset,
	pri.los,
	pri.oop_pay_amt AS pay_amt
FROM pri_cause pri
	LEFT JOIN bene_tbl bt ON pri.bene_id=bt.bene_id AND pri.mnth_id=bt.mnth AND pri.year_id=bt.yr
WHERE
	bt.sex_id IS NOT NULL AND 
	bt.age_id IS NOT NULL AND 
	zip5 IS NOT NULL
),

/*
 * Zip Mapping Pre-Collapse Data, using zipmap to proportionally reassign spending
 * And encounter counts to county level.
 */
zip_mapped_pre_collapse AS
(
SELECT
	pc.bene_id,
	pc.n_claim,
	pc.prov_id,
	pc.toc,
	pc.sex_id,
	pc.acause,
	pc.pri_payer,
	pc.payer,
	pc.age_id,
	pc.year_id,
	pc.geo,
	zm.mcnty_resi AS location,
	pc.state AS state,
	pc.dataset,
	zm.residence_wt*pc.pay_amt AS pay_amt,
	zm.residence_wt*pc.los AS los,
	zm.residence_wt AS n
FROM pre_collapse pc
LEFT JOIN zip_map zm ON pc.zip5=zm.zip5_resi
WHERE location IS NOT NULL
),

/*
 * Counting beneficiaries per year, month, sex, age, and location.
 * This is used for masking and as a denominator in later metrics.
 */
denom_bene AS
(
SELECT
	COUNT(DISTINCT(zmpc.bene_id)) AS n_bene,
	zmpc.year_id,
	zmpc.sex_id,
	zmpc.age_id,
	zmpc.location,
	zmpc.state
FROM
	zip_mapped_pre_collapse zmpc
GROUP BY zmpc.year_id, zmpc.sex_id, zmpc.age_id, zmpc.location, zmpc.state
HAVING
	zmpc.year_id IS NOT NULL
	AND zmpc.sex_id IS NOT NULL
	AND zmpc.age_id IS NOT NULL
	AND zmpc.location IS NOT NULL
	AND zmpc.state IS NOT NULL
),

/*
 * Counting providers per year and location.
 * This is used for masking.
 */
denom_prov AS
(
SELECT 
	COUNT(DISTINCT(zmpc.prov_id)) AS n_prov,
	zmpc.year_id,
	zmpc.location,
	zmpc.state
FROM
	zip_mapped_pre_collapse zmpc
GROUP BY zmpc.year_id, zmpc.location, zmpc.state
HAVING
	zmpc.year_id IS NOT NULL
	AND zmpc.location IS NOT NULL
	AND zmpc.state IS NOT NULL
),

/*
 * Collapse, data is grouped into collapse categories and metrics are calculated.
 * Masking values are also calculated in this step, using information from above.
 */
collapse AS
(
SELECT
	zmpc.toc,
	zmpc.sex_id,
	zmpc.acause,
	zmpc.pri_payer,
	zmpc.payer,
	zmpc.age_id,
	zmpc.year_id,
	zmpc.geo,
	zmpc.location,
	zmpc.state,
	zmpc.dataset,
	AVG(zmpc.pay_amt) AS spend_per_encounter,
	CASE
		WHEN SUM(zmpc.n)<=1 THEN 2.6*AVG(zmpc.pay_amt)
		ELSE STDDEV(zmpc.pay_amt)/SQRT(SUM(zmpc.n))
	END AS se_spend_per_encounter,
	CASE
		WHEN AVG(zmpc.los) = 0 THEN 0
		ELSE AVG(zmpc.pay_amt)/AVG(zmpc.los)
	END AS spend_per_day,
	CASE
		WHEN SUM(zmpc.n)<=1 THEN 2.6*AVG(zmpc.pay_amt)
		ELSE STDDEV(zmpc.pay_amt/zmpc.los)/SQRT(SUM(zmpc.n))
	END AS se_spend_per_day,
	AVG(zmpc.los) AS days_per_encounter,
	CASE
		WHEN SUM(zmpc.n)<=1 THEN 2.6*AVG(zmpc.los)
		ELSE STDDEV(zmpc.los)/SQRT(SUM(zmpc.n))
	END AS se_days_per_encounter,
	SUM(zmpc.n)/MIN(db.n_bene) AS encounters_per_person,
	SUM(zmpc.n) AS n,
	MIN(db.n_bene) AS n_bene,
	SUM(SUM(zmpc.n_claim)) OVER (PARTITION BY zmpc.toc, zmpc.sex_id, zmpc.acause, zmpc.age_id, zmpc.year_id, zmpc.state, zmpc.location) AS n_claim,
	MIN(dp.n_prov) AS n_prov
FROM zip_mapped_pre_collapse zmpc
LEFT JOIN denom_bene db ON zmpc.year_id=db.year_id AND zmpc.sex_id=db.sex_id AND zmpc.age_id=db.age_id AND zmpc.location=db.location AND zmpc.state=db.state
LEFT JOIN denom_prov dp ON zmpc.year_id=dp.year_id AND zmpc.location=dp.location AND zmpc.state=dp.state
GROUP BY zmpc.toc, zmpc.sex_id, zmpc.acause, zmpc.pri_payer, zmpc.payer, zmpc.age_id, zmpc.year_id, zmpc.geo, zmpc.location, zmpc.state, zmpc.dataset
)

SELECT * FROM collapse
