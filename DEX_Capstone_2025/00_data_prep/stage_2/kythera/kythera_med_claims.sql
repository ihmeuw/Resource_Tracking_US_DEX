/* 
This code is written to run on Kythera's Wayfinder plaform.
This was last run on 08/2024 by Max Weil.
*/

CREATE TABLE hive_metastore.ihme.mx_claimline_rev AS

  -- Table with mx_claimline columns of relevance for DEX team
  WITH mx_claimline AS (
  SELECT DISTINCT
    COALESCE(YEAR(statement_from_date), YEAR(admission_date)) AS year_adm,
    YEAR(statement_to_date) AS year_dchg,
    patient_id AS bene_id,
    vendor_claim_number AS claim_id,
    COALESCE(YEAR(statement_from_date), YEAR(admission_date)) - patient_birth_year AS age,
    patient_gender AS patient_gender,
    patient_origin_zipcode AS zip_5_resi,
    patient_state AS st_resi,
    facility_zipcode AS zip_5_serv,
    facility_state AS st_serv,
    diagnosis_1_codeset AS code_system,
    diagnosis_code_1 AS dx1,
    diagnosis_code_2 AS dx2,
    diagnosis_code_3 AS dx3,
    diagnosis_code_4 AS dx4,
    diagnosis_code_5 AS dx5,
    diagnosis_code_6 AS dx6,
    diagnosis_code_7 AS dx7,
    diagnosis_code_8 AS dx8,
    statement_to_date AS discharge_date,
    statement_from_date AS service_date,
    claim_type AS fac_prof_ind,
    bill_type AS bill_type_code,
    revenue_code AS revenue_code,
    place_of_service_code AS place_of_service_code,
    DATEDIFF(statement_to_date, statement_from_date) AS los,
    primary_payer_pay_type AS pri_payer,
    primary_payer_plan_type AS pri_payer_plan,
    secondary_payer_pay_type AS sec_payer,
    secondary_payer_plan_type AS sec_payer_plan,
    vendor_payer_id AS pri_payer_id,
    secondary_payer_id AS sec_payer_id
  FROM
    ihme.asset.mx_claimline
  WHERE substring(partition_key,0,4) <= '2019'
  GROUP BY ALL
    )  ,

  -- Payment information from the remit_payments table, aggregated to contain a single payment amount for each claim/payer id combination
  payer_payments AS (
  SELECT
    MAX(claim_payment_amount) AS pay_amt,
    MAX(total_claim_charge_amount) AS chg_amt,
    MAX(patient_amount_paid) AS oop_pay_amt,
    MAX(patient_responsibility_amount) AS oop_chg_amt,
    vendor_claim_number AS claim_id,
    payer_id
  FROM
    ihme.asset.mx_remit_payments
  GROUP BY vendor_claim_number, payer_id
  ),

  -- Race/ethnicity table, aggregated to have a single race/ethnicity per patient_id
  race_eth AS (
  SELECT
    re3.patient_id,
    re3.race,
    re3.ethnicity
  FROM 
    (
      SELECT
        re2.patient_id,
        re2.race,
        re2.ethnicity,
        ROW_NUMBER() OVER (PARTITION BY re2.patient_id ORDER BY COUNT(re2.race) DESC) AS n_race,
        ROW_NUMBER() OVER (PARTITION BY re2.patient_id ORDER BY COUNT(re2.ethnicity) DESC) AS n_eth
      FROM
        (
          SELECT
          re1.patient_id,
          CASE
            WHEN re1.race = 'UNKNOWN' THEN NULL
            ELSE re1.race
          END as race,
          CASE
            WHEN re1.ethnicity IN ('Declined/Unknown', 'Unknown') THEN NULL
            WHEN re1.ethnicity IN ('Hispanic', 'Hispanic or Latino') THEN 'Hispanic'
            WHEN re1.ethnicity IN ('Non-Hispanic', 'Not Hispanic or Latino') THEN 'Non-Hispanic'
            ELSE re1.ethnicity
          END as ethnicity
        FROM
          ihme.asset_sandbox.patient_race_ethnicity re1
        WHERE
          (re1.race IS NOT NULL AND re1.race != 'UNKNOWN') 
          OR
          ((re1.race IS NULL OR re1.race = 'UNKNOWN') AND (re1.ethnicity IS NOT NULL AND re1.ethnicity NOT IN ('Declined/Unknown', 'Unknown')))
        ) re2
      GROUP BY
        re2.patient_id,
        re2.race,
        re2.ethnicity
    ) re3
  WHERE
    re3.n_race = 1
    AND re3.n_eth = 1
  )

  -- This is the final table to be extracted for use in the DEX project
  SELECT
    mx.year_adm,
    mx.year_dchg,
    mx.bene_id,
    mx.claim_id,
    mx.age,
    mx.patient_gender,
    re.race,
    re.ethnicity,
    mx.zip_5_resi,
    mx.st_resi,
    mx.zip_5_serv,
    mx.st_serv,
    mx.code_system,
    mx.dx1,
    mx.dx2,
    mx.dx3,
    mx.dx4,
    mx.dx5,
    mx.dx6,
    mx.dx7,
    mx.dx8,
    mx.discharge_date,
    mx.service_date,
    mx.fac_prof_ind,
    mx.bill_type_code,
    mx.revenue_code,
    mx.place_of_service_code,
    mx.los,
    mx.pri_payer,
    mx.pri_payer_plan,
    mx.sec_payer,
    mx.sec_payer_plan,
    p1.pay_amt AS pri_pay_amt,
    p1.chg_amt AS tot_chg_amt,
    p1.oop_pay_amt AS oop_pay_amt,
    p1.oop_chg_amt AS oop_chg_amt,
    p2.pay_amt AS sec_pay_amt
  FROM
    mx_claimline mx
    LEFT JOIN payer_payments p1 ON mx.claim_id=p1.claim_id AND mx.pri_payer_id=p1.payer_id
    LEFT JOIN payer_payments p2 ON mx.claim_id=p2.claim_id AND mx.sec_payer_id=p2.payer_id
    LEFT JOIN race_eth re ON mx.bene_id=re.patient_id