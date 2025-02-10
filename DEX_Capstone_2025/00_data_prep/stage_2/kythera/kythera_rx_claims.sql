/* 
This code is written to run on Kythera's Wayfinder plaform.
This was last run on 08/2024 by Max Weil.
*/

CREATE TABLE hive_metastore.ihme.rx_claims AS

-- Table with rx_claims columns of relevance for DEX team
WITH rx_claims AS (
SELECT DISTINCT
  YEAR(date_of_service) AS year_id,
  patient_id AS bene_id,
  vendor_claim_number AS claim_id,
  YEAR(date_of_service) - patient_birth_year AS age,
  patient_gender AS patient_gender,
  patient_zip3 AS zip_3_resi,
  patient_state AS st_resi,
  pharmacy_location_postal_code AS zip_5_serv,
  national_drug_code AS ndc,
  days_supply AS days_supply,
  date_of_service AS service_date,
  payer_pay_type AS pri_payer,
  payer_plan_type AS pri_payer_plan,
  patient_pay_amount AS oop_pay_amt,
  patient_paid_amount_submitted AS oop_chg_amt,
  total_amount_paid AS pri_pay_amt,
  gross_amount_due_submitted AS tot_chg_amt,
  prescription_or_service_reference_number,
  transaction_code,
  response_code
FROM
  ihme.asset.rx_claims
WHERE
  YEAR(date_of_service) <= 2019
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
  rx.year_id,
  rx.bene_id,
  rx.claim_id,
  rx.age,
  rx.patient_gender,
  re.race,
  re.ethnicity,
  rx.zip_3_resi,
  rx.st_resi,
  rx.zip_5_serv,
  rx.ndc,
  rx.days_supply,
  rx.service_date,
  rx.pri_payer,
  rx.pri_payer_plan,
  rx.oop_pay_amt AS oop_pay_amt,
  rx.oop_chg_amt AS oop_chg_amt,
  rx.pri_pay_amt AS pri_pay_amt,
  rx.tot_chg_amt AS tot_chg_amt,
  rx.prescription_or_service_reference_number,
  rx.transaction_code,
  rx.response_code
FROM
  rx_claims rx
  LEFT JOIN race_eth re ON rx.bene_id=re.patient_id
