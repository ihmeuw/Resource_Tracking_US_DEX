## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Constants for the CAUSEMAP step
## ==================================================

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
import pandas as pd

## --------------------------------------------------------------------------------------------
## Define constants
## --------------------------------------------------------------------------------------------

## possible NA vals
na_vals = [-1, "-1", "None", "NA", "<NA>", "UNK", "UNKNOWN", "unknown", ""]

## expected columns for each dataset
expected_cols = {
    "KYTHERA" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "claim_id",
        "code_system",
        "discharge_date",
        "dual_ind",
        "dx",
        "dx_level",
        "fac_prof_ind",
        "los",
        "mc_ind",
        "mcnty_resi",
        "mcnty_serv",
        "mdcd_chg_amt",
        "mdcd_pay_amt",
        "mdcr_chg_amt",
        "mdcr_pay_amt",
        "oop_chg_amt",
        "oop_pay_amt",
        "payer_2",
        "pri_payer",
        "priv_chg_amt",
        "priv_pay_amt",
        "service_date",
        "sex_id",
        "st_resi",
        "st_serv",
        "toc",
        "tot_chg_amt",
        "tot_pay_amt",
        "year_id",
        "year_dchg",
        "zip_3_resi",
        "zip_5_resi", 
        "days_supply"],
    "MDCD" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "claim_id",
        "code_system",
        "discharge_date",
        "CLM_FROM_DT",
        "dual_ind",
        "dx",
        "dx_level",
        "fac_prof_ind",
        "los",
        "mc_ind",
        "mc_status",
        "mcnty_resi",
        "mdcd_chg_amt",
        "mdcd_pay_amt",
        "mdcr_chg_amt",
        "mdcr_pay_amt",
        "MSIS_ID",
        "oop_chg_amt",
        "oop_pay_amt",
        "payer_2",
        "payer_3",
        "pri_payer",
        "priv_chg_amt",
        "priv_pay_amt",
        "service_date",
        "sex_id",
        "st_resi",
        "toc",
        "tot_chg_amt",
        "tot_pay_amt",
        "year_id", 
        "year_dchg", 
        "zip_5_resi", 
        "BRND_GNRC_CD", "DOSAGE_FORM_CD_line", "ndc_qty", "days_supply"],
    "MDCR" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "claim_id",
        "code_system",
        "discharge_date",
        "CLM_FROM_DT",
        "dx",
        "dx_level",
        "ENHANCED_FIVE_PERCENT_FLAG",
        "CLM_HHA_TOT_VISIT_CNT",
        "los",
        "mc_ind",
        "dual_ind",
        "mcnty_resi",
        "mdcd_chg_amt",
        "mdcd_pay_amt",
        "mdcr_chg_amt",
        "mdcr_pay_amt",
        "oop_chg_amt",
        "oop_pay_amt",
        "payer_2",
        "payer_3",
        "pri_payer",
        "priv_chg_amt",
        "priv_pay_amt",
        "service_date",
        "sex_id",
        "st_resi",
        "st_serv",
        "toc",
        "tot_chg_amt",
        "tot_pay_amt",
        "year_id", 
        "year_dchg", 
        "zip_5_resi", 
        "BRND_GNRC_CD", "ndc_qty", "days_supply"],
    "MSCAN" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "claim_id",
        "code_system",
        "discharge_date",
        "dx",
        "dx_level",
        "fac_prof_ind",
        "los",
        "mcnty_resi",
        "mcnty_serv",
        "mdcd_pay_amt",
        "mdcr_pay_amt",
        "oop_pay_amt",
        "pri_payer",
        "priv_pay_amt",
        "service_date",
        "sex_id",
        "st_resi",
        "st_serv",
        "toc",
        "tot_pay_amt",
        "year_id", 
        "year_dchg",
        "days_supply"],
    "NEDS" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "code_system",
        "dx",
        "dx_level",
        "encounter_id",
        "mcnty_resi",
        "mcnty_serv",
        "mdcd_chg_amt",
        "mdcr_chg_amt",
        "oop_chg_amt",
        "payer_2",
        "pri_payer",
        "priv_chg_amt",
        "sex_id",
        "st_resi",
        "st_serv",
        "survey_wt",
        "toc",
        "tot_chg_amt",
        "year_id", 
        "year_dchg"],
    "NIS" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "code_system",
        "dx",
        "dx_level",
        "encounter_id",
        "los",
        "mcnty_resi",
        "mcnty_serv",
        "mdcd_chg_amt",
        "mdcr_chg_amt",
        "oop_chg_amt",
        "payer_2",
        "pri_payer",
        "priv_chg_amt",
        "sex_id",
        "st_resi",
        "st_serv",
        "survey_wt",
        "toc",
        "tot_chg_amt",
        "year_id", 
        "year_dchg"],
    "SEDD" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "code_system",
        "dx",
        "dx_level",
        "encounter_id",
        "los",
        "mcnty_resi",
        "mcnty_serv",
        "mdcd_chg_amt",
        "mdcr_chg_amt",
        "oop_chg_amt",
        "payer_2",
        "pri_payer",
        "priv_chg_amt",
        "sex_id",
        "st_resi",
        "st_serv",
        "toc",
        "tot_chg_amt",
        "year_id", 
        "year_dchg",
        "zip_3_resi",
        "zip_5_resi"],
    "SIDS" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "code_system",
        "dx",
        "dx_level",
        "encounter_id",
        "los",
        "mcnty_resi",
        "mcnty_serv",
        "mdcd_chg_amt",
        "mdcr_chg_amt",
        "oop_chg_amt",
        "payer_2",
        "pri_payer",
        "priv_chg_amt",
        "sex_id",
        "st_resi",
        "st_serv",
        "toc",
        "tot_chg_amt",
        "year_id",
        "year_dchg", 
        "zip_3_resi",
        "zip_5_resi"],
    "CHIA_MDCR" : [
        "age_group_id",
        "age_group_years_start",
        "bene_id",
        "claim_id",
        "code_system",
        "discharge_date",
        "CLM_FROM_DT",
        "dx",
        "dx_level",
        "ENHANCED_FIVE_PERCENT_FLAG",
        "CLM_HHA_TOT_VISIT_CNT",
        "los",
        "mc_ind",
        "dual_ind",
        "mcnty_resi",
        "mdcd_chg_amt",
        "mdcd_pay_amt",
        "mdcr_chg_amt",
        "mdcr_pay_amt",
        "oop_chg_amt",
        "oop_pay_amt",
        "payer_2",
        "payer_3",
        "pri_payer",
        "priv_chg_amt",
        "priv_pay_amt",
        "service_date",
        "sex_id",
        "st_resi",
        "st_serv",
        "toc",
        "tot_chg_amt",
        "tot_pay_amt",
        "year_id", 
        "year_dchg",
        "zip_5_resi",
        "BRND_GNRC_CD", "ndc_qty", "days_supply"]
}

## A few columns that need renaming from the raw data
rename_cols = {
    ## HCUP
    "NIS_none": {"sec_payer": "payer_2"},
    "NEDS_none": {"sec_payer": "payer_2"},
    "SIDS_none": {"sec_payer": "payer_2"},
    "SEDD_none": {"sec_payer": "payer_2"},
    ## MDCD
    "MDCD_ot_taf": {"mc_status_ps": "mc_status"}
}

## columns from `expected_cols` that should only apply for RX
rx_only_cols = ["BRND_GNRC_CD", "ndc_qty", "days_supply", "DOSAGE_FORM_CD_line"]

## Exceptions to the expected cols for each sub_dataset
## For instance: discharge_date is expected for MDCD data, 
## but it's NOT in the `rx_taf` sub_dataset
## ... 
## keys: SOURCE_sub_dataset
expected_cols_subdataset_exceptions = {
    ## MDCR: 
    "MDCR_carrier": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'],
    "MDCR_carrier_part_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'], 
    "MDCR_hha": ['payer_3', 'st_serv'], 
    "MDCR_hha_part_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'],
    "MDCR_hop": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'],
    "MDCR_hop_part_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'],
    "MDCR_hosp": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'],
    "MDCR_ip": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'],
    "MDCR_ip_part_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'],
    "MDCR_nf": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'], 
    "MDCR_nf_medpar": ['bene_id', 'CLM_FROM_DT', 'ENHANCED_FIVE_PERCENT_FLAG', 'CLM_HHA_TOT_VISIT_CNT', 'mcnty_resi', 'payer_3', 'st_serv', 'zip_5_resi'],
    "MDCR_nf_part_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'],
    "MDCR_nf_saf": ['bene_id', 'CLM_FROM_DT', 'ENHANCED_FIVE_PERCENT_FLAG', 'CLM_HHA_TOT_VISIT_CNT', 'dual_ind', 'mcnty_resi', 'payer_3', 'zip_5_resi'],
    "MDCR_rx": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'], 
    "MDCR_rx_part_c": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'],
    "MDCR_dv_carrier": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'], 
    "MDCR_dv_hop": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv', 'payer_3'], 
    "MDCR_dv_hop_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'], 
    "MDCR_dv_carrier_c": ['discharge_date', 'CLM_HHA_TOT_VISIT_CNT', 'payer_2', 'payer_3'], 
    ## CHIA_MDCR
    "CHIA_MDCR_carrier": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'],
    "CHIA_MDCR_hha": ['payer_3', 'st_serv'], 
    "CHIA_MDCR_hop": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'],
    "CHIA_MDCR_hosp": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'],
    "CHIA_MDCR_ip": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'], 
    "CHIA_MDCR_nf": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'],
    "CHIA_MDCR_dv_carrier": ['CLM_HHA_TOT_VISIT_CNT', 'st_serv'], 
    "CHIA_MDCR_dv_hop": ['CLM_HHA_TOT_VISIT_CNT', 'payer_3', 'st_serv'],
    ## MDCD:
    "MDCD_rx_taf": ['discharge_date', 'CLM_FROM_DT'],
    ## MSCAN: 
    "MSCAN_rx": ['discharge_date', 'fac_prof_ind', 'los'],
    ## KYTHERA: 
    "KYTHERA_rx": ['discharge_date', 'fac_prof_ind', 'los', 'payer_2']
}

## what type should each column be when writing the data out?
types = { 
    ## bool
    "carrier": "bool",
    ## small integer
    "sex_id": "int16",
    # "mc_ind_clm": "int16",
    "primary_cause": "int16",
    ## integer
    "year_id": "int32",
    "age_group_id": "int32", 
    "age_group_years_start": "int32",
    "pri_payer": "int32",
    ## nullable integer
    "mc_ind": "Int16",
    "year_adm": "Int32",
    "year_clm": "Int32",
    "year_dchg": "Int32",
    "los": "Int32",
    "payer_2": "Int32",
    "payer_3": "Int32",
    "mcnty_resi": "Int32",
    "mcnty_serv": "Int32",
    "dual_ind": "Int32",
    "CLM_HHA_TOT_VISIT_CNT": "Int32",
    "ndc_qty": "Int32",
    "days_supply": "Int32",
    ## string
    "BRND_GNRC_CD": "str",
    "DOSAGE_FORM_CD_line": "str",
    "CLM_FROM_DT": "str",
    "mc_status": "str",
    "zip_3_resi": "str",
    "zip_5_resi": "str",
    "toc": "str", 
    "st_resi": "str",
    "st_serv": "str",
    "bene_id": "str",
    "claim_id": "str",
    "encounter_id": "str",
    "fac_prof_ind": "str",
    "ip_trans_dchg": "str",
    "MSIS_ID": "str",
    "service_date": "str",
    "discharge_date": "str",
    "ENHANCED_FIVE_PERCENT_FLAG": "str",
    "code_system": "str",
    "dx": "str",
    "dx_level": "str",
    "acause": "str", 
    "sub_dataset": "str",
    ## float
    "mdcd_chg_amt": "float64", 
    "mdcd_pay_amt": "float64",
    "mdcr_chg_amt": "float64",
    "mdcr_pay_amt": "float64",
    "oop_chg_amt": "float64",
    "oop_pay_amt": "float64", 
    "priv_chg_amt": "float64",
    "priv_pay_amt": "float64", 
    "tot_chg_amt": "float64",
    "fac_chg_amt": "float64",
    "tot_pay_amt": "float64",
    "survey_wt": "float64"
}

## what are the age group ids for each age group years start
age_ids = pd.DataFrame(data = {
    "age_group_id": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 28, 30, 31, 32, 235], 
    "age_group_years_start": [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 0, 80, 85, 90, 95], 
})
