## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Constants for the COLLAPSE step
## ==================================================

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
import pandas as pd

## --------------------------------------------------------------------------------------------
## Define constants
## --------------------------------------------------------------------------------------------

## What are the payment columns?
pay_cols = ['mdcr_pay_amt', 'mdcd_pay_amt', 'priv_pay_amt', 'oop_pay_amt']

## What columns are included in the output?
final_columns = ['toc', 'sex_id', 'acause', 'pri_payer', 'payer', 'mc_ind', 'age_group_years_start', 'year_id', 'geo', 'location', 'location_name', 'dataset', 'metric', 'raw_val', 'se', 'n_obs', 'n_encounters', 'n_people', 'n_days']
## ^ payer will be 'na' for utilization data, but need to preserve for parquet to read correctly from partitioned files

## defining primary payer dict
pri_payer_ids = {
    1  : 'mdcr',
    2  : 'priv',
    3  : 'mdcd',
    4  : 'oop',
    5  : 'oth_not_priv',
    6  : 'oth_not_mdcr',
    7  : 'oth_not_mdcd',
    8  : 'oth_not_oop',
    9  : 'oth_not_mdcd_mdcr',
    10 : 'oth_not_mdcd_oop',
    11 : 'oth_not_mdcd_priv',
    12 : 'oth_not_mdcr_oop',
    13 : 'oth_not_mdcr_priv',
    14 : 'oth_not_oop_priv',
    15 : 'oth_not_mdcd_mdcr_oop',
    16 : 'oth_not_mdcd_mdcr_priv',
    17 : 'oth_not_mdcd_oop_priv',
    18 : 'oth_not_mdcr_oop_priv',
    19 : 'oth_not_mdcd_mdcr_oop_priv',
    20 : 'nc',
    21 : 'unk',
    22 : 'mdcr_mdcd',
    23 : 'mdcr_priv',
    24 : 'mdcr_mc',
    25 : 'mdcd_mc',
    26 : 'mdcr_mc_mdcd'
}

## Various CSVs to convert from ids to names
names_dir = "[repo_root]/static_files/GEOGRAPHY/"
state_names = pd.read_csv(names_dir + 'state_names.csv')
county_names = pd.read_csv(names_dir + 'county_names.csv')
county_names['location'] = county_names['location'].astype(str)