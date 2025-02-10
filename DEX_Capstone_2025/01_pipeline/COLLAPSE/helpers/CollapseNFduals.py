## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that calculate the proportion of dual-enrollees in the NF data
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## by_race = FALSE (no race/ethincity information was used)
## ...

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
## generic modules
import numpy as np
import pandas as pd
## custom functions
from helpers import (
    CollapseDataPrep as dp
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------
def get_prop_dual_by_group(data, group_cols, value_col):

    ## Get total duals and total N
    ## (flexble to either denom or data to be int or str)
    sum_dual = data[(data['pri_payer'].isin([22, "mdcr_mdcd"]))].groupby(group_cols)[value_col].sum().reset_index(name='dual')
    sum_total = data[(data['pri_payer'].isin([1, 22, "mdcr", "mdcr_mdcd"]))].groupby(group_cols)[value_col].sum().reset_index(name='total')

    ## Merge the two sums
    merged_data = pd.merge(sum_dual, sum_total, on = group_cols, how = 'outer')
    
    ## fill NAs with 0 
    ## (if no duals in a group, then prop_dual = 0)
    ## (if no total, then will be dropped below)
    merged_data['dual'] = merged_data['dual'].fillna(0)
    merged_data['total'] = merged_data['total'].fillna(0)

    ## drop places where total is 0
    merged_data = merged_data[merged_data['total'] > 0]
    
    ## calculate proportion of duals
    merged_data['prop_dual'] = merged_data['dual'] / merged_data['total']
    
    # return merged_data
    return merged_data[group_cols + ["prop_dual"]]

def get_NF_dual_rates(denom, denom_19, util_data_19, by_race):

    ## filter to FFS data only
    ## (since Medpar and Saf, which need the imputation, have only mc_ind == 0)
    denom = denom[denom["mc_ind"] == 0]
    denom_19 = denom_19[denom_19["mc_ind"] == 0]
    util_data_19 = util_data_19[util_data_19["mc_ind"] == 0]

    ## define group columns
    id_cols = dp.create_group_cols(['state'], by_race)

    ## rename "denom" column in 2019 data to "denom_19" to avoid conflict
    denom_19 = denom_19.rename(columns = {'denom': 'denom_19'})

    ## get proportion of duals in denom for both years
    prop_dual_enrol_yx = get_prop_dual_by_group(denom, id_cols, 'denom')
    prop_dual_enrol_19 = get_prop_dual_by_group(denom_19, id_cols, 'denom_19')
    prop_dual_enrol_19 = prop_dual_enrol_19.rename(columns = {'prop_dual': 'prop_dual_19'})
    
    ## merge years together
    prop_dual_enrol = pd.merge(prop_dual_enrol_yx, prop_dual_enrol_19, on = id_cols, how = 'left')

    ## calculate ratio of prop_dual/prop_dual_19
    prop_dual_enrol['relative_change'] = prop_dual_enrol['prop_dual'] / prop_dual_enrol['prop_dual_19']
    
    ## if either of the above is missing, set relative_change to 1
    prop_dual_enrol.loc[prop_dual_enrol['prop_dual'].isnull(), "relative_change"] = 1
    prop_dual_enrol.loc[prop_dual_enrol['prop_dual_19'].isnull(), "relative_change"] = 1
    ## if 2019 is 0, set relative change = 1
    prop_dual_enrol.loc[prop_dual_enrol['prop_dual_19'] == 0, "relative_change"] = 1

    ## drop helper cols
    prop_dual_enrol.drop(columns = ['prop_dual', 'prop_dual_19'], inplace = True)

    ## filter data to only encounters_per_person
    util_data_19 = util_data_19[util_data_19['metric'] == 'encounters_per_person']

    ## get proportion of duals in data for 2019
    assert all(util_data_19["geo"] == "state")
    util_data_19 = util_data_19.rename(columns = {'location': 'state'})
    prop_dual_util_19 = get_prop_dual_by_group(util_data_19, id_cols, 'n_encounters')
    prop_dual_util_19 = prop_dual_util_19.rename(columns = {'prop_dual': 'prop_dual_19'})

    ## apply relative change from prop_dual_enrol to prop_dual_util_19
    output = pd.merge(prop_dual_util_19, prop_dual_enrol, on = id_cols, how = 'left')
    output["prop_dual_yearX"] = output["prop_dual_19"]*output["relative_change"]

    ## ensure no values > 1
    output.loc[output["prop_dual_yearX"] > 1, "prop_dual_yearX"] = 1

    ## return data
    return output[id_cols + ["prop_dual_yearX"]]

