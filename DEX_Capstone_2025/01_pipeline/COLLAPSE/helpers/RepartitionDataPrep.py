## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that clean/prep data
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## by_race = FALSE (no race/ethincity information was used)
## ...


## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
import numpy as np
import pandas as pd

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def create_group_cols(group_cols, by_race):
    if by_race:
        group_cols = group_cols + ['race_cd']
    return group_cols

def combine_chia(data, toc):

    ## add OG dataset for tracking
    data['og_dataset'] = data['dataset']

    ## make state column    
    data['state'] = np.where(data['geo'] == 'county', data['location_name'].str.split(' - ').str[0], data['location'])

    ## make sure CHIA data only has MA state
    data = data[~( (data['dataset'].isin(['CHIA_MDCR'])) & (data['state'] != 'MA') )]

    ## [CHIA_MDCR]
    ## exclude CHIA_MDCR RX (shouldn't exist from formatting, but just in case)
    ## otherwise, replace MDCR with CHIA_MDCR for select conditions
    if toc == 'RX':
        data = data[~(data['dataset'] == 'CHIA_MDCR')]
    else:
        ## build filter: what to keep from CHIA_MDCR
        CHIA_MDCR_keep = (
            (data['state'] == 'MA') ## Massachusets
            & (data['mc_ind'] == 0) ## non-managed care
        )
        ## build filter: what to keep from MDCR
        CHIA_MDCR_years = data[(data['dataset'] == 'CHIA_MDCR') & (CHIA_MDCR_keep)]['year_id'].unique()
        MDCR_keep = (
            ~(CHIA_MDCR_keep) ## Anything outside of the above (non-Massachusets, or managed care, or some combination therein)
            | ~(data['year_id'].isin(CHIA_MDCR_years)) ## OR any year not in CHIA_MDCR (could be Massachusets & non-managed care & if it's a year not in CHIA_MDCR_years)
        )
        ## filter our data
        data = data[
            ((data['dataset'] == 'CHIA_MDCR') & (CHIA_MDCR_keep))
            | ((data['dataset'] == 'MDCR') & (MDCR_keep))
            | ~(data['dataset'].isin(['CHIA_MDCR', 'MDCR']))
        ]

        ## rename CHIA_MDCR to MDCR
        data.loc[(data['dataset'] == 'CHIA_MDCR'), 'dataset'] = 'MDCR'

    ## drop state column
    data.drop(columns = ['state'], inplace = True)

    return data

def price_drops(data, toc):
    
    ## Dropping prices of 0
    data = data[data['raw_val'] > 0]
    
    ## Dropping managed care
    data = data[data['mc_ind'] <= 0] 

    ## drop MDCR HH oop spend (these are 99% NA)
    if toc == 'HH':
        data = data[~((data['dataset'] == 'MDCR') & (data['payer'] == 'oop'))]

    return data

def restrict_hcup(data, RES_hcup):

    ## prep restrictions
    id_cols = ['year_id', 'state']
    sids_sy = RES_hcup.loc[RES_hcup['dataset'] == 'SIDS', id_cols].reset_index(drop = True)
    sedd_sy = RES_hcup.loc[RES_hcup['dataset'] == 'SEDD', id_cols].reset_index(drop = True)

    ## make state column, and split data into parts
    data['state'] = np.where(data['geo'] == 'county', data['location_name'].str.split(' - ').str[0], data['location'])
    sids_data = data[data['dataset'] == 'SIDS']
    sedd_data = data[data['dataset'] == 'SEDD']
    nis_data = data[data['dataset'] == 'NIS']
    neds_data = data[data['dataset'] == 'NEDS']
    other_data = data[~(data['dataset'].isin(['SIDS', 'SEDD', 'NIS', 'NEDS']))]

    ## we want SIDS and SEDD data to have ONLY these state-years
    sids_data = pd.merge(sids_data, sids_sy, on = id_cols, how = 'inner')
    sedd_data = pd.merge(sedd_data, sedd_sy, on = id_cols, how = 'inner')

    ## we want to DROP these state-years from NIS/NEDS (respectively)
    ## (just in case any came through collapse)
    sids_sy['drop_nis'] = 1
    sedd_sy['drop_neds'] = 1
    nis_data = pd.merge(nis_data, sids_sy, on = id_cols, how = 'left')
    neds_data = pd.merge(neds_data, sedd_sy, on = id_cols, how = 'left')
    nis_data = nis_data[nis_data['drop_nis'].isnull()]
    neds_data = neds_data[neds_data['drop_neds'].isnull()]    

    ## remove tmp column
    nis_data.drop(columns='drop_nis', inplace = True)
    neds_data.drop(columns='drop_neds', inplace = True)

    ## now that there's no state-year intersection between sids/nis or sedd/neds 
    ## we can rename state-level NIS/NEDS to be SIDS/SEDD
    nis_data.loc[nis_data['geo'] == 'state', 'dataset'] = 'SIDS'
    neds_data.loc[neds_data['geo'] == 'state', 'dataset'] = 'NEDS'

    ## recombine data by column name and concat
    data = pd.concat([sids_data, sedd_data, nis_data, neds_data, other_data], ignore_index = True)

    ## drop state column
    data.drop(columns = ['state'], inplace = True)

    return data

def rename_mc(data, metric):

    # We do not want to include mdcr_priv where mc_ind = 1
    data = data[~((data['pri_payer'] == 'mdcr_priv') & (data['mc_ind'] == 1))]

    mc = ((data['mc_ind'] == 1)&(data['pri_payer'].isin(['mdcr','mdcd'])))
    data_mc = data[mc]
    data.loc[mc, 'pri_payer'] = data_mc['pri_payer'] + '_mc'
    
    #add mc where mdcr_mdcd has mc
    mc1 = ((data['mc_ind'] == 1)&(data['pri_payer']=='mdcr_mdcd'))
    data.loc[mc1, 'pri_payer'] =  'mdcr_mc_mdcd'
    
    ## managed care only in utilization, and only for mdcr/mdcd
    if metric in ['spend_per_encounter', 'spend_per_day']:
        assert sum(mc) == 0
    else:
        assert all(data['pri_payer'].isin(['mdcr', 'mdcr_mc', 'mdcr_mdcd', 'mdcr_priv', 'mdcr_mc_mdcd','mdcd', 'mdcd_mc', 'priv', 'oop'])) 
    data.drop(columns = 'mc_ind', inplace = True)
    
    return data

def restrict_toc_cause_age_sex(data, toc, acause, RES_toc_cause):

    ## ensure valid
    asr = RES_toc_cause[(RES_toc_cause['acause'] == acause) & ((RES_toc_cause['toc'] == toc))].reset_index(drop = True)
    assert len(asr) == 1 ## only one row
    assert asr['include'].values[0] == 1 ## include = yes
    assert asr['gc_nec'].values[0] == 0 ## not gc/nec

    ## age
    ages = [0,1] + list(range(5,90,5))
    ages = [x for x in ages if x >= asr['age_start'].values[0] and x <= asr['age_end'].values[0]]

    ## sex
    female = asr['female'].values[0]
    male = asr['male'].values[0]
    if(male and female):
        sexes = [1,2]
    elif(male):
        sexes = [1]
    else:
        sexes = [2]

    ## restrict
    data = data[(data['age_group_years_start'].isin(ages)) & (data['sex_id'].isin(sexes))]

    return data

def restrict_dataset_pripayer_payer(data, RES_dataset_payer):
    data = pd.merge(data, RES_dataset_payer, on = list(RES_dataset_payer.columns), how = 'inner')
    return data

def misc_drops(data, toc, acause):
    # Drop any instances of MDCR data - managed care outside of years 2016 or 2019
    data = data[~((data['dataset'] == 'MDCR') & (data['pri_payer']).isin(['mdcr_mc', 'mdcr_mc_mdcd']) & (~(data['year_id'].isin([2016, 2019]))))]
    ## Drop dataset-STATE-year-tocs with too many zero-valued encounters
    data = data[~((data['dataset'] == 'MSCAN') & (data['location'].str.contains('HI')))]
    data = data[~((data['dataset'] == 'MDCD') & (data['location'].str.contains('RI')) & (data['year_id'] == 2019))]
    if toc in ['ED', 'NF']:
        data = data[~((data['dataset'] == 'MDCD') & (data['location'].str.contains('TN')) & (data['year_id'] == 2000))]
    data = data[~((data['dataset'] == 'MDCD') & (data['location'].str.contains('WY')) & (data['year_id'] == 2016))]
    ## Drop 2017 CHIA_MDCR data
    data = data[~((data['dataset'] == 'CHIA_MDCR') & (data['year_id'] == 2017))]
    return data

def not_too_many_zeros(data):
    data['max'] = data.groupby(['pri_payer', 'sex_id', 'dataset', 'year_id'])['raw_val'].transform('max') ## and acause/toc
    ## if all ages/locations are zero (within a given cause/toc/pri_payer/sex/dataset/year
    data['drop_zeros'] = np.where(data['max'] == 0, 1, 0)
    ## drop the zeros
    data = data[data['drop_zeros'] == 0]
    data.drop(columns = ['max', 'drop_zeros'], inplace = True)
    return data

def asserts(data, toc, metric):
    ## [ raw_val ]
    ## ensure no NAs/infs/negatives in raw_val
    assert sum(data['raw_val'].isnull()) == 0
    assert sum(np.isinf(data['raw_val'])) == 0
    assert sum(data['raw_val'] < 0) == 0
    ## ensure no 0s in raw_val (for all metrics except encounters_per_person)
    if metric in ['days_per_encounter', 'spend_per_encounter', 'spend_per_day']:
        assert sum(data['raw_val'] == 0) == 0
    ## [ se ]
    ## ensure no NAs/infs/negatives/0s in SE
    assert sum(data['se'].isnull()) == 0
    assert sum(np.isinf(data['se'])) == 0
    assert sum(data['se'] <= 0) == 0
    ## [ n_obs, n_encounters ]
    ## ensure no NAs/infs/negatives in n_obs, n_encounters
    assert sum(data['n_obs'].isnull()) == 0
    assert sum(data['n_encounters'].isnull()) == 0
    assert sum(np.isinf(data['n_obs'])) == 0
    assert sum(np.isinf(data['n_encounters'])) == 0
    assert sum(data['n_obs'] < 0) == 0
    assert sum(data['n_encounters'] < 0) == 0
    ## ensure no 0s in n_obs, n_encounters (for all metrics except encounters_per_person)
    if (metric =='days_per_encounter') & (toc =='NF'):
        data=data.drop(data[(data['n_obs']==0)].index)
    if metric in ['days_per_encounter', 'spend_per_encounter', 'spend_per_day']:
        data=data.drop(data[(data['n_encounters']==0)].index)
        assert sum(data['n_obs'] == 0) == 0
        assert sum(data['n_encounters'] == 0) == 0
    ## ensure no NAs/Infs/negatives/0s in n_people (for encounters_per_person)
    if metric in ['encounters_per_person']:
        assert sum(data['n_people'].isnull()) == 0
        assert sum(np.isinf(data['n_people'])) == 0
        assert sum(data['n_people'] <= 0) == 0
        ## ensure no NAs/Infs/negatives in n_days (for days metrics)
    if metric in ['days_per_encounter', 'spend_per_day']:
        assert sum(data['n_days'].isnull()) == 0
        assert sum(np.isinf(data['n_days'])) == 0
        assert sum(data['n_days'] < 0) == 0
        ## ensure no 0s in n_days for spend_per_day
        if metric == 'spend_per_day':
            assert sum(data['n_days'] == 0) == 0

def N_columns(data, groupby_cols):

    ## add new cell-count columns 
    print('Adding extra cell count N_* columns')
    data['XXX'] = 1
    
    ## add columns
    data['N_years'] = data.groupby(groupby_cols)['year_id'].transform('nunique')
    data['N_ages'] = data.groupby(groupby_cols)['age_group_years_start'].transform('nunique')
    data['N_locations'] = data.groupby(groupby_cols)['location'].transform('nunique')
    data['N_total'] = data.groupby(groupby_cols)['XXX'].transform('size')
    data.drop(columns = ['XXX'], inplace = True)

    ## mark if all 0
    data['max'] = data.groupby(groupby_cols)['raw_val'].transform('max')
    data['all_zero'] = np.where(data['max'] == 0, 1, 0)
    data.drop(columns = ['max'], inplace = True)

    return data
