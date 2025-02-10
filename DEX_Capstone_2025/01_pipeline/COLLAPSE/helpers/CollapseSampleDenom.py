## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that read and prepare sample denominators
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## by_race = FALSE (no race/ethincity information was used)
## race_col = "NA" 
## ...

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
## generic modules
import re
import glob
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
## custom functions
from helpers import (
    CollapseReadWrite as rw,
    CollapseDataPrep as dp
)
from helpers.CollapseConstants import (
    pri_payer_ids
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def get_denom(source, denom_config, toc, year, age, sex, age_list, weight_meps, mdcd_mc_combine, mdcr_dual_combine, no_race_source, by_race, race_col):

    ## refresh global constants
    from helpers.CollapseConstants import (
        state_names
    )
    
    #Get race specific file paths
    if by_race: 
        if race_col == 'raw':
            denom_config = denom_config['raw_race_data_output_dir']
        elif race_col == 'imp':
            denom_config = denom_config['race_data_output_dir']  
        else:
            raise ValueError('This race_col is not supported')
    else:
        denom_config = denom_config['data_output_dir']
    
    denom_fp = denom_config[source]
        
    if source == 'MSCAN':
        denom_fp = denom_fp + '*_' + str(year) + '.parquet'
        denom_fp = glob.glob(denom_fp)
    else:
        denom_schema = rw.replace_schema(denom_fp, partitioning='hive')
    
    if source in ['MDCR', 'MDCD', 'CHIA_MDCR']:
        denom_parquet_file = pq.ParquetDataset(
            denom_fp, 
            filters=[
                ('year_id', '=', year),
                ('age_start', 'in', age_list), 
            ], 
            schema=denom_schema
        )
        denom = denom_parquet_file.read().to_pandas()
        ## fix location names
        denom.rename(columns = {'state_name': 'location_name'}, inplace = True)
        denom = pd.merge(denom, state_names, on = 'location_name', how = 'left')
        denom.rename(columns = {'location': 'state'}, inplace = True)
        ## ensure correct types
        denom['sex_id'] = denom['sex_id'].astype(int)
        denom = denom[denom['sex_id'] == sex]
        denom.reset_index(inplace = True, drop = True)
        ## instantiate empty mcnty column as needed
        if by_race:
            denom["mcnty"] = "-1"

        if source in ['MDCR', 'CHIA_MDCR']:
            if source == "CHIA_MDCR":
                denom['part_cd']=denom['part_c']*denom['part_d'] ## 1 if both, 0 if not both
            ## change shape of mdcr denom
            denom['part_a']=denom['part_a']*denom['n']
            denom['part_b']=denom['part_b']*denom['n']
            denom['part_c']=denom['part_c']*denom['n']
            denom['part_d']=denom['part_d']*denom['n']
            denom['part_cd']=denom['part_cd']*denom['n']
            denom['part_ab']=denom['part_ab']*denom['n']
            denom['any_coverage']=denom['any_coverage']*denom['n']
            denom.drop(['n'], axis=1, inplace=True)
            
            group_cols = dp.create_group_cols(['age_start', 'mcnty', 'location_name', 'sample', 'year_id', 'age_group_id', 'sex_id','state', 'dual_enrol'], by_race)
            denom = denom.groupby(group_cols).agg(part_a=('part_a', 'sum'),
                                                  part_b=('part_b', 'sum'),
                                                  part_c=('part_c', 'sum'),
                                                  part_d=('part_d', 'sum'),
                                                  part_cd=('part_cd', 'sum'),
                                                  part_ab=('part_ab', 'sum'),
                                                  any_coverage=('any_coverage', 'sum')).reset_index()
            group_cols = dp.create_group_cols(['mcnty', 'state', 'sample', 'dual_enrol', 'part_a', 'part_b', 'part_c', 'part_d', 'part_cd', 'part_ab', 'any_coverage'], by_race)
            id_cols = dp.create_group_cols(['mcnty', 'state', 'sample', 'dual_enrol'], by_race)
            denom = denom[group_cols]
            denom = denom.melt(id_vars = id_cols)
            denom = denom[denom['variable'].isin(['part_a', 'part_b','part_c', 'part_d', 'part_cd'])] ## make toc specific
            denom['mc_ind'] = 0
            denom.loc[denom['variable'].isin(['part_c', 'part_cd']), 'mc_ind'] = 1
            denom.rename(columns = {'value': 'denom'}, inplace = True)
            if source == 'MDCR': 
                if toc in ['AM', 'ED', 'DV']:
                    ## only use 5% sample and select years
                    denom = denom.loc[denom['sample'] == 'five_pct']
                    denom = denom.loc[denom['variable'].isin(['part_b', 'part_c'])]
                    if year not in [2000, 2010, 2014, 2015, 2016, 2019]:
                        denom = denom[0:0]
                elif toc in ['IP','HH','NF']:
                    ## use full sample and part_a and part_c
                    denom = denom.loc[denom['sample'] == 'full'] ## use full sample
                    denom = denom.loc[denom['variable'].isin(['part_a', 'part_c'])] ## use part_A and managed care (which becomes mc_ind = 1)
                elif toc == 'RX':
                    ## use full sample (as in not just the 5% of benes that are also in the carrier data) and part d
                    denom = denom.loc[denom['sample'] == 'full']
                    denom = denom.loc[denom['variable'].isin(['part_cd', 'part_d'])]
                    ## MDCR RX 2019 we only have 40% of claims, so only take 40% of the denom
                    if year == 2019:
                        denom['denom']=denom['denom']*0.4 
            elif source == 'CHIA_MDCR': #Don't have part d for CHIA MDCR
                if toc in ['AM', 'ED', 'DV']: 
                    ## use full sample and part_b
                    denom = denom.loc[denom['sample'] == 'full'] ## use full sample because we have 100% carrier data for CHIA
                    denom = denom.loc[denom['variable'] == 'part_b']
                elif toc in ['IP','HH','NF']:
                    ## use full sample and part_a and part_c
                    denom = denom.loc[denom['sample'] == 'full'] ## use full sample
                    denom = denom.loc[denom['variable'].isin(['part_a', 'part_c'])] ## use part_A and managed care (which becomes mc_ind = 1)
            group_cols = dp.create_group_cols(['mcnty', 'state', 'mc_ind', 'dual_enrol', 'denom'], by_race)
            denom = denom[group_cols]
            if mdcr_dual_combine:
                ## create pri_payer mdcr and pri_payer mdcr_mdcd
                ## > where 'mdcr' includes both duals and non-duals
                group_cols = dp.create_group_cols(['mcnty', 'state', 'mc_ind'], by_race)
                denom_pp_mdcr = denom.groupby(group_cols)['denom'].sum().reset_index()
                denom_pp_mdcr_mdcd = denom.loc[denom['dual_enrol'] == 1].groupby(group_cols)['denom'].sum().reset_index()
                denom_pp_mdcr['pri_payer'] = 1
                denom_pp_mdcr_mdcd['pri_payer'] = 22
                denom = pd.concat([denom_pp_mdcr, denom_pp_mdcr_mdcd], ignore_index = True)
            else:
                ## create pri_payer mdcr and pri_payer mdcr_mdcd
                ## > where 'mdcr' includes non-duals only
                denom['pri_payer'] = np.where(denom['dual_enrol'] == 1, 22, 1)
            group_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer', 'mc_ind', 'denom'], by_race)
            denom = denom[group_cols]

        elif source == 'MDCD':
            denom['mc_ind'] = 0
            denom.loc[denom['mc_status'].isin(['partial_mc', 'mc_only']), 'mc_ind'] = 1
            denom.rename(columns = {'enrollment': 'denom'}, inplace = True)
            group_cols = dp.create_group_cols(['mcnty', 'state', 'mc_ind', 'denom'], by_race)
            denom = denom[group_cols]
            ## re-sum values
            group_cols = dp.create_group_cols(['mcnty', 'state', 'mc_ind'], by_race)
            denom = denom.groupby(group_cols)['denom'].sum().reset_index()
            ## align the sample denom with the pop denom (and encounter data) on the 11 states that aren't supposed to have MC according to kff
            denom.loc[denom['state'].isin(['AL','AK','CT','ID','ME','MT','NC','OK','SD','VT','WY']), 'mc_ind'] = 0
            ## determine if we want to use all data to generate estimates for each mc_ind value (average then duplicate)
            if mdcd_mc_combine: 
                group_cols = dp.create_group_cols(['mcnty', 'state'], by_race)
                denom = denom.groupby(group_cols)['denom'].sum().reset_index()
            ## be explicit about pri_payer
            denom['pri_payer'] = 3

    elif source == 'MSCAN':
        denom = pd.DataFrame()
        for file in denom_fp:
            sub_dataset = re.sub('_.+', '', re.sub('.+denom/', '', file))
            denom_tmp = pd.read_parquet(file)
            if sub_dataset == 'ccae':
                pri_payer = 2 ## priv
            elif sub_dataset == 'mdcr':
                pri_payer = 23 ## mdcr-priv
            denom_tmp['pri_payer'] = pri_payer
            denom = pd.concat([denom, denom_tmp], ignore_index = True)
        ## fix location and age names
        denom.rename(columns = {'location': 'state'}, inplace = True)
        ## ensure correct types
        denom['sex_id'] = denom['sex_id'].astype(int)
        denom['age_group_years_start'] = denom['age_group_years_start'].astype(int)
        denom['pri_payer'] = denom['pri_payer'].astype(int)
        ## If age>=65, remove priv (2) as a possible payer
        denom = denom[~((denom['age_group_years_start']>=65)&(denom['pri_payer']==2))]
        ## filter to age/sex/pri_payers
        denom = denom[(denom['age_group_years_start'].isin(age_list)) & (denom['sex_id'] == sex)]
        denom.reset_index(inplace = True, drop = True)
        denom = denom[denom['pri_payer'].isin([1,2,3,4,22,23])]
        denom.rename(columns = {'pop': 'denom'}, inplace = True)
        denom = denom[['state', 'pri_payer', 'denom']]
    
    elif source == 'KYTHERA':
        denom_parquet_file = pq.ParquetDataset(
            denom_fp, 
            filters=[
                ('toc', '=', toc),
                ('year_id', '=', year),
                ('age_group_years_start', 'in', age_list), 
                ('sex_id', '=', sex), 
            ], 
            schema=denom_schema
        )

        denom = denom_parquet_file.read().to_pandas()
        denom.reset_index(inplace = True, drop = True)
        ## instantiate empty mcnty column as needed
        if by_race:
            denom["mcnty"] = "-1"
        group_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer', 'denom'], by_race)
        denom = denom[group_cols]

    elif source == 'MEPS':
        denom_parquet_file = pq.ParquetDataset(
            denom_fp, 
            filters=[
                ('year_id', '=', year),
                ('age_group_years_start', 'in', age_list), 
                ('sex_id', '=', sex)
            ], 
            schema=denom_schema
        )
        denom = denom_parquet_file.read().to_pandas()
        denom.loc[~(denom['pri_payer'].isin([1,2,3,4])), 'pri_payer']=4 #if payer isn't in main payers, assign to oop

        if age >= 65:
            denom.loc[(denom['pri_payer']==2), 'pri_payer'] = 23
        denom = denom[denom['pri_payer'].isin([1,2,3,4,23])]

        if weight_meps:
            denom.rename(columns = {'pop': 'denom'}, inplace = True) ## weighted denom 
        else:
            denom.rename(columns = {'n_obs': 'denom'}, inplace = True) #unweighted denom

        ## check sex_ids are 1 and 2
        assert all(denom['sex_id'].isin([1,2]))
        group_cols = dp.create_group_cols(['pri_payer'], by_race)
        denom = denom[group_cols + ['denom']]
        denom = denom.groupby(group_cols)['denom'].sum().reset_index()

    elif source in ['SIDS', 'SEDD', 'NIS', 'NEDS']:
        denom_parquet_file = pq.ParquetDataset(
            denom_fp, 
            filters=[
                ('year_id', '=', year),
                ('age_group_years_start', 'in', age_list), 
                ('sex_id', '=', sex)
            ], 
            schema=denom_schema
        )
        denom = denom_parquet_file.read().to_pandas()
        denom.reset_index(inplace = True, drop = True)
        if source in ['NIS', 'NEDS']:
            group_cols = dp.create_group_cols(['pri_payer', 'denom'], by_race )
            denom = denom[group_cols]
        else:
            group_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer', 'denom'], by_race)
            denom = denom[group_cols]
        ## ensure only certain pri_payers
        assert all(denom['pri_payer'].isin([2, 23])) ## priv, mdcr_priv

    ## add in uninsured sample denom (pri_payer == oop) for HCUP
    ## (there's no sample denom for uninsured, so we use pop denom)
    if source in ['SIDS', 'SEDD', 'NIS', 'NEDS']:
        pop_filters = [
            ('toc', '=', toc),
            ('year_id', '=', year),
            ('age_group_years_start', 'in', age_list), 
            ('sex_id', '=', sex), 
            ('pri_payer', '=', 'oop')
        ]

        ## read pop denom to use
        denom_fp_pop = denom_config['POP_DENOM']
        denom_schema_pop = rw.replace_schema(denom_fp_pop, partitioning='hive')
        denom_parquet_file_pop = pq.ParquetDataset(
            denom_fp_pop, 
            filters= pop_filters, 
            schema=denom_schema_pop
        )
        denom_pop = denom_parquet_file_pop.read().to_pandas()
        denom_pop.reset_index(inplace = True, drop = True)
        ## make pri_payer int
        pri_payer_ids_inverted = {v: k for k, v in pri_payer_ids.items()}
        denom_pop['pri_payer'] = denom_pop['pri_payer'].map(pri_payer_ids_inverted)

        if source in ['NIS', 'NEDS']:
            denom_pop=denom_pop[(denom_pop['geo']=='national')]
            denom_pop = denom_pop[dp.create_group_cols(['pri_payer','denom'], by_race)]
        elif source in ['SIDS', 'SEDD']:
            if by_race:
                denom_pop=denom_pop[(denom_pop['geo']=='state')] ## no county info for race
                denom_pop=denom_pop[['state','pri_payer','race_cd', 'denom']]
            else:
                denom_pop=denom_pop[(denom_pop['geo']=='county')]
                denom_pop=denom_pop[['location','state','pri_payer','denom']]
                denom_pop.rename(columns = {'location': 'mcnty'}, inplace=True)
        denom = pd.concat([denom, denom_pop], ignore_index = True)
    
    ## ensure mcnty type is right
    if 'mcnty' in denom.columns:
        if denom['mcnty'].dtype == object:
            denom['mcnty'] = denom['mcnty'].fillna('-1')
        else:
            denom['mcnty'] = denom['mcnty'].fillna(-1)
            denom['mcnty'] = denom['mcnty'].astype('Int32').astype(str)
        denom['mcnty'] = denom['mcnty'].astype(str)
    if 'state' in denom.columns:
        denom.loc[denom['state'] == "unknown", 'state'] = "-1"
        denom['state'] = denom['state'].fillna('-1')

    ## age 85 = new age max
    if age == 85:
        denom['age_group_years_start'] = 85
        if source == 'MDCD':
            if mdcd_mc_combine: ## in this case, there is no mc_ind column
                id_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer'], by_race)
            else:
                id_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer', 'mc_ind'], by_race)
        elif source in ['MDCR', 'CHIA_MDCR']:
            id_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer', 'mc_ind'], by_race)
        elif source in ['KYTHERA', 'SIDS', 'SEDD']:
            id_cols = dp.create_group_cols(['mcnty', 'state', 'pri_payer'], by_race)
        elif source in ['MSCAN']:
            id_cols = ['state', 'pri_payer']
        elif source in ['MEPS', 'NIS', 'NEDS']:
            id_cols = dp.create_group_cols(['pri_payer'], by_race)
        denom = denom.groupby(id_cols)['denom'].sum().reset_index()

    ## remove NAs and Zeros
    denom = denom[~(denom['denom'].isnull())]
    denom = denom[denom['denom'] > 0]

    ## reindex
    denom.reset_index(inplace = True, drop = True)
    
    ## check race codes 
    if by_race and source not in no_race_source:
        ## correct MEPS misspelling
        denom.loc[denom['race_cd'] == 'BLK', 'race_cd'] = 'BLCK'
        ## convert 'OTH' and 'MULT' to 'UNK'
        denom.loc[denom['race_cd'].isin(['OTH', 'MULT']), 'race_cd'] = 'UNK'
        ## set any possible NAs to 'UNK'
        denom.loc[denom['race_cd'].isnull(), 'race_cd'] = 'UNK'
        na_vals = ['None']
        denom.loc[denom['race_cd'].isin(na_vals), 'race_cd'] = 'UNK'
        ## make sure race_cd is standardized
        assert all(denom['race_cd'].isin(['WHT', 'BLCK', 'HISP', 'AIAN', 'API', 'UNK']))
        assert sum(denom.race_cd.isnull()) == 0
    
    return denom
