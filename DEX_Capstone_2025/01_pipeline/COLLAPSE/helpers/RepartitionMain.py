## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper function that applies all other helper functions in order
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## by_race = FALSE (no race/ethincity information was used)
## ...

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
from atexit import register
from cProfile import run
from curses.ascii import isalnum
from dataclasses import replace
from warnings import resetwarnings
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import os
from dexdbload.pipeline_runs import parsed_config
## ...
from helpers import (
    RepartitionReadWrite as rw, 
    RepartitionDataPrep as dp, 
    RepartitionImputeSE as se
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def main(raw_config, PRI, toc, acause, metric, COMORB, RES_hcup, RES_toc_cause, RES_dataset_payer, by_race, test = False):
    
    print('--------------------------------------')
    print('Collapsing by race: ' + str(by_race))
    print('--------------------------------------')

    print('Setting up config')
    collapse_config = parsed_config(config = raw_config, key = 'COLLAPSE', run_id = PRI)

    print('Setting up directories')
    input_dir = ''.join([collapse_config['collapse_output_dir'], 'tmp/acause=', acause, '/toc=', toc, '/metric=', metric])
    if by_race:
        input_dir = ''.join([collapse_config['collapse_output_dir'], 'tmp_race/acause=', acause, '/toc=', toc, '/metric=', metric])

    ## final col order
    col_order = ['toc', 'sex_id',  'acause', 'pri_payer', 'payer', 'age_group_years_start', 'year_id', 'geo', 'location', 'location_name', 'dataset', 'og_dataset', 'metric', 'raw_val', 'se', 'n_obs', 'n_encounters', 'n_people', 'n_days']
    if by_race: ## insert race_cd before acause
        col_order.insert(col_order.index('acause'), 'race_cd') 

    ## check if data exists
    if not os.path.exists(Path(input_dir)):
        print('No data for this acause/toc/metric combination')
        return
        
    ## read data
    print('Reading')
    data_schema = rw.replace_schema(input_dir, partitioning='hive')
    parquet_file = pq.ParquetDataset(
        input_dir, 
        schema=data_schema
    )
    data = parquet_file.read().to_pandas().reset_index(drop = True)

    ## check if data is empty
    if data.empty:
        print("No data for this acause/toc/metric combination")
        return

    ## run data prep functions
    print('Data cleaning')
    data = dp.combine_chia(data, toc)
    if metric in ['spend_per_encounter', 'spend_per_day']:
        data = dp.price_drops(data, toc)
    if toc in ['ED', 'IP']:
        data = dp.restrict_hcup(data, RES_hcup)
    data = dp.rename_mc(data, metric)
    data = dp.restrict_toc_cause_age_sex(data, toc, acause, RES_toc_cause)
    data = dp.restrict_dataset_pripayer_payer(data, RES_dataset_payer)
    data = dp.misc_drops(data, toc, acause)
    data = dp.not_too_many_zeros(data)

    ## impute SEs
    print('Imputing zero-valued SEs')
    ## split out HCCI (we don't want HCCI's SE to inform imputation of missing/0-valued SE)
    HCCI = data[data['dataset'] == 'HCCI']
    other = data[data['dataset'] != 'HCCI']
    ## only impute 0-valued SE for/using non-HCCI data
    other, imputed_se, keepgoin = se.impute_se0(other, acause, toc, metric, by_race)
    data = pd.concat([HCCI, other], ignore_index = True)
    ## ^^ at one point, we wrote out imputed_se to inspect it
    ## if there is ever a need, we could revert to doing that
    if not keepgoin: 
        print('    >>> Cant impute SE, but this is expected. Terminating without error.')
        return
    print('Imputing SE in HCCI')
    data = se.fix_HCCI_se(data)

    ## check assert statements
    dp.asserts(data, toc, metric)

    ## get final columns
    data[['toc', 'acause', 'metric']] = [toc, acause, metric]
    data = data[col_order]

    ## Make 'combos_post_repartition'
    group_cols = dp.create_group_cols(['dataset', 'toc', 'metric', 'geo', 'year_id', 'pri_payer', 'payer'], by_race)
    combos_post_repartition = data[group_cols].drop_duplicates()

    ## add N columns
    n_groupby = dp.create_group_cols(['toc', 'sex_id', 'acause', 'pri_payer', 'payer', 'geo', 'metric'], by_race)
    data = dp.N_columns(data, n_groupby)

    ## return data if test
    if test:
        return data

    ## save combos post repartition
    outpath = collapse_config['collapse_output_dir'] + 'combos_post_repartition'
    rw.write_combos(combos_post_repartition, outpath, acause, by_race)

    ## make sure sex/age/year are integer
    data['sex_id'] = data['sex_id'].astype(int)
    data['age_group_years_start'] = data['age_group_years_start'].astype(int)
    data['year_id'] = data['year_id'].astype(int)

    ## write output data
    print('Repartitioning data')
    if COMORB:
        ## split data between final output and comorb
        if metric in ['encounters_per_person', 'days_per_encounter'] or toc in ['RX', 'DV']:
            ## write final format
            sexes = list(data.sex_id.unique())
            for sex in sexes:
                data_s = data[data['sex_id'] == sex].reset_index(drop = True)
                rw.write_final(data_s, collapse_config['pipeline_output_dir'] + 'data', acause, sex, by_race)
        else:
            ## write out as a comorb input
            rw.write_comorb(data, collapse_config['collapse_output_dir'] + 'data', acause, by_race)
    else:
        ## write final format
        sexes = list(data.sex_id.unique())
        for sex in sexes:
            data_s = data[data['sex_id'] == sex].reset_index(drop = True)
            rw.write_final(data_s, collapse_config['pipeline_output_dir'] + 'data', acause, sex, by_race)
