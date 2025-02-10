## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper function that applies all other helper functions in order
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## race_option = 'no_re' ## (no race/ethincity information was used)
## race_col = 'no_re' ## (no race/ethincity information was used)
## no_race_source = not used
## race = FALSE (no race/ethincity information was used)
## norace = TRUE

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
## generic modules
import re
import os
from atexit import register
from cProfile import run
from curses.ascii import isalnum
from dataclasses import replace
from warnings import resetwarnings
import pandas as pd
pd.options.mode.chained_assignment = None
## database modules
from dexdbload.pipeline_runs import parsed_config
## custom functions
from helpers import (
    CollapseReadWrite as rw,
    CollapseDataPrep as dp,
    CollapsePrice as pr, 
    CollapseDeflate as df,
    CollapseSampleDenom as sd,
    CollapseUtil as ut,
    CollapseNFduals as nf
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def main(
    raw_config, PRI, source, toc, year, acause,  ## main args
    race_option, race_col, no_race_source, ## race options
    test = False, test_ages = None, test_sexes = None, ## testing args (not used)
    weight_meps = False, mdcd_mc_combine = True, mdcr_dual_combine = False ## default options (unchanged)
): 

    ## refresh global constants
    from helpers.CollapseConstants import (
        pri_payer_ids
    )

    ## set up race options
    print('--------------------------------------')
    print('Collapse RE option: ' + race_option)
    print('--------------------------------------')
    race = race_option in ['re', 'both'] and source not in no_race_source
    norace = race_option in ['no_re', 'both']

    ## parse PRI to get the actual run_id if testing
    pattern = re.compile(r'.+[a-zA-Z]')
    if pattern.match(PRI):
        input_PRI = PRI.split('.')[0]
    else:
        input_PRI = PRI

    ## get configs
    config = parsed_config(config = raw_config, key = 'COLLAPSE', run_id = PRI)
    denom_config = parsed_config(config = raw_config, key = 'SAMPLE_DENOM', run_id = input_PRI)
    metadata = raw_config['METADATA']

    ## get filepaths
    input_fp1, input_fp2 = rw.get_filepaths(config, PRI, source, toc, year)
    ## input_fp2 only exists for DV data from MDCR because of how it's partitioned

    ## get schema
    schema1 = rw.replace_schema(input_fp1)
    if input_fp2 == None:
        schema2 = None
    else: 
        if os.path.exists(input_fp2): 
            schema2 = rw.replace_schema(input_fp2)
        else:
            schema2 = None

    ## instantiate iterators
    if test:
        ages = test_ages
        sexes = test_sexes
    else:
        ages = [0,1] + list(range(5,90,5))
        sexes = [1,2]

    ## instantiate empty lists
    all_ages_price = [pd.DataFrame()]
    all_ages_util = [pd.DataFrame()]
    all_combos_pre_collapse = [pd.DataFrame()]
    all_missing_spend = [pd.DataFrame()]
    all_missing_denom = [pd.DataFrame()]
    all_ages_price_race = [pd.DataFrame()]
    all_ages_util_race = [pd.DataFrame()]
    all_combos_pre_collapse_race = [pd.DataFrame()]
    all_missing_spend_race = [pd.DataFrame()]
    all_missing_denom_race = [pd.DataFrame()]

    ## loop through ages/sexes
    for age in ages:

        ## age 85 = new age max
        if age == 85:
            age_list = [85, 90, 95]
        else:
            age_list = [age]

        for sex in sexes:

            print('AGE:', age, ' SEX:', sex)

            print(' > Reading data')
            IN = rw.read_data(input_fp1, input_fp2, schema1, schema2, source, toc, acause, age_list, sex)

            print(' > Cleaning data')
            IN = dp.clean_data(IN, source, toc, age, year, no_race_source, race_option, race_col)

            ## ----------------------------------------------------------------------------
            ## PRICE
            ## ----------------------------------------------------------------------------
            if len(IN) == 0:

                ## if no input data, make empty output dataframes (for price)
                if norace: price_data_race, missing_spend_race = pd.DataFrame(), pd.DataFrame()
                if race: price_data, missing_spend = pd.DataFrame(), pd.DataFrame()

            else:

                ## IF there is input data...
                ## Make 'combos_pre_collapse', and append to list
                if norace: 
                    group_cols = dp.create_group_cols(['toc', 'year_id', 'pri_payer', 'mc_ind'], by_race = False)
                    combos_pre_collapse = IN[group_cols].drop_duplicates()
                    combos_pre_collapse['dataset'] = source
                    all_combos_pre_collapse.append(combos_pre_collapse)
                if race:
                    group_cols_race = dp.create_group_cols(['toc', 'year_id', 'pri_payer', 'mc_ind'], by_race = True)
                    combos_pre_collapse_race = IN[group_cols_race].drop_duplicates()
                    combos_pre_collapse_race['dataset'] = source
                    all_combos_pre_collapse_race.append(combos_pre_collapse_race)
                
                ## IF there is input data...
                ## Collapse price, deflate, track missing, and append to lists
                print(' > Collapsing price')
                if norace: 
                    print('    - (non-race-specific)')
                    price_data, missing_spend = pr.collapse_price(IN, metadata, source, toc, age, weight_meps, mdcr_dual_combine, by_race = False)
                    if len(price_data) > 0:
                        price_data = df.deflate(price_data, val_columns = 'raw_val')
                        all_ages_price.append(price_data)
                    if len(missing_spend) > 0:
                        all_missing_spend.append(missing_spend)
                if race:
                    print('    - (race-specific)')
                    price_data_race, missing_spend_race = pr.collapse_price(IN, metadata, source, toc, age, weight_meps, mdcr_dual_combine, by_race = True)
                    if len(price_data_race) > 0:
                        price_data_race = df.deflate(price_data_race, val_columns = 'raw_val')
                        all_ages_price_race.append(price_data_race)
                    if len(missing_spend_race) > 0:
                        all_missing_spend_race.append(missing_spend_race)

            ## ----------------------------------------------------------------------------
            ## UTILIZATION
            ## ----------------------------------------------------------------------------
            ## Regardless of whether there is input data
            ## (since no data just means encounters_per_person == 0)
            ## Get denom, collapse util, track missing, and append to lists

            ## special utilization collapse for HCCI
            if norace and source == 'HCCI':
                print(' > Collapsing util for HCCI')
                print('    - (non-race-specific)')
                util_data = ut.collapse_util_hcci(IN)
                if len(util_data) > 0:
                    all_ages_util.append(util_data)
            
            ## skipping utilization collapse for known case of not having a denom
            elif source == "MDCR" and toc == "NF" and year < 2008:
                print(' > We have not created a denominator here. Not collapsing util.')

            ## otherwise, if denom, run collapse for util
            elif source in denom_config['targets']:

                ## get denom
                print(' > Getting denom')
                if norace: 
                    print('    - (non-race-specific)')
                    denom = sd.get_denom(source, denom_config, toc, year, age, sex, age_list, weight_meps, mdcd_mc_combine, mdcr_dual_combine, no_race_source, by_race = False, race_col = 'NA')
                if race: 
                    print('    - (race-specific)')
                    denom_race = sd.get_denom(source, denom_config, toc, year, age, sex, age_list, weight_meps, mdcd_mc_combine, mdcr_dual_combine, no_race_source, by_race = True, race_col = race_col)

                ## IF MDCR and NF and pre-2019, we do not have dual information
                ## so, we first need to:
                ## 0. get year_X sample denom (above)
                ## 1. get 2019 sample denom
                ## 2. get 2019 collapsed data
                ## 3. using year_X sample denom from (0.) and 2019 sample denom from (1.)
                ##    > calculate ratio of prop_dual/prop_dual_19
                ## 4. using 2019 collapsed data
                ##    > calculate proportion of utilization that is dual in 2019 (prop_dual_util_19)
                ## 5. using prop_dual_util_19 from (4.) and ratio from (3.)
                ##    > multiply together to estimate prop_dual_util_yearX
                ## 6. pass that to collapse_util and use to impute dual rates
                ## ...
                if source == 'MDCR' and toc == 'NF' and year < 2019:
                    print(' > Collapsing 2019 data to help impute NF dual rates')
                    print('   (reading 2019)')
                    input_fp_19, not_used = rw.get_filepaths(config, PRI, source, toc, 2019)
                    schema_19 = rw.replace_schema(input_fp_19)
                    IN_19 = rw.read_data(input_fp_19, None, schema_19, None, source, toc, acause, age_list, sex)
                    print('   (cleaning)')
                    IN_19 = dp.clean_data(IN_19, source, toc, age, 2019, no_race_source, race_option, race_col)
                    print('   (collapsing)')
                    if norace: 
                        print('    - (non-race-specific)')
                        denom_19 = sd.get_denom(source, denom_config, toc, 2019, age, sex, age_list, weight_meps, mdcd_mc_combine, mdcr_dual_combine, no_race_source, by_race = False, race_col = 'NA')
                        util_data_19, NOT_USED = ut.collapse_util(IN_19, metadata, source, toc, 2019, acause, age, sex, denom_19, weight_meps, mdcd_mc_combine, mdcr_dual_combine, by_race = False, state_only = True)
                        nf_dual_rates = nf.get_NF_dual_rates(denom, denom_19, util_data_19, by_race = False)
                    if race: 
                        print('    - (race-specific)')
                        denom_19_race = sd.get_denom(source, denom_config, toc, 2019, age, sex, age_list, weight_meps, mdcd_mc_combine, mdcr_dual_combine, no_race_source, by_race = True, race_col = race_col)
                        util_data_19_race, NOT_USED = ut.collapse_util(IN_19, metadata, source, toc, 2019, acause, age, sex, denom_19_race, weight_meps, mdcd_mc_combine, mdcr_dual_combine, by_race = True, state_only = True)
                        nf_dual_rates_race = nf.get_NF_dual_rates(denom_race, denom_19_race, util_data_19_race, by_race = True)
                else:
                    nf_dual_rates = None
                    nf_dual_rates_race = None

                ## Collapse actual util, track missing, and append to lists
                print(' > Collapsing util') 
                if norace:
                    print('    - (non-race-specific)')
                    util_data, missing_denom = ut.collapse_util(IN, metadata, source, toc, year, acause, age, sex, denom, weight_meps, mdcd_mc_combine, mdcr_dual_combine, by_race = False, mdcr_nf_dual_rates = nf_dual_rates)
                    if len(util_data) > 0:
                        all_ages_util.append(util_data)
                    if len(missing_denom) > 0:
                        all_missing_denom.append(missing_denom)
                if race:
                    print('    - (race-specific)')
                    util_data_race, missing_denom_race = ut.collapse_util(IN, metadata, source, toc, year, acause, age, sex, denom_race, weight_meps, mdcd_mc_combine, mdcr_dual_combine, by_race = True, mdcr_nf_dual_rates = nf_dual_rates_race)
                    if len(util_data_race) > 0:
                        all_ages_util_race.append(util_data_race)
                    if len(missing_denom_race) > 0:
                        all_missing_denom_race.append(missing_denom_race)

    ## concat lists into dataframes to write out
    print("Concatonating all ages + sexes")
    if norace:
        all_ages_price = pd.concat(all_ages_price, ignore_index = True)
        all_ages_util = pd.concat(all_ages_util, ignore_index = True)
        all_combos_pre_collapse = pd.concat(all_combos_pre_collapse, ignore_index = True).drop_duplicates()
        all_missing_spend = pd.concat(all_missing_spend, ignore_index = True)
        all_missing_denom = pd.concat(all_missing_denom, ignore_index = True)
    if race:
        all_ages_price_race = pd.concat(all_ages_price_race, ignore_index = True)
        all_ages_util_race = pd.concat(all_ages_util_race, ignore_index = True)
        all_combos_pre_collapse_race = pd.concat(all_combos_pre_collapse_race, ignore_index = True).drop_duplicates()
        all_missing_spend_race = pd.concat(all_missing_spend_race, ignore_index = True)
        all_missing_denom_race = pd.concat(all_missing_denom_race, ignore_index = True)
    
    ## IF test, just return outputs
    if test:
        print('Since test=True, just returning datasets (not writing)')
        if race_option == 'no_re': 
            return all_ages_price, all_ages_util, None, None, 
        if race_option == 're': 
            return None, None, all_ages_price_race, all_ages_util_race
        if race_option == 'both':
            return all_ages_price, all_ages_util, all_ages_price_race, all_ages_util_race

    ## Otherwise, write out data (price and util)
    print("Saving data")
    data_output_dir = config['collapse_output_dir'] + 'tmp'
    if norace: 
        if len(all_ages_price) > 0:
            rw.write_collapsed_data(all_ages_price, data_output_dir, year, by_race = False)
        if (source in denom_config['targets'] or source == 'HCCI') and len(all_ages_util) > 0:
            rw.write_collapsed_data(all_ages_util, data_output_dir, year, by_race = False)
    if race:
        if len(all_ages_price_race) > 0:
            rw.write_collapsed_data(all_ages_price_race, data_output_dir, year, by_race = True)
        if (source in denom_config['targets'] or source == 'HCCI') and len(all_ages_util_race) > 0:
            rw.write_collapsed_data(all_ages_util_race, data_output_dir, year, by_race = True)

    ## And save out diagnostics
    print("Saving diagnostics")
    miss_spend_dir = config['collapse_output_dir'] + 'missing_spend'
    miss_denom_dir = config['collapse_output_dir'] + 'missing_denom'
    combos_pre_dir = config['collapse_output_dir'] + 'combos_pre_collapse'
    combos_post_dir = config['collapse_output_dir'] + 'combos_post_collapse'
    combos_post_cols = ['dataset', 'toc', 'metric', 'geo', 'year_id', 'pri_payer', 'mc_ind', 'payer']
    if norace:
        if len(all_missing_spend) > 0:
            rw.write_diagnostic_data(all_missing_spend, miss_spend_dir, acause, by_race = False)
        if len(all_missing_denom) > 0:
            rw.write_diagnostic_data(all_missing_denom, miss_denom_dir, acause, by_race = False)
        if len(all_combos_pre_collapse) > 0:
            ## replace pri_payer ids to align with combos_post_collapse
            all_combos_pre_collapse = all_combos_pre_collapse.replace({'pri_payer': pri_payer_ids})
            rw.write_diagnostic_data(all_combos_pre_collapse, combos_pre_dir, acause, by_race = False)
        if len(all_ages_price) > 0:
            combos_post_collapse_p = all_ages_price[dp.create_group_cols(combos_post_cols, by_race = False)].drop_duplicates()
        else:
            combos_post_collapse_p = pd.DataFrame()
        if len(all_ages_util) > 0:
            combos_post_collapse_u = all_ages_util[dp.create_group_cols(combos_post_cols, by_race = False)].drop_duplicates()
        else:
            combos_post_collapse_u = pd.DataFrame()
        all_combos_post_collapse = pd.concat([combos_post_collapse_p, combos_post_collapse_u], ignore_index = True)
        if len(all_combos_post_collapse) > 0:
            rw.write_diagnostic_data(all_combos_post_collapse, combos_post_dir, acause, by_race = False)
    if race:
        if len(all_missing_spend_race) > 0:
            rw.write_diagnostic_data(all_missing_spend_race, miss_spend_dir, acause, by_race = True)
        if len(all_missing_denom_race) > 0:
            rw.write_diagnostic_data(all_missing_denom_race, miss_denom_dir, acause, by_race = True)
        if len(all_combos_pre_collapse_race) > 0:
            ## replace pri_payer ids to align with combos_post_collapse
            all_combos_pre_collapse_race = all_combos_pre_collapse_race.replace({'pri_payer': pri_payer_ids})
            rw.write_diagnostic_data(all_combos_pre_collapse_race, combos_pre_dir, acause, by_race = True)
        if len(all_ages_price_race) > 0:
            combos_post_collapse_p_race = all_ages_price_race[dp.create_group_cols(combos_post_cols, by_race = True)].drop_duplicates()
        else:
            combos_post_collapse_p_race = pd.DataFrame()
        if len(all_ages_util_race) > 0:
            combos_post_collapse_u_race = all_ages_util_race[dp.create_group_cols(combos_post_cols, by_race = True)].drop_duplicates()
        else:
            combos_post_collapse_u_race = pd.DataFrame()
        all_combos_post_collapse_race = pd.concat([combos_post_collapse_p_race, combos_post_collapse_u_race], ignore_index = True)
        if len(all_combos_post_collapse_race) > 0:
            rw.write_diagnostic_data(all_combos_post_collapse_race, combos_post_dir, acause, by_race = True)
