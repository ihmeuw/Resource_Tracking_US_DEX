## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that calculate encounters-per-person fractions for modeling
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## by_race = FALSE (no race/ethincity information was used)
## ...

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
## generic modules
import numpy as np
import pyarrow.parquet as pq
import pandas as pd
from statsmodels.stats.weightstats import DescrStatsW
## custom functions
from helpers import (
    CollapseDataPrep as dp,
    CollapseReadWrite as rw
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def collapse_util(data, metadata, source, toc, year, acause, age, sex, denom, weight_meps, mdcd_mc_combine, mdcr_dual_combine, by_race, mdcr_nf_dual_rates = None, state_only = False):

    ## refresh global constants
    from helpers.CollapseConstants import (
        pri_payer_ids, 
        state_names, 
        county_names, 
        final_columns
    )
        
    if by_race:
        final_columns = final_columns + ['race_cd']

    ## ensure object (string) types
    assert data['mcnty_resi'].dtype == object
    assert county_names['location'].dtype == object
    if 'mcnty' in denom.columns:
        assert denom['mcnty'].dtype == object
    ## ensure no decimal places 
    assert sum(data['mcnty_resi'].str.contains('\\.')) == 0
    assert sum(county_names['location'].str.contains('\\.')) == 0
    if 'mcnty' in denom.columns:
        assert sum(denom['mcnty'].str.contains('\\.')) == 0

    ## Impute dual_ind for MDCR NF pre-2019 for sub_dataset 'nf_medpar'
    if source == 'MDCR' and toc == 'NF' and year != 2019: 

        ## Separate the NF data that needs dual_ind imputed (nf_saf and nf_medpar) from the other nf datasets
        data_impute_dual = data[data['sub_dataset'].isin(['nf_medpar','nf_saf'])]
        data_other = data[~data['sub_dataset'].isin(['nf_medpar','nf_saf'])]

        ## ensure we are not trying to impute any mc_ind
        ## the assumption is that medpar/saf only have mc_ind == 0
        assert sum(data_impute_dual["mc_ind"] == 1) == 0
        
        ## Save data lengths
        OG_data_impute_length = len(data_impute_dual)
        OG_data_other_length = len(data_other)

        ## only run if we have data to impute
        if OG_data_impute_length > 0:

            ## Initially set dual_ind to 1 for the data that needs imputation
            data_impute_dual['dual_ind'] = 1

            ## Format the dual proportions for the data that needs imputation
            ## (rename)
            mdcr_nf_dual_rates = mdcr_nf_dual_rates.rename(columns = {'prop_dual_yearX': 'prop_dual'})
            ## (invert proportion)
            mdcr_nf_dual_rates['prop_nondual'] = 1 - mdcr_nf_dual_rates['prop_dual']
            
            ## Make sure no negatives
            if len(mdcr_nf_dual_rates) > 0:
                assert mdcr_nf_dual_rates['prop_nondual'].min() >= 0
                assert mdcr_nf_dual_rates['prop_dual'].min() >= 0

            ## Merge in the dual proportions
            mdcr_nf_dual_rates = mdcr_nf_dual_rates.rename(columns = {'state': 'st_resi'})
            ## define group columns
            dual_merge_cols = dp.create_group_cols(['st_resi'], by_race)
            ## merge
            data_impute_dual = pd.merge(data_impute_dual, mdcr_nf_dual_rates, on = dual_merge_cols, how = 'left')

            ## if missing, assume it's nondual
            data_impute_dual.loc[data_impute_dual["prop_dual"].isnull(), "prop_dual"] = 0
            data_impute_dual.loc[data_impute_dual["prop_nondual"].isnull(), "prop_nondual"] = 1

            ## make a duplicated dataset with dual_ind = 0
            data_impute_nondual = data_impute_dual.copy()
            data_impute_nondual['dual_ind'] = 0

            ## apply proportions to split admission_count by dual/nondual
            data_impute_dual['admission_count'] = data_impute_dual['admission_count']*data_impute_dual['prop_dual']
            data_impute_nondual['admission_count'] = data_impute_nondual['admission_count']*data_impute_nondual['prop_nondual']

            ## make sure pri_payer is right
            data_impute_dual["pri_payer"] = 22 ## 'mdcr_mdcd'
            data_impute_nondual["pri_payer"] = 1 ## 'mdcr'

            ## recombine dual and non-dual and drop extra columns
            data_imputed = pd.concat([data_impute_dual, data_impute_nondual])
            data_imputed.drop(columns = ['prop_dual', 'prop_nondual'], inplace = True)

            ## recombine with original data
            data = pd.concat([data_imputed, data_other], ignore_index = True)

            ## check length
            assert len(data) == 2*OG_data_impute_length + OG_data_other_length

            ## drop 0-valued admission_count
            data = data[data['admission_count'] > 0]

    ## reassign pri_payer
    data = dp.reassign_pri_payer(data, source, age, mdcr_dual_combine, step = 'util')
    
    ## don't collapse utilization estimates for duals from MDCD
    if source == 'MDCD':
        data = data[data['pri_payer'] != 22]

    ## constants -------------------------------------------------
    if state_only:
        geos = ['state']
    else:
        geos = metadata['geos'][source]
        if by_race: ## no county for race (or if specified)
            geos = [x for x in geos if x != 'county']
    
    
    ## id columns
    util_id_cols = dp.create_group_cols(['toc', 'sex_id', 'acause', 'pri_payer', 'age_group_years_start', 'year_id', 'mc_ind'], by_race)
    ## location columns
    if all([x == 'national' for x in geos]):
        util_loc_cols = []
    elif all([x in ['state', 'national'] for x in geos]):
        util_loc_cols = ['st_resi']
    else: 
        util_loc_cols = ['st_resi', 'mcnty_resi']
    ## extra columns
    util_extra_cols = ['admission_count']
    ## whether we calculate days_per_encounter
    days_per_encounter = toc in ['RX']
    if days_per_encounter:
        util_extra_cols = util_extra_cols + ['los'] ## days_supply is named los for RX
    ## whether we use survey weights
    if weight_meps:
        use_weight = source in ['MEPS'] ## turn on use_weight if source == MEPS
    else:
        use_weight = False ## never use weight
    if use_weight:
        util_extra_cols = util_extra_cols + ['survey_wt']

    ## drop mc_ind information in certain cases
    if source == 'MDCD' and mdcd_mc_combine: 
        util_id_cols = [x for x in util_id_cols if not x == 'mc_ind']

    ## functions to summarize data -------------------------------------------------
    ## 'encounters_per_person'
    ##  - raw_val = total encounters / sample denom (will happen after merging denom in)
    ##      -> total encounters = the sum of the admission_count
    ##  - std = std of raw_val across most granular geo (will be calculated after aggregation)
    ##  - n = sample denom (will happen after merging denom in)
    def aggregate_encounters_per_person(group, weight = use_weight):
        if weight:
            return pd.Series({
                'raw_val': np.nan,
                'std': np.nan,
                'n_encounters' : sum(group.admission_count*group.survey_wt),
                'n_obs': len(group), 
                'metric': 'encounters_per_person'
            })
        else:
            return pd.Series({
                'raw_val': np.nan,
                'std': np.nan,
                'n_encounters' : sum(group.admission_count),
                'n_obs': len(group), 
                'metric': 'encounters_per_person'
            })

    ## 'days_per_encounter'
    ##  - raw_val = total los / total encounters
    ##      -> total encounters = the sum of the admission_count
    ##  - std = std of los
    ##  - n = # observations
    def aggregate_days_per_encounter(group, weight = use_weight):
        if weight: 
            return pd.Series({
                ## calling 'raw_val' X for now to align with the above so concat works cleanly
                'raw_val': sum(group.los*group.survey_wt)/sum(group.admission_count*group.survey_wt) if sum(group.admission_count*group.survey_wt) else np.nan, ## if non-zero denom, otherwise NA
                'n_encounters' : sum(group.admission_count*group.survey_wt),
                'n_days': sum(group.los*group.survey_wt),
                'n_obs': len(group),
                'metric': 'days_per_encounter'
            })
        else:
            return pd.Series({
                ## calling 'raw_val' X for now to align with the above so concat works cleanly
                'raw_val': sum(group.los)/sum(group.admission_count) if sum(group.admission_count) else np.nan, ## if non-zero denom, otherwise NA
                'n_days': sum(group.los),
                'n_encounters' : sum(group.admission_count),
                'n_obs': len(group), 
                'metric': 'days_per_encounter'
            })
    #Standard deviation for days_per_encounter
    def stdev_days_per_encounter(group, weight = use_weight):
        if weight: 
            return pd.Series({
                ## calling 'raw_val' X for now to align with the above so concat works cleanly
                'std': DescrStatsW(group.los, weights=group.survey_wt, ddof=1).std,
                'n_obs_stdev': len(group),
                'metric': 'days_per_encounter'
            })
        else:
            return pd.Series({
                ## calling 'raw_val' X for now to align with the above so concat works cleanly
                'std': np.std(group.los),
                'n_obs_stdev': len(group), 
                'metric': 'days_per_encounter'
            })

    #Combine days_per_encounter and stdev:
    def add_stdev_dpe(df, df_collapsed):
        if 'location' in df:
            df.drop(columns=['location'], axis=1, inplace=True)
        df_stdev = df.loc[~df['los'].isnull()].groupby(util_id_cols).apply(stdev_days_per_encounter).reset_index()
        join_cols = dp.create_group_cols(['toc', 'sex_id', 'acause', 'pri_payer', 'age_group_years_start','year_id','metric'], by_race) 
        if source == 'MDCD' and mdcd_mc_combine: 
            join_cols = join_cols
        else:
            if 'dual_ind' in df.columns:
                join_cols = join_cols + ['mc_ind', 'dual_ind']
            else:
                join_cols = join_cols + ['mc_ind']
        df_collapsed = pd.merge(df_collapsed, df_stdev, on = join_cols, how = 'left')
        df_collapsed['std'] = np.where(df_collapsed['n_obs_stdev'] == 1, df_collapsed['raw_val']*2.6, df_collapsed['std'])
        #drop n_obs_stdev:
        df_collapsed.drop(columns=['n_obs_stdev'], axis=1, inplace=True)

        return df_collapsed

    ## recode 0 and negative LOS
    ## already done in CAUSEMAP, just ensuring
    if days_per_encounter:
        data.loc[data['los'] == 0, 'los'] = 1 
        data.loc[data['los'] < 0, 'los'] = np.nan

    ## simplify data
    data = data[util_id_cols + util_loc_cols + util_extra_cols]

    ## ensure mc_ind is not 1 if pri_payer is oop/priv
    if not (source == 'MDCD' and mdcd_mc_combine):
        if source == 'HCCI':
            data.loc[data['pri_payer'].isin(['priv','oop']), 'mc_ind'] = 0
        else:
            data.loc[data['pri_payer'].isin([2,4]), 'mc_ind'] = 0
        
    ## aggregate utilization  -------------------------------------------------
    if 'national' in geos: 
        if len(data) == 0:
            collapsed_national = pd.DataFrame()
        else:
            collapsed_national = data.groupby(util_id_cols).apply(aggregate_encounters_per_person).reset_index()
            
            if days_per_encounter and len(data.loc[~data['los'].isnull()]) > 0:
                ## drop NA los in this aggregation
                collapsed_national_day = data.loc[~data['los'].isnull()].groupby(util_id_cols).apply(aggregate_days_per_encounter).reset_index()
                collapsed_national_day = add_stdev_dpe(data, collapsed_national_day)
                collapsed_national = pd.concat([collapsed_national, collapsed_national_day], ignore_index = True)
            collapsed_national[['geo', 'location']] = ['national', 'USA']
    else:
        collapsed_national = pd.DataFrame()
    
    if 'state' in geos: 
        states = data[data['st_resi'].isin(state_names.location)]
        if len(states) == 0:
            collapsed_state = pd.DataFrame()
        else:
            collapsed_state = states.groupby(util_id_cols + ['st_resi']).apply(aggregate_encounters_per_person).reset_index()
            if days_per_encounter and len(states.loc[~states['los'].isnull()]) > 0:
                ## drop NA los in this aggregation
                collapsed_state_day = states.loc[~states['los'].isnull()].groupby(util_id_cols + ['st_resi']).apply(aggregate_days_per_encounter).reset_index()
                collapsed_state_day = add_stdev_dpe(states, collapsed_state_day)
                collapsed_state = pd.concat([collapsed_state, collapsed_state_day], ignore_index = True)
            collapsed_state.rename(columns = {'st_resi': 'location'}, inplace = True)
            collapsed_state['geo'] = 'state'
    else:
        collapsed_state = pd.DataFrame()

    if 'county' in geos: 
        cntys = data[data['mcnty_resi'].isin(county_names.location)]
        if len(cntys) == 0:
            collapsed_cnty = pd.DataFrame()
        else:
            collapsed_cnty = cntys.groupby(util_id_cols + ['mcnty_resi']).apply(aggregate_encounters_per_person).reset_index()
            if days_per_encounter and len(cntys.loc[~cntys['los'].isnull()]) > 0:
                ## drop NA los in this aggregation
                collapsed_cnty_day = cntys.loc[~cntys['los'].isnull()].groupby(util_id_cols + ['mcnty_resi']).apply(aggregate_days_per_encounter).reset_index()
                collapsed_cnty_day = add_stdev_dpe(cntys, collapsed_cnty_day)
                collapsed_cnty = pd.concat([collapsed_cnty, collapsed_cnty_day], ignore_index = True)
            collapsed_cnty.rename(columns = {'mcnty_resi': 'location'}, inplace = True)
            collapsed_cnty['geo'] = 'county'
    else:
        collapsed_cnty = pd.DataFrame()

    ## combine
    collapsed = pd.concat([collapsed_cnty, collapsed_state, collapsed_national], ignore_index = True)

    ## dropping any NAs
    if 'metric' not in collapsed.columns: collapsed['metric'] = None
    if 'n_encounters' not in collapsed.columns: collapsed['n_encounters'] = None
    if 'n_days' not in collapsed.columns: collapsed['n_days'] = None
    if 'raw_val' not in collapsed.columns: collapsed['raw_val'] = None
    collapsed = collapsed[~((collapsed['metric'] == 'encounters_per_person') & (collapsed['n_encounters'].isnull()))]
    collapsed = collapsed[~((collapsed['metric'] == 'days_per_encounter') & (collapsed['raw_val'].isnull()))]

    ## add location name
    if 'location' not in collapsed.columns: collapsed['location'] = None
    usa_names = pd.DataFrame(data = {'location': ['USA'], 'location_name': ['USA']})
    names = pd.concat([usa_names, state_names, county_names], ignore_index = True)
    collapsed = pd.merge(collapsed, names, on = 'location', how = 'left')
    
    ## ensure no NA locations
    assert sum(collapsed['location'] == '-1') == 0
    assert sum(collapsed['location'].isnull()) == 0

    ## prep denominator file -------------------------------------------------
    ## aggregate geos for denom
    ## make fake group column to make groupby work in all cases
    denom['group_none'] = 'all'
    if 'national' in geos:
        denom_ntl_ids = [x for x in list(denom.columns) if x not in ['mcnty', 'state', 'denom']]
        denom_data_ntl = denom.groupby(denom_ntl_ids)['denom'].agg(['sum']).reset_index().rename(columns = {'sum': 'denom'})
        denom_data_ntl['location'] = 'USA'
    else: 
        denom_data_ntl = pd.DataFrame()
    if 'state' in geos:
        denom_st_ids = [x for x in list(denom.columns) if x not in ['mcnty', 'denom']]
        denom_data_st = denom[denom['state'].isin(state_names.location)].groupby(denom_st_ids)['denom'].agg(['sum']).reset_index().rename(columns = {'sum': 'denom', 'state': 'location'})
    else: 
        denom_data_st = pd.DataFrame()
    if 'county' in geos:
        denom_cnty_ids = [x for x in list(denom.columns) if x not in ['state', 'denom']]
        denom_data_cnty = denom[denom['mcnty'].isin(county_names.location)].groupby(denom_cnty_ids)['denom'].agg(['sum']).reset_index().rename(columns = {'sum': 'denom', 'mcnty': 'location'})
    else: 
        denom_data_cnty = pd.DataFrame()
    denom = pd.concat([denom_data_ntl, denom_data_st, denom_data_cnty], ignore_index = True)
    denom.drop(columns = ['group_none'], inplace = True)

    ## make sure no '-1' in denom location column
    assert sum(denom['location'] == '-1') == 0
            
    ## drop 0s and NULLS from denom (if any)
    denom = denom[(denom['denom'] > 0) & ~(denom['denom'].isnull())]
    
    ## split by metric
    out_epp = collapsed.loc[collapsed['metric'] == 'encounters_per_person'].reset_index(drop = True)
    out_dpe = collapsed.loc[collapsed['metric'] == 'days_per_encounter'].reset_index(drop = True)

    ## assert no missing data
    assert sum(out_epp['n_encounters'].isnull()) == 0
    assert sum(out_dpe['raw_val'].isnull()) == 0

    ## merge denom with data for encounters_per_person
    merge_ids = list(set(final_columns) & set(list(denom.columns)))
    for m in merge_ids:
        if m not in out_epp.columns: out_epp[m] = None
    if len(out_epp) == 0 and len(denom) == 0:
        out_epp = pd.concat([out_epp, denom], ignore_index = True)
    else:
        out_epp = pd.merge(out_epp, denom, on = merge_ids, how = 'outer')

    ## save out what's missing, then drop those rows
    ## if nothing is missing, we just have a row-count saved out
    if len(out_epp[~(out_epp['n_encounters'].isnull())]) > 0: ## if we have any encounters
        if source == 'MDCD' and mdcd_mc_combine: 
            mis_denom_cols = dp.create_group_cols(['toc', 'geo', 'year_id', 'pri_payer', 'age_group_years_start', 'sex_id'], by_race)
        else:
            mis_denom_cols = dp.create_group_cols(['toc', 'geo', 'year_id', 'pri_payer', 'mc_ind', 'age_group_years_start', 'sex_id'], by_race)
        total = out_epp.groupby(mis_denom_cols)['n_encounters'].agg('sum').reset_index().rename(columns = {'n_encounters': 'n'})
        missing = out_epp.loc[out_epp['denom'].isnull()].groupby(mis_denom_cols)['n_encounters'].agg('sum').reset_index().rename(columns = {'n_encounters': 'n_missing_denom'})
        missing_denom = pd.merge(total, missing, on = mis_denom_cols, how = 'outer')
        #the missing dataframe will be empty when there is no missing denom, so set NA's to 0 so that in the missing_denom excel,
        # these combinations will show 0% missing instead of not be included at all
        if missing.empty:
            missing_denom['n_missing_denom'].fillna(0, inplace= True)
        out_epp = out_epp[~out_epp['denom'].isnull()]
        if source == 'MDCD' and mdcd_mc_combine:
            missing_denom['mc_ind'] = 999 ## numeric NA
    else:
        missing_denom = pd.DataFrame()

    ## if we have denom but no encounters, 
    ## set encounters_per_person to 0
    if sum(out_epp['n_encounters'].isnull()) > 0:

        ## just drop zeros for certain sources        
        if source in ['KYTHERA', 'NIS', 'NEDS', 'SIDS', 'SEDD']:

            out_epp = out_epp[~out_epp['n_encounters'].isnull()]

        ## for other sources, assert zeros
        else:

            ## split out denom for missing data
            set_0 = out_epp[out_epp['n_encounters'].isnull()][merge_ids + ['denom']]
            out_epp = out_epp[~out_epp['n_encounters'].isnull()]
            
            ## set ID columns 
            set_0[['toc', 'year_id', 'acause', 'age_group_years_start', 'sex_id', 'metric']] = toc, year, acause, age, sex, 'encounters_per_person'
            
            ## merge in location names and geos
            cn = county_names.copy()
            sn = state_names.copy()
            cn['geo'] = 'county'
            sn['geo'] = 'state'
            locations = pd.concat([cn, sn, pd.DataFrame({'location': ['USA'], 'location_name': ['USA'], 'geo': ['national']})], ignore_index = True)
            set_0 = pd.merge(set_0, locations, on = ['location'], how = 'left')

            ## set value columns
            set_0[['n_encounters', 'n_obs']] = 0
            set_0[['raw_val', 'std', 'n_days']] = np.nan

            ## for MDCR, MDCD we ONLY want to assert 0s for pri_payer = mdcr and pri_payer = mdcd, respectively
            if source in ['MDCR', 'CHIA_MDCR']:
                set_0 = set_0[set_0['pri_payer'] == 1]
            elif source == 'MDCD':
                set_0 = set_0[set_0['pri_payer'] == 3]

            ## MDCR and MDCD will have mc_ind in the denom, so mc_ind will be in set_0
            ## other sources won't have mc_ind in the deno, so we'll need to fill it in...
            ## BUT other sources will always have mc_ind == 0
            if 'mc_ind' not in set_0.columns and source not in ['MDCR', 'MDCD', 'CHIA_MDCR']:
                set_0['mc_ind'] = 0

            ## recombine with out_epp
            datacols = list(out_epp.columns)
            zerocols = list(set_0.columns)
            datacols.sort()
            zerocols.sort()
            if len(out_epp) > 0: ## otherwise it's missing cols
                assert datacols == zerocols
            out_epp = pd.concat([out_epp, set_0], ignore_index = True)

    ## prep values for 'encounters_per_person'
    ##  - n = denominator (need to merge in)
    ##  - raw_val = cell-level encounters_per_person = cell-level encounter counts/denom = (X/n)
    ##  - std = std of raw_val across counties
    if 'std' in out_epp.columns:
        out_epp.drop(columns = ['std'], inplace = True)  ## drop std column, which was produced only for 'days_per_encounter'
    out_epp.rename(columns = {'denom': 'n_people'}, inplace = True)
    out_epp['raw_val'] = out_epp['n_encounters']/out_epp['n_people']
    
    ## fill missing cols
    for m in util_id_cols:
        if m not in out_epp.columns: out_epp[m] = None
    if 'std' not in out_epp.columns: out_epp['std'] = None
    if 'std' not in out_dpe.columns: out_dpe['std'] = None
    if 'n_people' not in out_epp.columns: out_epp['n_people'] = None
    if 'n_obs' not in out_dpe.columns: out_dpe['n_obs'] = None
    if 'n_days' not in out_epp.columns: out_epp['n_days'] = None

    ## if only national, replace STD with 0 & impute in repartition
    if 'geo' not in out_epp.columns: out_epp['geo'] = None
    if all([x == 'national' for x in geos]):
        out_epp['std'] = 0 ## replace with 0, fill later

    else:
        ## take STD across most granular geography
        if 'county' in geos: 
            out_epp_geo_std = out_epp[out_epp['geo'] == 'county']
        elif 'state' in geos:
            out_epp_geo_std = out_epp[out_epp['geo'] == 'state']
        else:
            out_epp_geo_std = out_epp[out_epp['geo'] == 'national']

        ## calculate std
        out_epp_geo_std['std'] = out_epp_geo_std.groupby(util_id_cols)['raw_val'].transform(lambda x: np.std(x))
        out_epp_geo_std = out_epp_geo_std[util_id_cols + ['std']]

        ## count number of n locs by row
        out_epp_geo_std = out_epp_geo_std.value_counts().reset_index().rename(columns = {0: 'n_locs'})

        ## merge in
        if 'std' in out_epp.columns: out_epp.drop(columns = 'std', inplace = True)
        out_epp = pd.merge(out_epp, out_epp_geo_std, on = util_id_cols, how = 'left')

        ## if we had NO locations, set n_locs 0
        out_epp.loc[out_epp['n_locs'].isnull(), 'n_locs'] = 0
        
        ## if we only had one (or zero) locations, set STD to 2.6*raw_val
        if 'std' not in out_epp.columns: out_epp['std'] = None
        if 'n_locs' not in out_epp.columns: out_epp['n_locs'] = None
        out_epp['std'] = np.where(out_epp['n_locs'] <= 1, out_epp['raw_val']*2.6, out_epp['std'])
        out_epp.drop(columns = 'n_locs', inplace = True)

    ## calculate SE
    out_epp['se'] = out_epp['std']/(out_epp['n_people']**(1/2))
    out_dpe['se'] = out_dpe['std']/(out_dpe['n_obs']**(1/2))
    
    #All MDCD >=65 will only be mdcr-mdcd, so don't include any denoms for just mdcd for this age group
    if source == 'MDCD' and toc == 'RX' and age >= 65:
        out_epp = out_epp[out_epp['pri_payer'] != 3]

    ## recombine by metric
    collapsed = pd.concat([out_epp, out_dpe], ignore_index = True)

    ## recode primary payer from numeric code to human readable payer
    collapsed = collapsed.replace({'pri_payer': pri_payer_ids})
    missing_denom = missing_denom.replace({'pri_payer': pri_payer_ids})

    ## make 'n' columns float (since it's sometimes a fraction)
    collapsed['n_obs'] = collapsed['n_obs'].astype(float)
    collapsed['n_encounters'] = collapsed['n_encounters'].astype(float)
    collapsed['n_people'] = collapsed['n_people'].astype(float)

    ## whether to duplicate utilization data out across mc_ind
    if source == 'MDCD' and mdcd_mc_combine: 
        collapsed_copy = collapsed.copy()
        collapsed['mc_ind'] = 0
        collapsed_copy['mc_ind'] = 1
        collapsed_copy = collapsed_copy[~(collapsed_copy['pri_payer'].isin(['priv', 'oop']))] ## can't have priv/oop where mc_ind == 1
        collapsed = pd.concat([collapsed, collapsed_copy], ignore_index = True)

    ## finalize datasets
    collapsed.reset_index(inplace = True, drop = True)
    collapsed['payer'] = 'na' ## no payer for utilization, but need a column
    collapsed['dataset'] = source
    missing_denom['dataset'] = source
    collapsed = collapsed[final_columns]

    ## set some types
    for cl in ['raw_val', 'se', 'n_obs', 'n_encounters', 'n_people', 'n_days']:
        collapsed[cl] = collapsed[cl].astype('float64')

    ## UTILIZATION CHECKS -------------------------------------------------
    ## [raw_value] 
    ## ensure no NAs/infs/negatives (can have zero)
    assert sum(collapsed['raw_val'].isnull()) == 0
    assert sum(np.isinf(collapsed['raw_val'])) == 0
    assert sum(collapsed['raw_val'] < 0) == 0
    ## [se]
    ## ensure no NAs/infs/negatives (can have zero for now -- these get fixed in repartition.py)
    collapsed['se'].fillna(0, inplace=True)
    collapsed['se']=np.where(((collapsed['se'] == np.inf) |(collapsed['se'] == -np.inf)), 0,collapsed['se'])
    assert sum(collapsed['se'].isnull()) == 0
    assert sum(np.isinf(collapsed['se'])) == 0
    assert sum(collapsed['se'] < 0) == 0
    ## [n_obs]
    ## ensure no NAs/infs/negatives (can have zero)
    assert sum(collapsed['n_obs'].isnull()) == 0
    assert sum(np.isinf(collapsed['n_obs'])) == 0
    assert sum(collapsed['n_obs'] < 0) == 0
    ## [n_encounters]
    ## ensure no NAs/infs/negatives (can have zero)
    assert sum(collapsed['n_encounters'].isnull()) == 0
    assert sum(np.isinf(collapsed['n_encounters'])) == 0
    assert sum(collapsed['n_encounters'] < 0) == 0
    ## METRIC-SPECIFIC UTILIZATION CHECKS -------------------------------------------------
    ## [n_people] -- only for encounters_per_person
    ## ensure no NAs/infs/negatives/0s
    assert sum((collapsed['metric'] == 'encounters_per_person') & (collapsed['n_people'].isnull())) == 0
    assert sum((collapsed['metric'] == 'encounters_per_person') & (np.isinf(collapsed['n_people']))) == 0
    assert sum((collapsed['metric'] == 'encounters_per_person') & (collapsed['n_people'] <= 0)) == 0
    ## ensure no 0s in raw_val for days_per_encounter (we dropped all zeros for los, so we couldn't have days_per_encounter = 0)
    collapsed = collapsed.drop(collapsed[(collapsed['metric'] == 'days_per_encounter') & (collapsed['raw_val'] == 0)].index)
    if days_per_encounter:
        assert sum((collapsed['metric'] == 'days_per_encounter') & (collapsed['raw_val'] == 0)) == 0

    ## ensure no mc_ind == 1 in pri_payer oop or priv 
    assert sum((collapsed['pri_payer'].isin(['priv', 'oop'])) & (collapsed['mc_ind'] == 1)) == 0

    ## reindex
    collapsed.reset_index(inplace = True, drop = True)
    missing_denom.reset_index(inplace = True, drop = True)

    return collapsed, missing_denom

def collapse_util_hcci(data):

    ## refresh global constants
    from helpers.CollapseConstants import (
        final_columns
    )

    ## select columns
    ## add days column
    data['n_days'] = np.nan
    util_data = data[final_columns]
    util_data = util_data[util_data['metric'].isin(['encounters_per_person','days_per_encounter'])]

    #Make all payers = 'na', drop duplicate rows
    util_data['payer'] = 'na'
    util_data.drop_duplicates(inplace=True)

    ## [raw_value] -ensure no NAs/infs/negatives (can have zero)
    ## [se]-ensure no NAs/infs/negatives (can have zero for now -- these get fixed in repartition.py)
    # Drop infinite values
    util_data = util_data.replace([np.inf, -np.inf], np.nan).dropna(subset=['raw_val', 'se','n_obs','n_encounters'])
    # Drop NaNs
    util_data = util_data.dropna(subset=['raw_val', 'se','n_obs','n_encounters'])
    # Drop negative values
    util_data = util_data[(util_data['raw_val'] >= 0) & (util_data['se'] >= 0)& (util_data['n_obs'] >= 0)& (util_data['n_encounters'] >= 0)]

    ## METRIC-SPECIFIC UTILIZATION -------------------------------------------------
    ## [n_people] -- only for encounters_per_person
    ## ensure no NAs/infs/negatives/0s
    # Drop instances where metric is 'encounters_per_person' and n_people is null
    util_data = util_data[~((util_data['metric'] == 'encounters_per_person') & (util_data['n_people'].isnull()))]
    # Drop instances where metric is 'encounters_per_person' and n_people is infinite
    util_data = util_data[~((util_data['metric'] == 'encounters_per_person') & (np.isinf(util_data['n_people'])))]
    # Drop instances where metric is 'encounters_per_person' and n_people is less than or equal to 0
    util_data = util_data[~((util_data['metric'] == 'encounters_per_person') & (util_data['n_people'] <= 0))]

    # Ensure no 0s in raw_val for days_per_encounter
    if 'days_per_encounter' in util_data['metric'].unique():
        util_data = util_data[~((util_data['metric'] == 'days_per_encounter') & (util_data['raw_val'] == 0))]

    # make sure no mc_ind in output for non-'mdcr'/'mdcd'
    util_data = util_data[~((util_data['mc_ind'] == 1) & (~util_data['pri_payer'].isin(['mdcr', 'mdcd'])))]

    # UTILIZATION CHECKS -------------------------------------------------
    # [raw_value]
    # ensure no NAs/infs/negatives (can have zero)
    assert sum(util_data['raw_val'].isnull()) == 0
    assert sum(np.isinf(util_data['raw_val'])) == 0
    assert sum(util_data['raw_val'] < 0) == 0

    # [se]
    # ensure no NAs/infs/negatives (can have zero for now -- these get fixed in repartition.py)
    assert sum(util_data['se'].isnull()) == 0
    assert sum(np.isinf(util_data['se'])) == 0
    assert sum(util_data['se'] < 0) == 0

    # [n_obs]
    # ensure no NAs/infs/negatives (can have zero)
    assert sum(util_data['n_obs'].isnull()) == 0
    assert sum(np.isinf(util_data['n_obs'])) == 0
    assert sum(util_data['n_obs'] < 0) == 0

    # [n_encounters]
    # ensure no NAs/infs/negatives (can have zero)
    assert sum(util_data['n_encounters'].isnull()) == 0
    assert sum(np.isinf(util_data['n_encounters'])) == 0
    assert sum(util_data['n_encounters'] < 0) == 0

    # METRIC-SPECIFIC UTILIZATION CHECKS -------------------------------------------------
    # [n_people] -- only for encounters_per_person
    # ensure no NAs/infs/negatives/0s
    assert sum((util_data['metric'] == 'encounters_per_person') & (util_data['n_people'].isnull())) == 0
    assert sum((util_data['metric'] == 'encounters_per_person') & (np.isinf(util_data['n_people']))) == 0
    assert sum((util_data['metric'] == 'encounters_per_person') & (util_data['n_people'] <= 0)) == 0

    # ensure no 0s in raw_val for days_per_encounter (we dropped all zeros for los, so we couldn't have days_per_encounter = 0)
    #if days_per_encounter:
    assert sum((util_data['metric'] == 'days_per_encounter') & (util_data['raw_val'] == 0)) == 0

    # mc_ind checks (this bugged before)
    assert all(util_data.loc[util_data['mc_ind'] == 1, 'pri_payer'].isin(['mdcr', 'mdcd']))

    ## reindex
    util_data.reset_index(inplace = True, drop = True)

    return util_data
