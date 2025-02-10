## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that calculate spending-per-encounter fractions for modeling
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
from statsmodels.stats.weightstats import DescrStatsW
## custom functions
from helpers import (
    CollapseDataPrep as dp
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def collapse_price(data, metadata, source, toc, age, weight_meps, mdcr_dual_combine, by_race):

    ## refresh global constants
    from helpers.CollapseConstants import (
        pay_cols,
        pri_payer_ids, 
        state_names, 
        county_names, 
        final_columns
    )

    ## make sure final columns are prepped
    final_columns = dp.create_group_cols(final_columns, by_race)
    
    ## ensure object (string) types
    assert data['mcnty_resi'].dtype == object
    assert county_names['location'].dtype == object
    ## ensure no decimal places 
    assert sum(data['mcnty_resi'].str.contains('\\.')) == 0
    assert sum(county_names['location'].str.contains('\\.')) == 0

    ## constants -------------------------------------------------
    geos = metadata['geos'][source]
    if by_race: ## no county for race
        geos = [x for x in geos if x != 'county']

    ## id columns
    price_id_cols = dp.create_group_cols(['toc', 'sex_id', 'acause', 'pri_payer', 'age_group_years_start', 'year_id', 'mc_ind'], by_race)
    ## location columns
    if all([x == 'national' for x in geos]):
        price_loc_cols = []
    elif (all([x in ['state', 'national'] for x in geos])) & (source != 'HCCI'):
        price_loc_cols = ['st_resi']
    elif source == 'HCCI':
        price_loc_cols = []
    else: 
        price_loc_cols = ['st_resi', 'mcnty_resi']
    ## extra columns
    price_extra_cols = ['admission_count', 'admission_flag', 'sub_dataset']
    ## whether we calculate spend_per_day
    spend_per_day = toc in ['RX'] 
    if (spend_per_day) & (source != 'HCCI'):
        price_extra_cols = price_extra_cols + ['los'] ## days_supply is named los for RX
    ## whether we use survey weights
    if weight_meps:
        use_weight = source in ['MEPS'] ## turn on use_weight if source == MEPS
    else:
        use_weight = False ## never use weight
    if use_weight:
        price_extra_cols = price_extra_cols + ['survey_wt']
        
    ## functions to summarize data -------------------------------------------------
    ## 'spend_per_encounter'
    ##  - raw_val = total spend / total encounters
    ##      -> total encounters = the sum of the admission_count
    ##  - n = # observations
    def aggregate_spend_per_encounter(group, weight = use_weight):
        if weight:
            return pd.Series({
                'raw_val': sum(group.spend*group.survey_wt)/sum(group.admission_count*group.survey_wt) if sum(group.admission_count*group.survey_wt) else np.nan, ## if non-zero denom, otherwise NA
                'n_encounters' : sum(group.admission_count*group.survey_wt),
                'n_obs': len(group), 
                'metric': 'spend_per_encounter'
            })
        else:
            return pd.Series({
                'raw_val': sum(group.spend)/sum(group.admission_count) if sum(group.admission_count) else np.nan, ## if non-zero denom, otherwise NA
                'n_encounters' : sum(group.admission_count),
                'n_obs': len(group), 
                'metric': 'spend_per_encounter'
            })
    #Standard dev needs to be a different aggregation, so need to group in another function
    ##  - std = std of spend across all locations
    ##  - n = # observations
    def stdev_spend_per_encounter(group, weight = use_weight):
        if weight:
            return pd.Series({
                'std': DescrStatsW(group.spend, weights=group.survey_wt, ddof=1).std,
                'n_obs_stdev': len(group), 
                'metric': 'spend_per_encounter'
            })
        else:
            return pd.Series({
                'std': np.std(group.spend),
                'n_obs_stdev': len(group),  
                'metric': 'spend_per_encounter'
            })

    ## 'spend_per_day'
    ##  - raw_val = total spend / total los
    ##      -> total los = the sum of los where we had admissions (where admission flag == 1)
    ##      -> since each los already incorporates admission_count
    ##  - n = # observations
    def aggregate_spend_per_day(group, weight = use_weight):
        if weight:
            return pd.Series({
                'raw_val': sum(group.spend*group.survey_wt)/sum(group.los*group.admission_flag*group.survey_wt) if sum(group.los*group.admission_flag*group.survey_wt) else np.nan,  ## if non-zero denom, otherwise NA
                'n_encounters' : sum(group.admission_count*group.survey_wt),
                'n_obs': len(group), 
                'n_days': sum(group.los*group.admission_flag*group.survey_wt),
                'spend': sum(group.spend*group.survey_wt),
                'metric': 'spend_per_day'
            })
        else:
            return pd.Series({
                'raw_val': sum(group.spend)/sum(group.los*group.admission_flag) if sum(group.los*group.admission_flag) else np.nan, ## if non-zero denom, otherwise NA
                'n_encounters' : sum(group.admission_count),
                'n_obs': len(group), 
                'n_days': sum(group.los*group.admission_flag),
                'spend': sum(group.spend),
                'metric': 'spend_per_day'
            })

    ## function for spend_per_day standard deviation
    ##  - std = std of (spend/los) across all locations
    ##  - n = # observations
    def stdev_spend_per_day(group, weight = use_weight):
        if weight:
            return pd.Series({
                'std': DescrStatsW(group.spend_perday, weights=group.survey_wt, ddof=1).std,
                'n_obs_stdev': len(group), 
                'metric': 'spend_per_day'
            })
        else:
            return pd.Series({
                'std': np.std(group.spend_perday),
                'n_obs_stdev': len(group),  
                'metric': 'spend_per_day'
            })
        
    ## Adding standard deviation to spend 
    ## (std is calculated across locations, so we need to add it in separately)
    def add_stdev_spend(df, df_collapsed, function):
        if 'location' in df:
            df.drop(columns=['location'], axis=1, inplace=True)
        df_stdev = df.groupby(price_id_cols + ['payer']).apply(function).reset_index()
        group_cols = dp.create_group_cols(['toc', 'sex_id', 'acause', 'pri_payer', 'payer','age_group_years_start','year_id', 'mc_ind','metric'], by_race)
        df_collapsed = pd.merge(df_collapsed, df_stdev, on = group_cols, how = 'left')
        df_collapsed['std'] = np.where(df_collapsed['n_obs_stdev'] == 1, df_collapsed['raw_val']*2.6, df_collapsed['std'])
        #drop n_obs_stdev:
        df_collapsed.drop(columns=['n_obs_stdev'], axis=1, inplace=True)

        return df_collapsed
    
    ## function to count missing_spend (0 or na)
    def summarize_missing_spend(group):
        return pd.Series({
            'n': len(group),
            'n_na': sum(group.spend.isnull()),
            'n_zero': sum(group.spend == 0)
        })
    
    ## set up data -------------------------------------------------

    ## recode 0 and negative LOS
    ## already done in CAUSEMAP, just ensuring
    if spend_per_day and source != 'HCCI':
        data.loc[data['los'] == 0, 'los'] = 1
        data.loc[data['los'] < 0, 'los'] = np.nan
    
    if source != 'HCCI':
        ## simplify data
        data = data[price_id_cols + price_loc_cols + price_extra_cols + pay_cols]
            
    if source == 'HCCI': ## HCCI already collapsed, so just need missing spend
        ## filter data to just spend metrics
        data = data[data['metric'].isin(['spend_per_encounter', 'spend_per_day'])]
        ## add days column
        data['n_days'] = np.nan
        OUT = data[final_columns]
        
        MISSING_SPEND = pd.DataFrame()
        for p in pay_cols:

            ## rename $ column
            tmp = data.copy()
            tmp.rename(columns = {p: 'spend'}, inplace = True)
            payer = p.split('_')[0]
            tmp['payer'] = payer

            ## recode negative spend
            tmp.loc[tmp['spend'] < 0, 'spend'] = np.nan

            ## ensure mc_ind is not 1 if pri_payer is oop/priv
            tmp.loc[(tmp['pri_payer'].isin(['priv','oop'])), 'mc_ind'] = 0
            
            ## reassign pri_payer
            tmp = dp.reassign_pri_payer(tmp, source, age, mdcr_dual_combine, step = 'price') 
            
            ## count missing_spend (0 or na)
            if len(tmp) > 0:
                missing_spend = tmp.groupby(['sub_dataset', 'toc', 'year_id', 'pri_payer', 'mc_ind', 'payer', 'age_group_years_start', 'sex_id']).apply(summarize_missing_spend).reset_index()
                MISSING_SPEND = pd.concat([MISSING_SPEND, missing_spend], ignore_index = True)
                
        # Drop infinite values
        OUT = OUT.replace([np.inf, -np.inf], np.nan).dropna(subset=['raw_val'])
        # Drop NaNs
        OUT = OUT.dropna(subset=['raw_val'])
        # Drop negative values - can have 0s
        OUT = OUT[OUT['raw_val'] >= 0]
        
        ## [se]
        ## ensure no NAs/infs/negatives (can have zero for now -- these get fixed in repartition.py)
        # Drop infinite values
        OUT = OUT.replace([np.inf, -np.inf], np.nan).dropna(subset=['se'])
        # Drop NaNs
        OUT = OUT.dropna(subset=['se'])
        # Drop negative values - can have 0s
        OUT = OUT[OUT['se'] >= 0]
        ## [n_obs]
        ## ensure no NAs/infs/negatives/0s
        # Drop infinite values
        OUT = OUT.replace([np.inf, -np.inf], np.nan).dropna(subset=['n_obs'])
        # Drop NaNs
        OUT = OUT.dropna(subset=['n_obs'])
        # Drop negative values - can have 0s
        OUT = OUT[OUT['n_obs'] > 0]

        ## [n_encounters]
        ## ensure no NAs/infs/negatives/0s
        # Drop infinite values
        OUT = OUT.replace([np.inf, -np.inf], np.nan).dropna(subset=['n_encounters'])
        # Drop NaNs
        OUT = OUT.dropna(subset=['n_encounters'])
        # Drop negative values - can have 0s
        OUT = OUT[OUT['n_encounters'] > 0]
        
    else: ## for non-HCCI

        ## aggregate price  -------------------------------------------------
        ## loop through price columns
        OUT = pd.DataFrame()
        MISSING_SPEND = pd.DataFrame()
        for p in pay_cols:

            ## rename $ column
            tmp = data.copy()
            tmp.rename(columns = {p: 'spend'}, inplace = True)
            payer = p.split('_')[0]
            tmp['payer'] = payer

            ## recode negative spend
            tmp.loc[tmp['spend'] < 0, 'spend'] = np.nan

            ## ensure mc_ind is not 1 if pri_payer is oop/priv
            tmp.loc[(tmp['pri_payer'].isin([2,4])), 'mc_ind'] = 0
            
            ## reassign pri_payer
            tmp = dp.reassign_pri_payer(tmp, source, age, mdcr_dual_combine, step = 'price') 

            ## count missing_spend (0 or na)
            if len(tmp) > 0:
                group_cols = dp.create_group_cols(['sub_dataset', 'toc', 'year_id', 'pri_payer', 'mc_ind', 'payer', 'age_group_years_start', 'sex_id'], by_race)
                missing_spend = tmp.groupby(group_cols).apply(summarize_missing_spend).reset_index()
                MISSING_SPEND = pd.concat([MISSING_SPEND, missing_spend], ignore_index = True)

            ## drop all spend NAs
            tmp = tmp[~tmp['spend'].isnull()]
            
            ## drop sub_dataset now that we've counted NAs/Zeros by that
            tmp.drop(columns = ['sub_dataset'], inplace = True)

            ## make spend_perday if applicable
            if spend_per_day and len(tmp) > 0:
                tmp['spend_perday'] = tmp['spend']/tmp['los']

            ## aggregate price
            if 'national' in geos: 
                if len(tmp) == 0:
                    collapsed_national = pd.DataFrame()
                else:
                    collapsed_national = tmp.groupby(price_id_cols + ['payer']).apply(aggregate_spend_per_encounter).reset_index()
                    #add in standard dev
                    collapsed_national = add_stdev_spend(tmp, collapsed_national, stdev_spend_per_encounter)
                    if spend_per_day and len(tmp.loc[~tmp['spend_perday'].isnull()]) > 0:
                        ## drop NA spend_perday in this aggregation
                        collapsed_national_day = tmp.loc[~tmp['spend_perday'].isnull()].groupby(price_id_cols + ['payer']).apply(aggregate_spend_per_day).reset_index()
                        df=tmp
                        df_collapsed = collapsed_national_day
                        function = stdev_spend_per_day
                        if 'location' in df:
                            df.drop(columns=['location'], axis=1, inplace=True)
                        df_stdev = df.groupby(price_id_cols + ['payer']).apply(function).reset_index()
                        group_cols = dp.create_group_cols(['toc', 'sex_id', 'acause', 'pri_payer', 'payer','age_group_years_start', 'year_id', 'mc_ind','metric'], by_race)
                        df_collapsed = pd.merge(df_collapsed, df_stdev, on = group_cols, how = 'left')
                        df_collapsed['std'] = np.where(df_collapsed['n_obs_stdev'] == 1, df_collapsed['raw_val']*2.6, df_collapsed['std'])
                        #drop n_obs_stdev:
                        df_collapsed.drop(columns=['n_obs_stdev'], axis=1, inplace=True)
                        collapsed_national = pd.concat([collapsed_national, df_collapsed], ignore_index = True)#collapsed_national_day
                    collapsed_national[['geo', 'location']] = ['national', 'USA']
            else:
                collapsed_national = pd.DataFrame()

            if 'state' in geos: 
                states = tmp[tmp['st_resi'].isin(state_names.location)]
                if len(states) == 0:
                    collapsed_state = pd.DataFrame()
                else:
                    collapsed_state = states.groupby(price_id_cols + ['st_resi', 'payer']).apply(aggregate_spend_per_encounter).reset_index()
                    #add in standard dev
                    collapsed_state = add_stdev_spend(states, collapsed_state, stdev_spend_per_encounter)
                    if spend_per_day and len(states.loc[~states['spend_perday'].isnull()]) > 0:
                        ## drop NA spend_perday in this aggregation
                        collapsed_state_day = states.loc[~states['spend_perday'].isnull()].groupby(price_id_cols + ['st_resi', 'payer']).apply(aggregate_spend_per_day).reset_index()
                        collapsed_state_day = add_stdev_spend(states, collapsed_state_day, stdev_spend_per_day)
                        collapsed_state = pd.concat([collapsed_state, collapsed_state_day], ignore_index = True)#collapsed_national_day
                    collapsed_state.rename(columns = {'st_resi': 'location'}, inplace = True)
                    collapsed_state['geo'] = 'state'
            else:
                collapsed_state = pd.DataFrame()

            if 'county' in geos: 
                cntys = tmp[tmp['mcnty_resi'].isin(county_names.location)]
                if len(cntys) == 0:
                    collapsed_cnty = pd.DataFrame()
                else:
                    collapsed_cnty = cntys.groupby(price_id_cols + ['mcnty_resi', 'payer']).apply(aggregate_spend_per_encounter).reset_index()
                    #add in standard dev
                    collapsed_cnty = add_stdev_spend(cntys, collapsed_cnty, stdev_spend_per_encounter)
                    if spend_per_day and len(cntys.loc[~cntys['spend_perday'].isnull()]) > 0:
                        ## drop NA spend_perday in this aggregation
                        collapsed_cnty_day = cntys.loc[~cntys['spend_perday'].isnull()].groupby(price_id_cols + ['mcnty_resi', 'payer']).apply(aggregate_spend_per_day).reset_index()
                        collapsed_cnty_day = add_stdev_spend(cntys, collapsed_cnty_day, stdev_spend_per_day)
                        collapsed_cnty = pd.concat([collapsed_cnty, collapsed_cnty_day], ignore_index = True)#collapsed_national_day
                    collapsed_cnty.rename(columns = {'mcnty_resi': 'location'}, inplace = True)
                    collapsed_cnty['geo'] = 'county'
            else:
                collapsed_cnty = pd.DataFrame()

            ## combine
            collapsed = pd.concat([collapsed_cnty, collapsed_state, collapsed_national], ignore_index = True)

            ## only continue if there are rows
            if len(collapsed) > 0:

                ## drop any NAs
                collapsed = collapsed[~(collapsed['raw_val'].isnull())]

                ## only continue if there are rows
                if len(collapsed) > 0:
                    ## add location name
                    usa_names = pd.DataFrame(data = {'location': ['USA'], 'location_name': ['USA']})
                    names = pd.concat([usa_names, state_names, county_names], ignore_index = True)
                    collapsed = pd.merge(collapsed, names, on = 'location', how = 'left')

                    ## ensure no NA locations
                    assert sum(collapsed['location'] == '-1') == 0
                    assert sum(collapsed['location'].isnull()) == 0

                    ## convert std to se
                    collapsed['se'] = collapsed['std']/(collapsed['n_obs']**(1/2))

                    ## finalize dataset
                    collapsed.reset_index(inplace = True, drop = True)
                    collapsed['dataset'] = source
                    collapsed['n_people'] = np.nan ## no denom tracked here
                    if 'n_days' not in collapsed.columns: collapsed['n_days'] = None
                    collapsed = collapsed[final_columns]

                    ## append to OUT
                    OUT = pd.concat([OUT, collapsed], ignore_index = True)
    
    if len(OUT) > 0:
        ## recode primary payer
        OUT = OUT.replace({'pri_payer': pri_payer_ids})

        ## set some types
        for cl in ['raw_val', 'se', 'n_obs', 'n_encounters', 'n_people', 'n_days']:
            OUT[cl] = OUT[cl].astype('float64')

        ## PRICE CHECKS -------------------------------------------------
        ## [raw_value] 
        ## ensure no NAs/infs/negatives (can have zero)
        assert sum(OUT['raw_val'].isnull()) == 0
        assert sum(np.isinf(OUT['raw_val'])) == 0
        assert sum(OUT['raw_val'] < 0) == 0
        ## [se]
        ## ensure no NAs/infs/negatives (can have zero for now -- these get fixed in repartition.py)
        assert sum(OUT['se'].isnull()) == 0
        assert sum(np.isinf(OUT['se'])) == 0
        assert sum(OUT['se'] < 0) == 0
        ## [n_obs]
        ## ensure no NAs/infs/negatives/0s
        assert sum(OUT['n_obs'].isnull()) == 0
        assert sum(np.isinf(OUT['n_obs'])) == 0
        assert sum(OUT['n_obs'] <= 0) == 0
        ## [n_encounters]
        ## ensure no NAs/infs/negatives/0s
        assert sum(OUT['n_encounters'].isnull()) == 0
        assert sum(np.isinf(OUT['n_encounters'])) == 0
        assert sum(OUT['n_encounters'] <= 0) == 0
        ## --------------------------------------------------------

    ## add dataset col to missing_spend and recode pri_payer
    if len(MISSING_SPEND) > 0:
        MISSING_SPEND['dataset'] = source
        MISSING_SPEND = MISSING_SPEND.replace({'pri_payer': pri_payer_ids})

    ## reindex
    OUT.reset_index(inplace = True, drop = True)
    MISSING_SPEND.reset_index(inplace = True, drop = True)

    return OUT, MISSING_SPEND
