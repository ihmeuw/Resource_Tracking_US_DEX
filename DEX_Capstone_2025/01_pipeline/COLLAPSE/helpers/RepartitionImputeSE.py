## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that impute missing or zero-valued standard errors
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
from math import isnan

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def impute_se0(data, acause, toc, metric, by_race):

    ## set value to continue
    CONTINUE = True

    ## track length of data
    old_len = len(data)

    ## need to fill places where SE == 0
    if sum(data['se'] == 0) == 0:
        print('  > No missing (0-valued) SEs')
        imputed_se = []
    else:
        print('  > Imputing')

        ## FILL SE
        ## For a given sex/pri_payer/payer/dataset/year/location (and of course, toc/cause/metric, which are parallelized over)
        ## Take the mean of (SE/raw_val) 'across all' ages (where SE isn't zero)
        ## If all ages missing (0), take the mean 'across all' ages/years/datasets (where SE isn't zero)
        ## If still all missing (0), take the mean  “across all” ages/years/datasets/sexes (where SE isn't zero)
        ## If still all missing (0), take the mean  “across all” ages/years/datasets/sexes/primary payers (where SE isn't zero)
        ## If still all missing (0), take the mean  “across all” ages/years/datasets/sexes/primary payers/geos (where SE isn't zero)
        ## If still all missing (0), take the mean  “across all” ages/years/datasets/sexes/primary payers/geos/payers (i.e. everything for that toc/cause/metric) (where SE isn't zero)
        ## -> Fill the missing (0) valued se with the least aggregated mean

        ## determine groups - if producing by race, add race_cd to all the groups
        if by_race:
            group_0 = ['sex_id', 'pri_payer', 'payer', 'year_id', 'geo', 'dataset', 'race_cd']
        else:
            group_0 = ['sex_id', 'pri_payer', 'payer', 'year_id', 'geo', 'dataset'] ## just repeat group_1 to keep code simple
        group_1 = ['sex_id', 'pri_payer', 'payer', 'year_id', 'geo', 'dataset']
        group_2 = ['sex_id', 'pri_payer', 'payer', 'geo']
        group_3 = ['pri_payer', 'payer', 'geo']
        group_4 = ['payer', 'geo']
        group_5 = ['payer']

        ## split out what we're imputing
        impute_se = data[data['se'] == 0].reset_index(drop = True)
        impute_from_se = data[data['se'] > 0].reset_index(drop = True)
        
        if len(impute_from_se)==0:

            ## for RE, this one hit this condition
            if acause == 'mental_conduct' and toc == 'RX' and metric == 'encounters_per_person':
                print('    >>> We expect not to be able to impute this one.')
                CONTINUE = False
                imputed_se = [0]
            else:
                raise NotImplementedError('We have no non-zero SEs. Unable to impute (' + " acause: " + acause + " toc: " + toc + " metric: " + metric, ")")
        else:

            ## calculate relative SE
            impute_from_se['rel_se'] = impute_from_se['se']/impute_from_se['raw_val']
            ## for raw_val == 0, set rel_se to se
            impute_from_se.loc[impute_from_se['raw_val'] == 0, 'rel_se'] = impute_from_se.loc[impute_from_se['raw_val'] == 0, 'se']

            ## calculate mean relative SE for each group (dropping NAs in calculation)
            imputer0 = impute_from_se.groupby(group_0)['rel_se'].mean().reset_index().rename(columns = {'rel_se': 'mean_rel_se_0'})
            imputer1 = impute_from_se.groupby(group_1)['rel_se'].mean().reset_index().rename(columns = {'rel_se': 'mean_rel_se_1'})
            imputer2 = impute_from_se.groupby(group_2)['rel_se'].mean().reset_index().rename(columns = {'rel_se': 'mean_rel_se_2'})
            imputer3 = impute_from_se.groupby(group_3)['rel_se'].mean().reset_index().rename(columns = {'rel_se': 'mean_rel_se_3'})
            imputer4 = impute_from_se.groupby(group_4)['rel_se'].mean().reset_index().rename(columns = {'rel_se': 'mean_rel_se_4'})
            imputer5 = impute_from_se.groupby(group_5)['rel_se'].mean().reset_index().rename(columns = {'rel_se': 'mean_rel_se_5'})
            imputer6 = impute_from_se['rel_se'].mean()

            ## merge imputer back into impute_se
            impute_se = pd.merge(impute_se, imputer0, on = group_0, how = 'left')
            impute_se = pd.merge(impute_se, imputer1, on = group_1, how = 'left')
            impute_se = pd.merge(impute_se, imputer2, on = group_2, how = 'left')
            impute_se = pd.merge(impute_se, imputer3, on = group_3, how = 'left')
            impute_se = pd.merge(impute_se, imputer4, on = group_4, how = 'left')
            impute_se = pd.merge(impute_se, imputer5, on = group_5, how = 'left')
            impute_se['mean_rel_se_6'] = imputer6

            if isnan(imputer6):
                raise NotImplementedError('The most aggregated SE-imputer is NA. Unable to impute.')
            else:
                ## track if we need to use 5 or 6
                if sum(impute_se['mean_rel_se_4'].isnull()) > 0:
                    print('    - Needed to fill SEs with mean across location as well... (group 5)')
                if sum(impute_se['mean_rel_se_5'].isnull()) > 0:
                    print('    - Needed to fill SEs with mean across everything')

                ## fill in SEs
                impute_se['se'] = np.nan
                impute_se['se'].fillna(impute_se['mean_rel_se_0'], inplace=True)
                impute_se['se'].fillna(impute_se['mean_rel_se_1'], inplace=True)
                impute_se['se'].fillna(impute_se['mean_rel_se_2'], inplace=True)
                impute_se['se'].fillna(impute_se['mean_rel_se_3'], inplace=True)
                impute_se['se'].fillna(impute_se['mean_rel_se_4'], inplace=True)
                impute_se['se'].fillna(impute_se['mean_rel_se_5'], inplace=True)
                impute_se['se'].fillna(impute_se['mean_rel_se_6'], inplace=True)
                ## ensure filling worked
                assert sum(impute_se['se'].isnull()) == 0

                ## ensure filling worked in the right way (if previous SE is null, we filled with latter SE)
                TEST0 = ~impute_se['mean_rel_se_0'].isnull()
                TEST1 = (~impute_se['mean_rel_se_1'].isnull()) & (impute_se['mean_rel_se_0'].isnull())
                TEST2 = (~impute_se['mean_rel_se_2'].isnull()) & (impute_se['mean_rel_se_1'].isnull()) & (impute_se['mean_rel_se_0'].isnull())
                TEST3 = (~impute_se['mean_rel_se_3'].isnull()) & (impute_se['mean_rel_se_2'].isnull()) & (impute_se['mean_rel_se_1'].isnull()) & (impute_se['mean_rel_se_0'].isnull())
                TEST4 = (~impute_se['mean_rel_se_4'].isnull()) & (impute_se['mean_rel_se_3'].isnull()) & (impute_se['mean_rel_se_2'].isnull()) & (impute_se['mean_rel_se_1'].isnull()) & (impute_se['mean_rel_se_0'].isnull())
                TEST5 = (~impute_se['mean_rel_se_5'].isnull()) & (impute_se['mean_rel_se_4'].isnull()) & (impute_se['mean_rel_se_3'].isnull()) & (impute_se['mean_rel_se_2'].isnull()) & (impute_se['mean_rel_se_1'].isnull()) & (impute_se['mean_rel_se_0'].isnull())
                TEST6 = (~impute_se['mean_rel_se_6'].isnull()) & (impute_se['mean_rel_se_5'].isnull()) & (impute_se['mean_rel_se_4'].isnull()) & (impute_se['mean_rel_se_3'].isnull()) & (impute_se['mean_rel_se_2'].isnull()) & (impute_se['mean_rel_se_1'].isnull()) & (impute_se['mean_rel_se_0'].isnull())
                assert all(impute_se.loc[TEST0, 'mean_rel_se_0'] == impute_se.loc[TEST0, 'se'])
                assert all(impute_se.loc[TEST1, 'mean_rel_se_1'] == impute_se.loc[TEST1, 'se'])
                assert all(impute_se.loc[TEST2, 'mean_rel_se_2'] == impute_se.loc[TEST2, 'se'])
                assert all(impute_se.loc[TEST3, 'mean_rel_se_3'] == impute_se.loc[TEST3, 'se'])
                assert all(impute_se.loc[TEST4, 'mean_rel_se_4'] == impute_se.loc[TEST4, 'se'])
                assert all(impute_se.loc[TEST5, 'mean_rel_se_5'] == impute_se.loc[TEST5, 'se'])
                assert all(impute_se.loc[TEST6, 'mean_rel_se_6'] == impute_se.loc[TEST6, 'se'])

                ## drop extra cols
                impute_se.drop(columns = ['mean_rel_se_0', 'mean_rel_se_1','mean_rel_se_2','mean_rel_se_3','mean_rel_se_4','mean_rel_se_5','mean_rel_se_6'], inplace = True)
                impute_from_se.drop(columns = ['rel_se'], inplace = True)

                ## censor at (limit max to) 2.6
                impute_se.loc[impute_se['se'] > 2.6, 'se'] = 2.6

                ## if raw_val > 0, se should be meal_rel_se * raw_val
                impute_se['se'] = np.where(impute_se['raw_val'] > 0, impute_se['se']*impute_se['raw_val'], impute_se['se'])

                ## recombine with known SE
                data = pd.concat([impute_from_se, impute_se], ignore_index = True)

                ## extract imputed_se
                imputed_se = impute_se['se'].values

    assert len(data) == old_len
    return data, pd.DataFrame(data = {'imputed_se': imputed_se}), CONTINUE
    
def fix_HCCI_se(data):

    ## track length of data
    old_len = len(data)

    ## only run if we have rows with dataset == HCCI
    if sum(data['dataset'] == 'HCCI') == 0:
        print('  > No HCCI data')
    else:
        print('  > Imputing')

        ## determine groups
        ## since we won't have MSCAN/KYTHERA data for every cell where we have HCCI data...
        ## ... we take a series of means to ensure we always have something to replace with
        group_0 = ['sex_id', 'pri_payer', 'payer', 'age_group_years_start', 'year_id', 'geo']  ## mean across dataset
        group_1 = ['sex_id', 'pri_payer', 'payer', 'year_id', 'geo'] ## mean across age/dataset
        group_2 = ['sex_id', 'pri_payer', 'payer', 'geo'] ## mean across age/dataset/year
        group_3 = ['pri_payer', 'payer', 'geo'] ## mean across age/dataset/year/sex
        group_4 = ['payer', 'geo'] ## mean across age/dataset/year/sex/pri_payer
        group_5 = ['payer'] ## mean across age/dataset/year/sex/pri_payer/geo

        ## split data by HCCI / MSCAN+KYTHERA / other
        HCCI = data[data['dataset'] == 'HCCI']
        MS_KY = data[data['dataset'].isin(['MSCAN', 'KYTHERA'])]
        other = data[~(data['dataset'].isin(['HCCI', 'MSCAN', 'KYTHERA']))]

        ## back out STD from MS_KY
        MS_KY['std'] = MS_KY['se'] * (MS_KY['n_obs']**(1/2))
        
        ## if n_obs == 0, just make STD <- SE
        MS_KY['std'] = np.where(MS_KY['n_obs'] == 0, MS_KY['se'], MS_KY['std'])
        
        if len(MS_KY)==0:
            raise NotImplementedError('We have no data from MSCAN/KYTHERA with which to adjust the HCCI SEs. Unable to continue.')
        else:
            
            ## calculate mean SE for each group from the MS_KY data (dropping NAs in calculation)
            imputer0 = MS_KY.groupby(group_0)['std'].mean().reset_index().rename(columns = {'std': 'mean_std_0'})
            imputer1 = MS_KY.groupby(group_1)['std'].mean().reset_index().rename(columns = {'std': 'mean_std_1'})
            imputer2 = MS_KY.groupby(group_2)['std'].mean().reset_index().rename(columns = {'std': 'mean_std_2'})
            imputer3 = MS_KY.groupby(group_3)['std'].mean().reset_index().rename(columns = {'std': 'mean_std_3'})
            imputer4 = MS_KY.groupby(group_4)['std'].mean().reset_index().rename(columns = {'std': 'mean_std_4'})
            imputer5 = MS_KY.groupby(group_5)['std'].mean().reset_index().rename(columns = {'std': 'mean_std_5'})
            imputer6 = MS_KY['std'].mean()

            ## merge imputer back into impute_se
            HCCI = pd.merge(HCCI, imputer0, on = group_0, how = 'left')
            HCCI = pd.merge(HCCI, imputer1, on = group_1, how = 'left')
            HCCI = pd.merge(HCCI, imputer2, on = group_2, how = 'left')
            HCCI = pd.merge(HCCI, imputer3, on = group_3, how = 'left')
            HCCI = pd.merge(HCCI, imputer4, on = group_4, how = 'left')
            HCCI = pd.merge(HCCI, imputer5, on = group_5, how = 'left')
            HCCI['mean_std_6'] = imputer6

            if isnan(imputer6):
                raise NotImplementedError('The most aggregated STD-imputer (for imputing HCCI STDs) is NA. Unable to impute.')
            else:

                ## fill in SEs
                HCCI['std'] = np.nan
                HCCI['std'].fillna(HCCI['mean_std_0'], inplace=True)
                HCCI['std'].fillna(HCCI['mean_std_1'], inplace=True)
                HCCI['std'].fillna(HCCI['mean_std_2'], inplace=True)
                HCCI['std'].fillna(HCCI['mean_std_3'], inplace=True)
                HCCI['std'].fillna(HCCI['mean_std_4'], inplace=True)
                HCCI['std'].fillna(HCCI['mean_std_5'], inplace=True)
                HCCI['std'].fillna(HCCI['mean_std_6'], inplace=True)
                ## ensure filling worked
                assert sum(HCCI['std'].isnull()) == 0

                ## ensure filling worked in the rstday (if previous SE is null, we filled with latter SE)
                TEST0 = ~HCCI['mean_std_0'].isnull()
                TEST1 = (~HCCI['mean_std_1'].isnull()) & (HCCI['mean_std_0'].isnull())
                TEST2 = (~HCCI['mean_std_2'].isnull()) & (HCCI['mean_std_1'].isnull()) & (HCCI['mean_std_0'].isnull())
                TEST3 = (~HCCI['mean_std_3'].isnull()) & (HCCI['mean_std_2'].isnull()) & (HCCI['mean_std_1'].isnull()) & (HCCI['mean_std_0'].isnull())
                TEST4 = (~HCCI['mean_std_4'].isnull()) & (HCCI['mean_std_3'].isnull()) & (HCCI['mean_std_2'].isnull()) & (HCCI['mean_std_1'].isnull()) & (HCCI['mean_std_0'].isnull())
                TEST5 = (~HCCI['mean_std_5'].isnull()) & (HCCI['mean_std_4'].isnull()) & (HCCI['mean_std_3'].isnull()) & (HCCI['mean_std_2'].isnull()) & (HCCI['mean_std_1'].isnull()) & (HCCI['mean_std_0'].isnull())
                TEST6 = (~HCCI['mean_std_6'].isnull()) & (HCCI['mean_std_5'].isnull()) & (HCCI['mean_std_4'].isnull()) & (HCCI['mean_std_3'].isnull()) & (HCCI['mean_std_2'].isnull()) & (HCCI['mean_std_1'].isnull()) & (HCCI['mean_std_0'].isnull())
                assert all(HCCI.loc[TEST0, 'mean_std_0'] == HCCI.loc[TEST0, 'std'])
                assert all(HCCI.loc[TEST1, 'mean_std_1'] == HCCI.loc[TEST1, 'std'])
                assert all(HCCI.loc[TEST2, 'mean_std_2'] == HCCI.loc[TEST2, 'std'])
                assert all(HCCI.loc[TEST3, 'mean_std_3'] == HCCI.loc[TEST3, 'std'])
                assert all(HCCI.loc[TEST4, 'mean_std_4'] == HCCI.loc[TEST4, 'std'])
                assert all(HCCI.loc[TEST5, 'mean_std_5'] == HCCI.loc[TEST5, 'std'])
                assert all(HCCI.loc[TEST6, 'mean_std_6'] == HCCI.loc[TEST6, 'std'])

                ## convert back to SE (using HCCI's n_obs)
                HCCI['se'] = HCCI['std'] / (HCCI['n_obs']**(1/2))

                ## drop extra cols
                HCCI.drop(columns = ['std', 'mean_std_0', 'mean_std_1','mean_std_2','mean_std_3','mean_std_4','mean_std_5','mean_std_6'], inplace = True)
                MS_KY.drop(columns = ['std'], inplace = True)

                ## recombine data
                data = pd.concat([HCCI, MS_KY, other], ignore_index = True)

    assert len(data) == old_len
    return data