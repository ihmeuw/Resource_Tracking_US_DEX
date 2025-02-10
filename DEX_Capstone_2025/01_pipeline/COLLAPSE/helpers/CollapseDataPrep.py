## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that prep/clean data
## ==================================================

## Although this code includes functionality to collapse by race this functionality was not used.
## For all occurences, the following setting were used:
## ...
## race_option = "no_re" (no race/ethincity information was used)
## by_race = FALSE (no race/ethincity information was used)
## race_col = "NA" (no race/ethincity information was used)
## no_race_source = not used

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
## generic modules
import numpy as np
import pandas as pd

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def create_group_cols(group_cols, by_race):
    return group_cols + ['race_cd'] if by_race else group_cols

def clean_data(data, source, toc, age, year, no_race_source, race_option, race_col):  

    ## refresh global constants
    from helpers.CollapseConstants import (
        pay_cols
    ) 

    ## add toc/year back in
    ## since we're reading from toc/year partitions, those columns aren't in the data
    data['toc'] = toc
    data['year_id'] = year

    ## make days_per_encounter for RX using days_supply
    ## (this was not used in the final model, but was included in the code)
    if toc == 'RX': 
        if 'los' in data.columns:
            data.drop(columns = ['los'], inplace = True)
        if 'days_supply' in data.columns:
            data['los'] = data['days_supply']
            ## replace any missing with np.nan
            data['los'] = data['los'].fillna(np.nan)
            ## set integer
            data['los'] = data['los'].astype(float).astype('Int32')
            ## fill missing with the mean
            datalos = data.loc[~(data['los'].isnull())]
            if datalos.empty == False:
                mean_los = int(datalos["los"].mean())
                data['los'] = data['los'].fillna(mean_los)
        else:
            data['los'] = np.nan

    ## double check cxorrect values in MDCR
    if source in ['MDCR', 'CHIA_MDCR'] and toc == 'RX':
        if (data['oop_pay_amt'].sum()==0):
            data['oop_pay_amt']=data['oop_chg_amt']
            data['oop_chg_amt']=np.nan
        data['pri_payer']=np.where(data['pri_payer']==4,1,data['pri_payer'])

    ## drop ot_taf and ot rows for MDCD in IP/NF
    if source == 'MDCD' and toc in ['IP', 'NF']:
        data = data[data['sub_dataset'] != 'ot']
        data = data[data['sub_dataset'] != 'ot_taf']
    
    ## for MDCD MAX specifically, recode zeros to NA
    if source == 'MDCD' and year <= 2014:
        for i in pay_cols:
            data.loc[data[i] == 0, i] = np.nan
    
    ## for MDCR carrier specifically, recode zeros in payer == oop to NA
    if source == 'MDCR':
        data.loc[(data['sub_dataset'] == 'carrier') & (data['oop_pay_amt'] == 0), 'oop_pay_amt'] = np.nan

    ## ensure mcnty type is right
    if 'mcnty_resi' in data.columns:
        if data['mcnty_resi'].dtype == object:
            data['mcnty_resi'] = data['mcnty_resi'].fillna('-1')
        else:
            data['mcnty_resi'] = data['mcnty_resi'].fillna(-1)
            data['mcnty_resi'] = data['mcnty_resi'].astype('Int32').astype(str)
    else: 
        assert (source == 'MEPS') | (source == 'HCCI'), 'source other than MEPS/HCCI is missing mcnty_resi'
        data['mcnty_resi'] = '-1'

    ## drop __index_level_0__
    if '__index_level_0__' in data.columns:
        data.drop(columns = ['__index_level_0__'], inplace = True)

    ## make all-NA columns for pay columns that don't exist (artefact of C2E)
    for k in pay_cols:
        if k not in data.columns:
            data[k] = np.nan

    ## ensure managed care column always present
    ## setting missing/NA to ffs
    if source == 'MDCD':
        ffs_value = 3
    else:
        ffs_value = 0
    if 'mc_ind' not in data.columns:
        data['mc_ind'] = ffs_value
    data.loc[data['mc_ind'].isnull(), 'mc_ind'] = ffs_value
    data['mc_ind'] = data['mc_ind'].astype(int)

    ## decide where to put mc_ind values
    data['mc_ind'] = np.where(data['mc_ind'].isin([1,2]), 1, 0)
    ## for non-MCDD data:
    ## --- 1 -> 1, 0 -> 0 (i.e. no changes)
    ## for MDCD data:
    ## --- [1,2] -> 1, [3] -> 0
    
    ## ensure dual ind column always present
    if 'dual_ind' not in data.columns:
        data['dual_ind'] = 0
    ## replace NA with 0
    data.loc[data['dual_ind'].isnull(), 'dual_ind'] = 0
    data['dual_ind'] = data['dual_ind'].astype(int)

    ## ensure no negatives in admission count...
    if 'admission_count' in data.columns:
        assert sum(data.admission_count < 0) == 0

    ## ensure admission_count always present
    if 'admission_count' not in data.columns:
        data['admission_count'] = 1
    ## replace NA with 1
    data.loc[data['admission_count'].isnull(), 'admission_count'] = 1
    data['admission_count'] = data['admission_count'].astype(int)

    ## make admission_flag
    data['admission_flag'] = np.where(data['admission_count'] > 0, 1, 0)

    ## Set new age max to 85
    if age == 85:
        data['age_group_years_start'] = 85

    ## fix type for pri payer
    if source in ['MSCAN', 'KYTHERA']:
        data = data[~data['pri_payer'].isnull()]
        data['pri_payer'] = data['pri_payer'].astype(int)

    ## add sub-dataset if non-existent
    if 'sub_dataset' not in data.columns:
        data['sub_dataset'] = 'none'

    ## for MDCR AM/ED, use five_pct sample and select years
    if source == 'MDCR' and toc in ['AM', 'ED']:
        data = data[data['ENHANCED_FIVE_PERCENT_FLAG'] == 'Y']
        data = data[data['year_id'].isin([2000, 2010, 2014, 2015, 2016, 2019])]

    ## for SIDS/SEDD impute missing st_resi with st_serv
    if source in ['SIDS', 'SEDD']:
        data['st_resi'] = data['st_resi'].fillna(data['st_serv'])

        ## for SIDS/SEDD, keep only where st_resi == st_serv (only 2% of the encounters)
        data = data[data['st_resi'] == data['st_serv']]    

    ## for CHIA data, only keep st_resi == MA
    if source == 'CHIA_MDCR':
        data = data[data['st_resi'] == 'MA']

    #Reassign pri_payers
    data = reassign_pri_payer(data, source, age, mdcr_dual_combine = None, step = 'clean') 
    ## mdcr_dual_combine doesn't mean anything for the 'clean' step

    ## check race codes 
    if race_option in ['re', 'both'] and source not in no_race_source:
        #Which race col to use
        if race_col == 'raw':
            data.rename(columns = {'race_cd_raw':'race_cd'}, inplace=True)
            data.drop(['race_cd_imp'], inplace = True, axis =1)
        elif race_col == 'imp':
            data.rename(columns = {'race_cd_imp':'race_cd'}, inplace=True)
            data.drop(['race_cd_raw'], inplace = True, axis =1)
        else:
            raise ValueError('This race_col is not supported')
        
        ## correct MEPS misspelling
        data.loc[data['race_cd'] == 'BLK', 'race_cd'] = 'BLCK'
        ## convert 'OTH' and 'MULT' to 'UNK'
        data.loc[data['race_cd'].isin(['OTH', 'MULT']), 'race_cd'] = 'UNK'
        ## set any possible NAs to 'UNK'
        data.loc[data['race_cd'].isnull(), 'race_cd'] = 'UNK'
        na_vals = ['None']
        data.loc[data['race_cd'].isin(na_vals), 'race_cd'] = 'UNK'
        ## make sure race_cd is standardized
        assert all(data['race_cd'].isin(['WHT', 'BLCK', 'HISP', 'AIAN', 'API', 'UNK']))
        assert sum(data.race_cd.isnull()) == 0
    else:
        data['race_cd'] = 'UNK' ## set all race_cd to 'UNK' if we're not using race
    
    return data

def reassign_pri_payer(data, source, age, mdcr_dual_combine, step):

    #---------------------------------------------------------------------------------
    #Part 1: Reassignment that doesn't need to occur within the utilization or price function
    #        and may include columns that don't exist w/in util or price functions
    #---------------------------------------------------------------------------------
    if step=='clean': 
        if source != 'HCCI': # HCCI already mapped to payers
            
            if source == 'MDCD':
                data.loc[(data['dual_ind'] == 0) & (data['pri_payer'] != 2) &((data['mdcr_pay_amt'] == 0)|data['mdcr_pay_amt'].isnull()==True), 'pri_payer'] = 3
                data.loc[((data['dual_ind'] == 1) | (data['mdcr_pay_amt'] > 0)| (data['pri_payer'] ==1))& (data['pri_payer'] != 2) & (data['toc'] != 'RX'),'pri_payer'] = 22
                ## Create duals for RX, different logic bc those over 65 w/ part D are required to have MDCR
                if age >= 65:
                    data.loc[(data['pri_payer'] != 2) & (data['toc'] == 'RX'),'pri_payer'] = 22
                data.loc[(data['pri_payer'] == 1) &((data['mdcd_pay_amt'] == 0)|data['mdcd_pay_amt'].isnull()==True), 'pri_payer'] = 1
            
            elif source in ["MDCR", "CHIA_MDCR"]:
                ## for MDCR, ensure pri_payer and dual are aligned (they should contain the same information)
                data.loc[~(data['pri_payer'].isin([1,2,3,22])), "pri_payer"] = 1 ## set any other pri_payers to "mdcr"
                data.loc[(data['pri_payer'] == 3), 'pri_payer'] = 22 ## set "mdcd" to "mdcr_mdcd"
                data.loc[(data['pri_payer'] == 22), 'dual_ind'] = 1 ## if pripayer == "mdcr_mcdd", set dual to 1
                data.loc[(data['dual_ind'] == 1), 'pri_payer'] = 22 ## if dual == 1, make sure all pripayer is "mdcr_mdcr"
                data.loc[data['pri_payer'] == 2, 'dual_ind'] = 0 ## if pri_payer == "priv", make sure dual_ind is 0
                assert all(data.loc[data['pri_payer'] == 22, 'dual_ind'] == 1)
                assert all(data.loc[data['pri_payer'] != 22, 'dual_ind'] != 1)
                assert all(data.loc[data['dual_ind'] == 1, 'pri_payer'] == 22)
                assert all(data.loc[data['dual_ind'] != 1, 'pri_payer'] != 22)

            elif source == 'MSCAN': 
                data.loc[((data['pri_payer']==2) | (data['age_group_years_start']< 65)) & (data['pri_payer']!=1), 'pri_payer'] = 2
                data.loc[(data['age_group_years_start']>= 65) | (data['pri_payer']==1), 'pri_payer'] = 23

            elif source == 'MEPS':
                data.loc[(data['oop_pay_amt']>0)& ((data['mdcd_pay_amt'] == 0)|data['mdcd_pay_amt'].isnull()==True)&  ((data['mdcr_pay_amt'] == 0)|(data['mdcr_pay_amt'].isnull()==True))& ((data['priv_pay_amt'] == 0)|(data['priv_pay_amt'].isnull()==True)), 'pri_payer']=4
                data.loc[~(data['pri_payer'].isin([1,2,3,4])), 'pri_payer']=4 ## if payer isn't in main payers, assign to oop
                if age >= 65:
                    data.loc[(data['pri_payer']==2), 'pri_payer'] = 23

            elif source in ['NIS','NEDS','SIDS','SEDD']:
                data.loc[(data['pri_payer']==1)&(data['payer_2']==3), 'pri_payer'] = 22
                data.loc[(data['pri_payer']==1)&(data['payer_2']==2), 'pri_payer'] = 23
                data.loc[(data['pri_payer']==1)&(data['payer_2']==4), 'pri_payer'] = 1
                if age >= 65:
                    data.loc[(data['pri_payer']==2), 'pri_payer'] = 23
            
            elif source == "KYTHERA":
                if age >= 65:
                    data.loc[(data['pri_payer']==2), 'pri_payer'] = 23
                
            ## remove any extraneous pri_payers
            data = data[data['pri_payer'].isin([1,2,3,4,22,23])] 
        
        else: ## for HCCI
            
            ## remove any extraneous pri_payers
            data=data[data['pri_payer'].isin(['mdcr','priv','mdcd','oop', 'mdcr_priv', 'mdcr_mdcd'])]
            
            ## For utilization
            data.loc[((data['pri_payer'] == 'mdcr') & (data['metric'].isin(['days_per_encounter','encounters_per_person']))), 'pri_payer'] = 'mdcr_priv'
            ## For spend
            data.loc[((data['pri_payer'] == 'mdcr') & (data['metric'].isin(['spend_per_day','spend_per_encounter'])) & (data['payer'].isin(['priv','oop']))), 'pri_payer'] = 'mdcr_priv'

    #---------------------------------------------------------------------------------
    # Part 2: Reassignment for price metrics
    #---------------------------------------------------------------------------------
    elif step == 'price':

        if source in ['MDCR', 'CHIA_MDCR']:
            if mdcr_dual_combine:
                ## If we are using both duals and nonduals to inform pri_payer "mdcr"
                ## then we want to count the duals towards "mdcr" prices
                ## (we don't need to preserve the duals separately, since we don't model prices for "mdcr_mdcd")
                data.loc[(data['pri_payer'] == 22), 'pri_payer'] = 1 
            else:
                ## otherwise (if only using non-duals to inform "mdcd")
                ## then we can just drop the duals, since we don't model prices for them
                data = data[data['pri_payer'] != 22]

        ## Ensure consistent pri_payers
        if source == "HCCI":
            assert all(data['pri_payer'].isin(['mdcr','priv','mdcd','oop', 'mdcr_priv', 'mdcr_mdcd']))
        else:
            assert all(data['pri_payer'].isin([1,2,3,4,22,23]))

    #---------------------------------------------------------------------------------
    # Part 2: Reassignment for utilization metrics
    #---------------------------------------------------------------------------------
    elif step == 'util':
            
        ## Duplicate utilization for Duals in MDCR, so One row pri_payer = MDCR, one row = MDCR-MDCD
        ## we do this section because we want our MDCR duals to be included in the dual model (pri_payer 'mdcr_mdcd') AND the non-dual model (pri_payer 'mdcr')
        if source in ['MDCR', 'CHIA_MDCR']:

            ## if mdcr_dual_combine is True, we will use both the duals and non-duals to inform 'mdcr'
            if mdcr_dual_combine:
                ## Split data
                data_dual = data.loc[(data['dual_ind'] == 1)]
                data_no_dual = data.loc[(data['dual_ind'] == 0)]
                ## Dual 2 will be our data that was a dual but now will be counted as just 'mdcr'
                data_dual2 = data_dual.copy()
                data_dual2['dual_ind'] = 0 # set to 0 so it can be combined with the rest of the mdcr in that combo
                data_dual2['pri_payer'] = 1 # set to mdcr
                ## recombine
                ## add back together
                data = pd.concat([data_no_dual, data_dual, data_dual2], ignore_index = True)
                ## >> data_no_dual == our true non-dual data
                ## >> data_dual == our true dual data
                ## >> data_dual2 == our true dual data, but now set to non-duals
            
            ## drop duplicates, for some reason
            data = data.drop_duplicates()

        ## Ensure consistent pri_payers
        if source == "HCCI":
            assert all(data['pri_payer'].isin(['mdcr','priv','mdcd','oop', 'mdcr_priv', 'mdcr_mdcd']))
        else:
            assert all(data['pri_payer'].isin([1,2,3,4,22,23]))
    
    else:
        raise ValueError('That is not an option for step input, options are clean, util, price')

    return data
