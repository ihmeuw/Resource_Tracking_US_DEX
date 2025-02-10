## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that inflate/deflate dollar amounts
## ==================================================

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
import pandas as pd

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def deflate(data, val_columns, old_year = 'year_id', new_year = 2019):
    
    ## get deflators
    deflators = pd.read_csv("[repo_root]/static_files/DEFLATORS/best.csv")

    ## copy dataset
    DT = data.copy()

    ## if val_columns is a string, convert to a list of length 1
    if type(val_columns) == str:
        val_columns = [val_columns]

    ## check for val columns
    if not pd.Series(val_columns).isin(DT.columns).all():
        raise ValueError('Not all val_columns are column names in data')
        return

    ## currency convert
    if type(old_year) == int and type(new_year) == int:
        ## converting from year X to year Y (base to base)
        target_year_value = deflators[deflators.year == new_year].annual_cpi.values[0]
        deflators['deflator_scalar'] = deflators['annual_cpi']/target_year_value
        deflator_scalar_val = deflators[deflators.year == old_year].deflator_scalar.values[0]
        for j in val_columns:
            DT[j] = DT[j]/deflator_scalar_val
        
    elif type(old_year) == str and type(new_year) == int:
        ## converting from year in column to year X (current to base)
        
        if old_year not in DT.columns: 
            raise ValueError('"' + old_year + '" not a column name in data')
            return

        target_year_value = deflators[deflators.year == new_year].annual_cpi.values[0]
        deflators['deflator_scalar'] = deflators['annual_cpi']/target_year_value
        deflators = deflators.drop(columns =  'annual_cpi')
        deflators = deflators.rename(columns={'year': old_year})
        DT = pd.merge(DT, deflators, on = old_year)
        for j in val_columns:
            DT[j] = DT[j]/DT['deflator_scalar']
        DT = DT.drop(columns = 'deflator_scalar')
        
    elif  type(old_year) == int and type(new_year) == str:
        ## converting year X to year in column (base to current)
        
        if new_year not in DT.columns: 
            raise ValueError('"' + new_year + '" not a column name in data')
            return
        
        target_year_value = deflators[deflators.year == old_year].annual_cpi.values[0]
        deflators['deflator_scalar'] = target_year_value/deflators['annual_cpi']
        deflators = deflators.drop(columns =  'annual_cpi')
        deflators = deflators.rename(columns={'year': new_year})
        DT = pd.merge(DT, deflators, on = new_year)
        for j in val_columns:
            DT[j] = DT[j]/DT['deflator_scalar']
        DT = DT.drop(columns = 'deflator_scalar')
    else:
        raise ValueError('Please check that old_year and new_year are specified correctly')
        return

    return DT 
