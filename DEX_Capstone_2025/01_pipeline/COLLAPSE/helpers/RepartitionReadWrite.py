## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Helper functions that read and write data
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## by_race = FALSE (no race/ethincity information was used)
## ...

import pyarrow.dataset as ds
import pyarrow as pa

## function to replace schema
def replace_schema(filepath, partitioning=None):
    
    # Loading in interpreted dataset schema from Pyarrow
    data_schema = ds.dataset(filepath, partitioning=partitioning).schema

    # Replacing any null-column schemas with strings. The default schema is interpreted from the first file,
    # Which can result in improper interpretations of column types. These are usually handled by pyarrow
    # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
    # On later files, which will fail if any values in the column are not null. Interpreting as string
    # changes all nulls to None types.
    null_idxs = [idx for idx, datatype in enumerate(data_schema.types) if datatype == pa.null()]
    for idx in null_idxs:
        data_schema = data_schema.set(idx, data_schema.field(idx).with_type(pa.string()))
    
    return data_schema

def write_comorb(df, outpath, acause, by_race): ## for comorb
    if by_race:
        outpath = outpath + '_race'
    df.reset_index(inplace = True, drop = True)
    df.to_parquet(
        outpath, 
        basename_template=f'acause_{acause}_part{{i}}.parquet', 
        partition_cols=['metric', 'toc', 'year_id', 'age_group_years_start', 'sex_id'], 
        existing_data_behavior='overwrite_or_ignore'
    )

def write_final(df, outpath, acause, sex, by_race): ## final output
    if by_race:
        outpath = outpath + '_race'
    df.reset_index(inplace = True, drop = True)
    df.to_parquet(
        outpath, 
        basename_template=f'acause_{acause}_sex{str(int(sex))}_part{{i}}.parquet', 
        partition_cols=['metric', 'geo', 'toc', 'pri_payer', 'payer'], 
        existing_data_behavior='overwrite_or_ignore'
    )
    
def write_combos(df, outpath, acause, by_race):
    if by_race:
        outpath = outpath + '_race'
    df.reset_index(inplace = True, drop = True)
    df.to_parquet(
        outpath, 
        basename_template=f'acause_{(acause)}_part{{i}}.parquet', 
        partition_cols=['dataset', 'toc', 'metric'], 
        existing_data_behavior='overwrite_or_ignore'
    )   