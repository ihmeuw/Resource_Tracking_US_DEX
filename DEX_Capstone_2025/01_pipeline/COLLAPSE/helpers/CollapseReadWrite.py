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

## --------------------------------------------------------------------------------------------
## Imports
## --------------------------------------------------------------------------------------------
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
import pandas as pd
import re
import os

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def get_filepaths(config, PRI, source, toc, year):
    ## get normal dir, or DV/RX specific dir
    ## HCCI needs to read in RX from RDP input folder instead of output folder
    if toc in ['DV', 'RX']:
        ## parse through MDCR carrier partition as needed
        if source in ['MDCR', 'CHIA_MDCR']:
            if toc == 'DV':
                input_fp1 = config['data_input_dir_DV_RX'][source] + 'data/carrier=false'
                input_fp2 = config['data_input_dir_DV_RX'][source] + 'data/carrier=true'
            else:
                input_fp1 = config['data_input_dir_DV_RX'][source] + 'data/carrier=false'
        else:
            input_fp1 = config['data_input_dir_DV_RX'][source] + 'data'
    else:
        input_fp1 = config['data_input_dir'][source] + 'data'

    ## fix to use actual PRI not test name
    pattern = re.compile(r'.+[a-zA-Z]')
    if pattern.match(PRI):
        input_PRI = PRI.split('.')[0]
    else:
        input_PRI = PRI
    if source in ['MDCR', 'CHIA_MDCR'] and toc == 'DV':
        input_fp1 = input_fp1.replace('run_' + PRI, 'run_' + input_PRI) + '/toc=' + toc + '/year_id=' + str(year)
        input_fp2 = input_fp2.replace('run_' + PRI, 'run_' + input_PRI) + '/toc=' + toc + '/year_id=' + str(year)
    else:
        input_fp1 = input_fp1.replace('run_' + PRI, 'run_' + input_PRI) + '/toc=' + toc + '/year_id=' + str(year)
        input_fp2 = None
    
    return input_fp1, input_fp2 ## fp2 only exists for DV data from MDCR because of how it's partitioned

def replace_schema(filepath, partitioning='hive'):
    
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

def read_data(input_fp1, input_fp2, schema1, schema2, source, toc, acause, age_list, sex):
    
    ## no primary cause
    if source == 'HCCI': 
        parquet_file = pq.ParquetDataset(
            input_fp1, 
            filters=[
                ('age_group_years_start', 'in', age_list), 
                ('sex_id', '=', sex), 
                ('acause', '=', acause), 
            ], 
            schema=schema1
        )
        
    ## split into two reads
    elif source in ['MDCR', 'CHIA_MDCR'] and toc == 'DV': 
        parquet_file1 = pq.ParquetDataset(
            input_fp1, 
            filters=[
                ('age_group_years_start', 'in', age_list), 
                ('sex_id', '=', sex), 
                ('acause', '=', acause), 
                ('primary_cause', '=', 1) 
            ], 
            schema=schema1
        )
        if os.path.exists(input_fp2):
            parquet_file2 = pq.ParquetDataset(
                input_fp2, 
                filters=[
                    ('age_group_years_start', 'in', age_list), 
                    ('sex_id', '=', sex), 
                    ('acause', '=', acause), 
                    ('primary_cause', '=', 1) 
                ], 
                schema=schema2
            )
    
    ## everything else: normal read
    else: 
        parquet_file = pq.ParquetDataset(
            input_fp1, 
            filters=[
                ('age_group_years_start', 'in', age_list), 
                ('sex_id', '=', sex), 
                ('acause', '=', acause), 
                ('primary_cause', '=', 1) 
            ], 
            schema=schema1
        )

    ## recombine two reads as needed
    if source in ['MDCR', 'CHIA_MDCR'] and toc == 'DV': 
        IN1 = parquet_file1.read().to_pandas()
        if os.path.exists(input_fp2):
            IN2 = parquet_file2.read().to_pandas()
        else:
            IN2 = pd.DataFrame()
        IN = pd.concat([IN1, IN2], ignore_index = True)
        del(IN1)
        del(IN2)
        IN.reset_index(inplace = True, drop = True)
    else:
        IN = parquet_file.read().to_pandas()
        IN.reset_index(inplace = True, drop = True)

    return IN

def write_collapsed_data(df, outpath, year, by_race):
    if by_race:
        outpath = outpath + '_race'
    df.reset_index(inplace = True, drop = True)
    df.to_parquet(
        outpath, 
        basename_template=f'{str(year)}_part{{i}}.parquet',
        partition_cols=['acause', 'toc', 'metric', 'dataset'], 
        existing_data_behavior='overwrite_or_ignore'
    )    

def write_diagnostic_data(df, outpath, acause, by_race):
    if by_race:
        outpath = outpath + '_race'
    df.reset_index(inplace = True, drop = True)
    df.to_parquet(
        path=outpath, 
        basename_template=f'acause_{str(acause)}_part{{i}}.parquet',
        partition_cols=['dataset', 'toc', 'year_id'], 
        existing_data_behavior='overwrite_or_ignore'
    )
