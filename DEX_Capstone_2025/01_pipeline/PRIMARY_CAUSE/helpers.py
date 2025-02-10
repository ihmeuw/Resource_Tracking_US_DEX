## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that prepare the data for the PRIMARY_CAUSE step
## ==================================================

from ast import AsyncFunctionDef
from cmath import nan
from curses.ascii import isalnum
from dataclasses import asdict, replace
from warnings import resetwarnings
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetDataset
import pyarrow.dataset as ds
import pyarrow as pa
import pandas as pd
import re
pd.options.mode.chained_assignment = None
import numpy as np
from dexdbload.pipeline_runs import parsed_config
import helpershelpers as hh ## this needs to be in a different (simpler) script so we can source it in DETRUNC as well

## --------------------------------------------------------------------------------------------
## READ/WRITE FUNCTIONS
## --------------------------------------------------------------------------------------------

## function to replace schema
def replace_null_schema(filepath, partitioning = "hive"):

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

## function to read input data
def read_input(filepath, schema, toc, year, age, sex, state):

    ## read
    if state != "all": ## filter to age/sex/state
        parquet_file = pq.ParquetDataset(
            filepath, 
            filters=[
                ("age_group_years_start", "=", int(age)), 
                ("sex_id", "=", sex),
                ("st_resi", "=", state)
            ], 
            schema=schema
        )
    else: ## filter to age/sex
        parquet_file = pq.ParquetDataset(
            filepath, 
            filters=[
                ("age_group_years_start", "=", int(age)), 
                ("sex_id", "=", sex)
            ], 
            schema=schema
        )
    data = parquet_file.read().to_pandas()
    data.reset_index(inplace = True, drop = True)

    ## add toc/year back in
    ## since we're reading from toc/year partitions, those columns aren't in the data
    data["toc"] = toc
    data["year_id"] = year
    
    return data 

## write parquet
def write_parquet(df, outpath, state = False):
    partition_cols = ["toc", "year_id", "age_group_years_start", "sex_id"]
    if state: 
        partition_cols = partition_cols + ["st_resi"]
    name = f"part{{i}}.parquet" ## only one file per partition
    df.to_parquet(outpath, basename_template=name, partition_cols=partition_cols, existing_data_behavior="overwrite_or_ignore")

## --------------------------------------------------------------------------------------------
## Bring it all together
## --------------------------------------------------------------------------------------------
def main(raw_config, PRI, CAUSEMAP_MVI, source, toc, year, state, test = False, test_age = None, test_sex = None):
    
    print("Setting up config")
    primary_cause_config = parsed_config(raw_config, key = "PRIMARY_CAUSE", run_id = PRI)
    causemap_config = parsed_config(raw_config, key = "CAUSEMAP", run_id = PRI, map_version_id = CAUSEMAP_MVI)
    zipfix_config = parsed_config(raw_config, key = "ZIPFIX", run_id = PRI)

    print("Defining input directories")
    filepath = primary_cause_config['data_input_dir'][source] + "data"
    filepath = filepath + "/toc=" + toc + "/year_id=" + str(year)
    ## define schema
    schema = replace_null_schema(filepath)

    ## loop through ages/sexes and apply functions
    if test:
        ages = [test_age]
        sexes = [test_sex]
    else:
        ages = [0,1] + list(range(5,100,5))
        sexes = [1, 2]
    for age in ages:
        for sex in sexes:
            print("AGE:", age, " SEX:", sex)

            print("    Reading data")
            data = read_input(filepath, schema, toc, year, age, sex, state)

            ## skip to next iteration if no data
            if len(data) == 0:
                print("    ! No data for this age/sex !")
                continue

            ## cleanup
            if toc == "RX":
                data["dx_level"] = "dx_1"
                data["admission_count"] = 1
            else:
                ## ensure we have our encounter col
                assert "encounter_id" in data.columns

                ## clean up some columns
                if data.dx_level.dtype == "float64":
                    data["dx_level"] = "dx_" + data["dx_level"].astype(int).astype(str)
                if "n_row" in data.columns:
                    data.drop(columns = ["n_row"], inplace = True)
                if "encounter_los" in data.columns:
                    data.drop(columns = ["encounter_los"], inplace = True)
                if "los" in data.columns:
                    data.loc[np.isinf(data["los"]), "los"] = np.nan

                ## standardize non-C2E data
                if "admission_count" not in data.columns:
                    data["admission_count"] = 1

                ## define function to check for multiple dx1
                def check_dx1(data):
                    dx1_counts = data[data["dx_level"] == "dx_1"].groupby("encounter_id").size().reset_index().rename(columns = {0: "n"})
                    all_encounter_ids = pd.DataFrame({'encounter_id': data['encounter_id'].unique()})
                    dx1_counts = all_encounter_ids.merge(dx1_counts, on='encounter_id', how='left').fillna(0)
                    zero = dx1_counts.loc[dx1_counts['n'] == 0, "encounter_id"]
                    zero = data.loc[data["encounter_id"].isin(zero) & data["dx_level"].str.contains("dx"), "encounter_id"] 
                    ## don't want to flag as zero dx1 if we have no dx because we only have ex codes
                    more = dx1_counts.loc[dx1_counts['n'] > 1, "encounter_id"]
                    return zero, more
                
                ## ensure exactly one dx_1 per encounter_id
                zero, more = check_dx1(data)

                ## assert we do not have any encounters with more than 1 dx_1
                assert(len(more) == 0)
                fixus = zero

                ## fix zeros
                if len(fixus) > 0:
                    print("    ! Refactoring dx_level !")
                    fix = data[data["encounter_id"].isin(fixus)]
                    data = data[~data["encounter_id"].isin(fixus)]
                    fix_dx = fix[fix["dx_level"].str.contains("dx")]
                    fix_ex = fix[fix["dx_level"].str.contains("ex")]
                    fix_dx.loc[:,"dx_level_tmp"] = [int(re.sub("dx_", "", x)) for x in fix_dx["dx_level"]]
                    fix_dx['dx_level_tmp'] = fix_dx.groupby('encounter_id')['dx_level_tmp'].rank(method='first').astype(int) ## enforce to run from 1 to n
                    fix_dx['dx_level'] = "dx_" + fix_dx['dx_level_tmp'].astype(str)
                    fix_dx.drop(columns = ["dx_level_tmp"], inplace = True)
                    data = pd.concat([fix_dx, fix_ex, data], ignore_index = True)

                ## check again
                zero, more = check_dx1(data)
                assert(len(more) == 0)
                assert(len(zero) == 0)

            ## assign primary if not RX
            if toc == "RX":
                if test: ## if test, just return data
                    return data
                continue ## otherwise, go to next iteration of loop

            else:
                print("    Assigning primary cause")
                data = hh.add_primary_cause_indicator(data, toc, age, causemap_config, CAUSEMAP_MVI)
                 
                if test: ## if test, just return data
                    return data

                ## otherwise, write out data
                out_dir = primary_cause_config["data_output_dir"][source] + "data"
                print("    Saving to... ", out_dir)
                write_parquet(data, out_dir, state = (source in ["MDCR", "MDCD", "MSCAN", "KYTHERA"]))

