## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script for the DETRUNC step that aggregates spending by 3-digit ICD code
## ==================================================


print("Importing modules")
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
import argparse
import re
from dexdbload.pipeline_runs import (
    get_config, 
    parsed_config
)
pd.options.mode.chained_assignment = None  # default="warn"

print("Loading config")
raw_config = get_config()

## get restrictions
restrictions = pd.read_csv(raw_config["METADATA"]["toc_cause_restrictions_PRE_COLLAPSE_path"])
restrictions = restrictions[(restrictions["include"] == 1) & (restrictions["gc_nec"] == 0)]

print("Defining functions")
## write parquet
def write_parquet(df, outpath):
    partition_cols = ["source", "toc"]
    df.to_parquet(outpath, partition_cols=partition_cols)

## function to replace schema
def replace_schema(filepath, partitioning="hive"):
    
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

## function to create detrunc map
def create_detrunc_map(data, toc, age, sex, res = restrictions):
    
    ## get toc-age-sex-specific restrictions for this task
    R = res[(res["toc"] == toc) & (res["age_start"] <=age) & (res["age_end"] >= age)]
    if sex == 1:
        R = R[R["male"] == 1]
    else:
        R = R[R["female"] == 1]
    R = list(R.acause.values)
    data = data[data["acause"].isin(R)]

    ## drop < 3 digit ICDs
    data = data[data["orig_dx"].str.len() >= 3]

    ## make 3 digit ICD
    data["icd3"] = data["orig_dx"].str[0:3]
    
    ## select columns of interest
    data = data[["age_group_years_start", "sex_id", "acause", "code_system", "icd3"]]

    ## make map
    map = data.value_counts().reset_index().rename(columns = {0: "N"})

    return map

if __name__ == "__main__":

    print("Parsing arguments")
    parser = argparse.ArgumentParser(description="Pull in source/(sub_dataset)/year")
    parser.add_argument("-s", "--source", help="Source of data", required=True)
    parser.add_argument("-t", "--toc", help="TOC", required=True)
    parser.add_argument("-y", "--year", help="Year", type = int, required=True)
    parser.add_argument("-l", "--state", help="State ('all' if not using state for this source)", required=True)
    parser.add_argument("-p", "--PRI", help="Phase run ID", required=True)
    parser.add_argument("-m", "--MVI", help="DETRUNC map version id", required=True)
    args = vars(parser.parse_args())
    print(args)
    
    source = args["source"]
    toc = args["toc"]
    year = args["year"]
    state = args["state"]
    PRI = args["PRI"]
    MVI = args["MVI"]
    
    print("Setting up config")
    detrunc_config = parsed_config(raw_config, key = "DETRUNC", run_id = PRI, map_version_id = MVI)

    print("Getting filepaths and replacing schema")
    filepath = detrunc_config['data_input_dir'][source] + "data"

    ## replacing schema/reading from the entire dataset takes a while
    ## making filepath specific to toc/year-partition and adding those values back in is way faster
    filepath = filepath + "/toc=" + toc + "/year_id=" + str(year)
    data_schema = replace_schema(filepath)

    all_age_dt = pd.DataFrame()
    ages = [0,1] + list(range(5,100,5))
    for age in ages:
        print("  AGE:", age)

        for sex in [1, 2]:
            print("    sex:", sex)

            if state != "all": ## filter to age/sex/state
                parquet_file = pq.ParquetDataset(
                    filepath, 
                    filters=[
                        ("age_group_years_start", "=", int(age)), 
                        ("sex_id", "=", sex),
                        ("st_resi", "=", state)
                    ], 
                    schema=data_schema
                )
            else: ## filter to age/sex
                parquet_file = pq.ParquetDataset(
                    filepath, 
                    filters=[
                        ("age_group_years_start", "=", int(age)), 
                        ("sex_id", "=", sex)
                    ], 
                    schema=data_schema
                )
            IN = parquet_file.read().to_pandas()
            IN.reset_index(inplace = True, drop = True)

            ## skip to next iteration if no data
            if len(IN) == 0:
                print("    No data for this age/sex")
                continue

            ## make detrunc map
            out = create_detrunc_map(IN, toc, age, sex)

            ## add in columns
            ## since we're reading from toc/year partitions, those columns aren't in the data
            out["source"] = source
            out["toc"] = toc
            out["year_id"] = year
            
            ## append if data
            if len(out) > 0:
                all_age_dt = pd.concat([all_age_dt, out], ignore_index = True)

    ## save out
    if len(all_age_dt) == 0:
        print("No data across all ages-sexes")
    else:
        out_dir = detrunc_config["map_output_dir"] + "tmp"
        print("    Saving to... ", out_dir)
        write_parquet(all_age_dt, out_dir)
