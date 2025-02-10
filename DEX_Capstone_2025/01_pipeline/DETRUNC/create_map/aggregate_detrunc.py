## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script for the DETRUNC step that creates a map from 3-digit ICD code to acause
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
from pathlib import Path
import os
import re
from dexdbload.pipeline_runs import (
    get_config, 
    parsed_config
)
pd.options.mode.chained_assignment = None  # default="warn"

print("Loading config")
raw_config = get_config()

print("Defining functions")
## write parquet
def write_parquet(df, outpath):
    partition_cols = ["source", "toc"]
    df.to_parquet(outpath, partition_cols=partition_cols)

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

## function to aggregate detrunc map
def aggregate_detrunc(data):
    out = data.groupby(["toc", "age_group_years_start", "sex_id", "code_system", "icd3", "acause"])["N"].sum().reset_index()
    return out

## function to finalize map
def make_detrunc_map(data, agg_cols):
    cols = agg_cols + ["code_system", "icd3"]
    counts = data.groupby(cols + ["acause"])["N"].sum().reset_index()
    denoms = data.groupby(cols)["N"].sum().reset_index()
    denoms.rename(columns = {"N" : "denom"}, inplace = True)
    out = pd.merge(counts, denoms, on = cols)
    out["prop"] = out["N"]/out["denom"]
    out.drop(columns = ["denom"], inplace = True)
    return out

if __name__ == "__main__":

    print("Parsing arguments")
    parser = argparse.ArgumentParser(description="Pull in args")
    parser.add_argument("-m", "--MVI", help="DETRUNC map version id", required=True)
    args = vars(parser.parse_args())
    print(args)
    
    MVI = args["MVI"]
    
    print("Setting up config")
    detrunc_config = parsed_config(raw_config, key = "DETRUNC", map_version_id = MVI)

    print("Aggregating map")
    all_source = pd.DataFrame()
    for source in ["MDCR", "MDCD", "MSCAN"]:

        print(source)
        source_dir = detrunc_config['map_output_dir'] + "tmp" + "/source=" + source
        tocs = os.listdir(source_dir)

        for toc in tocs:
            print("   " + toc)
            filepath = source_dir + "/" + toc
            data_schema = replace_schema(filepath, partitioning="hive")
            parquet_file = pq.ParquetDataset(
                filepath,
                schema=data_schema
            )
            IN = parquet_file.read().to_pandas()
            IN.reset_index(inplace = True, drop = True)
            IN[["source", "toc"]] = [source, toc.replace("toc=", "")]

            agg = aggregate_detrunc(IN)
            all_source = pd.concat([all_source, agg], ignore_index = True)
    
    ## re-aggregate across sources
    all_source = aggregate_detrunc(all_source)

    ## make maps at different aggregation levels
    out_tas = make_detrunc_map(all_source, ["toc", "age_group_years_start", "sex_id"])
    out_ts = make_detrunc_map(all_source, ["toc", "sex_id"])
    out_t = make_detrunc_map(all_source, ["toc"])
    out = make_detrunc_map(all_source, [])
    ## make all-toc maps for RX
    out_as = make_detrunc_map(all_source, ["age_group_years_start", "sex_id"])
    out_s = make_detrunc_map(all_source, ["sex_id"])

    ## save out
    out_dir = detrunc_config["map_output_dir"] + "maps/"
    Path(out_dir).mkdir(exist_ok = True, parents = True)
    print("    Saving to... ", out_dir)
    out_tas.to_feather(out_dir + "detrunc_toc_age_sex.feather")
    out_ts.to_feather(out_dir + "detrunc_toc_sex.feather")
    out_t.to_feather(out_dir + "detrunc_toc.feather")
    out.to_feather(out_dir + "detrunc.feather")
    out_as.to_feather(out_dir + "detrunc_age_sex.feather")
    out_s.to_feather(out_dir + "detrunc_sex.feather")