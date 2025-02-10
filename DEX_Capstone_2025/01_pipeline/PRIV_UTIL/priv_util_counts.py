## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script that aggregates private insurance utilization (used in sample denom imputation)
## ==================================================

import pandas as pd
import argparse
from pathlib import Path
import sys
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa
import re

## import functions from collapse to reassign the primary payer
## so when we save out PRIV_UTIL to make the HCUP/KYTHERA sample denoms
## the primary payers there are treated the same as those in the encounters data in collapse
sys.path.append(str(Path(__file__).resolve().parents[1] / 'COLLAPSE/helpers/'))
import CollapseDataPrep as collapse

BASE_COLS = ['toc', 'year_id', 'pri_payer', 'age_group_years_start', 'sex_id']
SOURCE_COLS = {
    "MSCAN" : ['st_resi'],
    "NIS" : [],
    "NEDS" : [],
    "SIDS" : ['st_resi', 'mcnty_resi'],
    "SEDD" : ['st_resi', 'mcnty_resi'],
    "KYTHERA" : ['st_resi', 'mcnty_resi']
}
READ_COLS = BASE_COLS + ['st_resi', 'mcnty_resi', 'dx_level', 'admission_count', 'payer_2', 'mdcr_pay_amt', 'priv_pay_amt', 'mdcd_pay_amt', 'oop_pay_amt']

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
def read_input(source, filepath, schema, age, sex, state):

    ## read
    if "toc=RX" in filepath or "toc=DV" in filepath: ## for rx/dv files, no primary cause filter
        if state == "all":
            parquet_file = pq.ParquetDataset(
                filepath, 
                schema=schema,
                filters=[
                    ("age_group_years_start", "=", int(age)), 
                    ("sex_id", "=", sex)
                ]
            )
        else:
            parquet_file = pq.ParquetDataset(
                filepath, 
                schema=schema,
                filters=[
                    ("age_group_years_start", "=", int(age)), 
                    ("sex_id", "=", sex), 
                    ("st_resi", "=", state)
                ]
            )
    else: ## for non-rx files, filter to primary_cause = 1
        if state == "all":
            parquet_file = pq.ParquetDataset(
                filepath, 
                schema=schema,
                filters=[
                    ("primary_cause", "=", 1),
                    ("age_group_years_start", "=", int(age)), 
                    ("sex_id", "=", sex)
                ]
            )
        else:
            parquet_file = pq.ParquetDataset(
                filepath, 
                schema=schema,
                filters=[
                    ("primary_cause", "=", 1),
                    ("age_group_years_start", "=", int(age)), 
                    ("sex_id", "=", sex),
                    ("st_resi", "=", state)
                ]
            )
    data = parquet_file.read(columns=[c for c in READ_COLS if c in schema.names]).to_pandas()
    data.reset_index(inplace = True, drop = True)

    # Extract source, toc and year_id from filepath
    data['source'] = source
    matches = re.search(r'toc=(\w+)/year_id=(\d+)', filepath)
    if matches:
        toc, year_id = matches.groups()
        data["toc"] = toc
        data["year_id"] = int(year_id)
    else:
        raise ValueError("toc and year_id not found in filepath")
    
    return data 

def priv_util_counts(source, filepath, outpath):
    
    print("Getting schema")
    schema = replace_null_schema(filepath)

    ## loop through ages/sexes and apply functions
    all_age_sex = []
    ages = [0,1] + list(range(5,100,5))
    sexes = [1, 2]
    states = ["all"] if source not in ["MSCAN", "KYTHERA"] else [
        "-1", "unknown", "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", 
        "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", 
        "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", 
        "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
        "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
    ]
    for age in ages:
        for sex in sexes:
            print("AGE:", age, " SEX:", sex)
            for state in states:

                df = read_input(source, filepath, schema, age, sex, state)

                # Reassign pri payer (from collapse function)
                df = collapse.reassign_pri_payer(df, source, age, mdcr_dual_combine = None, step = 'clean')
                # Drop unused pri_payers
                df = df[df["pri_payer"].isin([2, 23])] ## keep priv and mdcr_priv only

                ## stop if no data
                if df.empty:
                    continue

                # Getting utilization counts
                group_cols = ["source"] + BASE_COLS + SOURCE_COLS[source]
                if "toc=RX" in filepath or "toc=DV" in filepath:
                    df['admission_count'] = 1
                priv_counts = df.groupby(group_cols, dropna=False)["admission_count"].sum().reset_index(name='util')
                
                ## append
                all_age_sex.append(priv_counts)
    
    ## concat all_age_sex list and write out
    print("Saving out")
    ALL = pd.concat(all_age_sex, ignore_index=True)
    partition_by_cols = ["toc", "year_id"]
    ALL.to_parquet(
        outpath,
        basename_template="priv_util_part-{i}.parquet",
        partition_cols=partition_by_cols,
        existing_data_behavior="overwrite_or_ignore"
    )

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parse arguments for private utilization counts.')
    parser.add_argument('--source', type=str, help='Data source to run private utilization for.')
    parser.add_argument('--filepath', type=str, help='Input path to data.')
    parser.add_argument('--outpath', type=str, help='Output path for data.')
    args = vars(parser.parse_args())
    print(args)
    source = args['source']
    filepath = args['filepath']
    outpath = args['outpath']

    priv_util_counts(source, filepath, outpath)
