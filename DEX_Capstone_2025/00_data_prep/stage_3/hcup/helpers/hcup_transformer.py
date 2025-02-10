## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to transform HCUP data.
## ==================================================

import argparse
import pyarrow as pa
import pyarrow.dataset as ds
from pathlib import Path

import hcup_transformer_helper


STG_3_COLS = {
    "nid": "string",
    "year_id": "Int64",
    "year_adm": "Int64",
    "year_dchg": "Int64",
    "year_clm": "Int64",
    "bene_id": "string",
    "claim_id": "string",
    "encounter_id": "string",
    "age_group_years_start": "Int64",
    "age_group_id": "Int64",
    "sex_id": "Int64",
    "race_cd": "string",
    "prim_lang": "string",
    "mcnty_resi": "string",
    "cnty_loc_id_resi": "string",
    "zip_3_resi": "string",
    "zip_5_resi": "string",
    "st_resi": "string",
    "st_loc_id_resi": "string",
    "mcnty_serv": "string",
    "cnty_loc_id_serv": "string",
    "zip_3_serv": "string",
    "zip_5_serv": "string",
    "st_serv": "string",
    "st_loc_id_serv": "string",
    "code_system": "string",
    "dx_level": "string",
    "dx": "string",
    "ndc": "string",
    "service_date": "datetime64",
    "discharge_date": "datetime64",
    "fac_prof_ind": "string",
    "toc": "string",
    "los": "Int64",
    "pri_payer": "string",
    "sec_payer": "string",
    "tot_pay_amt": "float",
    "mdcr_pay_amt": "float",
    "mdcd_pay_amt": "float",
    "priv_pay_amt": "float",
    "oop_pay_amt": "float",
    "oth_pay_amt": "float",
    "tot_chg_amt": "float",
    "mdcr_chg_amt": "float",
    "mdcd_chg_amt": "float",
    "priv_chg_amt": "float",
    "oop_chg_amt": "float",
    "oth_chg_amt": "float",
    "survey_wt": "float",
}


def df_from_batch(filepath, idx):

    # Creating Pyarrow dataset object to batch. Identical to
    # Object in launcher script. Order of rows and row group
    # Size should be consistent
    # Creating Pyarrow dataset object to batch
    data_schema, data_partitioning = replace_schema(filepath)
    dataset = ds.dataset(filepath, partitioning=data_partitioning, schema=data_schema)
    data_batches = dataset.to_batches(batch_size=1000000)

    # Skip to the appropriate batch (this is slower for later indices)
    for i in range(0, idx):
        next(data_batches)

    # Creating pandas dataframe of given index
    df = next(data_batches).to_pandas()

    return df


def replace_schema(filepath, partitioning=None):

    # Loading in interpretted dataset schema from Pyarrow
    data_schema = ds.dataset(filepath, partitioning=partitioning).schema
    data_partitioning = ds.dataset(filepath, partitioning=partitioning).partitioning

    # Replacing any null-column schemas with strings. The default_schema is interpretted from the first file,
    # Which can result in improper interpretations of column types. These are usually handled by pyarrow
    # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
    # On later files, which will fail if any values in the column are not null. Interpretting as string
    # changes all nulls to None types.
    null_idxs = [
        idx for idx, datatype in enumerate(data_schema.types) if datatype == pa.null()
    ]
    for idx in null_idxs:
        data_schema = data_schema.set(
            idx, data_schema.field(idx).with_type(pa.string())
        )

    return data_schema, data_partitioning


def run_transformer(df, dataset):

    # Coding race abbreviation
    hcup_transformer_helper.get_race(df)

    # Standardizing primary payer name
    hcup_transformer_helper.get_payer(df, type="pri", col="pay1")

    # Standardizing secondary payer name
    hcup_transformer_helper.get_payer(df, type="sec", col="pay2")

    # Age binning data and recording age column used
    hcup_transformer_helper.get_age_group(df)

    # Getting residence location from FIPS code
    hcup_transformer_helper.get_loc(
        df, df_col="fips_6_4_code", loc_map_col="fips", type="resi"
    )

    # Getting service locations for state abbreviation
    hcup_transformer_helper.get_loc(
        df, df_col="state_id", loc_map_col="abbreviation", type="serv"
    )

    # Getting ZIP information for this dataset
    hcup_transformer_helper.get_zip(df, dataset)

    # Adding TOC to dataframe
    hcup_transformer_helper.get_toc(df, dataset)

    # Recoding ICD version using dictionary
    hcup_transformer_helper.get_icd_ver(df)

    # Getting FIPS and state info from NIS, if available
    if dataset == "NIS":
        if "hospstco" in df.columns:
            df["fips_6_4_code"] = (
                df["hospstco"].astype("Int64").astype("str").str.zfill(5)
            )
            hcup_transformer_helper.get_loc(
                df, df_col="fips_6_4_code", loc_map_col="fips", type="serv"
            )
        elif "hospst" in df.columns:
            hcup_transformer_helper.get_loc(
                df, df_col="hospst", loc_map_col="abbreviation", type="serv"
            )
        else:
            print("No location column available. Skipping.")

    # Renaming NIS/NEDS specific columns
    df.rename(columns={"discwt": "survey_wt"}, inplace=True)
    df.rename(columns={"key_nis": "key"}, inplace=True)
    df.rename(columns={"key_ed": "key"}, inplace=True)

    # Adding encounter_id, each HCUP entry is a unique encounter
    df = hcup_transformer_helper.create_uuid(df, "encounter_id", ["key"])

    # If available, adding a bene_id column
    if "pnum_r" in df.columns:
        df["bene_id"] = df["pnum_r"]
    elif "mrn_r" in df.columns:
        df["bene_id"] = df["mrn_r"]
    elif "mrn_s" in df.columns:
        df["bene_id"] = df["mrn_s"]
    else:
        df["bene_id"] = None

    # Adding claim and discharge year
    df["year_clm"] = df["year_id"]
    df["year_dchg"] = df["year_id"]

    # Adding charge columns for main payer categories
    # HCUP does not specify charge for each payer
    df["mdcr_chg_amt"] = None
    df["mdcd_chg_amt"] = None
    df["priv_chg_amt"] = None
    df["oop_chg_amt"] = None
    df["oth_chg_amt"] = None

    # Renaming columns to match DEX standard
    df.rename(
        columns={"metric_total_charges": "tot_chg_amt", "adm_year": "year_adm"},
        inplace=True,
    )

    # Cutting down columns before wide to long conversion
    df = hcup_transformer_helper.reduce_cols(
        df,
        col_keep=list(STG_3_COLS.keys()),
        regex_col_keep=["^dx_\d*$", "^ecode_\d*$", "^(.*)chg_amt$"],
    )

    # Transforming dx/ecode from wide to long
    df = hcup_transformer_helper.w2l_transform(df)

    # Casting, subsetting, and ordering columns and ordering
    oth_cols = {i: "string" for i in df.columns if i not in STG_3_COLS}
    df = hcup_transformer_helper.cast_columns(df, {**STG_3_COLS, **oth_cols})
    df = df[[c for c in STG_3_COLS if c in df.columns]]

    return df


def write_parquet(df, filepath, outpath, idx):

    # Nulls values are automatically dropped for partition columns
    # Assigning a flag of -1 for any nulls in partition columns to prevent data loss
    partition_cols = ["age_group_years_start", "sex_id"]
    df.fillna({col: -1 for col in partition_cols}, inplace=True)

    # To_parquet wants partition cols to have consistent typing across rows, so converting to str
    df[partition_cols] = df[partition_cols].astype("str")

    # Writing out parquet file with partitions. Directory is written to by multiple tasks,
    # Resulting in appending data. Index used in name to ensure unique filename across all
    # Tasks (preventing overwriting), as long these condtions must be met:
    # 1) MUST BE PARTIONED BY SOMETHING
    # 2) basename_template must be unique
    # 3) existing_data_behavior must be "overwrite_or_ignore" (this is the default)
    df.to_parquet(
        outpath,
        partition_cols=partition_cols,
        basename_template="_".join(Path(filepath).stem.split("_")[1:])
        + f"_idx{idx}_{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )


if __name__ == "__main__":

    # Requesting input paths, output path, and dataset name arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--filepath",
        type=str,
        required=True,
        help="Filepath of .parquet file to process.",
    )
    parser.add_argument(
        "--index", type=int, required=True, help="Row group batch index to process."
    )
    parser.add_argument(
        "--outpath",
        type=str,
        required=True,
        help="Output path for formatted .parquet file.",
    )

    # Assigning input args to variables
    args = vars(parser.parse_args())
    filepath = args["filepath"]
    idx = args["index"]
    outpath = args["outpath"]

    # Getting info from path name
    dataset = [item[5:] for item in Path(filepath).parts if item.startswith("HCUP_")][0]

    # Getting dataframe to process based on batch index
    df = df_from_batch(filepath, idx)

    # Transforming dataframe
    transformed_df = run_transformer(df, dataset)

    # Saving out dataframe to outpath as indexed chunk
    write_parquet(transformed_df, filepath, outpath, idx)
