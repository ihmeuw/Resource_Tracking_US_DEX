## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to format MarketScan data.
## ==================================================

import argparse
import pandas as pd
from pathlib import Path
import mktscn_formatter_helper


def run_formatter(batch_path, dataset, year, table):

    # Loading in dataframe
    df = pd.read_parquet(batch_path)

    # Fixing encoding for 2019 and 2020
    if year in ["2019", "2020"]:
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].str.decode("utf-8")

    if year == "2019":
        for date_col in [
            "SVCDATE",
            "TSVCDAT",
            "PDDATE",
            "ADMDATE",
            "DISDATE",
            "DTEND",
            "DTSTART",
            "DEACTDT",
            "REACTDT",
        ]:
            try:
                df[date_col] = pd.to_datetime("1960-1-1") + pd.to_timedelta(
                    df[date_col], unit="D"
                )
            except KeyError:
                pass

    # Getting NID for data
    mktscn_formatter_helper.get_nid(df, dataset, year)

    # Standardizing primary payer name
    mktscn_formatter_helper.get_primary_payer(df, dataset, table)

    # Age binning data
    mktscn_formatter_helper.get_age_group(df)

    # Getting residence location from state of residence
    mktscn_formatter_helper.get_loc(
        df, df_col="EGEOLOC", loc_map_col="abbreviation", type="resi"
    )

    # Getting service locations from hospital state
    mktscn_formatter_helper.get_loc(
        df, df_col="STATE", loc_map_col="abbreviation", type="serv"
    )

    # Merging on transfer status if outpatient/inpatient, needed for getting TOC and extra DXs
    if table in ["s", "o"]:
        df = mktscn_formatter_helper.merge_fac(df, year, table)

    # Finding ICD version
    mktscn_formatter_helper.get_icd_ver(df, year, table)

    # Getting year of admission, discharge, claim end, and service date
    mktscn_formatter_helper.get_dates(df, table)

    # Adding TOC to dataframe
    mktscn_formatter_helper.get_toc(df, table)

    # Calculating payment amounts
    mktscn_formatter_helper.get_pay_amts(df, dataset, table)

    # Creating copies of columns that do not need changes
    if table == "i":
        copy_cols = {
            "DAYS": "los",
            "SEX": "sex_id",
            "SEQNUM": "claim_id",
            "ENROLID": "bene_id",
        }
    elif table in ["s", "o"]:
        copy_cols = {
            "SEX": "sex_id",
            "FACPROF": "fac_prof_ind",
            "SEQNUM": "claim_id",
            "ENROLID": "bene_id",
        }
    elif table == "d":
        copy_cols = {
            "SEX": "sex_id",
            "DAYSUPP": "days_supply",
            "NDCNUM": "ndc",
            "SEQNUM": "claim_id",
            "ENROLID": "bene_id",
        }
    df[list(copy_cols.values())] = df[list(copy_cols.keys())]

    # Converting select columns to appropriate datatypes
    stg_3_cols = {
        "nid": "string",
        "year_id": "Int64",
        "year_adm": "Int64",
        "year_dchg": "Int64",
        "year_clm": "Int64",
        "bene_id": "string",
        "claim_id": "string",
        "age_group_years_start": "Int64",
        "age_group_id": "Int64",
        "sex_id": "Int64",
        "race_cd": "string",
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
        "days_supply": "int",
        "ndc": "string",
        "service_date": "datetime64",
        "discharge_date": "datetime64",
        "fac_prof_ind": "string",
        "toc": "string",
        "los": "Int64",
        "pri_payer": "string",
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
    oth_cols = {i: "string" for i in df.columns if i not in stg_3_cols}
    df = mktscn_formatter_helper.cast_columns(df, {**stg_3_cols, **oth_cols})

    return df


if __name__ == "__main__":

    # Requesting input and output path arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch_path", type=str, required=True, help="Input parquet file to process."
    )

    parser.add_argument(
        "--outpath",
        type=str,
        required=True,
        help="Output path for formatted .parquet file.",
    )

    # Assigning input args to variables
    args = vars(parser.parse_args())
    batch_path = Path(args["batch_path"])
    outpath = Path(args["outpath"])

    # Looking at parent parquet folder for dataset, table, and year info
    data_vars = batch_path.parent.stem.split("_")
    dataset = data_vars[0]
    table = data_vars[1]
    year = data_vars[2]

    # Formatting dataframe
    formatted_df = run_formatter(batch_path, dataset, year, table)

    # Saving batch dataframe to outpath. Each batch_path name is unique
    # In the submission script, so no data will be overwritten.
    formatted_df.to_parquet(
        outpath / batch_path.name,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
