## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to assist with formatting MarketScan data.
## ==================================================

from db_queries import get_age_metadata
import pandas as pd
import numpy as np
from pathlib import Path
import dexdbload.pipeline_runs as dex_pr


def get_nid(df, dataset, year):
    nid_map = {
        "ccae": {
            "2000": "223670",
            "2010": "224501",
            "2011": "307820",
            "2012": "224502",
            "2013": "307821",
            "2014": "307822",
            "2015": "307823",
            "2016": "377003",
            "2017": "418020",
            "2018": "448322",
            "2019": "482637",
            "2020": "501676",
            "2021": "517738",
            "2022": "547238",
        },
        "mdcr": {
            "2000": "223672",
            "2010": "224499",
            "2011": "307815",
            "2012": "224500",
            "2013": "307817",
            "2014": "307818",
            "2015": "307819",
            "2016": "377002",
            "2017": "418019",
            "2018": "448349",
            "2019": "482638",
            "2020": "501679",
            "2021": "517739",
            "2022": "547237",
        },
    }
    df["nid"] = nid_map[dataset][year]
    return df


def get_primary_payer(df: pd.DataFrame, dataset: str, table: str):
    """Mapping primary payer from MarketScan values to DEX IDs.
    First transforms to abbrevations, then numerical IDs for clarity.

    Args:
    df -- Pandas dataframe
    """

    # For commercial claim...
    if dataset == "ccae":
        if table == "i":
            # If COB > net payments, we cannot confirm marketscan is primary.
            # Therefore, the payer could be another private insurer or medicare
            df.loc[(df["TOTCOB"].fillna(0) > df["TOTNET"].fillna(0)), "pri_payer"] = (
                "oth_not_mdcd_oop"
            )

            # Otherwise, we expect private primary payer
            df.loc[(df["TOTCOB"].fillna(0) <= df["TOTNET"].fillna(0)), "pri_payer"] = (
                "priv"
            )

        elif table in ["s", "o", "d"]:
            # If COB > net payments, we cannot confirm marketscan is primary.
            # Therefore, the payer could be another private insurer or medicare
            df.loc[(df["COB"].fillna(0) > df["NETPAY"].fillna(0)), "pri_payer"] = (
                "oth_not_mdcd_oop"
            )

            # Otherwise, we expect private primary payer
            df.loc[(df["COB"].fillna(0) <= df["NETPAY"].fillna(0)), "pri_payer"] = (
                "priv"
            )

    # For medicare, all claims have medicare as the primary payer
    elif dataset == "mdcr":
        df["pri_payer"] = "mdcr"

    # Payer abbreviation to DEX IDs
    dex_lookup = {
        "mdcr": 1,
        "priv": 2,
        "mdcd": 3,
        "oop": 4,
        "oth_not_priv": 5,
        "oth_not_mdcr": 6,
        "oth_not_mdcd": 7,
        "oth_not_oop": 8,
        "oth_not_mdcd_mdcr": 9,
        "oth_not_mdcd_oop": 10,
        "oth_not_mdcd_priv": 11,
        "oth_not_mdcr_oop": 12,
        "oth_not_mdcr_priv": 13,
        "oth_not_oop_priv": 14,
        "oth_not_mdcd_mdcr_oop": 15,
        "oth_not_mdcd_mdcr_priv": 16,
        "oth_not_mdcd_oop_priv": 17,
        "oth_not_mdcr_oop_priv": 18,
        "oth_not_mdcd_mdcr_oop_priv": 19,
        "nc": 20,
        "unk": 21,
    }

    # Applying dictionary to create new payer columns
    df["pri_payer"] = df["pri_payer"].map(dex_lookup)
    return df


def get_age_group(df: pd.DataFrame, age_col: str = "AGE"):
    """Determine DEX age group for each row in dataframe.

    Args:
    df -- Pandas dataframe
    age_col -- Column that contains age information
    """

    # Getting age bin data and reordering
    ages = (
        get_age_metadata(age_group_set_id=27, gbd_round_id=7)
        .sort_values(by="age_group_years_start")
        .reset_index()
    )

    # Creating age bins, includes final closed interval from last age_group_years_end
    age_bins = ages["age_group_years_start"].to_list() + [
        ages["age_group_years_end"].to_list()[-1]
    ]

    # Binning data in new mapper column
    df["age_mapper"] = pd.cut(df[age_col], bins=age_bins, right=False, labels=False)

    # Mapping all columns of interest to dataframe
    age_data_cols = ["age_group_id", "age_group_years_start", "age_group_years_end"]
    for column in age_data_cols:
        df[column] = df["age_mapper"].map(ages[column].to_dict())

    # Dropping mapper column
    df.drop(columns="age_mapper", inplace=True)
    return df


def get_loc(df: pd.DataFrame, df_col: str, loc_map_col: str, type: str):
    """Add location columns to dataframe.

    Args:
    df -- Pandas dataframe
    df_col -- Name of the column we want to map on (in provided dataframe)
    loc_map_col -- Name of the column we want to map to (in location dataframe)
    type - Location type to add to new columns as suffix. Should be either 'serv' or 'resi' for DEX.
    """

    # MarketScan EGEOLOC map from data dictionary
    egeoloc_map = {
        "01": "-1",
        "02": "-1",
        "03": "-1",
        "04": "CT",
        "05": "ME",
        "06": "MA",
        "07": "NH",
        "08": "RI",
        "09": "VT",
        "10": "-1",
        "11": "NJ",
        "12": "NY",
        "13": "PA",
        "14": "-1",
        "15": "-1",
        "16": "IL",
        "17": "IN",
        "18": "MI",
        "19": "OH",
        "20": "WI",
        "21": "-1",
        "22": "IA",
        "23": "KS",
        "24": "MN",
        "25": "MO",
        "26": "NE",
        "27": "ND",
        "28": "SD",
        "29": "-1",
        "30": "-1",
        "31": "DC",
        "32": "DE",
        "33": "FL",
        "34": "GA",
        "35": "MD",
        "36": "NC",
        "37": "SC",
        "38": "VA",
        "39": "WV",
        "40": "-1",
        "41": "AL",
        "42": "KY",
        "43": "MS",
        "44": "TN",
        "45": "-1",
        "46": "AR",
        "47": "LA",
        "48": "OK",
        "49": "TX",
        "50": "-1",
        "51": "-1",
        "52": "AZ",
        "53": "CO",
        "54": "ID",
        "55": "MT",
        "56": "NV",
        "57": "NM",
        "58": "UT",
        "59": "WY",
        "60": "-1",
        "61": "AK",
        "62": "CA",
        "63": "HI",
        "64": "OR",
        "65": "WA",
        "97": "PR",
    }

    # Reading in state and merged county maps
    state_map = pd.read_csv("FILEPATH")
    mcnty_map = pd.read_csv("FILEPATH")

    # Creating new fips column on merged county map with leading zeros of string type
    mcnty_map["fips"] = mcnty_map["cnty"].astype("str").str.zfill(5)

    # Renaming location id columns
    mcnty_map.rename(columns={"location_id": "cnty_location_id"}, inplace=True)
    state_map.rename(columns={"location_id": "st_location_id"}, inplace=True)

    # Merging state and county info into one dataframe
    locs = mcnty_map.merge(
        state_map[["abbreviation", "st_location_id"]], on="abbreviation", how="left"
    )

    # Mapping all columns of interest
    loc_data_cols = {
        "mcnty": f"mcnty_{type}",
        "cnty_location_id": f"cnty_loc_id_{type}",
        "abbreviation": f"st_{type}",
        "st_location_id": f"st_loc_id_{type}",
    }
    for loc_name, df_name in loc_data_cols.items():
        if locs[loc_name].nunique() <= locs[loc_map_col].nunique():
            try:
                df[df_name] = (
                    df[df_col]
                    .map(egeoloc_map)
                    .map(dict(zip(locs[loc_map_col], locs[loc_name])))
                    .fillna("-1")
                )
            except KeyError:
                df[df_name] = "-1"
                print(
                    f"{df_col} column does not exist in dataframe. Filling {df_name} with missing val (-1)."
                )
        else:
            df[df_name] = "-1"
            print(
                f"Cannot map {df_name} column due to one-to-many mapping. Filling with missing val (-1)."
            )

    return df


def merge_fac(df: pd.DataFrame, year: str, table: str):
    """Merging on discharge status for service-level files.

    Args:
    df -- Pandas dataframe
    year -- Year of data
    table -- Table type, either outpatient (o) or inpatient (s) service-level
    """

    # Getting config filepaths
    config = dex_pr.get_config()

    # Getting stage 1 path
    stage_1_path = Path(
        dex_pr.parsed_config(config, key="FORMAT")["stage_1_dir"]["MSCAN"]
    )

    # Finding facility file to merge on
    fac_file_path = stage_1_path / f"ccae_f_{year}.parquet"

    # Columns we want to add from facility file
    fac_cols = ["DX5", "DX6", "DX7", "DX8", "DX9"]
    if table == "o":
        fac_cols.append("DSTATUS")

    # Merging given df with each parquet batch in facility header data
    joined_dfs_list = []
    for path in fac_file_path.iterdir():
        fac_df = pd.read_parquet(path)

        # Joining facility header file using FACHDID
        joined_df = pd.merge(
            df,
            fac_df[["FACHDID", "SEQNUM"] + fac_cols],
            on="FACHDID",
            how="left",
            suffixes=("_s", "_f"),
        )

        # If join was succesful..
        if "SEQNUM_f" in joined_df.columns:

            # New dataframe to join on remove all rows with successful join
            df = joined_df[joined_df["SEQNUM_f"].isna()].drop(
                columns=fac_cols + ["SEQNUM_f"]
            )

            # Save rows with successful join
            joined_dfs_list.append(joined_df[~joined_df["SEQNUM_f"].isna()])

    # Add remaining unsucessfully joined rows
    joined_dfs_list.append(df)

    # Merge all rows into dataframe
    df = pd.concat(joined_dfs_list)

    # If no merges were successful, add DSTATUS column full of NaNs
    if "DSTATUS" not in df.columns:
        df["DSTATUS"] = np.nan

    # Renaming service SEQNUM and removing facility header SEQNUM
    df.rename(columns={"SEQNUM_s": "SEQNUM"}, inplace=True)
    df.drop(columns=["SEQNUM_f"], inplace=True)

    return df


def get_toc(df, table):

    # Mapping inpatient to IP and pharma claims to RX,
    # Otherwise using map from place of service (STDPLAC)
    if table in ["i", "s"]:
        df["toc"] = "IP"
    elif table == "d":
        df["toc"] = "RX"
    elif table == "o":
        pos_toc_df = pd.read_csv("FILEPATH")
        pos_toc_map = dict(
            zip(pos_toc_df["Place of Service Code"], pos_toc_df["DEX TOC"])
        )
        df["toc"] = df["STDPLAC"].map(pos_toc_map)

        # Anything in OP admitted to ER and discharged to IP
        # We will consider IP. Below are discharge codes that
        # Are considered discharge to IP.
        ip_dchg = [2, 9, 43, 65, 82, 85, 93, 99]
        df.loc[(df["DSTATUS"].isin(ip_dchg)) & (df["toc"] == "ER"), "toc"] = "IP"

        # Anything in OP called IP or NF, but with a length of stay <1 should be AM
        df.loc[(df["toc"].isin(["IP", "NF"]) & (df["los"] < 1)), "toc"] = "AM"

    return df


def get_icd_ver(df, year, table):

    # Only mapping inpatient or outpatient
    # Pharma claims do not have DX codes
    if table in ["i", "s", "o"]:

        # Map for ICD version
        icd_map = {"0": "icd10", "9": "icd9"}
        try:
            df["code_system"] = df["DXVER"].map(icd_map)

        # If DXVER column is not available, then code_system is ICD-9
        # Exception should only occur in data prior to 2015
        except KeyError:
            if int(year) < 2015:
                df["code_system"] = "icd9"
            else:
                raise RuntimeError(
                    f"{year} year data is expected to have DXVER column."
                )

        # Finding any failed mappings
        null_version = df["code_system"].isna()

        # Manually mapping ICD version based on date. Any admissions/services on or after October 1st, 2015
        # Are considered ICD-10 and any before this date are considered ICD-9. Any mislabeled rows will be
        # Fixed in causemapping step
        if table in ["i", "s"]:
            df.loc[null_version, "code_system"] = np.where(
                df.loc[null_version, "DISDATE"] >= pd.to_datetime("2015-10-01"),
                "icd10",
                "icd9",
            )
        elif table == "o":
            df.loc[null_version, "code_system"] = np.where(
                df.loc[null_version, "SVCDATE"] >= pd.to_datetime("2015-10-01"),
                "icd10",
                "icd9",
            )
    else:
        pass

    return df


def get_dates(df, table):

    # Finding year variables for inpatient data
    if table in ["i", "s"]:

        # Getting discharge year from discharge date or admission date plus length of stay
        try:
            df["year_dchg"] = pd.DatetimeIndex(pd.to_datetime(df["DISDATE"])).year
        except KeyError:
            try:
                df["year_dchg"] = pd.DatetimeIndex(
                    pd.to_datetime(df["ADMDATE"])
                    + pd.to_timedelta(df["DAYS"].astype("int"), unit="D")
                ).year
            except KeyError:
                df["year_dchg"] = pd.DatetimeIndex(pd.to_datetime(df["ADMDATE"])).year

        # Getting admission year from admission date
        df["year_adm"] = pd.DatetimeIndex(pd.to_datetime(df["ADMDATE"])).year

        # Getting dates for C2E
        df["service_date"] = pd.to_datetime(df["ADMDATE"])
        df["discharge_date"] = pd.to_datetime(df["DISDATE"])

    # For outpatient and pharma claims, discharge and admission year are file year
    elif table in ["o", "d"]:
        df["year_adm"] = df["YEAR"]
        df["year_dchg"] = df["YEAR"]
        df["service_date"] = pd.to_datetime(df["SVCDATE"])

        if table == "o":
            df["discharge_date"] = pd.to_datetime(df["TSVCDAT"])

    # Getting length of stay
    if table in ["i", "s", "o"]:
        df["los"] = (df["discharge_date"] - df["service_date"]).dt.days

    # Getting year id and year claim id from file year
    df["year_id"] = df["YEAR"]
    df["year_clm"] = df["YEAR"]

    return df


def get_pay_amts(df, dataset, table):

    if table == "i":
        # We only know medicare paid amount for medicare claims
        if dataset == "ccae":
            df["mdcr_pay_amt"] = np.nan
        elif dataset == "mdcr":
            df["mdcr_pay_amt"] = df["TOTCOB"]

        # Calculating private, oop, and total payments
        df["priv_pay_amt"] = df["TOTNET"]
        df["oop_pay_amt"] = df["TOTPAY"] - df["TOTNET"] - df["TOTCOB"]
        df["tot_pay_amt"] = df["TOTPAY"]

    elif table in ["s", "o", "d"]:

        # Creating out-of-pocket paid amount as non-negative difference between PAY, NETPAY, and COB
        # If NETPAY+COB > PAY, then oop_pay_amt = 0 and we re-scale NETPAY and COB to PAY
        df["oop_pay_amt"] = np.where(
            df["PAY"].sub(df["NETPAY"], fill_value=0).sub(df["COB"], fill_value=0) < 0,
            0,
            df["PAY"].sub(df["NETPAY"], fill_value=0).sub(df["COB"], fill_value=0),
        )

        # We only know medicare paid amount for medicare claims. Re-scaling paid amount using PAY column as reference
        if dataset == "ccae":
            df["mdcr_pay_amt"] = np.nan
        elif dataset == "mdcr":
            df["mdcr_pay_amt"] = df["COB"]

        # Calculating private and total payments. Re-scaling private paid amount using PAY column as reference
        df["priv_pay_amt"] = df["NETPAY"]
        df["tot_pay_amt"] = (
            df["NETPAY"].fillna(0) + df["COB"].fillna(0) + df["oop_pay_amt"].fillna(0)
        )

    # Medicaid is not included in marketscan data
    df["mdcd_pay_amt"] = np.nan

    return df


def cast_columns(df: pd.DataFrame, dtype_dict: dict = {}):
    """Cast a list of column to specified type, ignores missing columns.

    Args:
    df -- Pandas dataframe
    dtype_dict -- Dictionary of column names and type to cast to
    """

    for c, t in dtype_dict.items():
        try:
            df[c] = df[c].astype(t)
        except KeyError:
            print(f"{c} not found in dataframe. Skipping conversion to integer.")
        except TypeError:
            df[c] = df[c].astype("float").astype(t)

    return df
