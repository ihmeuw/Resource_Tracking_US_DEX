## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to transform Kythera data.
## ==================================================

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import uuid

import kythera_transformer_helper


def run_format_transform(batch_path, table):

    ###############################
    # Setting up maps and constants
    ###############################
    # Mapping Kythera sexes to DEX sexes
    sex_map = {"M": 1, "F": 2}

    # Mapping Kythera code systems to DEX code systems
    code_sys_map = {"ICD9": "icd9", "ICD10": "icd10", "ICD10CM": "icd10"}

    # Mapping Kythera facility/professional indicator to DEX facility/professional indicator
    fac_prof_map = {"I": "F", "P": "P"}

    # Reading in place-of-service and type-of-bill to type-of-care maps
    pos_map = pd.read_csv("FILEPATH")
    tob_map = pd.read_csv("FILEPATH")

    # Creating dictionaries from the above csvs
    pos_map_dict = dict(
        zip(pos_map["Place of Service Code"].astype("str"), pos_map["DEX TOC"])
    )
    tob_map_dict = dict(
        zip(tob_map["Type of Bill Code"].astype("str"), tob_map["DEX TOC"])
    )

    # Reading in location information to map state and ZIP code information
    state_map = pd.read_csv("FILEPATH")
    mcnty_map = pd.read_csv("FILEPATH")
    zip_map = pd.read_feather("FILEPATH")

    # Creating new fips column on merged county map with leading zeros of string type
    mcnty_map["fips"] = mcnty_map["cnty"].astype("str").str.zfill(5)

    # Merging state and county info into one dataframe
    locs = (
        mcnty_map.merge(
            state_map[["abbreviation", "location_id"]], on="abbreviation", how="left"
        )
        .merge(zip_map, left_on="mcnty", right_on="mcnty_resi")
        .drop(columns="mcnty_resi")
    )

    # Creating a dataframe with mapping between ZIP3, ZIP5, state abbreviation, and state location id
    locs = locs[["abbreviation", "location_id_y", "zip5_resi"]].drop_duplicates()
    locs = locs.rename(
        columns={
            "zip5_resi": "zip_5",
            "location_id_y": "st_loc_id",
            "abbreviation": "st",
        }
    )
    locs["zip_3"] = locs["zip_5"].str[0:3]

    # Getting location specific maps for Kythera
    state_id_map = dict(zip(locs.st, locs.st_loc_id))
    zip_state_map = dict(zip(locs.zip_5, locs.st))
    zip3_state_map = dict(zip(locs.zip_3, locs.st))

    # List of all stage 3 columns and typing in a somewhat sensible order
    stage_3_cols = {
        "nid": "string",  ## Basic info
        "year_id": "Int16",
        "year_adm": "Int16",
        "year_dchg": "Int16",
        "year_clm": "Int16",
        "bene_id": "string",
        "claim_id": "string",
        "age_group_years_start": "Int16",  ## Demographic info
        "age_group_id": "Int16",
        "sex_id": "Int16",
        "race_cd": "string",
        "mcnty_resi": "string",  ## Residence loc info
        "cnty_loc_id_resi": "string",
        "zip_3_resi": "string",
        "zip_5_resi": "string",
        "st_resi": "string",
        "st_loc_id_resi": "string",
        "mcnty_serv": "string",  ## Service loc info
        "cnty_loc_id_serv": "string",
        "zip_3_serv": "string",
        "zip_5_serv": "string",
        "st_serv": "string",
        "st_loc_id_serv": "string",
        "code_system": "string",  ## Encounter info
        "dx_level": "string",
        "dx": "string",
        "ndc": "string",
        "days_supply": "Int16",
        "service_date": "string",
        "discharge_date": "string",
        "fac_prof_ind": "string",
        "toc": "string",
        "los": "Int16",
        "pri_payer": "Int16",
        "payer_2": "Int16",
        "dual_ind": "Int16",
        "mc_ind": "Int16",
        "tot_pay_amt": "float",  ## Payment info
        "mdcr_pay_amt": "float",
        "mdcd_pay_amt": "float",
        "priv_pay_amt": "float",
        "oop_pay_amt": "float",
        "oth_pay_amt": "float",
        "tot_chg_amt": "float",  ## Charge info
        "mdcr_chg_amt": "float",
        "mdcd_chg_amt": "float",
        "priv_chg_amt": "float",
        "oop_chg_amt": "float",
        "oth_chg_amt": "float",
        "survey_wt": "float",
    }  ## Other info

    ###############################
    # Loading data and transforming
    ###############################
    # # Reading in set of files to process
    df = pd.read_parquet(batch_path)

    ###############################
    # Location, year, and NID mapping
    ###############################
    # Setting county location info to none (will be fixed in ZIPFIX)
    # Also setting zip3 info
    df.loc[:, ["mcnty_resi", "cnty_loc_id_resi", "mcnty_serv", "cnty_loc_id_serv"]] = (
        "-1"
    )
    df["zip_3_serv"] = df["zip_5_serv"].astype("str").str[:3]

    if table == "rx_claims":
        # Subsetting to only final RX claim
        df = df.loc[
            df["transaction_code"].isin(["B1", "B3"]) & (df["response_code"] == "P"), :
        ]

        # Mapping NID
        df["nid"] = "505395"

        # Mapping year info
        df["year_adm"] = df["year_id"]
        df["year_clm"] = df["year_id"]
        df["year_dchg"] = df["year_id"]

        # Filling missing zip info with nulls
        df["zip_5_resi"] = "-1"

        # Mapping state from zip info, selecting zip 5 mapped state when possible,
        # Otherwise falling back to zip 3 mapped state
        df["st_resi2"] = df["zip_3_resi"].map(zip3_state_map)
        df["st_serv"] = (
            df["zip_5_serv"]
            .map(zip_state_map)
            .replace("None", "-1")
            .replace("XX", "-1")
            .fillna("-1")
        )
        df["st_resi"] = np.where(
            df["st_resi"].isnull() | df["st_resi"] == "XX",
            df["st_resi2"],
            df["st_resi"],
        )
        df["st_resi"] = (
            df["st_resi"].replace("None", "-1").replace("XX", "-1").fillna("-1")
        )

    elif table == "mx_claimline":

        # Subsetting to only rows with fac_prof_ind (i.e. exlcuding dental claims)
        df = df.loc[df["fac_prof_ind"].isin(["I", "P"]), :]

        # Setting flag for claims with emergency department revenue code in any line
        # Then removing any duplicate lines and preserving this flag to assign TOC later on
        ED_rev_codes = ["0450", "0451", "0452", "0456", "0459", "0981"]
        df["ED_rev_flag"] = df.groupby("claim_id")["revenue_code"].transform(
            lambda x: (x.isin(ED_rev_codes)).any()
        )
        df.drop_duplicates(["claim_id", "ED_rev_flag"], inplace=True)

        # Mapping NID
        df["nid"] = "505396"

        # Mapping year info
        df["year_id"] = df["year_adm"]
        df["year_clm"] = df["year_dchg"]

        # Setting zip3 info
        df["zip_3_resi"] = df["zip_5_resi"].astype("str").str[:3]

        # Mapping state from zip info, selecting zip 5 mapped state when possible,
        # Otherwise falling back to zip 3 mapped state
        df["st_resi2"] = df["zip_5_resi"].map(zip_state_map)
        df["st_serv2"] = df["zip_5_serv"].map(zip_state_map)
        df["st_resi"] = np.where(
            df["st_resi"].isnull() | df["st_resi"] == "XX",
            df["st_resi2"],
            df["st_resi"],
        )
        df["st_serv"] = np.where(
            df["st_serv"].isnull() | df["st_serv"] == "XX",
            df["st_serv2"],
            df["st_serv"],
        )
        df["st_resi"] = (
            df["st_resi"].replace("None", "-1").replace("XX", "-1").fillna("-1")
        )
        df["st_serv"] = (
            df["st_serv"].replace("None", "-1").replace("XX", "-1").fillna("-1")
        )

        # Mapping on state location ids
        df["st_loc_id_resi"] = df["st_resi"].map(state_id_map)
        df["st_loc_id_serv"] = df["st_serv"].map(state_id_map)

    ###############################
    # Payer and payment info mapping
    ###############################
    # Mapping primary payer column to DEX standard
    df["pri_payer_DEX"] = np.where(
        df["pri_payer"] == "Medicare",
        1,
        np.where(
            df["pri_payer"] == "Commercial",
            2,
            np.where(
                df["pri_payer"] == "Medicaid",
                3,
                np.where(
                    df["pri_payer_plan"] == "Self Pay",
                    4,
                    np.where(
                        df["pri_payer"] == "Dual: Medicare & Medicaid",
                        1,
                        np.where(df["pri_payer"].isnull(), 21, 19),
                    ),
                ),
            ),
        ),
    )

    # Filling nulls for certain charge amounts
    df.loc[:, ["mdcr_chg_amt", "mdcd_chg_amt", "priv_chg_amt", "oth_chg_amt"]] = np.nan

    # Mapping managed care indicator to DEX standard
    df["mc_ind"] = np.where(
        df["pri_payer_plan"].isin(["Managed Medicaid", "Medicare Advantage"]), 1, 0
    )

    if table == "rx_claims":

        # Getting medicare paid amount
        df["mdcr_pay_amt"] = np.where(
            df["pri_payer_DEX"] == 1, df["pri_pay_amt"], np.nan
        ).astype("float")

        # Getting private paid amount
        df["priv_pay_amt"] = np.where(
            df["pri_payer_DEX"] == 2, df["pri_pay_amt"], np.nan
        ).astype("float")

        # Getting medicaid paid amount
        df["mdcd_pay_amt"] = np.where(
            df["pri_payer_DEX"] == 3, df["pri_pay_amt"], np.nan
        ).astype("float")

        # Getting out-of-pocket paid amount
        df["oop_pay_amt"] = np.where(
            df["pri_payer_DEX"] == 4, df["pri_pay_amt"] + df["oop_pay_amt"], np.nan
        )

        # Fixing issues with oop_chg_amt column
        df["oop_chg_amt"] = (
            df["oop_chg_amt"]
            .astype("str")
            .str.lstrip("0")
            .replace("None", np.nan)
            .astype("float")
        )

        # Getting other paid amount
        df["oth_pay_amt"] = np.where(
            df["pri_payer_DEX"].isin([19, 21]), df["pri_pay_amt"], np.nan
        ).astype("float")

        # Mapping dual enrollment indicator to DEX standard
        df["dual_ind"] = np.where(df["pri_payer"] == "Dual: Medicare & Medicaid", 1, 0)

    elif table == "mx_claimline":

        # Mapping secondary payer column to DEX standard
        df["sec_payer_DEX"] = np.where(
            df["sec_payer"] == "Medicare",
            1,
            np.where(
                df["sec_payer"] == "Commercial",
                2,
                np.where(
                    df["sec_payer"] == "Medicaid",
                    3,
                    np.where(
                        df["sec_payer_plan"] == "Self Pay",
                        4,
                        np.where(
                            (
                                (df["sec_payer"] == "Dual: Medicare & Medicaid")
                                & (
                                    df["pri_payer"].isin(
                                        ["Medicare", "Dual: Medicare & Medicaid"]
                                    )
                                )
                            ),
                            3,
                            np.where(
                                df["sec_payer"] == "Dual: Medicare & Medicaid",
                                1,
                                np.where(df["pri_payer"].isnull(), 21, 19),
                            ),
                        ),
                    ),
                ),
            ),
        )

        # Getting medicare paid amount
        df["mdcr_pay_amt"] = np.where(
            ((df["pri_payer_DEX"] == 1) & (df["sec_payer_DEX"] == 1)),
            df["pri_pay_amt"] + df["sec_pay_amt"],
            np.where(
                df["pri_payer_DEX"] == 1,
                df["pri_pay_amt"],
                np.where(df["sec_payer_DEX"] == 1, df["sec_pay_amt"], np.nan),
            ),
        ).astype("float")

        # Getting private paid amount
        df["priv_pay_amt"] = np.where(
            ((df["pri_payer_DEX"] == 2) & (df["sec_payer_DEX"] == 2)),
            df["pri_pay_amt"] + df["sec_pay_amt"],
            np.where(
                df["pri_payer_DEX"] == 2,
                df["pri_pay_amt"],
                np.where(df["sec_payer_DEX"] == 2, df["sec_pay_amt"], np.nan),
            ),
        ).astype("float")

        # Getting medicaid paid amount
        df["mdcd_pay_amt"] = np.where(
            ((df["pri_payer_DEX"] == 3) & (df["sec_payer_DEX"] == 3)),
            df["pri_pay_amt"] + df["sec_pay_amt"],
            np.where(
                df["pri_payer_DEX"] == 3,
                df["pri_pay_amt"],
                np.where(df["sec_payer_DEX"] == 3, df["sec_pay_amt"], np.nan),
            ),
        ).astype("float")

        # Getting out-of-pocket paid amount
        df["oop_pay_amt"] = np.where(
            ((df["pri_payer_DEX"] == 4) & (df["sec_payer_DEX"] == 4)),
            df["pri_pay_amt"] + df["sec_pay_amt"] + df["oop_pay_amt"],
            np.where(
                df["pri_payer_DEX"] == 4,
                df["pri_pay_amt"] + df["oop_pay_amt"],
                np.where(
                    df["sec_payer_DEX"] == 4,
                    df["sec_pay_amt"] + df["oop_pay_amt"],
                    np.nan,
                ),
            ),
        )
        # Fixing issues with oop_chg_amt column
        df["oop_chg_amt"] = (
            df["oop_chg_amt"]
            .astype("str")
            .str.lstrip("0")
            .replace("None", np.nan)
            .astype("float")
        )

        # Getting other paid amount
        df["oth_pay_amt"] = np.where(
            (df["pri_payer_DEX"].isin([19, 21])) & (df["sec_payer_DEX"].isin([19, 21])),
            df["pri_pay_amt"] + df["sec_pay_amt"],
            np.where(
                df["pri_payer_DEX"].isin([19, 21]),
                df["pri_pay_amt"],
                np.where(df["sec_payer_DEX"].isin([19, 21]), df["sec_pay_amt"], np.nan),
            ),
        ).astype("float")

        # Mapping dual enrollment indicator to DEX standard
        df["dual_ind"] = np.where(
            (
                (df["pri_payer"] == "Dual: Medicare & Medicaid")
                | (
                    (df["sec_payer"] == "Dual: Medicare & Medicaid")
                    & (df["pri_payer"].isin(["Medicare", "Medicaid"]))
                )
            ),
            1,
            0,
        )

        # Renaming secondary payer column
        df = df.drop(columns=["sec_payer"]).rename(columns={"sec_payer_DEX": "payer_2"})

    # Renaming primary payer column
    df = df.drop(columns=["pri_payer"]).rename(columns={"pri_payer_DEX": "pri_payer"})

    # Calculating total paid amount and total charged amount
    df["tot_pay_amt"] = (
        df[
            [
                "mdcr_pay_amt",
                "priv_pay_amt",
                "mdcd_pay_amt",
                "oop_pay_amt",
                "oth_pay_amt",
            ]
        ]
        .sum(axis=1, min_count=1)
        .astype("float")
    )
    df["tot_chg_amt"] = np.where(
        df["tot_chg_amt"].isna(),
        df[
            [
                "mdcr_chg_amt",
                "priv_chg_amt",
                "mdcd_chg_amt",
                "oop_chg_amt",
                "oth_chg_amt",
            ]
        ].sum(axis=1, min_count=1),
        df["tot_chg_amt"],
    ).astype("float")
    df["tot_chg_amt"] = np.where(
        df["tot_pay_amt"] > df["tot_chg_amt"], df["tot_pay_amt"], df["tot_chg_amt"]
    ).astype("float")

    ###############################
    # Demographic info mapping
    ###############################
    # Mapping age column to DEX bins
    df = kythera_transformer_helper.get_age_group(df, "age")

    # Mapping race/ethnicity columns to DEX standard
    df["race_cd"] = np.where(
        ((df["race"] == "HISPANIC") | (df["ethnicity"] == "Hispanic")),
        "HISP",
        np.where(
            (df["race"] == "WHITE")
            | ((df["race"] == "OTHER") & (df["ethnicity"] == "White"))
            | ((df["race"].isnull()) & (df["ethnicity"] == "White")),
            "WHT",
            np.where(
                (df["race"] == "AFRICAN AMERICAN")
                | (
                    (df["race"] == "OTHER")
                    & (df["ethnicity"] == "Black or African American")
                )
                | (
                    (df["race"].isnull())
                    & (df["ethnicity"] == "Black or African American")
                ),
                "BLCK",
                np.where(
                    (df["race"] == "ASIAN")
                    | ((df["race"] == "OTHER") & (df["ethnicity"] == "Asian"))
                    | ((df["race"].isnull()) & (df["ethnicity"] == "Asian")),
                    "API",
                    np.where(
                        (df["race"] == "OTHER") | (df["ethnicity"] == "Other"),
                        "OTH",
                        "UNK",
                    ),
                ),
            ),
        ),
    )

    # Mapping sex column to DEX standard
    df["sex_id"] = df["patient_gender"].map(sex_map)

    ###############################
    # TOC / claim info mapping
    ###############################
    if table == "rx_claims":

        # Mapping all RX claims to RX TOC
        df["toc"] = "RX"

        # Cleaning days supply column
        df["days_supply"] = (
            df["days_supply"]
            .str.replace(r"^(0+)", "")
            .fillna("0")
            .str.extract("(\d+)", expand=False)
            .astype("float")
            .astype("Int32")
        )

    elif table == "mx_claimline":

        # Mapping ICD code system to DEX standard
        df["code_system_DEX"] = df["code_system"].map(code_sys_map)

        # Mapping ICD code system using date if claim did not include ICD code system
        df["code_system"] = np.where(
            df["code_system_DEX"].notnull(),
            df["code_system_DEX"],
            np.where(
                pd.to_datetime(df["discharge_date"]) >= pd.Timestamp("2015-10-01"),
                "icd10",
                "icd9",
            ),
        )

        # Mapping facility/professional indicator and dropping old column
        df["fac_prof_ind"] = df["fac_prof_ind"].map(fac_prof_map)

        # Mapping POS and TOB to corresponding TOC
        df["type_of_bill_code"] = df["bill_type_code"].astype("str").str[:2]
        df["pos_toc"] = (
            pd.to_numeric(df["place_of_service_code"], errors="coerce")
            .astype("Int32")
            .astype("str")
            .map(pos_map_dict)
        )
        df["tob_toc"] = df["type_of_bill_code"].map(tob_map_dict)

        # If los is <1 for any IP/NF pos_toc values, re-assign as AM
        df["pos_toc"] = np.where(
            (df["pos_toc"].isin(["IP", "NF"]) & (df["los"] < 1)), "AM", df["pos_toc"]
        )

        # If los is <1 for any of the specified IP/NF tob_toc values, reassign as AM
        IP_AM_tob = [
            12,
            14,
            15,
            16,
            18,
            19,
            42,
            44,
            45,
            46,
            47,
            48,
            49,
            52,
            54,
            55,
            56,
            57,
            58,
            59,
            84,
            85,
            86,
            88,
            89,
        ]
        NF_AM_tob = [22, 24, 25, 26, 28, 29, 62, 64, 65, 66, 67, 68, 81]

        df["tob_toc"] = np.where(
            df["ED_rev_flag"],
            "ED",
            np.where(
                (df["type_of_bill_code"].isin(IP_AM_tob + NF_AM_tob) & (df["los"] < 1)),
                "AM",
                df["tob_toc"],
            ),
        )

        # Taking POS TOC first, if null then taking TOB TOC
        df["toc"] = np.where(df["pos_toc"].notnull(), df["pos_toc"], df["tob_toc"])
        df = df.drop(columns=["pos_toc", "tob_toc"])

        # Renaming dx cols to use DEX naming scheme
        df = df.rename(
            columns={
                "dx1": "dx_1",
                "dx2": "dx_2",
                "dx3": "dx_3",
                "dx4": "dx_4",
                "dx5": "dx_5",
                "dx6": "dx_6",
                "dx7": "dx_7",
                "dx8": "dx_8",
            }
        )

        # Transforming diagnosis columns into long format
        df = kythera_transformer_helper.w2l_transform(df)

    # Re-ordering and re-typing dataframe columns
    existing_cols = {k: v for k, v in stage_3_cols.items() if k in df.columns}
    for c, t in existing_cols.items():
        df[c] = df[c].astype(t)
    df = df.loc[:, existing_cols]
    return df


def write_parquet(df, outpath):

    # Nulls values are automatically dropped for partition columns
    # Assigning a flag of -1 for any nulls in partition columns to prevent data loss
    partition_cols = ["year_id", "age_group_years_start", "sex_id"]
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
        basename_template=f"{uuid.uuid4()}_{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )


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

    parser.add_argument(
        "--table", type=str, required=True, help="Kythera table being processed"
    )

    # Assigning input args to variables
    args = vars(parser.parse_args())
    batch_path = Path(args["batch_path"])
    outpath = Path(args["outpath"])
    table = args["table"]

    formatted_df = run_format_transform(batch_path, table)

    write_parquet(formatted_df, outpath)
