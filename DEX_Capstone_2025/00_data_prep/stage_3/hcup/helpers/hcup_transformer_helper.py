## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to assist with transforming HCUP data.
## ==================================================

from db_queries import get_age_metadata
import pandas as pd
import numpy as np
import re
import uuid


def reduce_cols(df: pd.DataFrame, col_keep: list = [], regex_col_keep: list = []):
    """Reduce columns in dataframe, keeping only some

    Args:
    df -- Pandas dataframe
    col_keep -- Names of columns to keep in dataframe
    regex_col_keep -- Regex expressions to match to columns. Matches will be kept
    """

    # Creating list of cols matching regex expressions
    matched_cols = [
        list(filter(re.compile(i).match, df.columns)) for i in regex_col_keep
    ]
    flat_matched_cols = [item for sublist in matched_cols for item in sublist]

    # Getting all specified columns that exist in dataframe
    all_cols = set([c for c in (flat_matched_cols + col_keep) if c in df.columns])

    # Subetting dataframe
    df = df[all_cols]

    return df


def get_race(df: pd.DataFrame, race_col: str = "race"):
    """Mapping race from HCUP values to DEX standard

    Args:
    df -- Pandas dataframe
    race_col -- Column that contains race information
    """

    # RTI race name dictionary
    race_lookup = {
        np.nan: None,
        0: "UNK",
        1: "WHT",
        2: "BLCK",
        3: "OTH",
        4: "API",
        5: "HISP",
        6: "AIAN",
    }

    # Applying dictionary to RTI encoded race column
    df["race_cd"] = df[race_col].map(race_lookup)

    # Adding language column to dataframe, if available
    if "primlang" in df.columns:
        df["prim_lang"] = df["primlang"].str.upper()
        df.loc[df["primlang"] == "EN", "prim_lang"] = "ENG"
    else:
        df["prim_lang"] = None

    return df


def get_payer(df: pd.DataFrame, type: str, col: str):
    """Mapping payer from HCUP values to DEX IDs

    Args:
    df -- Pandas dataframe
    """

    # HCUP payer to DEX abbreviation dictionary
    # Included just to clarify how HCUP is being mapped
    pay_lookup = {
        1: "mdcr",
        2: "mdcd",
        3: "priv",
        4: "oop",
        5: "nc",
        6: "oth_not_mdcd_mdcr_oop_priv",
    }

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

    # Applying mapping, if possible
    try:
        df[f"{type}_payer"] = df[col].map(pay_lookup).map(dex_lookup)
    except KeyError:
        df[f"{type}_payer"] = None
    return df


def get_age_group(df: pd.DataFrame, age_col: str = "age_disc"):
    """Determine DEX age group for each row in dataframe.

    Args:
    df -- Pandas dataframe
    age_col -- Column that contains age information
    """

    # Trying to create column for age at discharge
    try:
        df[age_col] = df["year"] - df["byear"]
    except KeyError:
        try:
            df[age_col] = df["age"] + (df["year"] - df["ayear"])

        # Defaulting to age at admission if age at discharge cannot be found
        except KeyError:
            print("Unable to establish discharge age. Using admission age instead.")
            age_col = "age_admit"

    # Getting age bin data and reordering
    ages = (
        get_age_metadata(age_group_set_id=27, gbd_round_id=7)
        .sort_values(by="age_group_years_start")
        .reset_index()
    )

    # Creating age bins
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

    # df_col is the name of the column we want to map on (already in the dataframe)
    # loc_map_col is the name of the column we want to use to map

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
                    .map(dict(zip(locs[loc_map_col], locs[loc_name])))
                    .fillna("-1")
                )
            except KeyError:
                df[df_name] = "-1"
                print(
                    f"{df_col} column does not exist in dataframe. Filling {df_name} with nulls."
                )
        else:
            df[df_name] = "-1"
            print(
                f"Cannot map {df_name} column due to one-to-many mapping. Filling with nulls."
            )

    return df


def get_zip(df: pd.DataFrame, dataset: str):
    """Add columns for 3 and 5 digit ZIP codes on HCUP data.

    Args:
    df -- Pandas dataframe
    dataset -- HCUP dataset name
    """

    # NIS may have service ZIP, but contains no residence ZIP
    if dataset == "NIS":

        # No residence ZIP available
        df["zip_3_resi"] = None
        df["zip_5_resi"] = None

        # Get ZIP info from HOSPZIP, if available
        try:
            df["zip_5_serv"] = df["hospzip"]
            df["zip_3_serv"] = df["hospzip"].str[2:]

        # Otherwise, no ZIP data can be mapped
        except KeyError:
            print("No ZIP info available.")
            df["zip_5_serv"] = None
            df["zip_3_serv"] = None
        return df

    # SIDS/SEDD may have residence ZIP, but contain no service ZIP
    elif dataset == "SIDS" or dataset == "SEDD":

        # No service ZIP available
        df["zip_3_serv"] = None
        df["zip_5_serv"] = None

        # If 3-digit ZIP is available, use that for ZIP-3
        # Otherwise, infer 3-digit ZIP from full ZIP
        try:
            df["zip_3_resi"] = df["zip3"]
        except KeyError:
            try:
                df["zip_3_resi"] = df["zip"].str[2:]
                df["zip_5_resi"] = df["zip"]
                return df
            # Otherwise, no ZIP data can be mapped
            except KeyError:
                print("No ZIP info available.")
                df["zip_3_resi"] = None
                df["zip_5_resi"] = None
                return df

        # If 3-digit ZIP is available, check if 5-digit ZIP also is
        try:
            df["zip_5_resi"] = df["zip"]
        except KeyError:
            print("No 5-digit ZIP info available.")
            df["zip_5_resi"] = None
        return df

    # NEDS has no ZIP info available
    elif dataset == "NEDS":

        # No residence ZIP available
        df["zip_3_resi"] = None
        df["zip_5_resi"] = None

        # No service ZIP available
        df["zip_3_serv"] = None
        df["zip_5_serv"] = None
        return df

    else:
        raise RuntimeError(
            "Unknown dataset provided: {dataset}. Should be SIDS, SEDD, NIS or NEDS."
        )


def w2l_transform(df: pd.DataFrame, pivot_cols: list = [r"^dx_\d*$", r"^ecode_\d*$"]):
    """Transform dataframe from wide to long format on chosen columns

    Args:
    df -- Pandas dataframe
    pivot_cols -- Columns to transform to long format. Regex notation expected.
    """

    # Finding all columns to tranform from wide to long
    # Uses regex expressions for dx and ecode by default
    matched_cols = [list(filter(re.compile(i).match, df.columns)) for i in pivot_cols]
    long_cols = [item for sublist in matched_cols for item in sublist]

    # Finding all columns that will not be transformed
    id_cols = [col for col in df.columns if col not in long_cols]

    # Melting dataframe and dropping any NA/None values in long format
    df = df.melt(
        id_vars=id_cols, value_vars=long_cols, var_name="dx_level", value_name="dx"
    )
    df.dropna(axis=0, subset=["dx", "dx_level"], inplace=True)

    # Abbreviating ecode values
    df["dx_level"].replace("ecode", "ex", regex=True, inplace=True)

    return df


def get_toc(df: pd.DataFrame, dataset: str):
    """Determine type-of-care for HCUP datasets.

    Args:
    df -- Pandas dataframe
    dataset -- HCUP dataset name
    """

    # If SEDD or NEDS, TOC is ED
    if "ED" in dataset:
        type_of_care = "ED"

    # If SIDS or NIS, TOC is IP
    elif "I" in dataset:
        type_of_care = "IP"

    # If neither, raise an error
    else:
        raise ValueError(
            f"{dataset} is expected to contain either ED or I to indicate TOC."
        )

    # Adding type of care to dataframe
    df["toc"] = type_of_care
    return df


def get_icd_ver(df: pd.DataFrame, icd_col: str = "code_system_id"):
    """Create new column with DEX ICD version name.

    Args:
    df -- Pandas dataframe
    icd_col -- Column to map to DEX ICD version name
    """

    # ICD recoding dictionary
    recode_dict = {1: "icd9", 2: "icd10"}

    # Adding new column and dropping old one
    df["code_system"] = df[icd_col].map(recode_dict)
    df.drop(columns=[icd_col], inplace=True)
    return df


def create_uuid(
    df: pd.DataFrame, uuid_col_name: str, group_cols: list = [], drop_na=False
):
    """Given a grouping of columns, create a UUID for that group in a new column.

    Args:
    df -- Pandas dataframe
    uuid_col_name -- Name for new UUID column
    group_cols -- Columns to group by
    drop_na -- Whether or not to drop null values in groupby statement
    """

    # Grouping columns and assigning a temporary unique id
    # Keeping NAs means that NA in any group_col will be
    # Treated as a normal unique value. Skipping NAs means
    # That NA in any group_col will result in that whole
    # row being mapped to NA in the uuid_col_name. This can
    # Result in unintended groupings, and likely should only
    # Be used if group_cols is only one column.
    df["temp_id"] = df.groupby(group_cols, dropna=drop_na).ngroup()

    # Getting all temporary ids as list
    groups = df["temp_id"].unique().tolist()

    # Creating a uuid for each unique id and mapping
    unique_id_map = {i: str(uuid.uuid4()) for i in groups}

    # Dropping any groups with null columns
    if drop_na == True:
        unique_id_map[-1] = None

    # Mapping uuid from temporary id column and dropping
    df[uuid_col_name] = df["temp_id"].map(unique_id_map)
    df.drop(columns=["temp_id"], inplace=True)

    # Ensuring all encounter ids are actually unique
    # There's a ridiculously small chance of duplicates, but better safe than sorry!
    if (
        len(df[uuid_col_name].unique())
        != df.groupby(group_cols, dropna=drop_na).ngroups
    ):
        raise RuntimeError("Some created ids are not unique. Please re-run this step.")

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
            print(f"{c} not found in dataframe. Skipping conversion.")
        except TypeError:
            df[c] = df[c].astype("float").astype(t)
    return df
