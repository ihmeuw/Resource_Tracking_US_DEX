## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to assist with transforming Kythera data.
## ==================================================

from db_queries import get_age_metadata
import pandas as pd
import numpy as np
import re


def get_age_group(df: pd.DataFrame, age_col: str = "age"):
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
    df["dx"] = df["dx"].replace("None", value=np.nan)
    df["dx_level"] = df["dx_level"].replace("None", value=np.nan)
    df.dropna(axis=0, subset=["dx", "dx_level"], inplace=True)

    # Abbreviating ecode values
    df["dx_level"].replace("ecode", "ex", regex=True, inplace=True)

    return df
