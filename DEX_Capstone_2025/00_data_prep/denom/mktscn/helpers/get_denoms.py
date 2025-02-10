## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to get the denominators for the MarketScan data.
## ==================================================

import pandas as pd
from db_queries import get_age_metadata
import argparse
from pathlib import Path

# MarketScan EGEOLOC map from data dictionary
EGEOLOC_MAP = {
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
    age_data_cols = ["age_group_years_start"]
    for column in age_data_cols:
        df[column] = df["age_mapper"].map(ages[column].to_dict())

    # Dropping mapper column
    df.drop(columns="age_mapper", inplace=True)
    return df


if __name__ == "__main__":

    # Requesting arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--filepath",
        type=str,
        required=True,
        help="Input filepath for MarketScan enrollment data.",
    )

    # Assigning input args to variables
    args = vars(parser.parse_args())
    filepath = Path(args["filepath"])

    # Getting info from filepath name
    dataset = filepath.stem[0:4]
    year = filepath.stem[7:11]

    # Read in enrollment file
    df = pd.read_parquet(filepath, columns=["SEX", "AGE", "EGEOLOC", "MEMDAYS"])

    # Fixing encoding for 2019 and 2020
    if year in ["2019", "2020"]:
        df["SEX"] = df["SEX"].str.decode("utf-8")
        df["EGEOLOC"] = df["EGEOLOC"].str.decode("utf-8")

    # Get year_id and sex_id
    df["year_id"] = year
    df.rename(columns={"SEX": "sex_id"}, inplace=True)

    # Get age_group_years_start
    df = get_age_group(df, age_col="AGE")

    # Get location
    df["location"] = df["EGEOLOC"].map(EGEOLOC_MAP).fillna("-1")

    # Group by variables of interest to get total days of enrollment
    df = df.groupby(
        ["year_id", "location", "age_group_years_start", "sex_id"], as_index=False
    )["MEMDAYS"].sum()

    # Get population from total days of enrollment
    df["pop"] = df["MEMDAYS"] / 365
    df.drop(columns=["MEMDAYS"], inplace=True)

    # Setting types
    df = df.astype(
        {
            "year_id": "int",
            "location": "str",
            "age_group_years_start": "int",
            "sex_id": "int",
            "pop": "float",
        }
    )

    # Save out denoms
    df.to_parquet("FILEPATH")
