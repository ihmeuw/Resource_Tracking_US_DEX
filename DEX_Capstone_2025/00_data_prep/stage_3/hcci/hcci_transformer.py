## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to transform HCCI data.
## ==================================================

import pandas as pd
import re
from pathlib import Path


def w2l_transform(df: pd.DataFrame, pivot_cols: list, var_name: str, val_name: str):
    """Transform dataframe from wide to long format on chosen columns

    Args:
    df -- Pandas dataframe
    pivot_cols -- Columns to transform to long format. Regex notation can be used.
    """

    # Finding all columns to tranform from wide to long
    # Uses regex expressions for dx and ecode by default
    matched_cols = [list(filter(re.compile(i).match, df.columns)) for i in pivot_cols]
    long_cols = [item for sublist in matched_cols for item in sublist]

    # Finding all columns that will not be transformed
    id_cols = [col for col in df.columns if col not in long_cols]

    # Melting dataframe and dropping any NA/None values in long format
    df = df.melt(
        id_vars=id_cols, value_vars=long_cols, var_name=var_name, value_name=val_name
    )
    df.dropna(axis=0, subset=[var_name, val_name], inplace=True)

    return df


if __name__ == "__main__":

    # Loading all HCCI data paths
    HCCI_basepath = Path("FILEPATH")
    filepaths = [i for i in HCCI_basepath.iterdir() if i.suffix == ".csv"]

    # Loading in HCCI data
    df = pd.concat([pd.read_csv(i) for i in filepaths]).reset_index(drop=True)

    # Defining metrics and IP/NF specific metrics
    metric_cols = [
        "encounters_per_person",
        "n",
        "n_bene",
        "n_claim",
        "se_spend_per_encounter",
        "spend_per_encounter",
        "se_encounters_per_person",
    ]
    ip_metric_cols = [
        "se_days_per_encounter",
        "se_spend_per_day",
        "spend_per_day",
        "days_per_encounter",
    ]

    # Removing rows with any NA or negative metric values. Ignoring NA IP metric when TOC is not IP.
    df["ind"] = (
        (df[metric_cols].isna().any(axis=1))
        | ((df[metric_cols] < 0).any(axis=1))
        | ((df["toc"] == "IP") & (df[ip_metric_cols].isna().any(axis=1)))
        | ((df["toc"] == "IP") & ((df[ip_metric_cols] < 0).any(axis=1)))
    )
    df.drop(index=df[df["ind"] == 1].index, inplace=True)
    df.drop(columns=["ind"], inplace=True)

    # Loading in location names for states and counties used in collapse data
    cnty_names = pd.read_csv("FILEPATH")
    state_names = pd.read_csv("FILEPATH")
    loc_name_map = dict(
        zip(
            cnty_names.location.astype(str).to_list() + state_names.location.to_list(),
            cnty_names.location_name.to_list() + state_names.location_name.to_list(),
        )
    )

    # Removing decimals where applicable (mcnty ids can appear as floats)
    df["location"] = df["location"].astype(str).replace("\.0", "", regex=True)

    # Mapping locations to location names
    df["location_name"] = df["location"].map(loc_name_map)

    # Mapping HCCI Age bins to DEX age_group_years_start
    # Uses the following map, avoids duplicating values for multiple age groups
    """
    0-17    -> 10-15
    18-24   -> 20-25
    25-34   -> 30-35
    35-44   -> 40-45
    45-54   -> 50-55
    55-64   -> 60-65
    65+     -> 65-70  
    """
    age_map = {1: 10, 2: 20, 3: 30, 4: 40, 5: 50, 6: 60, 7: 65}
    df["age_group_years_start"] = df["age_id"].map(age_map)

    # Creating column with value and standard error as tuple (for w2l transform)
    df["metric_spend_per_encounter"] = tuple(
        zip(df["spend_per_encounter"], df["se_spend_per_encounter"])
    )
    df["metric_spend_per_day"] = tuple(zip(df["spend_per_day"], df["se_spend_per_day"]))
    df["metric_encounters_per_person"] = tuple(
        zip(df["encounters_per_person"], df["se_encounters_per_person"])
    )
    df["metric_days_per_encounter"] = tuple(
        zip(df["days_per_encounter"], df["se_days_per_encounter"])
    )

    # Dropping old metric columns
    df.drop(
        columns=[
            "spend_per_day",
            "spend_per_encounter",
            "encounters_per_person",
            "days_per_encounter",
            "se_spend_per_day",
            "se_spend_per_encounter",
            "se_encounters_per_person",
            "se_days_per_encounter",
        ],
        inplace=True,
    )

    # Transforming metrics to be long
    df = w2l_transform(
        df,
        [
            "metric_spend_per_day",
            "metric_spend_per_encounter",
            "metric_encounters_per_person",
            "metric_days_per_encounter",
        ],
        "metric",
        "temp_val",
    ).copy()

    # Removing "metric_" from each metric name in the "metric" column
    df["metric"] = df["metric"].str[7:]

    # Getting metric value and standard error from tuples
    df["raw_val"] = df["temp_val"].str[0]
    df["se"] = df["temp_val"].str[1]

    # Renaming columns to appropriate collapse names
    df.rename(
        columns={"n_claim": "n_obs", "n": "n_encounters", "n_bene": "n_people"},
        inplace=True,
    )

    # Converting columns to correct typings
    type_dict = {
        "acause": object,
        "toc": object,
        "year_id": int,
        "age_group_years_start": int,
        "sex_id": int,
        "pri_payer": object,
        "payer": object,
        "geo": object,
        "location": object,
        "location_name": object,
        "dataset": object,
        "metric": object,
        "raw_val": float,
        "se": float,
        "n_obs": float,
        "n_encounters": float,
        "n_people": float,
    }
    df = df[type_dict.keys()].astype(type_dict)

    # Metrics were calculated for all TOCs, but DEX does not want _per_day metrics for non-IP/non-NF TOCs. Dropping those
    df = df[
        ~(
            ~df["toc"].isin(["NF", "IP"])
            & df["metric"].isin(["spend_per_day", "days_per_encounter"])
        )
    ]

    # Because both HCCI outpatient and inpatient data can contain IP as a TOC,
    # We may have duplicate rows with different metric values. To resolve this,
    # We calculate the average metric value, weighting by n_encounters. For the
    # Standard error, we keep the maximum value.

    # Get value weighted by n_encounters
    df["weighted_val"] = df["raw_val"] * df["n_encounters"]

    # Aggregate to remove duplicates, all values are summed except standard error (max value is kept)
    df = df.groupby(
        [
            "acause",
            "toc",
            "year_id",
            "age_group_years_start",
            "sex_id",
            "pri_payer",
            "payer",
            "geo",
            "location",
            "location_name",
            "dataset",
            "metric",
        ],
        as_index=False,
    ).agg(
        weighted_val=("weighted_val", "sum"),
        se=("se", "max"),
        n_obs=("n_obs", "sum"),
        n_encounters=("n_encounters", "sum"),
        n_people=("n_people", "sum"),
    )

    # Calculated weighted value average, remove original weighted value from df
    df["raw_val"] = df["weighted_val"] / df["n_encounters"]
    df.drop(columns=["weighted_val"], inplace=True)

    # Saving out properly formatted HCCI collapse data
    df.to_parquet(
        "FILEPATH",
        partition_cols=["toc", "year_id", "age_group_years_start"],
    )
