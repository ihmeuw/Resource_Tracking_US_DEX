import argparse
import pandas as pd
import numpy as np
import json
from pathlib import Path
from itertools import product
import pyarrow.dataset as ds
import duckdb


def add_or_filter(df: pd.DataFrame, col_val_dict: dict):
    """Given a Pandas dataframe and dictionary, adding dict keys
    as columns with dict value as value OR filtering to value if
    column already exists.
    """
    for col, val in col_val_dict.items():
        if col not in df.columns:
            df[col] = val
        else:
            df = df.loc[df[col] == val, :]
    return df


def invert_geo(d: dict):
    """Given a dictionary, replaces the "geo" key with opposite DEX geography.
    Returns a copy of the original dictionary with altered key.
    """
    invert_geo_dict = {"state": "county", "county": "state"}
    new_d = d.copy()
    new_d["geo"] = invert_geo_dict[new_d["geo"]]
    return new_d


def impute(param_dict, model_ver, draws, max_model_year):

    ################################
    # SETTING UP VARIOUS CONSTANTS #
    ################################
    # Loading in restrictions dataframe, used to determine valid ages for the data being imputed
    res_df = pd.read_csv("FILEPATH").query("include==1")
    age_range = res_df[
        (res_df["toc"] == param_dict["toc"])
        & (res_df["acause"] == param_dict["acause"])
    ][["age_start", "age_end"]].values[0]

    # Getting all ages we want to model
    age_list = [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85]
    full_ages = age_list[
        age_list.index(age_range[0]) : age_list.index(age_range[1]) + 1
    ]

    # Getting dictionaries of states/counties and their corresponding state
    with open("FILEPATH", "r") as fp:
        states = json.load(fp)
    with open("FILEPATH", "r") as fp:
        counties = json.load(fp)

    # Getting county/state locations we want to model
    state_locs = list(states.keys())
    cnty_locs = list(counties.keys())

    # Getting all years we want to model
    full_years = list(range(2000, max_model_year + 1))

    # Creating combinations of all locations/years/ages we want to model
    full_state_combos = list(product(state_locs, full_years, full_ages))
    full_cnty_combos = list(product(cnty_locs, full_years, full_ages))

    # Columns to group by when imputing (all non-value columns besides geo-specific columns)
    group_cols = [
        "year_id",
        "age_group_years_start",
        "payer",
        "acause",
        "toc",
        "metric",
        "pri_payer",
        "sex_id",
        "state",
    ]

    # Value columns that we are imputing. Currently set to 50 draws (draw_n) and median (median)
    val_cols = ["mean"] + ["draw_" + str(i) for i in range(1, draws + 1)]

    # Columns that are not used to define a model
    model_cols = ["location", "year_id", "age_group_years_start"]

    # Columns to use for reading in specific partitions and to partition by when saving
    partition_cols = ["geo", "toc", "metric", "pri_payer", "payer"]

    ################################
    #   RUNNING IMPUTATION STEPS   #
    ################################
    # Setting up data to query
    conn = duckdb.connect()
    dataset = ds.dataset(
        "FILEPATH",
        partitioning="hive",
    )

    # Setting up filter statement according to specified params to impute,
    # First filter gets specified params and geography, second filter gets
    # Specified params for the opposite geography
    filters = " AND ".join(
        [
            f"{k} = '{v}'" if type(v) == str else f"{k} = {v}"
            for k, v in param_dict.items()
        ]
    )
    invert_geo_filters = " AND ".join(
        [
            f"{k} = '{v}'" if type(v) == str else f"{k} = {v}"
            for k, v in invert_geo(param_dict).items()
        ]
    )

    # Loading in data for current and opposite geography
    to_impute_df = conn.query(f"SELECT * FROM dataset WHERE {filters}").to_df()
    invert_geo_df = conn.query(
        f"SELECT * FROM dataset WHERE {invert_geo_filters}"
    ).to_df()

    # Getting list of empirical (i.e. modeled) years and ages. If there is no modeled data (such as when neither state
    # nor county converged for a set of params), then use the full set of years/ages instead.
    if to_impute_df.empty and invert_geo_df.empty:
        emp_ages = full_ages
        emp_years = full_years
    else:
        emp_ages = list(
            set(
                to_impute_df["age_group_years_start"].unique().tolist()
                + invert_geo_df["age_group_years_start"].unique().tolist()
            )
        )
        emp_years = list(
            set(
                to_impute_df["year_id"].unique().tolist()
                + invert_geo_df["year_id"].unique().tolist()
            )
        )

    # Creating empirical combinations of locations/years/ages (non-model specific variables)
    emp_state_combos = list(product(state_locs, emp_years, emp_ages))
    emp_cnty_combos = list(product(cnty_locs, emp_years, emp_ages))

    # Creating two empty dataframes, one with only empirical years/ages and one with all valid years/ages
    if param_dict["geo"] == "state":
        # Creating empirical dataframe from modeled years/ages
        emp_df = pd.DataFrame(emp_state_combos, columns=model_cols)
        emp_df["state"] = emp_df["location"].map(states)
        emp_df = add_or_filter(emp_df, param_dict)

        # Creating full dataframe from all valid years/ages
        full_df = pd.DataFrame(full_state_combos, columns=model_cols)
        full_df["state"] = full_df["location"].map(states)
        full_df = add_or_filter(full_df, param_dict)
    elif param_dict["geo"] == "county":
        # Creating empirical dataframe from modeled years/ages
        emp_df = pd.DataFrame(emp_cnty_combos, columns=model_cols)
        emp_df["state"] = emp_df["location"].map(counties)
        emp_df = add_or_filter(emp_df, param_dict)

        # Creating full dataframe from all valid years/ages
        full_df = pd.DataFrame(full_cnty_combos, columns=model_cols)
        full_df["state"] = full_df["location"].map(counties)
        full_df = add_or_filter(full_df, param_dict)

    # Merging on any data available from the model being imputed to the empirical dataframe
    emp_df = emp_df.merge(
        to_impute_df.drop(columns=["model_version_id"]),
        on=group_cols + ["geo", "location"],
        how="left",
    )

    # Getting threshold value for this set of parameters. Values exceedthing threshold are considered implausible
    # Attempting to get threshold at toc/metric/acause level, otherwise using toc/metric specific threshold
    thresholds = pd.read_csv("FILEPATH")
    try:
        threshold_val = thresholds[
            (thresholds["toc"] == param_dict["toc"])
            & (thresholds["metric"] == param_dict["metric"])
            & (thresholds["acause"] == param_dict["acause"])
        ]["threshold"].values[0]
    except IndexError:
        threshold_val = thresholds[
            (thresholds["toc"] == param_dict["toc"])
            & (thresholds["metric"] == param_dict["metric"])
        ]["threshold"].values[0]

    # Finding implausible values and setting to NA for later
    emp_df[val_cols] = np.where(
        emp_df[val_cols] > threshold_val, np.nan, emp_df[val_cols]
    )

    # Getting values from the opposite geography to impute missing values
    # We group the data to the state level, resize this data to mirror emp_df (by using a join),
    # Then extract the value columns we want to use for imputation.
    invert_geo_df = invert_geo_df.groupby(group_cols, as_index=False).mean()
    invert_geo_df = emp_df[group_cols].merge(invert_geo_df, on=group_cols, how="left")
    invert_geo_vals = invert_geo_df.loc[:, val_cols]

    # Loading in level 2 and level 3 medians
    med_2_df = pd.read_csv("FILEPATH")
    med_3_df = pd.read_csv("FILEPATH")

    # Filtering to specific value, this should only result in a single row
    med_2_df = add_or_filter(med_2_df, param_dict)
    med_3_df = add_or_filter(med_3_df, param_dict)
    assert len(med_2_df) <= 1 and len(med_3_df) <= 1

    # Extracting the values filtered to, and resizing to mirror emp_df
    med_2_vals = (
        med_2_df[val_cols]
        .loc[med_2_df.index.repeat(len(emp_df))]
        .reset_index(drop=True)
    )
    med_3_vals = (
        med_3_df[val_cols]
        .loc[med_3_df.index.repeat(len(emp_df))]
        .reset_index(drop=True)
    )

    # Filling in NAs in emp_df, which correspond to missing or implausible values. For encounters_per_person,
    # We first try to fill with the oppotsite geography values, otherwise fill with zeros. For other metrics,
    # We first try to fill with the opposite geography values, then level 2 median values, and finally level 3 median values
    if param_dict["metric"] in [
        "spend_per_encounter",
        "days_per_encounter",
        "spend_per_day",
    ]:
        emp_df.loc[:, val_cols] = (
            emp_df.loc[:, val_cols]
            .fillna(invert_geo_vals)
            .fillna(med_2_vals)
            .fillna(med_3_vals)
        )
    elif param_dict["metric"] == "encounters_per_person":
        emp_df.loc[:, val_cols] = (
            emp_df.loc[:, val_cols].fillna(invert_geo_vals).fillna(0)
        )
    else:
        raise RuntimeError(f"Unknown metric: {param_dict['metric']}")

    # Modeling is expected to output a continuous set of years, but imputation
    # Can extend the year range forward/backward in this step if needed.
    # First, determine if there are any missing years before or after the specified range
    forwardfill_years = int(max(full_years) - max(emp_df["year_id"].unique()))
    backfill_years = int(min(emp_df["year_id"].unique()) - min(full_years))

    # Try to create a new dataframe with forwardfilled years. If no years
    # Need to be filled, this dataframe is empty
    try:
        forwardfill_df = pd.concat(
            [
                emp_df[emp_df["year_id"] == max(emp_df["year_id"].unique())].drop(
                    columns=["year_id"]
                )
            ]
            * forwardfill_years
        ).reset_index(drop=True)
        forwardfill_df["year_id"] = pd.Series(
            np.repeat(
                full_years[-forwardfill_years:], len(forwardfill_df) / forwardfill_years
            )
        )
    except ValueError:
        forwardfill_df = pd.DataFrame(columns=emp_df.columns)

    # Try to create a new dataframe with backfilled years. If no years
    # Need to be filled, this dataframe is empty
    try:
        backfill_df = pd.concat(
            [
                emp_df[emp_df["year_id"] == min(emp_df["year_id"].unique())].drop(
                    columns=["year_id"]
                )
            ]
            * backfill_years
        ).reset_index(drop=True)
        backfill_df["year_id"] = pd.Series(
            np.repeat(full_years[:backfill_years], len(backfill_df) / backfill_years)
        )
    except ValueError:
        backfill_df = pd.DataFrame(columns=emp_df.columns)

    # Concatenate all dataframes together again
    emp_df = pd.concat([backfill_df, emp_df, forwardfill_df])

    # Filling the full size dataframe with all imputed data from emp_df
    # The only NAs that should exist are where there are unmodeled ages that are valid
    # full_df = full_df.merge(emp_df, on=list(set(group_cols+model_cols+partition_cols)), how='left')
    full_df = emp_df.copy()

    ##############################
    # CONVERTING AND SAVING DATA #
    ##############################

    # Converting select columns to numeric types
    full_df[["year_id", "age_group_years_start", "sex_id"]] = (
        full_df[["year_id", "age_group_years_start", "sex_id"]]
        .astype("float")
        .astype("Int32")
    )
    full_df[val_cols] = full_df[val_cols].astype(float)

    # Confirming we either have all nulls or no nulls for each age bin
    t = full_df[val_cols].isnull().groupby(full_df["age_group_years_start"]).sum()
    t["n"] = full_df["age_group_years_start"].value_counts()
    t["percent"] = t["mean"] / t["n"]
    if ((t["percent"] != 0) & (t["percent"] != 1)).any():
        raise RuntimeError("Ruh Roh")

    if all(col in full_df.columns for col in val_cols):
        # Saving out data and partitioning as specified
        outpath = Path("FILEPATH")
        outpath.mkdir(parents=True, exist_ok=True)
        full_df.to_parquet(
            outpath,
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
        )

    else:
        raise RuntimeError(
            f"Some value columns were not imputed for these params: {full_df.columns}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    arg_cols = ["toc", "acause", "metric", "pri_payer", "sex_id", "payer", "geo"]

    parser.add_argument(
        "--model_version",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--draws",
        type=int,
        required=True,
    )
    parser.add_argument(
        "--max_model_year",
        type=int,
        required=True,
    )
    # Getting other args
    for arg in arg_cols:
        parser.add_argument(
            f"--{arg}",
            type=str,
            required=True,
        )

    # Getting args from parser
    args = vars(parser.parse_args())
    model_ver = args["model_version"]
    draws = args["draws"]
    max_model_year = args["max_model_year"]
    param_dict = {k: v for k, v in args.items() if k in arg_cols}

    # Convert sex_id to integer type
    param_dict["sex_id"] = int(param_dict["sex_id"])

    # Running imputation
    impute(param_dict, model_ver, draws, max_model_year)
