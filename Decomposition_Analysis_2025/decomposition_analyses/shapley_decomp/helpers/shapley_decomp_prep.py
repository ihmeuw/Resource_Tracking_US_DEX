## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to prep data for Shapley decomposition.
## ==================================================

import argparse
import duckdb
import pyarrow.dataset as ds
import pandas as pd
import numpy as np

# Defining constants
# Lists of all DEX metric columns
METRIC_COLS = ["spend", "vol"]


def run_shapley_decomp_prep(scale_ver, draw, incl_prev, year, toc, geo, outpath):

    ####################################
    # SETTING UP VARIABLES BASED ON ARGS
    ####################################

    # Setting shapley columns depending on if prevalence is used
    if incl_prev == "none":
        shapley_cols = {
            c: f"{c}_ln"
            for c in ["spend_per_pop", "pop_frac", "enc_per_pop", "spend_per_enc"]
        }
    else:
        shapley_cols = {
            c: f"{c}_ln"
            for c in [
                "spend_per_pop",
                "pop_frac",
                "prev_per_pop",
                "enc_per_prev",
                "spend_per_enc",
            ]
        }

    # Updating variables depending on if draw level or means are being used
    metric_cols = (
        ["mean_" + i for i in METRIC_COLS.copy()] if draw == -1 else METRIC_COLS.copy()
    )

    #################
    # LOADING IN DATA
    #################

    # Creating duckdb connection
    conn = duckdb.connect()

    # Getting draw level or means data depending on if draws are used and
    # Creating column value dictionary for filtering
    if draw == -1:
        dataset = ds.dataset(
            "FILEPATH",
            partitioning="hive",
        )
        col_val_dict = {"year_id": year}
    else:
        dataset = ds.dataset(
            "FILEPATH",
            partitioning="hive",
        )
        col_val_dict = {"year_id": year, "draw": draw}

    # Creating query statement
    where_statement = make_filter_statement(col_val_dict)
    query_statement = "SELECT * FROM dataset" + where_statement

    # Reading in data
    df = conn.query(query_statement).to_df()
    df["toc"] = toc
    df["draw"] = draw
    df = df.astype({"state": "str", "toc": "str", "payer": "str"})

    # Grouping data and subsetting to relevant columns
    df = df.groupby(
        [
            "draw",
            "year_id",
            "location",
            "state",
            "toc",
            "payer",
            "acause",
            "age_group_years_start",
            "sex_id",
        ],
        as_index=False,
    )[metric_cols].sum()

    # Calculating spending weights for each county
    weights = df.groupby(["location"], as_index=False)[metric_cols[0]].sum()
    weights["weight"] = weights[metric_cols[0]] / weights[metric_cols[0]].sum()

    ##########################
    # ADDING POP AND PREV DATA
    ##########################

    # Loading population and filtering to specified geo/year/state
    dex_pops = pd.read_csv("FILEPATH")
    dex_pops["location"] = dex_pops["location"].astype("str")
    dex_pops = dex_pops[(dex_pops["geo"] == "county") & (dex_pops["year_id"] == year)]

    # Getting age/sex/year/county specific populations
    pop_data_asy = dex_pops.groupby(
        ["age_group_years_start", "sex_id", "year_id", "location"], as_index=False
    )[["pop"]].sum()
    pop_data_asy.rename(columns={"pop": "population_asy"}, inplace=True)

    # Getting year/county specific populations
    pop_data_y = pop_data_asy.groupby(["year_id", "location"], as_index=False)[
        ["population_asy"]
    ].sum()
    pop_data_y.rename(columns={"population_asy": "population_y"}, inplace=True)

    # Getting prevalence data
    prev_data = prep_prev_data(incl_prev, draw, year)

    if incl_prev != "none":
        # Merging population and prevalence data to dex estimates
        df = (
            df.merge(
                pop_data_asy,
                on=["age_group_years_start", "sex_id", "year_id", "location"],
                how="inner",
            )
            .merge(pop_data_y, on=["year_id", "location"], how="inner")
            .merge(
                prev_data,
                on=[
                    "draw",
                    "year_id",
                    "location",
                    "acause",
                    "age_group_years_start",
                    "sex_id",
                ],
                how="inner",
            )
            .merge(weights[["location", "weight"]], on="location", how="left")
        )
    else:
        # Merging population and prevalence data to dex estimates
        df = (
            df.merge(
                pop_data_asy,
                on=["age_group_years_start", "sex_id", "year_id", "location"],
                how="inner",
            )
            .merge(pop_data_y, on=["year_id", "location"], how="inner")
            .merge(weights[["location", "weight"]], on="location", how="left")
        )

    # Copying data to add metrics
    if geo == "county":
        prepped_data = df.copy()
    elif geo == "state":
        prepped_data = (
            df.groupby(
                [
                    "draw",
                    "year_id",
                    "toc",
                    "payer",
                    "acause",
                    "age_group_years_start",
                    "sex_id",
                    "state",
                ]
            )
            .sum()
            .reset_index()
        )

    #####################
    # CALCULATING METRICS
    #####################
    prepped_data.loc[prepped_data["payer"] == "mdcd", "prev"] = (
        1.2 * prepped_data.loc[prepped_data["payer"] == "mdcd", "prev"]
    )
    prepped_data.loc[prepped_data["payer"] == "priv", "prev"] = (
        0.8 * prepped_data.loc[prepped_data["payer"] == "priv", "prev"]
    )

    if incl_prev != "none":
        # Getting metrics for shapley decomposition
        prepped_data["spend_per_pop"] = (
            prepped_data[metric_cols[0]] / prepped_data["population_y"]
        )
        prepped_data["pop_frac"] = (
            prepped_data["population_asy"] / prepped_data["population_y"]
        )
        prepped_data["prev_per_pop"] = (
            prepped_data["prev"] / prepped_data["population_asy"]
        )
        prepped_data["enc_per_prev"] = (
            prepped_data[metric_cols[1]] / prepped_data["prev"]
        )
        prepped_data["spend_per_enc"] = (
            prepped_data[metric_cols[0]] / prepped_data[metric_cols[1]]
        )
    else:
        # Getting metrics for shapley decomposition
        prepped_data["spend_per_pop"] = (
            prepped_data[metric_cols[0]] / prepped_data["population_y"]
        )
        prepped_data["pop_frac"] = (
            prepped_data["population_asy"] / prepped_data["population_y"]
        )
        prepped_data["enc_per_pop"] = (
            prepped_data[metric_cols[1]] / prepped_data["population_asy"]
        )
        prepped_data["spend_per_enc"] = (
            prepped_data[metric_cols[0]] / prepped_data[metric_cols[1]]
        )

    # Filling any zeros or infs with nans
    prepped_data[list(shapley_cols.keys())] = prepped_data[
        list(shapley_cols.keys())
    ].replace([np.inf, -np.inf, 0], np.nan)

    # Dropping any nans, which correspond to zero values, infs, or missing data
    prepped_data.dropna(subset=shapley_cols.keys(), inplace=True)

    #############################################
    # ADJUSTING METRICS W/ LOG AND MEAN-CENTERING
    #############################################

    # Calculating log of each metric column
    for col, new_col in shapley_cols.items():
        prepped_data[new_col] = np.log(prepped_data[col])

    # Subsetting to relevant columns
    if geo == "county":
        prepped_data = prepped_data[
            [
                "draw",
                "year_id",
                "location",
                "toc",
                "payer",
                "acause",
                "age_group_years_start",
                "sex_id",
                "state",
                "weight",
            ]
            + list(shapley_cols.values())
        ]
    elif geo == "state":
        prepped_data = prepped_data[
            [
                "draw",
                "year_id",
                "toc",
                "payer",
                "acause",
                "age_group_years_start",
                "sex_id",
                "state",
                "weight",
            ]
            + list(shapley_cols.values())
        ]

    # Getting means for each shapley column
    data_means = (
        prepped_data.groupby(
            ["acause", "age_group_years_start", "sex_id", "payer", "toc"]
        )[list(shapley_cols.values())]
        .mean()
        .reset_index()
    )

    # Merging means back to data
    prepped_data = prepped_data.merge(
        data_means, on=["acause", "age_group_years_start", "sex_id", "payer", "toc"]
    )

    # Adjusting shapley columns by subtracting mean from value
    for col in shapley_cols.values():
        prepped_data[col + "_adj"] = prepped_data[col + "_x"] - prepped_data[col + "_y"]

    #################
    # SAVING OUT DATA
    #################

    # Saving data and partitioning by variables
    prepped_data.to_parquet(
        outpath,
        partition_cols=["draw", "year_id", "payer", "toc"],
        existing_data_behavior="overwrite_or_ignore",
    )


def prep_prev_data(prev_type, draw, year):

    # Reading in prevalence data, mortality data, or not using prevalence data
    if prev_type == "prev":

        # Reading in county-level prevalence data
        # Getting draw level or means data depending on if draws are used
        if draw == -1:
            prev_data = pd.read_parquet(
                "FILEPATH",
                filters=[[("year_id", "=", year)]],
            )
        else:
            prev_data = pd.read_parquet(
                "FILEPATH",
                filters=[[("year_id", "=", year), ("draw", "=", draw)]],
            )
    elif prev_type == "mort":

        # Reading in county-level mortality data, not using draws
        prev_data = pd.read_parquet(
            "FILEPATH",
            filters=[[("year_id", "=", year)]],
        )
        prev_data["mcnty"] = prev_data["mcnty"].astype("int").astype("str")
        prev_data.rename(columns={"mcnty": "location", "deaths": "prev"}, inplace=True)
    else:
        return

    # Adding geo and draw columns
    prev_data["geo"] = "county"
    prev_data["draw"] = draw
    full_prev_data = prev_data[
        [
            "draw",
            "year_id",
            "location",
            "geo",
            "acause",
            "age_group_years_start",
            "sex_id",
            "prev",
        ]
    ]

    return full_prev_data


def make_filter_statement(col_val_dict):
    col_filters = []
    for col, val in col_val_dict.items():
        if type(val) == int:
            col_filters.append(" = ".join([col, str(val)]))
        else:
            col_filters.append(" = ".join([col, "'" + val + "'"]))

    filter_statement = " WHERE " + " AND ".join(col_filters)
    return filter_statement


if __name__ == "__main__":

    # Adding command line script arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scale_ver",
        type=str,
        required=True,
        help="Scaled version of data to prepare for decomposition.",
    )
    parser.add_argument(
        "--draw",
        type=int,
        required=True,
        help="Draw number to prepare decomposition for.",
    )
    parser.add_argument(
        "--incl_prev", type=str, required=True, help="Value to use for prevalence."
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Specific year of data to prepare for decomposition.",
    )
    parser.add_argument(
        "--toc",
        type=str,
        required=True,
        help="Specific TOC to prepare for decomposition.",
    )
    parser.add_argument(
        "--geo",
        type=str,
        required=True,
        help="Geographic level (state or county) to prep data for.",
    )
    parser.add_argument(
        "--outpath",
        type=str,
        required=True,
        help="Output path for prepared decomposition data.",
    )
    args = vars(parser.parse_args())

    # Parsing arguments
    scale_ver = args["scale_ver"]
    draw = args["draw"]
    incl_prev = args["incl_prev"]
    year = args["year"]
    toc = args["toc"]
    geo = args["geo"]
    outpath = args["outpath"]

    # Running decomp prep
    run_shapley_decomp_prep(scale_ver, draw, incl_prev, year, toc, geo, outpath)
