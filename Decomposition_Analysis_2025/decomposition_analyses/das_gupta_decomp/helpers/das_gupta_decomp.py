## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to run Das Gupta decomposition.
## ==================================================

import argparse
import duckdb
import pyarrow.dataset as ds
import pandas as pd
import numpy as np
import functools

# Importing dex_functions, including decomp function
import das_gupta
import deflate

# Setting column constants used in decomposition script
COL_TYPES = {
    "year_id": "float",
    "payer": "str",
    "toc": "str",
    "age_group_years_start": "float",
    "sex_id": "float",
    "geo": "str",
    "state": "str",
    "acause": "str",
    "location": "str",
}
GROUP_COLS = [
    "year_id",
    "payer",
    "toc",
    "age_group_years_start",
    "sex_id",
    "acause",
    "geo",
    "state",
    "location",
]
MERGE_COLS = ["year_id", "payer", "toc", "age_group_years_start", "sex_id", "acause"]
METRIC_COLS = ["spend", "vol"]


def run_das_gupta_decomp(
    scale_ver,
    draw,
    decomp_type,
    base_lvl,
    comp_lvl,
    spatial_year,
    decomp_cols,
    deflate_spend,
    use_prev,
    state,
    toc,
    outpath,
):

    # Updating variables depending on if draw level or means are being used
    metric_cols = (
        ["mean_" + i for i in METRIC_COLS.copy()] if draw == -1 else METRIC_COLS.copy()
    )
    col_types = dict(
        {"draw": "int"}, **COL_TYPES.copy(), **{m: "float" for m in metric_cols}
    )
    group_cols = ["draw"] + GROUP_COLS.copy()

    # Creating duckdb connection
    conn = duckdb.connect()

    # Getting draw level or means data depending on if draws are used
    if draw == -1:
        dataset = ds.dataset(
            "FILEPATH",
            partitioning="hive",
        )
    else:
        dataset = ds.dataset(
            "FILEPATH",
            partitioning="hive",
        )

    # Loading data for spatial decomp
    if decomp_type == "spatial":

        # Factor columns for spatial decomposition
        if use_prev:
            factor_cols = ["pop_frac", "prev_per_pop", "enc_per_prev", "spend_per_enc"]
        else:
            factor_cols = ["pop_frac", "enc_per_pop", "spend_per_enc"]

        # Creating comp_df query statement for duckdb
        if draw == -1:
            comp_col_val_dict = {
                "geo": comp_lvl,
                "state": state,
                "year_id": spatial_year,
                "toc": toc,
            }
        else:
            comp_col_val_dict = {
                "draw": draw,
                "geo": comp_lvl,
                "state": state,
                "year_id": spatial_year,
                "toc": toc,
            }
        select_statement = "SELECT * FROM dataset"
        where_statement = make_filter_statement(comp_col_val_dict)
        query_statement = select_statement + where_statement

        # Reading in comparison data for spatial decomp. comp_lvl can't be national, so don't need to remove state filter
        comp_df = conn.query(query_statement).to_df()
        comp_df["draw"] = draw

        # Creating base_df query statement for duckdb
        if draw == -1:
            base_col_val_dict = {
                "geo": base_lvl,
                "state": state,
                "year_id": spatial_year,
                "toc": toc,
            }
        else:
            base_col_val_dict = {
                "draw": draw,
                "geo": base_lvl,
                "state": state,
                "year_id": spatial_year,
                "toc": toc,
            }

        # If national, remove state filter
        if base_lvl == "national":
            base_col_val_dict = {
                k: v for k, v in base_col_val_dict.items() if k != "state"
            }
        select_statement = "SELECT * FROM dataset"
        where_statement = make_filter_statement(base_col_val_dict)
        query_statement = select_statement + where_statement

        # Reading in base data for spatial decomp. National level data is not state specific
        base_df = conn.query(query_statement).to_df()
        base_df["draw"] = draw

        # Ensure national data has location column
        if base_lvl == "national":
            base_df["location"] = "USA"

        # Aggregating dataframes across pri_payer and selecting relevant columns
        comp_df = (
            comp_df[col_types.keys()]
            .astype(col_types)
            .groupby(group_cols, as_index=False)[metric_cols]
            .sum()
        )
        base_df = (
            base_df[col_types.keys()]
            .astype(col_types)
            .groupby(group_cols, as_index=False)[metric_cols]
            .sum()
        )

        # Creating a matching row in base_df for each row in comp_df. comp_df will always have more rows by default since it is
        # The smaller geography. base_df_full will have duplicate rows, since it needs to match the number of rows in comp_df
        base_df_full = (
            comp_df.sort_values(group_cols)
            .reset_index(drop=True)[MERGE_COLS]
            .merge(base_df, on=MERGE_COLS, how="left")
        )

        # Merging base and comparison data into one dataframe
        df = pd.concat([base_df_full, comp_df], ignore_index=True)

    # Loading data for temporal decomp
    # Reading in all years between specified start and end year
    elif decomp_type == "temporal":

        # Factor columns for temporal decomposition
        if use_prev:
            factor_cols = [
                "population_y",
                "pop_frac",
                "prev_per_pop",
                "enc_per_prev",
                "spend_per_enc",
            ]
        else:
            factor_cols = ["population_y", "pop_frac", "enc_per_pop", "spend_per_enc"]

        # Creating temporal query statement for duckdb
        if draw == -1:
            col_val_dict = {"state": state, "toc": toc}
        else:
            col_val_dict = {"draw": draw, "state": state, "toc": toc}
        select_statement = "SELECT * FROM dataset"
        where_statement = make_filter_statement(col_val_dict)
        temporal_filter = (
            f" AND year_id >= {base_lvl} AND year_id <= {comp_lvl} AND geo = 'county'"
        )
        query_statement = select_statement + where_statement + temporal_filter

        # Reading in all years of data for temporal decomp
        df = conn.query(query_statement).to_df()

        # Aggregating dataframes across pri_payer and selecting relevant columns
        df = (
            df[col_types.keys()]
            .astype(col_types)
            .groupby(group_cols, as_index=False)[metric_cols]
            .sum()
        )

        # Get length of data for each year
        yr_sizes = {
            yr: len(df[df["year_id"] == yr])
            for yr in range(int(base_lvl), int(comp_lvl) + 1)
        }

        # Check if all years have the same number of rows, if not, then fix by using data from the year with the most rows
        if not all(list(yr_sizes.values())[0] == size for size in yr_sizes.values()):

            # Finding the year with the greatest number of rows
            max_yr = max(yr_sizes, key=yr_sizes.get)

            # Iterating over years
            df_list = []
            for yr in range(int(base_lvl), int(comp_lvl) + 1):

                # Creating extra rows for years that do not have the max number of possible rows
                if yr != max_yr:
                    join_cols = ([] if draw == -1 else ["draw"]) + [
                        "payer",
                        "toc",
                        "age_group_years_start",
                        "sex_id",
                        "acause",
                        "geo",
                        "state",
                        "location",
                    ]
                    new_df = (
                        df[df["year_id"] == max_yr]
                        .sort_values(group_cols)[join_cols]
                        .merge(df[df["year_id"] == yr], on=join_cols, how="left")
                    )
                    new_df["year_id"] = yr
                    df_list.append(new_df)
                # If year is the max year, just append the dataframe
                elif yr == max_yr:
                    df_list.append(df[df["year_id"] == yr].sort_values(group_cols))

            df = pd.concat(df_list, ignore_index=True)

    if deflate_spend:
        df = deflate.deflate(df, metric_cols[0], old_year=2019, new_year="year_id")

    # Loading dex population data
    dex_pops = pd.read_csv("FILEPATH")
    dex_pops["location"] = dex_pops["location"].astype("str")

    # Getting age/sex/year/county specific populations
    pop_data_asy = dex_pops.groupby(
        ["age_group_years_start", "sex_id", "year_id", "location", "geo"],
        as_index=False,
    )[["pop"]].sum()
    pop_data_asy.rename(columns={"pop": "population_asy"}, inplace=True)

    # Getting year/county specific populations
    pop_data_y = pop_data_asy.groupby(["year_id", "location", "geo"], as_index=False)[
        ["population_asy"]
    ].sum()
    pop_data_y.rename(columns={"population_asy": "population_y"}, inplace=True)

    if use_prev:
        # Getting prevalence data
        prev_data = prep_prev_data(draw, decomp_type, spatial_year, base_lvl, comp_lvl)

        # Aggregating prevalence data to larger geographic levels for spatial decomp
        if decomp_type == "spatial":

            # Getting comparison prevalence data for specified state and spatial year
            comp_prev_data = prev_data.loc[
                (
                    (prev_data["state"] == state)
                    & (prev_data["year_id"] == spatial_year)
                ),
                :,
            ].copy()

            # Aggregating comparison prevalence data to state level, if needed
            if comp_lvl == "state":
                comp_prev_data = comp_prev_data.groupby(
                    [
                        "draw",
                        "year_id",
                        "age_group_years_start",
                        "sex_id",
                        "acause",
                        "state",
                    ],
                    as_index=False,
                )["prev"].sum()
                comp_prev_data["location"] = state

            # Getting base prevalence data, using comparison prevalence data to ensure matching number of rows
            if base_lvl == "state":
                base_prev_data = (
                    comp_prev_data[
                        ["draw", "year_id", "age_group_years_start", "sex_id", "acause"]
                    ]
                    .merge(
                        prev_data,
                        on=[
                            "draw",
                            "year_id",
                            "age_group_years_start",
                            "sex_id",
                            "acause",
                        ],
                        how="left",
                    )
                    .groupby(
                        [
                            "draw",
                            "year_id",
                            "age_group_years_start",
                            "sex_id",
                            "acause",
                            "state",
                        ],
                        as_index=False,
                    )["prev"]
                    .sum()
                )
                base_prev_data["location"] = base_prev_data["state"]
            elif base_lvl == "national":
                base_prev_data = (
                    comp_prev_data[
                        ["draw", "year_id", "age_group_years_start", "sex_id", "acause"]
                    ]
                    .merge(
                        prev_data,
                        on=[
                            "draw",
                            "year_id",
                            "age_group_years_start",
                            "sex_id",
                            "acause",
                        ],
                        how="left",
                    )
                    .groupby(
                        [
                            "draw",
                            "year_id",
                            "age_group_years_start",
                            "sex_id",
                            "acause",
                        ],
                        as_index=False,
                    )["prev"]
                    .sum()
                )
                base_prev_data["location"] = "USA"

            # Setting geo column for prevalence data
            comp_prev_data["geo"] = comp_lvl
            base_prev_data["geo"] = base_lvl

            # Combining base and comparison prevalence data and subsetting columns
            prev_data = pd.concat([comp_prev_data, base_prev_data]).reset_index(
                drop=True
            )[
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
        else:
            prev_data = prev_data[
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

        # Merging population/prevalence data to dex estimates
        df = (
            df.merge(
                pop_data_asy,
                on=["age_group_years_start", "sex_id", "year_id", "location", "geo"],
                how="left",
            )
            .merge(pop_data_y, on=["year_id", "location", "geo"], how="left")
            .merge(
                prev_data,
                on=[
                    "draw",
                    "year_id",
                    "location",
                    "geo",
                    "acause",
                    "age_group_years_start",
                    "sex_id",
                ],
                how="inner",
            )
        )

        # Because of thresholding, some counties are missing prevalence data
        # We need to remove these rows from the base data (the inner join above removes it from the comparison data)
        if decomp_type == "spatial":
            join_cols = ([] if draw == -1 else ["draw"]) + [
                "year_id",
                "payer",
                "toc",
                "age_group_years_start",
                "sex_id",
                "acause",
            ]
            # Finding all rows in comp data and joining base data to this (removing rows that are not in comp data)
            new_base_lvl = df.loc[df["geo"] == comp_lvl, join_cols].merge(
                df.loc[df["geo"] == base_lvl, :].drop_duplicates(),
                on=join_cols,
                how="inner",
            )

            # Adding comp data and new base data back to dataframe
            df = pd.concat([df[df["geo"] == comp_lvl], new_base_lvl])

        # Getting rate factors to decompose
        df["pop_frac"] = (
            (df["population_asy"] / df["population_y"])
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )
        df["prev_per_pop"] = (
            (df["prev"] / df["population_asy"]).replace([np.inf, -np.inf], 0).fillna(0)
        )
        df["enc_per_prev"] = (
            (df[metric_cols[1]] / df["prev"]).replace([np.inf, -np.inf], 0).fillna(0)
        )
        df["spend_per_enc"] = (
            (df[metric_cols[0]] / df[metric_cols[1]])
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

    else:
        # Merging population/prevalence data to dex estimates
        df = df.merge(
            pop_data_asy,
            on=["age_group_years_start", "sex_id", "year_id", "location", "geo"],
            how="left",
        ).merge(pop_data_y, on=["year_id", "location", "geo"], how="left")

        # Getting rate factors to decompose
        df["pop_frac"] = (
            (df["population_asy"] / df["population_y"])
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )
        df["enc_per_pop"] = (
            (df[metric_cols[1]] / df["population_asy"])
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )
        df["spend_per_enc"] = (
            (df[metric_cols[0]] / df[metric_cols[1]])
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
        )

    # Creating dictionaries of dataframes to decompose
    decomp_dicts = []
    if decomp_type == "spatial":
        for geo in [base_lvl, comp_lvl]:
            decomp_dicts.append(
                {
                    "lvl": geo,
                    "data": df[df["geo"] == geo]
                    .sort_values(group_cols)
                    .reset_index(drop=True),
                }
            )
    elif decomp_type == "temporal":
        for yr in range(int(base_lvl), int(comp_lvl) + 1):
            decomp_dicts.append(
                {
                    "lvl": yr,
                    "data": df[df["year_id"] == yr]
                    .sort_values(group_cols)
                    .reset_index(drop=True),
                }
            )

    # Instantiating Das Gupta Decomposition object
    decomp_eqs = das_gupta.das_gupta_decomp(len(factor_cols))

    # Running Das Gupta Decomposition
    results = {}
    for idx in range(len(decomp_dicts)):
        if idx != len(decomp_dicts) - 1:
            print(
                f"Decomposing {decomp_dicts[idx]['lvl']} vs. {decomp_dicts[idx+1]['lvl']}"
            )

            # Decomposing base rate factors vs. comparison rate factor. For spatial decomposition, this
            # Is just base_lvl vs. comp_lvl. For temporal decomposition, each year from base_lvl up to
            # comp_lvl is decomposed against its prior year (i.e. 2010 vs 2011, 2011 vs 2012, etc.)
            result = decomp_eqs.decompose(
                decomp_dicts[idx]["data"], decomp_dicts[idx + 1]["data"], factor_cols
            )

            # Adding specified decomp columns from comparision dataframe onto the decomp result.
            # This allows for aggreagation of factors to the desired level.
            result = pd.concat(
                [decomp_dicts[idx + 1]["data"][decomp_cols], result], axis=1
            )
            result.set_index(decomp_cols, inplace=True)

            # Adding to results dictionary
            results[f"{decomp_dicts[idx]['lvl']}_{decomp_dicts[idx+1]['lvl']}"] = result
        else:
            print("Decomposition complete!")

    # Aggregating all results and grouping by specified decomp columns
    final_result = (
        functools.reduce(lambda a, b: a.add(b, fill_value=0), results.values())
        .groupby(decomp_cols)
        .sum()
        .reset_index()
    )

    # Getting base rate to add to final data
    base_data = decomp_dicts[0]["data"].drop_duplicates().copy()
    base_data["base_rate"] = base_data.loc[:, factor_cols].product(axis=1)
    base_data = base_data.groupby(decomp_cols, as_index=False)["base_rate"].sum()

    # Getting comp rate to add to final data
    comp_data = decomp_dicts[-1]["data"]
    comp_data["comp_rate"] = comp_data.loc[:, factor_cols].product(axis=1)
    comp_data = comp_data.groupby(decomp_cols, as_index=False)["comp_rate"].sum()

    # Adding rates to data, ignoring locations for spatial decomp
    if decomp_type == "spatial":
        non_loc_decomp_cols = [i for i in decomp_cols if i not in ["state", "location"]]
        final_result = final_result.merge(
            base_data[non_loc_decomp_cols + ["base_rate"]], on=non_loc_decomp_cols
        ).merge(comp_data, on=decomp_cols)
    else:
        final_result = final_result.merge(base_data, on=decomp_cols).merge(
            comp_data, on=decomp_cols
        )

    # Saving final result to outpath
    final_result.to_parquet(f"{outpath}/{draw}_{state}_{toc}.parquet")


def prep_prev_data(draw, decomp_type, spatial_year, base_lvl, comp_lvl):

    # Reading in county-level prevalence data
    # Getting draw level or means data depending on if draws are used
    if draw == -1:
        prev_data = pd.read_parquet("FILEPATH")
    else:
        prev_data = pd.read_parquet("FILEPATH")

    # Adding year, geo, and draw columns
    prev_data["year_id"] = spatial_year
    prev_data["geo"] = "county"
    prev_data["draw"] = draw

    if decomp_type == "temporal":
        # Get length of data for each year
        yr_sizes = {
            yr: len(prev_data[prev_data["year_id"] == yr])
            for yr in range(int(base_lvl), int(comp_lvl) + 1)
        }

        # Check if all years have the same number of rows, if not, then fix by using data from the year with the most rows
        if not all(list(yr_sizes.values())[0] == size for size in yr_sizes.values()):

            # Finding the year with the greatest number of rows
            max_yr = max(yr_sizes, key=yr_sizes.get)

            # Iterating over years
            df_list = []
            for yr in range(int(base_lvl), int(comp_lvl) + 1):

                # Creating extra rows for years that do not have the max number of possible rows
                if yr != max_yr:
                    join_cols = [
                        "draw",
                        "age_group_years_start",
                        "sex_id",
                        "acause",
                        "geo",
                        "location",
                    ]
                    new_df = (
                        prev_data[prev_data["year_id"] == max_yr]
                        .sort_values(
                            [
                                "draw",
                                "year_id",
                                "age_group_years_start",
                                "sex_id",
                                "acause",
                                "geo",
                                "location",
                            ]
                        )[join_cols]
                        .merge(
                            prev_data[prev_data["year_id"] == yr],
                            on=join_cols,
                            how="left",
                        )
                    )
                    new_df["year_id"] = yr
                    df_list.append(new_df)

                # If year is the max year, just append the dataframe
                elif yr == max_yr:
                    df_list.append(
                        prev_data[prev_data["year_id"] == yr].sort_values(
                            [
                                "year_id",
                                "age_group_years_start",
                                "sex_id",
                                "acause",
                                "geo",
                                "location",
                            ]
                        )
                    )

            full_prev_data = pd.concat(df_list, ignore_index=True)[
                [
                    "draw",
                    "year_id",
                    "location",
                    "state",
                    "geo",
                    "acause",
                    "age_group_years_start",
                    "sex_id",
                    "prev",
                ]
            ]
        else:
            full_prev_data = prev_data[
                [
                    "draw",
                    "year_id",
                    "location",
                    "state",
                    "geo",
                    "acause",
                    "age_group_years_start",
                    "sex_id",
                    "prev",
                ]
            ]

    elif decomp_type == "spatial":
        full_prev_data = prev_data[prev_data["year_id"] == spatial_year][
            [
                "draw",
                "year_id",
                "location",
                "state",
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
        help="Scaled version of data to use for decomposition.",
    )
    parser.add_argument(
        "--draw", type=int, required=True, help="Draw number to run decomposition on."
    )
    parser.add_argument(
        "--decomp_type",
        type=str,
        required=True,
        help="Type of Das Gupta Decomposition to run.",
    )
    parser.add_argument(
        "--base_lvl",
        type=str,
        required=True,
        help="Base year or geographic level to decompose against",
    )
    parser.add_argument(
        "--comp_lvl",
        type=str,
        required=True,
        help="Year or geographic level to decompose against base.",
    )
    parser.add_argument(
        "--spatial_year",
        type=int,
        help="Year to use for spatial decomposition. Required if decomp_type is spatial.",
    )
    parser.add_argument(
        "--decomp_cols",
        type=str,
        nargs="+",
        required=True,
        help="Columns to decompose by, which will be included in final decomposition output.",
    )
    parser.add_argument(
        "--deflate_spend",
        type=int,
        help="Whether to deflate spending to original year dollars or not. 1 to deflate, 0 to not deflate.",
    )
    parser.add_argument(
        "--use_prev",
        type=int,
        help="Whether to use prevalence estimates or not. 1 to use, 0 to not use.",
    )
    parser.add_argument(
        "--state",
        type=str,
        required=True,
        help="Specific state to run decomposition on.",
    )
    parser.add_argument(
        "--toc",
        type=str,
        required=True,
        help="Specific type-of-care to run decomposition on.",
    )
    parser.add_argument(
        "--outpath",
        type=str,
        required=True,
        help="Output path for decomposition results.",
    )
    args = vars(parser.parse_args())

    # Parsing arguments
    scale_ver = args["scale_ver"]
    draw = args["draw"]
    decomp_type = args["decomp_type"]
    base_lvl = args["base_lvl"]
    comp_lvl = args["comp_lvl"]
    spatial_year = args["spatial_year"]
    decomp_cols = args["decomp_cols"]
    deflate_spend = args["deflate_spend"]
    use_prev = args["use_prev"]
    state = args["state"]
    toc = args["toc"]
    outpath = args["outpath"]

    # Running decomp
    run_das_gupta_decomp(
        scale_ver,
        draw,
        decomp_type,
        base_lvl,
        comp_lvl,
        spatial_year,
        decomp_cols,
        deflate_spend,
        use_prev,
        state,
        toc,
        outpath,
    )
