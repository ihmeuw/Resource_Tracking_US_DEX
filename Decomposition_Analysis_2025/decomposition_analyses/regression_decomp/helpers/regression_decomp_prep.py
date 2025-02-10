## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to prep data for regression decomposition.
## ==================================================

import argparse
import duckdb
import pyarrow.dataset as ds
import pandas as pd
import numpy as np

# Defining constants
# Flag of whether to standardize payer data
STANDARDIZE_PAYER = 0

# List of all covariate columns to prep for potential use in regression
RHS_COLS = [
    "ahrf_mds_pc",
    "urban",
    "rural",
    "income_median",
    "insured_pct",
    "dentists_pc",
    "dentists_pc_cr",
    "edu_ba",
    "edu_hs",
    "homehealth_agencies_mdcr_pc",
    "hospbeds_pc",
    "hospitals_pc",
    "mds_pediatrics_pc",
    "pcp_pc_cr",
    "percent_insured_w_priv_aa",
    "primarycare_phys_nonfed_pc",
    "primarycare_phys_nonfed_ratio_to_mds",
    "prop_medicare_mc",
    "total_hosp_worker_fte_pc",
]

# List of possible LHS columns to use in regression
LHS_COLS = ["enc_per_prev", "spend_per_enc"]

# List of DEX metric columns
METRIC_COLS = ["spend", "vol"]


def run_regression_decomp_prep(scale_ver, draw, year, toc, outpath):

    # Updating variables depending on if draw level or means are being used
    metric_cols = (
        ["mean_" + i for i in METRIC_COLS.copy()] if draw == -1 else METRIC_COLS.copy()
    )

    # Updating regression column names, to be used later
    regression_cols = [f"{col}_ln" for col in LHS_COLS]

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

    # Loading in payer-specific and toc-specific denoms
    denoms = pd.read_parquet(
        "FILEPATH",
        filters=[
            [
                ("type", "=", "total"),
                ("year_id", "=", year),
                ("toc", "!=", "all"),
                ("pri_payer", "in", ["mdcr", "mdcd", "priv", "oop"]),
            ]
        ],
    )

    # Changing oop denoms to be population
    denoms.loc[denoms["pri_payer"] == "oop", "denom"] = denoms.loc[
        denoms["pri_payer"] == "oop", "pop"
    ]

    # Fixing columns and subsetting
    denoms.rename(columns={"pri_payer": "payer"}, inplace=True)
    denoms = denoms.astype(
        {
            "age_group_years_start": "int",
            "sex_id": "int",
            "payer": "str",
            "toc": "str",
            "location": "str",
            "pop": "float",
            "denom": "float",
        }
    )
    denoms = denoms[
        [
            "year_id",
            "age_group_years_start",
            "sex_id",
            "location",
            "payer",
            "toc",
            "denom",
            "pop",
        ]
    ]

    # Getting national level denoms
    nat_denoms = (
        denoms.groupby(["year_id", "age_group_years_start", "sex_id", "payer", "toc"])[
            ["denom", "pop"]
        ]
        .sum()
        .reset_index()
    )
    nat_denoms.rename(columns={"denom": "nat_denom", "pop": "nat_pop"}, inplace=True)

    # Joining denoms and pops to data
    df = df.merge(
        denoms,
        on=["year_id", "age_group_years_start", "sex_id", "location", "payer", "toc"],
        how="left",
    ).merge(
        nat_denoms,
        on=["year_id", "age_group_years_start", "sex_id", "payer", "toc"],
        how="left",
    )

    # Payer-standardizing data
    if STANDARDIZE_PAYER:
        df[f"{metric_cols[0]}_stndz"] = (
            (df[metric_cols[0]] / df["denom"])
            * df["pop"]
            * (df["nat_denom"] / df["nat_pop"])
        )
        df[f"{metric_cols[1]}_stndz"] = (
            (df[metric_cols[1]] / df["denom"])
            * df["pop"]
            * (df["nat_denom"] / df["nat_pop"])
        )
    else:
        df[f"{metric_cols[0]}_stndz"] = df[metric_cols[0]]
        df[f"{metric_cols[1]}_stndz"] = df[metric_cols[1]]

    # Aggregating across payer
    df = (
        df.groupby(
            [
                "draw",
                "year_id",
                "location",
                "state",
                "toc",
                "acause",
                "age_group_years_start",
                "sex_id",
            ]
        )[[f"{metric_cols[0]}_stndz", f"{metric_cols[1]}_stndz"]]
        .sum()
        .reset_index()
    )

    # Getting covariates
    covs = prep_covariate_data()

    # Getting prevalence data
    prev_data = prep_prev_data(draw, year)

    # Merging prevalence data and covariates to dex estimates
    prepped_data = (
        df.merge(
            prev_data,
            on=[
                "draw",
                "year_id",
                "location",
                "state",
                "acause",
                "age_group_years_start",
                "sex_id",
            ],
            how="inner",
        )
        .merge(covs, on=["year_id", "location"], how="inner")
        .merge(weights[["location", "weight"]], on="location", how="left")
    )

    # Getting metrics for shapley decomposition
    prepped_data["enc_per_prev"] = (
        prepped_data[f"{metric_cols[1]}_stndz"] / prepped_data["prev"]
    )
    prepped_data["spend_per_enc"] = (
        prepped_data[f"{metric_cols[0]}_stndz"]
        / prepped_data[f"{metric_cols[1]}_stndz"]
    )

    # Filling any zeros or infs with nans
    prepped_data[RHS_COLS + LHS_COLS] = prepped_data[RHS_COLS + LHS_COLS].replace(
        [np.inf, -np.inf], np.nan
    )

    # Calculating log of each metric column
    for col in LHS_COLS:
        prepped_data[col + "_ln"] = np.log(prepped_data[col])

    # Subsetting to relevant columns
    prepped_data = prepped_data[
        [
            "draw",
            "year_id",
            "location",
            "toc",
            "acause",
            "age_group_years_start",
            "sex_id",
            "state",
            "weight",
        ]
        + RHS_COLS
        + regression_cols
    ]

    # Getting means for ALL variables
    data_means = (
        prepped_data.groupby(["acause", "age_group_years_start", "sex_id", "toc"])[
            RHS_COLS + regression_cols
        ]
        .mean()
        .reset_index()
    )

    # Merging means back to data
    prepped_data = prepped_data.merge(
        data_means, on=["acause", "age_group_years_start", "sex_id", "toc"]
    )

    # Adjusting metic columns by subtracting mean from value
    for col in RHS_COLS + regression_cols:
        prepped_data[col + "_adj"] = prepped_data[col + "_x"] - prepped_data[col + "_y"]

    # Dropping original metric columns
    prepped_data.drop(
        columns=(
            [i + "_x" for i in RHS_COLS + regression_cols]
            + [i + "_y" for i in RHS_COLS + regression_cols]
        ),
        inplace=True,
    )

    # Saving out data
    prepped_data.to_parquet(
        outpath,
        partition_cols=["draw", "year_id", "toc"],
        existing_data_behavior="overwrite_or_ignore",
    )


def q3(x):
    return x.quantile(0.75)


def prep_prev_data(draw, year):

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

    # Adding geo and draw columns
    prev_data["geo"] = "county"
    prev_data["draw"] = draw
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

    return full_prev_data


def prep_covariate_data():
    # Loading in DEX covariates
    dex_covs = pd.read_csv("FILEPATH")
    dex_covs.rename(columns={"mcnty": "location"}, inplace=True)
    dex_covs = dex_covs.astype({"location": "string"})

    # Loading in USHD covariates
    ushd_covs = pd.read_csv("FILEPATH")
    ushd_covs.rename(columns={"mcnty": "location"}, inplace=True)
    ushd_covs = ushd_covs.astype({"location": "string"})

    # Subsetting USHD covariates to ones that don't overlap with DEX covariates
    ushd_covs = ushd_covs[
        ushd_covs["covariate"].isin(
            [
                i
                for i in ushd_covs["covariate"].unique()
                if i not in dex_covs["covariate"].unique()
            ]
        )
    ]

    # Combining DEX and USHD covariates
    covs = pd.concat([dex_covs, ushd_covs])

    # Subsetting to selected covariates and years of interest
    covs = covs.loc[
        (
            (covs["covariate"].isin(RHS_COLS))
            & (covs["year_id"].isin(range(2010, 2020)))
        ),
        :,
    ]

    # Pivoting to wide
    covs = covs.pivot(
        index=["location", "year_id"], columns="covariate", values="cov_value"
    ).reset_index()

    # Adding additional covariate for urban
    covs["urban"] = 1 - covs["rural"]

    # Getting county map
    mcnty_map = pd.read_csv("FILEPATH")
    mcnty_map = mcnty_map[mcnty_map["current"] == 1]
    mcnty_map = mcnty_map.astype({"mcnty": "string", "cnty": "string"})[
        ["state_name", "mcnty", "cnty"]
    ]

    # Loading and mapping HHI data to county locations from FIPS, then aggregating to mcnty level
    hhi_data = pd.read_feather("FILEPATH")
    hhi_data = hhi_data.merge(mcnty_map, on="cnty")
    hhi_data.rename(
        columns={"mcnty": "location", "drive_time_HOSPBD_30_minutes_gen_surg": "hhi"},
        inplace=True,
    )
    hhi_data = hhi_data.groupby(["year_id", "location"], as_index=False)["hhi"].mean()

    # Getting AHRF MDs per capita data
    ahrf_mds = pd.read_csv("FILEPATH")
    ahrf_mds["year_id"] = 2019
    ahrf_mds = ahrf_mds.astype({"location": "str"})
    ahrf_mds = ahrf_mds[["year_id", "location", "ahrf_mds_pc"]]

    # Loading and mapping RPP data to county locations from FIPS, then aggregating to mcnty level
    rpp_data = pd.read_csv("FILEPATH")
    rpp_data = pd.melt(
        rpp_data,
        id_vars="GeoName",
        value_vars=[str(i) for i in range(2008, 2020)],
        var_name="year_id",
        value_name="rpp",
    )
    rpp_data = rpp_data.astype({"year_id": "int"})
    rpp_data = rpp_data.merge(
        mcnty_map, left_on="GeoName", right_on="state_name", how="left"
    )[["year_id", "mcnty", "rpp"]]
    rpp_data.rename(columns={"mcnty": "location"}, inplace=True)
    rpp_data = rpp_data.groupby(["year_id", "location"], as_index=False)["rpp"].mean()

    # Merging HHI to other covariates
    covs = (
        covs.merge(hhi_data, on=["year_id", "location"], how="left")
        .merge(ahrf_mds, on=["year_id", "location"], how="left")
        .merge(rpp_data, on=["year_id", "location"], how="left")
    )

    # Getting medians and 75% percentile across locations for each covariate
    covs_meds = covs.groupby(["year_id"]).agg(
        {c: ["min", "max", "median", q3] for c in RHS_COLS}
    )
    covs_meds.columns = covs_meds.columns.map("_".join).str.strip("_")
    covs_meds.reset_index(inplace=True)

    # Merging median and 75% percentile data to covariates
    covs = covs.merge(covs_meds, on="year_id")

    # Standardizing covariate values
    for col in RHS_COLS:
        med_col = col + "_median"
        q3_col = col + "_q3"
        covs[col + "_stndz"] = (covs[col] - covs[med_col]) / (
            covs[q3_col] - covs[med_col]
        )
        covs.drop(columns=[col, med_col, q3_col], inplace=True)
        covs.rename(columns={col + "_stndz": col}, inplace=True)
    return covs


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
        "--outpath",
        type=str,
        required=True,
        help="Output path for prepared decomposition data.",
    )
    args = vars(parser.parse_args())

    # Parsing arguments
    scale_ver = args["scale_ver"]
    draw = args["draw"]
    year = args["year"]
    toc = args["toc"]
    outpath = args["outpath"]

    # Running decomp prep
    run_regression_decomp_prep(scale_ver, draw, year, toc, outpath)
