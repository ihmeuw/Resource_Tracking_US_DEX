## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that clean and prepare input data
## ==================================================

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

## function to drop specific things
def misc_drops(data, source, RX):

    ## drop toc="OTH"
    data = data.loc[data["toc"] != "OTH"] 
    
    ## only use 5% sample & specific years for AM/ED
    if source == "MDCR":
        data = data.loc[~(data["toc"].isin(["AM", "ED"])) | ((data["ENHANCED_FIVE_PERCENT_FLAG"] == "Y") & (data["year_id"].isin([2000, 2010, 2014, 2015, 2016, 2019])))]

    ## dop RX from non-rx sub-datasets
    if not RX:
        data = data.loc[data["toc"] != "RX"] 

    return data

## function to make sure LOS is not 0 or negative
def fix_los(data):
    if "los" in data.columns: 
        data.loc[(data["toc"].isin(["IP", "NF"])) & (data["los"] == 0), "los"] = 1
        data.loc[(data["toc"].isin(["IP", "NF"])) & (data["los"] < 0), "los"] = np.nan
    return data

## function to standardize the values for st_resi and st_serv columns
def clean_states(data, source, sub_dataset):

    ## refresh global constants
    from helpers.constants import (
        na_vals
    )

    ## list out states
    states = [
        "-1", "AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", 
        "FL", "GA", "HI", "IA", "ID", "IL", "IN", "KS", "KY", "LA", "MA", 
        "MD", "ME", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", 
        "NJ", "NM", "NV", "NY", "OH", "OK", "OR", "PA", "RI", "SC", "SD", 
        "TN", "TX", "UT", "VA", "VT", "WA", "WI", "WV", "WY"
    ]
    
    ## recode NAs to "-1" and drop states not in our list (e.g. other countries)
    if "st_resi" in data.columns:
        ## recode nulls
        data.loc[data["st_resi"].isnull(), "st_resi"] = "-1"
        ## recode NA vals
        data["st_resi"] = data["st_resi"].astype(str)
        data.loc[data["st_resi"].isin(na_vals), "st_resi"] = "-1"
        ## filter to in states (drop others)
        data = data[data["st_resi"].isin(states)]
    if "st_serv" in data.columns:
        ## recode nulls
        data.loc[data["st_serv"].isnull(), "st_serv"] = "-1"
        ## recode NA vals
        data["st_serv"] = data["st_serv"].astype(str)
        data.loc[data["st_serv"].isin(na_vals), "st_serv"] = "-1"
        ## don't drop the rest
    
    ## make sure we're using representative states from HCUP
    if source in ["SIDS", "SEDD"]:
        # when st_resi is missing, set st_resi equal to st_serv
        data.loc[data["st_resi"] == "-1", "st_resi"] = data.loc[data["st_resi"] == "-1", "st_serv"]
        # then, filter to where st_resi == st_serv
        data = data.loc[data["st_resi"] == data["st_serv"]]

    return data

## function to drop duplicates in specific places
def dedup(data, source, sub_dataset):
    if (
        (source == "MDCR" and sub_dataset == "nf_saf")
        or (source == "MSCAN" and sub_dataset == "rx")
        or (source == "KYTHERA" and sub_dataset in ["main", "rx"])
        or (source == "MDCD" and sub_dataset in ["ip_taf", "nf_taf", "ot_taf", "rx_taf", "dv_taf"])
        or (source in ["SIDS", "SEDD"])
    ):
        if data.duplicated().sum() > 0:
            print("    Prop duplicated...")
            print("     > " + str(round(data.duplicated().sum()/len(data), 2)))
            data.drop_duplicates(ignore_index = True, inplace = True)

    ## check for duplicates
    assert data.duplicated().sum() == 0
    return data

## function to count, drop, and recode NAs when appropriate
def handle_nas(data, source, sub_dataset, year, age, sex, RX, DV):

    ## refresh global constants
    from helpers.constants import (
        na_vals
    )

    ## get denom
    n_rows = len(data)

    ## get columns to drop NAs for
    drop_na_cols = [
        "toc",
        "age_group_years_start",
        "age_group_id",
        "pri_payer", 
        "sex_id",
        "code_system",
        "dx_level",
        "dx"
    ]

    ## exceptions, don't drop missing for these columns in RX/DV
    if DV:
        drop_na_cols = [x for x in drop_na_cols if x not in ["code_system", "dx_level", "dx"]]
    if RX:
        drop_na_cols = [x for x in drop_na_cols if x not in ["code_system", "dx_level"]]

    ## count NAS
    na_counts = pd.DataFrame(columns = ["variable", "n_na"])
    for j in data.columns:
        # print(j)
        N = len(data[ (data[j].isnull()) | (data[j].isin(na_vals)) ] )
        na_counts = pd.concat([na_counts, pd.DataFrame(data = {"variable" : [j], "n_na" : [N]})], ignore_index = True)
    
    ## drop NAs for select columns
    for j in data.columns:
        if j in drop_na_cols:
            data = data[~(data[j].isnull()) & ~(data[j].isin(na_vals)) ]

    ## track those we didn"t drop NAs for
    na_counts["dropped_nas"] = "NO"
    na_counts.loc[na_counts["variable"].isin(drop_na_cols), "dropped_nas"] = "YES"
    na_counts.reset_index(inplace = True, drop = True)

    ## add new columns
    na_counts[['source', 'sub_dataset', 'year', 'age', 'sex', 'n_rows']] = [source, sub_dataset, year, age, sex, n_rows]
    ## order columns
    na_counts = na_counts[["source", "sub_dataset", "year", "age", "n_rows", "variable", "n_na", "dropped_nas"]] 

    ## set mcnty NAs to -1
    if "mcnty_resi" in data.columns:
        data["mcnty_resi"] = pd.to_numeric(data['mcnty_resi']).astype('Int32')
        data.loc[data["mcnty_resi"].isnull(), "mcnty_resi"] = -1
        data.loc[data["mcnty_resi"].isin(na_vals), "mcnty_resi"] = -1
    if "mcnty_serv" in data.columns:
        data["mcnty_serv"] = pd.to_numeric(data['mcnty_serv']).astype('Int32')
        data.loc[data["mcnty_serv"].isnull(), "mcnty_serv"] = -1
        data.loc[data["mcnty_serv"].isin(na_vals), "mcnty_serv"] = -1
    ## ^ (state NAs were already handled in the clean_states function)

    ## assert no more NAs in places there should not be
    for j in drop_na_cols:
        assert len(data[data[j].isnull()]) == 0
        assert len(data[data[j].isin(na_vals)]) == 0

    ## return data and counts
    return data, na_counts
