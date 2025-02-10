## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that standardize columns
## ==================================================

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
import pandas as pd
pd.options.mode.chained_assignment = None

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

## function to ensure the appropriate id column (encounter_id or claim_id)
## is present in the data, and that only one of them is present
def standardize_id(data, source):

    ## refresh global constants
    from helpers.constants import (
        expected_cols
    )

    ## get the id we want (sources needing C2E should are at claim level)
    c2e_sources = ["MDCR", "MDCD", "MSCAN", "KYTHERA", "CHIA_MDCR"]
    if source in c2e_sources:
        wanted_id = "claim_id"
        unwanted_id = "encounter_id"
    else:
        wanted_id = "encounter_id"
        unwanted_id = "claim_id"
    ## ensure expected_cols has what we want
    assert wanted_id in expected_cols[source]
    assert unwanted_id not in expected_cols[source]
    ## ensure the data has only 1 (not both)
    assert sum(data.columns.isin([wanted_id, unwanted_id])) == 1
    ## rename if necessary (sometimes formatting has wrong name)
    if wanted_id not in data.columns:
        assert unwanted_id in data.columns
        data.rename(columns = {unwanted_id: wanted_id}, inplace = True)
    ## double check
    assert unwanted_id not in data.columns
    assert wanted_id in data.columns
    return data

## function to make sure the data has the right columns
def get_cols(data, source, sub_dataset, year, RX, DV):

    ## refresh global constants
    from helpers.constants import (
        expected_cols,
        rename_cols,
        rx_only_cols,
        expected_cols_subdataset_exceptions,
        age_ids
    )

    excols =  expected_cols[source].copy()
    
    ## Fill code_system when missing
    if source == "MDCD" and sub_dataset in ["ip", "ot"]:
        data["code_system"] = "icd9"

    ## missing when we open_dataset on a year partition
    if "year_id" not in data.columns:
        data["year_id"] = year

    ## missing for nf_medpar and nf_saf
    if "age_group_id" not in data.columns:
        data = pd.merge(data, age_ids, on = "age_group_years_start", how = "left")
    if "age_group_years_start" not in data.columns:
        data = pd.merge(data, age_ids, on = "age_group_id", how = "left")

    ## for RX/DV, drop columns we don't need, and mark everything primary_cause
    if RX or DV: 
        for x in ["code_system", "dx", "dx_level", "primary_cause"]:
            if x in data.columns:
                data.drop(columns = [x], inplace = True)
            data[x] = None 
        data["primary_cause"] = 1
    
    ## put "ndc" in "dx" column for RX
    if RX: 
        data.drop(columns = ["dx"], inplace = True) 
        data.rename(columns = {"ndc": "dx"}, inplace = True)

    ## make RX only cols None for non-RX
    if not RX:
        for r in rx_only_cols:
            if r in excols: data[r] = None

    ## rename cols using rename_cols dict
    source_sub = "_".join([source, sub_dataset])
    if source_sub in rename_cols:
        rename_col_keys = [x for x in rename_cols[source_sub] if x in data.columns]
        rename_me = {x: rename_cols[source_sub][x] for x in rename_col_keys}                   
        data.rename(columns = rename_me, inplace = True)

    ## make expected missing cols None
    if source_sub in expected_cols_subdataset_exceptions:
        M = expected_cols_subdataset_exceptions[source_sub]
        assert all(m not in data.columns for m in M)
        data[M] = None

    ## select columns using expected_cols dict
    if RX:
        data = data[excols + ["primary_cause"]]
    elif DV:
        data = data[excols + ["primary_cause", "acause"]]
    else:
        data = data[excols]

    return data

## function to set types for columns
def set_types(data, source):

    ## refresh global constants
    from helpers.constants import (
        types,
        na_vals
    )

    ## set types
    cols = list(data.columns)
    for c in cols:
        if c not in types:
            raise ValueError(f"Column {c} not in types dict")
        if all(pd.isnull(data[c])) or all(data[c].isin(na_vals)):
            pass
        else:
            data[c] = data[c].astype(types[c])
    return data
