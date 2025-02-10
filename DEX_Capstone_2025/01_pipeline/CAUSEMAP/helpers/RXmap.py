## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that map NDC codes to health conditions.
## ==================================================

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
import pyarrow.parquet as pq
import pandas as pd
import random
from helpers import (
    readwrite as rw
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

def read_rxmap(year, age, sex, config, raw_config):

    ## get causes we can use
    as_map = pd.read_csv(raw_config["METADATA"]["toc_cause_restrictions_PRE_COLLAPSE_path"])
    as_map = as_map[(as_map["include"] == 1) & (as_map["gc_nec"] == 0) & (as_map["toc"] == "RX") & (as_map["age_start"] <= age) & ((as_map["age_end"] >= age))]
    if sex == 1:
        as_map = as_map[as_map["male"] == 1]
    elif sex == 2:
        as_map = as_map[as_map["female"] == 1]
    rx_causes = as_map.acause.values

    ## convert age
    rx_age_bins = [0, 20, 45, 65]
    rx_age = max([x for x in rx_age_bins if age >= x])

    ## get map paths and schema
    rxmap_path_YAS = config["rx_map_dir"] + "rx_map_year_age_sex.parquet"
    rxmap_path_AS = config["rx_map_dir"] + "rx_map_age_sex.parquet"
    rxmap_path_S = config["rx_map_dir"] + "rx_map_sex.parquet"
    rxmap_path_0 = config["rx_map_dir"] + "rx_map.parquet"
    rxmap_schema_YAS = rw.replace_null_schema(rxmap_path_YAS)
    rxmap_schema_AS = rw.replace_null_schema(rxmap_path_AS)
    rxmap_schema_S = rw.replace_null_schema(rxmap_path_S)
    rxmap_schema_0 = rw.replace_null_schema(rxmap_path_0)
    
    ## year/age/sex
    parquet_file_YAS = pq.ParquetDataset(
        rxmap_path_YAS, 
        filters=[
            ("year_id", "=", year), 
            ("age_group_years_start", "=", rx_age), 
            ("sex_id", "=", sex),
            ("acause", "in", rx_causes)
        ], 
        schema=rxmap_schema_YAS
    )
    RXMAP_YAS = parquet_file_YAS.read().to_pandas()
    RXMAP_YAS.rename(columns = {"NDCNUM": "dx"}, inplace = True)
    RXMAP_YAS = RXMAP_YAS[["dx", "probability", "acause"]]

    ## age/sex
    parquet_file_AS = pq.ParquetDataset(
        rxmap_path_AS, 
        filters=[
            ("age_group_years_start", "=", rx_age), 
            ("sex_id", "=", sex), 
            ("acause", "in", rx_causes)
        ], 
        schema=rxmap_schema_AS
    )
    RXMAP_AS = parquet_file_AS.read().to_pandas()
    RXMAP_AS.rename(columns = {"NDCNUM": "dx"}, inplace = True)
    RXMAP_AS = RXMAP_AS[["dx", "probability", "acause"]]

    ## sex
    parquet_file_S = pq.ParquetDataset(
        rxmap_path_S, 
        filters=[
            ("sex_id", "=", sex), 
            ("acause", "in", rx_causes)
        ], 
        schema=rxmap_schema_S
    )
    RXMAP_S = parquet_file_S.read().to_pandas()
    RXMAP_S.rename(columns = {"NDCNUM": "dx"}, inplace = True)
    RXMAP_S = RXMAP_S[["dx", "probability", "acause"]]

    ## all
    parquet_file_0 = pq.ParquetDataset(
        rxmap_path_0, 
        filters=[
            ("acause", "in", rx_causes)
        ], 
        schema=rxmap_schema_0
    )
    RXMAP_0 = parquet_file_0.read().to_pandas()
    RXMAP_0.rename(columns = {"NDCNUM": "dx"}, inplace = True)
    RXMAP_0 = RXMAP_0[["dx", "probability", "acause"]]

    ## ensure no probabilities of 0
    RXMAP_YAS = RXMAP_YAS[RXMAP_YAS["probability"] > 0]
    RXMAP_AS = RXMAP_AS[RXMAP_AS["probability"] > 0]
    RXMAP_S = RXMAP_S[RXMAP_S["probability"] > 0]
    RXMAP_0 = RXMAP_0[RXMAP_0["probability"] > 0]

    ## return 
    return RXMAP_YAS, RXMAP_AS, RXMAP_S, RXMAP_0

## function to apply RX map
def map_rx(data, year, age, sex, config, raw_config):

    ## get RXMAP
    RXMAP_YAS, RXMAP_AS, RXMAP_S, RXMAP_0 = read_rxmap(year, age, sex, config, raw_config)

    ## set to string and pad with zeros
    data["dx"] = data["dx"].astype(str).str.zfill(11)

    ## find missing
    found = data["dx"].isin(RXMAP_0["dx"])
    missed = data[~found].reset_index(drop = True)
    missed = missed.groupby("dx").size().reset_index(name="count")
    missed[["year_id", "age_group_years_start", "sex_id"]] = [year, age, sex]
    data = data[found].reset_index(drop = True)

    ## apply map to data
    if len(data) == 0:
        results = pd.DataFrame()
    else:
        ## group choices, probabilities, data, and data size by ndc
        choices_YAS = RXMAP_YAS.groupby('dx')["acause"].apply(list)
        choices_AS = RXMAP_AS.groupby('dx')["acause"].apply(list)
        choices_S = RXMAP_S.groupby('dx')["acause"].apply(list)
        choices_0 = RXMAP_0.groupby('dx')["acause"].apply(list)
        probabilities_YAS = RXMAP_YAS.groupby('dx')["probability"].apply(list)
        probabilities_AS = RXMAP_AS.groupby('dx')["probability"].apply(list)
        probabilities_S = RXMAP_S.groupby('dx')["probability"].apply(list)
        probabilities_0 = RXMAP_0.groupby('dx')["probability"].apply(list)
        data = data.groupby('dx')
        
        ## iterate and map
        results = []
        for name, subdf in data:
            n_obs = len(subdf)
            if name in choices_YAS:
                subdf["acause"] = random.choices(choices_YAS.loc[name], weights = probabilities_YAS.loc[name], k = n_obs)
            elif name in choices_AS:
                subdf["acause"] = random.choices(choices_AS.loc[name], weights = probabilities_AS.loc[name], k = n_obs)
            elif name in choices_S:
                subdf["acause"] = random.choices(choices_S.loc[name], weights = probabilities_S.loc[name], k = n_obs)
            else:
                subdf["acause"] = random.choices(choices_0.loc[name], weights = probabilities_0.loc[name], k = n_obs)
            results.append(subdf)
        results = pd.concat(results, ignore_index = True)
        results.reset_index(inplace = True, drop = True)

    ## return mapped data and missed ndcs
    return results, missed
