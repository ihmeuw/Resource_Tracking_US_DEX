## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that map ICD codes to health conditions
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

## function to clean icd codes
def clean_icds(data, year):
    
    ## fixing types
    data["dx"] = data["dx"].astype(str)
    data["code_system"] = data["code_system"].astype(str)

    ## CLEAN
    ## ------------
    ## remove spaces
    data["dx"] = data["dx"].str.replace(" ", "", regex = False)
    ## remove dot
    data["dx"] = data["dx"].str.replace(".", "", regex = False)
    ## assign all letters uppercase
    data["dx"] = data["dx"].str.upper()
    
    ## Since data is sometimes wide on diagnosis, 
    ## secondary diagnoses can be icd9 but labeled as icd10.
    ## and vice versa.
    if year >= 2015:

        ## icd10s all start with alphabetical characters. 
        ## any "icd10" in DX2+ starting with a digit -> icd9
        data.loc[(data["code_system"] == "icd10") & (data["dx"].str.match(r"[\d].+")) & (data["dx_level"] != "dx_1"), "code_system"] = "icd9"

        ## all icd9s start with digits or E/V
        ## any icd9 starting with an alphabetical character other than E/V -> icd10
        data.loc[(data["code_system"] == "icd9") & (data["dx"].str.match(r"[A-DF-UW-Z].+")) & (data["dx_level"] != "dx_1"), "code_system"] = "icd10"

        ## icd9 E codes are only E0 and E8-E9
        ## any icd9 starting in E1...E7 -> icd10
        data.loc[(data["code_system"] == "icd9") & (data["dx"].str.match(r"E[1-7]")) & (data["dx_level"] != "dx_1"), "code_system"] = "icd10"

    ## ASSIGN _GC (this mostly only applies for MDCD, which has messy ICD codes)
    ## ------------
    ## strings with less than 3 digits -> _gc
    data.loc[data["dx"].str.len() < 3, "dx"] = "invl"
    ## any non alphanumeric characters -> _gc
    data.loc[~data["dx"].str.isalnum(), "dx"] = "invl"
    ## only characters (no digits) -> _gc
    data.loc[data["dx"].str.isalpha(), "dx"] = "invl"
    ## for icd9, can only start with E, V, or a number (otherwise -> _gc)
    data.loc[(data["code_system"] == "icd9") & (~data["dx"].str.match(r"[EV\d]")), "dx"] = "invl"
    ## for icd9, any icd9 starting in E1...E7 -> _gc
    data.loc[(data["code_system"] == "icd9") & (data["dx"].str.match(r"[E][1-7]")), "dx"] = "invl"
    ## for icd9, can only have letters at the beginning, not in the middle or at the end of the string (otherwise -> _gc)
    data.loc[(data["code_system"] == "icd9") & (data["dx"].str.match(r".+[A-Z]+")), "dx"] = "invl"
    ## for icd10, must start with a letter
    data.loc[(data["code_system"] == "icd10") & (~data["dx"].str.match(r"[A-Z]")), "dx"] = "invl"

    ## return cleaned data
    return data

## function to map ICDs
def map_cause(data, year, config):

    ## clean ICDs
    data = clean_icds(data, year)

    ## merge in cause map
    icdmap = pd.read_csv(config["icd_map_path"])
    icdmap.drop(columns = ["icd_name"], inplace = True)
    icdmap.rename(columns = {"icd_code": "dx", "code_system": "code_system"}, inplace = True)
    data.loc[:,["code_system", "dx"]] = data.loc[:,["code_system", "dx"]].astype(object)
    data = pd.merge(data, icdmap, on = ["code_system", "dx"], how = "left")

    ## separate out non-matching ICDs
    missed = data[data["acause"].isna()].reset_index(drop = True)
    data = data[data["acause"].notna()].reset_index(drop = True)
    missed_icds = missed[["code_system", "dx"]].reset_index(drop = True)
    missed_icds = missed_icds.groupby(["code_system", "dx"]).size().reset_index(name="count")

    ## return mapped data and missed ndcs
    data.reset_index(inplace = True, drop = True)
    return data, missed_icds

## function to apply age-sex restrictions
def age_sex_restrict(data, raw_config):

    ## set age 0 septicemia to neonatal_sepsis
    data.loc[(data["acause"] == "septicemia") & (data["age_group_years_start"] == 0), "acause"] = "neonatal_sepsis"
    
    ## get age-sex restrictions map
    as_map = pd.read_csv(raw_config["METADATA"]["toc_cause_restrictions_PRE_COLLAPSE_path"])
    as_map = as_map[["toc", "acause", "include", "age_start", "age_end", "male", "female"]]
    
    ## apply map to data
    data = pd.merge(data, as_map, on = ["toc", "acause"], how = "left")
    
    ## fix types
    data["age_group_years_start"] = data["age_group_years_start"].astype(float)
    data["sex_id"] = data["sex_id"].astype(float)

    ## prep replacements for age-sex restrictions
    replace_cols = ["acause", "dx"]
    replace_vals = ["_gc", "asr"]
 
    ## deal with toc/cause restrictions
    data.loc[data["include"] == 0, replace_cols] = replace_vals

    ## deal with age restrictions
    data.loc[(data["include"] == 1) & (data["age_group_years_start"] < data["age_start"]), replace_cols] = replace_vals
    data.loc[(data["include"] == 1) & (data["age_group_years_start"] > data["age_end"]), replace_cols] = replace_vals
    
    ## deal with sex restrictions
    data.loc[(data["include"] == 1) & (data["sex_id"] == 1) & (data["male"] == 0), replace_cols] = replace_vals
    data.loc[(data["include"] == 1) & (data["sex_id"] == 2) & (data["female"] == 0), replace_cols] = replace_vals

    ## remove restriction columns
    data.drop(columns = ["include", "age_start", "age_end", "male", "female"], inplace = True)

    ## return mapped data
    data.reset_index(inplace = True, drop = True)
    return data
