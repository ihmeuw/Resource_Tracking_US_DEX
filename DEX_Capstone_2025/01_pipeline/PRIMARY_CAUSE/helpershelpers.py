## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that determine the primary cause for each encounter
## ==================================================

import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import re

## --------------------------------------------------------------------------------------------
## PRIMARY CAUSE FUNCTIONS
## --------------------------------------------------------------------------------------------
## Define helper function for primary cause: make and merge on map by encounter_id, then shift indicator
def primary_cause_find_dx(full_df, map_df):

    ## Find min dx by encounter_id for map data (already subsetted to eligible rows)
    reassign_map = map_df.groupby(["encounter_id"])["dx_level_tmp"].agg("min").to_frame()
    reassign_map = reassign_map.rename_axis(["encounter_id"]).reset_index()
    reassign_map.rename(columns = {"dx_level_tmp":"dx_level_min"}, inplace = True)

    ## Merge map on to data by claim id
    reassign_df = pd.merge(full_df, reassign_map, on = ["encounter_id"], how = "left")
    ## Edit primary cause indicator
    ##    If the logic fails us, then dx_level_min will be null, so we don"t modify the primary cause (keep it on dx_1)
    ##    If the logic gives us something better, then reset indicator to 0 and add a 1 at dx_level_min
    reassign_df.loc[~reassign_df["dx_level_min"].isnull(), "primary_cause"] = 0
    reassign_df.loc[reassign_df["dx_level_tmp"] == reassign_df["dx_level_min"], "primary_cause"] = 1
    
    ## Drop helper column
    reassign_df.drop(["dx_level_min"], axis=1, inplace=True)

    ## Check: make sure there is just one row with primary cause == 1 per claim id
    print((len(reassign_df[reassign_df["primary_cause"]==1])))
    print(len(reassign_df.loc[reassign_df["primary_cause"]==1, "encounter_id"].unique()))
    print(len(reassign_df["encounter_id"].unique()))
    # print(reassign_df)
    assert(len(reassign_df[reassign_df["primary_cause"]==1]) == len(reassign_df.loc[reassign_df["primary_cause"]==1, "encounter_id"].unique()) == len(reassign_df["encounter_id"].unique()))

    ## return
    return reassign_df

## Where primary cause (generally dx_1) is GC, find first non-GC, (and non-ex)
def reassign_gc(data):
    print("      > gcs")

    ## get encounters with GC as primary
    list_encounter_ids_GC = data[(data["primary_cause"]==1) & ((data["acause"]=="_gc"))]["encounter_id"].unique()
    gc_reassign_df = data[(data["encounter_id"].isin(list_encounter_ids_GC))]
    no_reassign = data[(~data["encounter_id"].isin(list_encounter_ids_GC))]
    
    ## make map of possible non-gc codes
    map_df = gc_reassign_df[(gc_reassign_df["acause"] != "_gc") & (~gc_reassign_df["dx_level"].str.contains("ex"))]
    
    ## run reassign function on gc and recombine
    gc_reassign_df = primary_cause_find_dx(gc_reassign_df, map_df)
    out = pd.concat([gc_reassign_df, no_reassign], ignore_index = True)
    
    ## return
    return out

## Where primary cause (generally dx_1) is well/rf, find first non-GC, non-well/rf, (and non-ex) 
def reassign_well_rf(data, well_rf_list):
    print("      > well / rf")
    
    ## get encounters with well/rf as primary
    list_encounter_ids_well_rf = data[(data["primary_cause"]==1) & ((data["acause"].isin(well_rf_list)))]["encounter_id"].unique()
    well_rf_reassign_df = data[(data["encounter_id"].isin(list_encounter_ids_well_rf))]
    no_reassign = data[ (~data["encounter_id"].isin(list_encounter_ids_well_rf))]
    
    ## make map of possible non-gc, non-well/rf codes
    map_df = well_rf_reassign_df[
        (well_rf_reassign_df["acause"] != "_gc")
        & (~well_rf_reassign_df["acause"].isin(well_rf_list)) 
        & (~well_rf_reassign_df["dx_level"].str.contains("ex"))
    ]
    ## run reassign function on well/rf and recombine
    well_rf_reassign_df = primary_cause_find_dx(well_rf_reassign_df, map_df)
    out = pd.concat([well_rf_reassign_df, no_reassign], ignore_index = True)
    
    ## return
    return(out)

## Where primary cause (generally dx_1) is NEC or _other, find first non-GC, non-well/rf, non-NEC, non-other (and non-ex) in the same family
def reassign_other_nec(data, well_rf_list, other_causes_list, causemap_config):
    print("      > other / nec")

    ## get encounters with NEC or other as primary
    list_encounter_ids_NEC = data[(data["primary_cause"]==1) & ((data["acause"].str.contains("NEC")) | (data["acause"].isin(other_causes_list)))]["encounter_id"].unique()
    nec_reassign_df = data[data["encounter_id"].isin(list_encounter_ids_NEC)]
    no_reassign = data[(~data["encounter_id"].isin(list_encounter_ids_NEC))]
    
    ## merge on family 
    fam_map = pd.read_csv(causemap_config["causelist_path"])[["acause", "family"]]
    nec_reassign_df = pd.merge(nec_reassign_df, fam_map, on = ["acause"], how = "left")
    
    ## Identify and merge on family for dx_1 by claim id
    nec_map_1 = nec_reassign_df.loc[nec_reassign_df["primary_cause"]==1, ["encounter_id", "family"]]
    nec_map_1.rename(columns = {"family":"dx_1_family"}, inplace = True)
    nec_reassign_df = pd.merge(nec_reassign_df, nec_map_1, on = ["encounter_id"], how = "left")
    
    ##  Map data is non-GC, non-well/rf, non-NEC, non-other in the same family as dx_1
    map_df = nec_reassign_df[
        (nec_reassign_df["acause"] != "_gc")
        & (~nec_reassign_df["acause"].isin(well_rf_list)) 
        & (~nec_reassign_df["acause"].str.contains("NEC")) & (~nec_reassign_df["acause"].isin(other_causes_list)) 
        & (nec_reassign_df["dx_1_family"]==nec_reassign_df["family"])
        & (~nec_reassign_df["dx_level"].str.contains("ex"))
    ]
    
    ## run reassign function on nec/other and recombine
    nec_reassign_df = primary_cause_find_dx(nec_reassign_df, map_df)
    nec_reassign_df.drop(["dx_1_family", "family"], axis=1, inplace=True)
    out = pd.concat([nec_reassign_df, no_reassign], ignore_index = True)
    
    ## return
    return out

## Injuries! If an injury claim (dx_1 = N code) has an E code, shift primary cause indicator to the first non-GC E code 
## (Use inj map to identify E and N codes)
def reassign_injuries(new_data, causemap_config, which_code_system):
    print("      > injuries")

    ## Get E and N codes
    inj_map = pd.read_csv(causemap_config["injury_code_path"])
    e_code_list = inj_map.loc[(inj_map["code_type"] == "E code") & (inj_map["code_system"].isin(which_code_system)), "icd_code"]
    n_code_list = inj_map.loc[(inj_map["code_type"] == "N code") & (inj_map["code_system"].isin(which_code_system)), "icd_code"]

    ## List of injury claim ids (dx_1 = N code)
    list_encounter_ids_inj = new_data.loc[(new_data['dx'].isin(n_code_list)) & (new_data['dx_level']=="dx_1"), "encounter_id"].drop_duplicates()

    # Find which injury claims have E code in the dx chain, use that to subset
    list_encounter_ids_inje = new_data.loc[(new_data['encounter_id'].isin(list_encounter_ids_inj)) & (new_data["dx"].isin(e_code_list)), "encounter_id"].drop_duplicates()
    inj_reassign_df = new_data[new_data["encounter_id"].isin(list_encounter_ids_inje)]
    no_reassign_df = new_data[~new_data["encounter_id"].isin(list_encounter_ids_inje)]

    ## For inj only make sure ex's are returned before dx's
    inj_reassign_df["dx_level_tmp"] = np.where(inj_reassign_df["dx_level"].str.contains("dx"), inj_reassign_df["dx_level_tmp"]+1000, inj_reassign_df["dx_level_tmp"])

    # run reassign function on injuries and recombine
    map_df = inj_reassign_df[inj_reassign_df["dx"].isin(e_code_list)]
    inj_reassign_df = primary_cause_find_dx(inj_reassign_df, map_df)
    out = pd.concat([inj_reassign_df, no_reassign_df], ignore_index = True)
    
    ## return
    return out

## Final correction: make chronic conditions primary if they are in any dx (in NF)
def prioritize_chronic_conditions(data, chronic_causes, well_rf_list, other_causes_list, causemap_config):
    print("      > chronic conditions")

    ## for each cause, prioritize it as the primary_cause if it is in any dx
    ## from least to most prevalent, so most prevalent are prioritized over all others at the end of the loop
    for ch in chronic_causes:
        if ch in data["acause"].unique():    
            ## split
            chronic_ids = data.loc[(data['acause'] == ch), "encounter_id"].unique()
            chronic_reassign = data.loc[data["encounter_id"].isin(chronic_ids)]
            chronic_no_reassign = data[~(data["encounter_id"].isin(chronic_ids))]
            ## make map
            chronic_map = chronic_reassign.loc[chronic_reassign["acause"] == ch]
            ## run reassign function on dementia and recombine
            chronic_reassigned = primary_cause_find_dx(chronic_reassign, chronic_map)
            data = pd.concat([chronic_reassigned, chronic_no_reassign], ignore_index = True)
            ## make sure we have the right number of primary_cause
            assert sum(data["primary_cause"]) == len(data["encounter_id"].drop_duplicates())

    ## repeat NEC adjust
    out = reassign_other_nec(data, well_rf_list, other_causes_list, causemap_config)
    
    ## return
    return out

## Define function that adds primary cause indicator based on gc/nec/inj logic
def add_primary_cause_indicator(data, toc, age, causemap_config, CAUSEMAP_MVI):

    print("      > setup")
    ## add primary cause indicator to dx_1 everywhere
    start_len = len(data)
    data["primary_cause"] = 0
    data.loc[data["dx_level"]=="dx_1", "primary_cause"] = 1
    ## add temp helper column
    data.loc[:,"dx_level_tmp"] = [int(re.sub("[d,e]x_", "", x)) for x in data["dx_level"]]
    ## identify code system
    code_system = data["code_system"].unique()
    ## before inj step, make sure dx's have lower level than ex's
    data["dx_level_tmp"] = np.where(data["dx_level"].str.contains("ex"), data["dx_level_tmp"]+100, data["dx_level_tmp"]) 
    ## list of well/rf causes that we don't want to assign primary to
    if toc == "IP":
        ## the well-causes below were already addressed for toc==IP as per the toc-age-sex restrictions, but preserving here for complete logic
        well_rf_list = ["rf_hyperlipidemia", "rf_hypertension", "rf_tobacco", "exp_well_dental"]
        if age != 0:
            well_rf_list = well_rf_list + ["exp_well_person"]
    elif toc == "NF":
        ## the well-causes below were already addressed for toc==NF as per the toc-age-sex restrictions, but preserving here for complete logic
        well_rf_list = ["rf_hyperlipidemia", "rf_hypertension", "rf_obesity", "rf_tobacco", "exp_well_pregnancy", "exp_well_dental", "exp_donor", "exp_family_planning", "exp_social_services", "exp_well_person"]
    else:
        well_rf_list = []
    ## list of other causes that we treat like NECs
    other_causes_list  = ['cvd_other', 'digest_other', 'maternal_other', 'mental_other', 'neo_other_cancer', 'neo_other_benign', 'neonatal_other', 'neuro_other', 'resp_other', 'sense_other']

    ## run each main step
    data = reassign_gc(data)
    data = reassign_well_rf(data, well_rf_list)
    data = reassign_other_nec(data, well_rf_list, other_causes_list, causemap_config)
    data = reassign_injuries(data, causemap_config, code_system)    

    ## drop dx level tmp
    data.drop(["dx_level_tmp"], axis=1, inplace=True)

    ## if NF, prioritize chronic conditions
    if toc == "NF":
        ## getting list of chronic causes from NNHS for NF
        chronic_causes = pd.read_csv("[repo_root]/static_files/PRIMARY_CAUSE/NNHS_cause_ranks_from_causemap_mvi_%s.csv" %(CAUSEMAP_MVI))
        ## sort by reverse prevalence_rank
        ## the order is reversed so the least prevalent will be applied first, which can then be overwritten by the more prevalent, etc.
        chronic_causes = chronic_causes.sort_values(by = "prevalence_rank", ascending = False)
        ## keep only list of causes themselves (in order)
        chronic_causes = chronic_causes["acause"].tolist()
        ## reset dx_level_tmp
        data["dx_level_tmp"] = [int(re.sub("[d,e]x_", "", x)) for x in data["dx_level"]]
        data["dx_level_tmp"] = np.where(data["dx_level"].str.contains("ex"), data["dx_level_tmp"]+100, data["dx_level_tmp"]) 
        ## run prioritize chronic conditions
        data = prioritize_chronic_conditions(data, chronic_causes, well_rf_list, other_causes_list, causemap_config)

    ## Check no rows have been dropped throughout all steps
    assert len(data)==start_len
    return data
