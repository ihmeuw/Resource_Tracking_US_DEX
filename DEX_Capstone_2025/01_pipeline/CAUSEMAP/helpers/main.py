## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper function that applies all other helpers in order
## ==================================================

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
from pathlib import Path
import pyarrow as pa
import pandas as pd
import glob
from dexdbload.pipeline_runs import parsed_config
## ...
from helpers import (
    readwrite as rw,
    dataprep as dp,
    columns as cl,
    ICDmap as icdm,
    RXmap as rxm
)

## --------------------------------------------------------------------------------------------
## Define functions
## --------------------------------------------------------------------------------------------

## apply all relevant causemapping functions in order and save out the data
def main(raw_config, PRI, MVI, source, sub_dataset, year, state, age, sex, test = False):

    print("Setting up config")
    config = parsed_config(raw_config, key = "CAUSEMAP", run_id = PRI, map_version_id = MVI) 

    print("Defining input directories & schema")
    root_dir = config["data_input_dir"][source]
    if source in ["MDCD", "MDCR", "CHIA_MDCR"]:
        filepath = glob.glob(root_dir + sub_dataset + "/best/*")[0]
    elif source in ["MSCAN", "KYTHERA"]:
        filepath = glob.glob("".join([root_dir, "best_", sub_dataset, "/*", str(year), "*"]))[0]
    else:
        filepath = glob.glob("".join([root_dir, "*", str(year), "*"]))[0]
    print(" > " + filepath)

    ## define schema
    schema = rw.replace_null_schema(filepath)

    ## parsing RX or DV
    RX = "rx" in sub_dataset or "pharmacy" in sub_dataset
    DV = "dv" in sub_dataset or "dental" in sub_dataset        
    
    ## loop through ages/sexes and apply functions
    ## (if age/sex == 'all', otherwise just run for one)
    ages = ([-1, 0, 1] + list(range(5,100,5))) if age == 'all' else [int(age)]
    sexes = ([-1, 1, 2]) if sex == 'all' else [int(sex)]
    ## ---    
    for age in ages:
        for sex in sexes: 
            print("AGE:", age, " SEX:", sex)

            ## read data
            print("    Reading data")
            data = rw.read_formatted(filepath, schema, source, year, age, sex, state)

            if len(data) == 0:
                if test: 
                    return data
                else:
                    print("    ! No data here !")
                    continue

            print("    Defining output directories")
            
            ## data directory
            out_dir = config["data_output_dir"][source]
            data_out_dir = out_dir + "data"

            ## metadata directories
            if RX:
                missed_dir = out_dir + "missed_ndcs"
            else:
                missed_dir = out_dir + "missed_icds"
            na_dropped_dir = out_dir + "nas_dropped"
            if not test:
                Path(missed_dir).mkdir(parents=True, exist_ok=True)
                Path(na_dropped_dir).mkdir(parents=True, exist_ok=True)

            ## metadata filenames
            fp_parts = [source, sub_dataset, str(year), str(age), str(sex), state]
            fp_parts = [x for x in fp_parts if x not in ["none", "all"]]
            missed_fp = missed_dir + "/" + "_".join(fp_parts) + ".feather"
            na_dropped_fp = na_dropped_dir + "/" + "_".join(fp_parts) + ".feather"
            
            print("    Cleaning data")
            data = cl.standardize_id(data, source)
            data = cl.get_cols(data, source, sub_dataset, year, RX, DV)
            data = dp.misc_drops(data, source, RX)
            data = dp.fix_los(data)
            data = dp.clean_states(data, source, sub_dataset)
            data = dp.dedup(data, source, sub_dataset)

            print("    Dropping and counting NAs")
            data, na_dropped = dp.handle_nas(data, source, sub_dataset, year, age, sex, RX, DV)

            # write out na counts
            if not test:
                na_dropped.to_feather(na_dropped_fp)

            ## Skip to next iteration if no data
            if len(data) == 0:
                if test: 
                    return data
                else:
                    if age != -1 and sex != -1:
                        print("    ! All rows were dropped due to NAs !")
                    continue
            
            ## map ICDs to acauses
            if RX:
                print("    Mapping NDC to acause")
                data, missed = rxm.map_rx(data, year, age, sex, config, raw_config)
            elif not DV:
                print("    Mapping ICD to acause")
                data, missed = icdm.map_cause(data, year, config)
            else:
                missed = pd.DataFrame()
            
            ## Skip to next iteration if no data
            if len(data) == 0:
                if test: 
                    return data
                else:
                    print("    ! All rows were dropped due to missing ICDs !")
                    continue

            print("    Applying age/sex restrictions")
            data = icdm.age_sex_restrict(data, raw_config)

            ## adding carrier and sub-dataset columns
            if source in ["MDCR", "CHIA_MDCR"]:
                data['carrier'] = "carrier" in sub_dataset
            if sub_dataset != "none":
                data["sub_dataset"] = sub_dataset

            print("    Setting types")
            data = cl.set_types(data, source)

            ## final check for duplicates
            assert data.duplicated().sum() == 0

            ## if test, just return data
            if test:
                return data

            ## write data and missed ICDs
            print("    Saving to... ", data_out_dir)
            if len(data) > 0:
                rw.write_parquet(
                    data, 
                    sub_dataset,
                    data_out_dir, 
                    state = (source in ["MDCR", "CHIA_MDCR", "MDCD", "KYTHERA", "MSCAN"]), 
                    carrier = (source in ["MDCR", "CHIA_MDCR"])
                )
            if not DV:
                if len(missed) > 0:
                    ## "missed" ICDs are routinely added to the icd map as "empirical" ICDs
                    missed.to_feather(missed_fp)
