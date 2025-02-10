## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script for the CAUSEMAP step
## ==================================================

## ---------------------------------
## SETUP
## ---------------------------------
import argparse
from dexdbload.pipeline_runs import get_config
raw_config = get_config()
from helpers import (
    main as m
)

## ---------------------------------
## RUNNING main()
## ---------------------------------
if __name__ == "__main__":

    print("Parsing arguments")
    parser = argparse.ArgumentParser(description="Pull in source/(sub_dataset)/year")
    parser.add_argument("-s", "--source", help="Source of data", required=True)
    parser.add_argument("-b", "--sub_dataset", help="Sub-dataset ('none' if none)", required=True)
    parser.add_argument("-y", "--year", help="Year", type = int, required=True)
    parser.add_argument("-a", "--age", help="Age", type = str, required=True)
    parser.add_argument("-x", "--sex", help="Sex", type = str, required=True)
    parser.add_argument("-l", "--state", help="State ('all' if not using state for this source)", required=True)
    parser.add_argument("-p", "--PRI", help="Phase run ID", required=True)
    parser.add_argument("-m", "--MVI", help="Map version ID", required=True)
    args = vars(parser.parse_args())
    print(args)

    source = args["source"]
    sub_dataset = args["sub_dataset"]
    year = args["year"]
    age = args["age"]
    sex = args["sex"]
    state = args["state"]
    PRI = args["PRI"]
    MVI = args["MVI"]
    
    m.main(raw_config, PRI, MVI, source, sub_dataset, year, state, age, sex)
