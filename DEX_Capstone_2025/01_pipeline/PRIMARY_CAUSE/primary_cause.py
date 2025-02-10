## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Worker script for the PRIMARY_CAUSE step
## ==================================================

## ---------------------------------
## SETUP
## ---------------------------------
import argparse
from dexdbload.pipeline_runs import get_config
raw_config = get_config()
import helpers as h

## ---------------------------------
## Running the script
## ---------------------------------
if __name__ == "__main__":

    print("Parsing arguments")
    parser = argparse.ArgumentParser(description="Pull in source/(sub_dataset)/year")
    parser.add_argument("-s", "--source", help="Source of data", required=True)
    parser.add_argument("-t", "--toc", help="TOC", required=True)
    parser.add_argument("-y", "--year", help="Year", type = int, required=True)
    parser.add_argument("-l", "--state", help="State ('all' if not using state for this source)", required=True)
    parser.add_argument("-p", "--PRI", help="Phase run ID", required=True)
    parser.add_argument("-m", "--CAUSEMAP_MVI", help="CAUSEMAP map version id", required=True)
    args = vars(parser.parse_args())
    print(args)
    
    source = args["source"]
    toc = args["toc"]
    year = args["year"]
    state = args["state"]
    PRI = args["PRI"]
    CAUSEMAP_MVI = args["CAUSEMAP_MVI"]

    h.main(raw_config, PRI, CAUSEMAP_MVI, source, toc, year, state)