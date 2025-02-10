## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Worker script for the COLLAPSE step
## ==================================================

## Although this code includes functionality to collapse by race this functionality was not used.
## For all occurences, the following setting were used:
## ...
## race_option = 'no_re' ## (no race/ethincity information was used)
## race_col = 'no_re' ## (no race/ethincity information was used)
## no_race_source = not used

## ---------------------------------
## SETUP
## ---------------------------------
import os
os.umask(0o002)
import pandas as pd
import argparse
from dexdbload.pipeline_runs import get_config
raw_config = get_config()
from helpers import (
    CollapseMain as c
)

## ---------------------------------
## Running the script
## ---------------------------------
if __name__ == '__main__':

    ## parse arguments
    parser = argparse.ArgumentParser(description='Pull in arguments')
    parser.add_argument('-s', '--source', help='Source of data', type=str, required=True)
    parser.add_argument('-t', '--toc', help='Type of care', type=str, required=True)
    parser.add_argument('-y', '--year', help='Year of data', type = int, required=True)
    parser.add_argument('-a', '--acause', help='Cause', type=str, required=True)
    parser.add_argument('-p', '--PRI', help='Phase run ID', required=True)
    parser.add_argument('-r', '--race_option', help='re, no_re, both', type=str, required=True)
    parser.add_argument('-rc', '--race_col', help='raw, imp', type=str, required=True)
    parser.add_argument('-n', '--no_race_source', help='Which sources do not have race? format = "MSCAN,HCCI"', type=str, required=True)
    args = vars(parser.parse_args())
    print(args)
    source = args['source']
    toc = args['toc']
    year = args['year']
    acause = args['acause']
    PRI = str(args['PRI'])
    race_option = args['race_option']
    race_col = args['race_col']
    no_race_source = args['no_race_source'].split(',')

    ## run the function
    c.main(raw_config, PRI, source, toc, year, acause, race_option, race_col, no_race_source)
