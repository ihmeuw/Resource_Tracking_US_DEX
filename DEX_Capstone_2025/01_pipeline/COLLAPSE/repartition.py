## ==================================================
## Author(s): Sawyer Crosby, Meera Beauchamp
## Date: Jan 31, 2025
## Purpose: Worker script for the repartition step of COLLAPSE
## ==================================================

## Although this code includes functionality to collapse by race, this functionality was not used.
## For all occurences, the following setting were used:
## ...
## race_option = "no_re" (no race/ethincity information was used)
## ...

## ---------------------------------
## SETUP
## ---------------------------------
import os
os.umask(0o002)
import argparse
import pandas as pd
from dexdbload.pipeline_runs import get_config
raw_config = get_config()
from helpers import RepartitionMain as r

## ---------------------------------
## CONSTANTS
## ---------------------------------
COMORB = True ## True = repartition for comorb, False = write everything to pipeline_output dir 

## Restrictions
RES_hcup = pd.read_csv(raw_config['METADATA']['sids_sedd_restrictions_path'])
RES_toc_cause = pd.read_csv(raw_config['METADATA']['toc_cause_restrictions_path'])
RES_dataset_payer = pd.read_csv(raw_config['METADATA']['dataset_payer_restrictions_path'])
RES_dataset_payer_race = pd.read_csv(raw_config['METADATA']['dataset_payer_restrictions_race_path'])

## ---------------------------------
## Running the script
## ---------------------------------
if __name__ == '__main__':

    ## parse arguments (reading by partitions is much faster)
    print('Parsing arguments')
    parser = argparse.ArgumentParser(description='Pull in arguments')
    parser.add_argument('-a', '--acause', help='Cause', required=True)
    parser.add_argument('-t', '--toc', help='TOC', required=True)
    parser.add_argument('-m', '--metric', help='Metric', required=True)
    parser.add_argument('-p', '--PRI', help='Phase run ID', required=True)
    parser.add_argument('-r', '--race_option', help='re, no_re, both', required=True)
    args = vars(parser.parse_args())
    print(args)

    acause = args['acause']
    toc = args['toc']
    metric = args['metric']
    PRI = str(args['PRI'])
    race_option = args['race_option']

    ## run the function
    if race_option in ['no_re', 'both']:
        r.main(raw_config, PRI, toc, acause, metric, COMORB, RES_hcup, RES_toc_cause, RES_dataset_payer, by_race = False)
    if race_option in ['re', 'both']:
        r.main(raw_config, PRI, toc, acause, metric, COMORB, RES_hcup, RES_toc_cause, RES_dataset_payer_race, by_race = True)
    