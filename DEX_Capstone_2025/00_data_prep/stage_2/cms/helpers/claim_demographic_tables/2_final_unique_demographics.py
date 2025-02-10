############################################################
# Script 2 to create a table w/ unique
# BENE/MSIS/STATE level demographic info (date of birth, sex, race)
# to impute the sample denom with. Denom is missing DOB about 2% of time
# This is the second script to make it unique over dataset
# Author(s): Meera Beauchamp, Drew DeJarnatt
############################################################
#packages
import pandas as pd
import numpy as np
import argparse
import random
import pyarrow.parquet as pq
from pyarrow.parquet import ParquetDataset
import pyarrow.dataset as ds
import pyarrow as py
#Import helper functions and dictionaries
# adding this to make importing these easier if using git worktree
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.helpers import processing_functions as pf
from 00_data_prep.stage_2.cms.constants import paths as path
from 00_data_prep.stage_2.cms.constants import cms_dict as cd
from 00_data_prep.stage_2.cms.helpers import jobmon_submitter


def claim_demo_info(state, yr, mdcd_mdcr, chia):
    print(state, yr)
    #######################################
    # Step 1: Read in data
    #######################################
    cols = ['BENE_ID','dob_claim','race_claim','sex_claim']
    if mdcd_mdcr == 'mdcd':
        cols = cols + ['MSIS_ID']
        if yr in [2000, 2010, 2014]:
            max_taf = 'MAX'
        elif yr in [2016, 2019]:
            max_taf = 'TAF'
    
        filepath = str(path.path_claim_demog_mdcd)+ "FILEPATH"+str(mdcd_mdcr)+"_"+str(max_taf)+"FILEPATH" +str(yr)+'FILEPATH'+ str(state)
        
    elif mdcd_mdcr == 'mdcr':
        state=int(state)
        max_taf = ''
        filepath = str(path.path_claim_demog_mdcr)+ "FILEPATH"+str(mdcd_mdcr)+"FILEPATH" +str(yr)+'FILEPATH'+ str(state)
        if chia == 1:
            filepath = str(path.path_claim_demog_chia)+ "FILEPATH"+str(mdcd_mdcr)+"FILEPATH" +str(yr)+'FILEPATH'+ str(state)
    df = pd.read_parquet(filepath,
                        columns = cols).drop_duplicates()
    print(df.shape)

    #######################################
    # Step 2: Create table of unique values
    #######################################
    #Function to find the find the dob, race, or sex most commonly associated with a BENE_ID/MSIS_ID/STATE_CD
    def most_common(l):
        if len(l) == 1:
            return l[0]
        elif len(l)>1: #If tie for most common, pick randomly
            return random.choice(l)
        else:
            return np.nan #if all NA, return NA

    #apply function
    #Create dfs for the demographic info we are trying to fill in, then apply function
    if mdcd_mdcr == 'mdcd':
        group_cols = ['BENE_ID','MSIS_ID']
    elif mdcd_mdcr == 'mdcr':
        group_cols = ['BENE_ID']
    #Date of birth
    cols = group_cols+['dob_claim']
    df_dob = df[cols].drop_duplicates()
    print('dob: ' + str(df_dob.shape))
    df_dob = df_dob.groupby(group_cols)['dob_claim'].agg(lambda x: most_common(list(pd.Series.mode(x)))).reset_index(name='dob_claim')
    print('post most common - dob: ' + str(df_dob.shape))

    if max_taf == 'MAX':
        #Sex
        cols = group_cols+['sex_claim']
        df_sex = df[cols].drop_duplicates()
        print('sex: ' + str(df_sex.shape))
        df_sex = df_sex.groupby(group_cols)['sex_claim'].agg(lambda x: most_common(list(pd.Series.mode(x)))).reset_index(name='sex_claim')
        print('post most common - sex: ' + str(df_sex.shape))
        #Race
        cols = group_cols+['race_claim']
        df_race = df[cols].drop_duplicates()
        print('race: ' + str(df_race.shape))
        df_race = df_race.groupby(group_cols)['race_claim'].agg(lambda x: most_common(list(pd.Series.mode(x)))).reset_index(name='race_claim')
        print('post most common - race: ' + str(df_race.shape))
        #Join sex and race dfs onto df_dob
        df_dob = pd.merge(df_dob, df_race, how ='left', on =group_cols)
        df_dob = pd.merge(df_dob, df_sex, how ='left', on =group_cols)

    else:
        df_dob['sex_claim']=np.nan
        df_dob['race_claim']=np.nan
    #Create a column for dataset
    df_dob['year_id']=yr
    if mdcd_mdcr == 'mdcd':
        df_dob['STATE_CD']=state
        partition_by = ['year_id','STATE_CD']
    elif mdcd_mdcr == 'mdcr':
        df_dob['BENE_STATE_CD']=state
        partition_by = ['year_id','BENE_STATE_CD']

    #Save out data
    if chia == 1:
        dataset = "CHIA_MDCR"
    else:
        dataset = mdcd_mdcr.upper()
            
    df_dob.to_parquet('FILEPATH', 
                      partition_cols=partition_by,
                      basename_template=f"part-{{i}}",
                      existing_data_behavior='overwrite_or_ignore')

#--------------------------------------------------------------------#
#-----------------Argument parser for jobmon-----------------#
#--------------------------------------------------------------------#
if __name__ == "__main__":
    # Argument parser, must specify dataset to run  
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--state",
        type=str,
        required=True
        )
    parser.add_argument(
        "--yr",
        type=int,
        required=True
        )
    parser.add_argument(
        "--mdcd_mdcr",
        type=str,
        required=True
        )
    parser.add_argument(
        "--chia",
        type=int,
        required=True
        )
    
    args = vars(parser.parse_args())
    claim_demo_info(args['state'],args['yr'],args['mdcd_mdcr'], args['chia']) 
