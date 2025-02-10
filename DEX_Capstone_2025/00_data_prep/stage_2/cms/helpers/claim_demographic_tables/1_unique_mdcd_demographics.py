############################################################
# Script 1 to create a table w/ unique
# BENE/MSIS/STATE level demographic info (date of birth, sex, race)
# to impute the sample denom with. Denom is missing DOB about 2% of time
# Author(s): Meera Beauchamp, Drew DeJarnatt
############################################################
# Note MDCR RX has no state info, so can't use to impute denom
# MDCR NF SAF and Medpar don't have bene_id, so can't use to impute denom

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

def claim_demo_info(mdcd_mdcr, dataset, state, yr, partition):
    print(dataset, state, yr, partition)
    #######################################
    # Step 1: Read in data
    #######################################
    #To save out data
    p_5=2
    #Mdcd datasets states are int, so make int
    if mdcd_mdcr =='mdcr':
        state=int(state)

    #---Select columns-----------------------------------------------------------------------
    #MDCD datasets
    if dataset in ['ip_max','ot_max']:
        cms_cols=['BENE_ID','MSIS_ID','STATE_CD','EL_SEX_CD', 'EL_RACE_ETHNCY_CD', 'EL_DOB', 'TYPE_CLM_CD']
    elif dataset in ['ip_taf','ot_taf','ltc_taf','rx_taf']:
        cms_cols = ['BENE_ID','MSIS_ID','BIRTH_DT','STATE_CD']
        if dataset == ['rx_taf']:
            cms_cols.remove('STATE_CD')
    #MDCR datasets
    elif dataset in ['carrier_mdcr', 'hha_mdcr', 'hop_mdcr', 'hosp_mdcr','ip_mdcr', 'nf_mdcr',
                     'carrier_partc', 'hha_partc', 'hop_partc','ip_partc', 'nf_partc',
                     'chia_ip_mdcr','chia_hha_mdcr','chia_hosp_mdcr','chia_carrier_mdcr',
                     'chia_op_mdcr','chia_nf_mdcr']:
        cms_cols = ['BENE_ID',  'DOB_DT', 'GNDR_CD','BENE_RACE_CD','BENE_STATE_CD']

    # Select appropriate data filters
    data_filters = [("STATE_CD", "in", [state])]
    if dataset in ['ip_taf','ot_taf','ltc_taf','rx_taf']:
        data_filters = data_filters +[("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])]  

    #--Create file paths----------------------------------------
    # -- MDCD file paths --#
    if dataset == 'ip_max':
        cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ip)+ str(yr) + ".parquet"  
    elif dataset == 'ot_max':
        cms_yr = str(path.path_mdcd) + "/" + str(yr) + str(path.path_mdcd_ot) + str(state.lower()) +'_ot_'+ str(yr) + ".parquet"
    elif dataset == 'ip_taf':
        cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ip_taf)+ str(yr) + ".parquet"
    elif dataset == 'ot_taf':
        partition = int(partition)
        if yr == 2019:
                cms_yr='FILEPATH'

        elif yr ==2016:
                cms_yr='FILEPATH'
        if state =='CA':
            p_5=partition+10#20
            l=list(range(partition, p_5))
        else:
            p_5=partition+50
            l=list(range(partition, p_5))
        data_filters = data_filters + [("partition",'in',l)]

    elif dataset == 'ltc_taf':
        cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ltc_taf)+ str(yr) + ".parquet"
    
    # -- MDCR file paths --#
    elif dataset =='carrier_mdcr':
        cms_yr = str(path.path_mdcr) + "/" + str(yr) + str(path.path_mdcr_carrier)+ str(yr)+ ".parquet"
    elif dataset == 'hha_mdcr':
        if yr in [2000, 2010, 2014, 2015, 2016]:
            hh_path = path.path_mdcr_hha
        elif yr in [2019]:
            hh_path = path.path_mdcr_hha_2019
        print(path)
        cms_yr = str(path.path_mdcr) + "/" + str(yr) + str(hh_path) + str(yr) + ".parquet"
    elif dataset == 'hop_mdcr':
        if yr == 2015:
            state = str(state).rjust(2, '0') #The one digit # in 2015 are zero padded to be 2 digit
        else:
            state = str(state)
        #reuse files have a different file name
        if yr in [2000, 2010, 2014, 2015, 2016]:
            op_path = path.path_mdcr_op
        elif yr in [2008, 2009, 2011, 2012, 2013, 2017]:
            op_path = path.path_mdcr_op_reuse
        elif yr in [2019]:
            op_path = path.path_mdcr_op_2019
        print(op_path)
        cms_yr = str(path.path_mdcr) + '/' +str(yr) + str(op_path) + str(yr) + ".parquet"
    elif dataset == 'hosp_mdcr':
        cms_yr = str(path.path_mdcr) + '/'+str(yr) + str(path.path_mdcr_hosp) + str(yr) + ".parquet"
    elif dataset == 'ip_mdcr':
        #reuse files have a different file name
        if yr in [2000, 2010, 2014, 2015, 2016]:
            ip_path = path.path_mdcr_ip
        elif yr in [2008, 2009, 2011, 2012, 2013, 2017]:
            ip_path = path.path_mdcr_ip_reuse
        elif yr in [2019]:
            ip_path = path.path_mdcr_ip_2019
        print(path)
        cms_yr = str(path.path_mdcr) + "/" + str(yr) + str(ip_path) + str(yr) + ".parquet"
    elif dataset == 'nf_mdcr':
        cms_path = str(path.path_mdcr)+'/'+str(yr)+'/'+str(path.path_mdcr_nf)+str(yr)+".parquet"
    elif dataset == 'carrier_partc':
        cms_yr = str(path.path_mdcr) + '/' + str(yr) + str(path.path_mdcr_carrier_part_c) + str(yr) + ".parquet"
    elif dataset == 'hha_partc':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) +'/hha' + str(path.path_mdcr_part_c) + str(yr) + ".parquet"
    elif dataset == 'hop_partc':
        cms_yr = str(path.path_mdcr) + '/' + str(yr) + str(path.path_mdcr_hop_part_c) + str(yr) + ".parquet"
    elif dataset == 'ip_partc':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) +'/ip' + str(path.path_mdcr_part_c) + str(yr) + ".parquet"
    elif dataset == 'nf_partc':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) +'/snf' + str(path.path_mdcr_part_c) + str(yr) + ".parquet"
    
    # CHIA MDCR paths
    elif dataset == 'chia_ip_mdcr':
        cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
    elif dataset == 'chia_hha_mdcr':
        cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
    elif dataset == 'chia_hosp_mdcr':
        cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
    elif dataset == 'chia_carrier_mdcr':
        cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
    elif dataset == 'chia_op_mdcr':
        cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
    elif dataset == 'chia_nf_mdcr':
        cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
        

    ############################
    # Read in the data
    ############################
    #Read in mdcr data 
    def replace_schema(filepath, partitioning=None):

        # Loading in interpreted dataset schema from Pyarrow
        data_schema = ds.dataset(filepath, partitioning=partitioning).schema

        # Replacing any null-column schemas with strings. The default schema is interpreted from the first file,
        # Which can result in improper interpretations of column types. These are usually handled by pyarrow
        # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
        # On later files, which will fail if any values in the column are not null. Interpreting as string
        # changes all nulls to None types.
        null_idxs = [idx for idx, datatype in enumerate(data_schema.types) if datatype == py.null()]
        for idx in null_idxs:
            data_schema = data_schema.set(idx, data_schema.field(idx).with_type(py.string()))

        return data_schema
    
    if mdcd_mdcr == 'mdcr': 
        
        # ## get schema replace
        data_schema = replace_schema(cms_yr, partitioning="hive")
        if (dataset == 'hop_mdcr') & (yr == 2008):
            data_filters = [("PRVDR_STATE_CD", "in", [state])]
        else:
            data_filters = [("BENE_STATE_CD", "in", [state])]
            
        if (dataset in ['hop_partc','ip_mdcr','nf_partc']) & (yr==2019):
            state = str(state)
            if len(state) ==1:
                state_pad = str(state).zfill(2)
                state=[state]+[state_pad]
            data_filters = [("BENE_STATE_CD", "in", state)]
        
        if 'chia' in dataset:
            data_filters = [("BENE_STATE_CD", "in", [state])]
        
        cms1=pd.read_parquet(cms_yr,
                             columns=cms_cols,
                             use_legacy_dataset=False,
                             schema=data_schema,
                             filters = data_filters).drop_duplicates() 
        print(yr)
        print(type(yr))
        print(type(cms1))
        cms1['year_id']=yr
        cms1 = cms1.rename(columns={'DOB_DT': 'dob_claim',
                                    'GNDR_CD': 'sex_claim',
                                    'BENE_RACE_CD': 'race_claim'})
        print(cms1['dob_claim'].head())
    
    elif mdcd_mdcr == 'mdcd':
        if dataset == 'rx_taf':
            cms_cols.remove('STATE_CD')
            if (state == 'CA')  | (state=='NY'):
                print(partition)
                cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_rx_taf)+ str(yr)+'/STATE_CD='+str(state)+'/part_0_'+partition+'.parquet' 

            else:
                cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_rx_taf)+ str(yr)
            cms1=pd.read_parquet(cms_yr,
                                 filters = [("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])],
                                 columns=cms_cols
                                ).drop_duplicates()
            cms1['STATE_CD']=state
        else:
            cms1=pd.read_parquet(cms_yr,
                                 filters = data_filters,
                                columns=cms_cols,).drop_duplicates()

        if dataset in ['ip_max', 'ot_max']:
            #Need to datafiltering later in MAX bc the datatype isn't always right
            cms1["TYPE_CLM_CD"]=cms1["TYPE_CLM_CD"].astype(str)
            cms1=cms1[cms1["TYPE_CLM_CD"].isin(['1','3','A','C','U','W'])]
            cms1.drop('TYPE_CLM_CD', axis=1, inplace=True)
            cms1 = cms1.rename(columns={'EL_DOB': 'dob_claim',
                                       'EL_SEX_CD': 'sex_claim',
                                       'EL_RACE_ETHNCY_CD': 'race_claim'})
        if dataset in ['ip_taf','ot_taf','ltc_taf','rx_taf']:
            cms1 = cms1.rename(columns={'BIRTH_DT': 'dob_claim'})

    print("claims data read in")
    print(cms1.shape)
    
    if cms1.empty:
        print('DataFrame is empty!')
        exit()
    
    
    #######################################
    # Step 2: Create table of unique values
    #######################################
    #Function to find the find the dob, race, or sex most commonly associated with a BENE_ID/MSIS_ID/STATE_CD
    #if all na, mode returns empty list
    def most_common(l):
        if len(l) == 1:
            return l[0]
        elif len(l)>1: #If tie for most common, pick randomly
            return random.choice(l)
        else:
            return np.nan #if all NA, return NA

    if mdcd_mdcr == 'mdcd':
        group_cols = ['BENE_ID','MSIS_ID','STATE_CD']
    elif mdcd_mdcr == 'mdcr':
        group_cols = ['BENE_ID','BENE_STATE_CD']
        #Create dfs for the demographic info we are trying to fill in, then apply function
        #Date of birth
    cols = group_cols + ['dob_claim']
    df_dob = cms1[cols].drop_duplicates()
    print('dob: ' + str(df_dob.shape))
    df_dob = df_dob.groupby(group_cols)['dob_claim'].agg(lambda x: most_common(list(pd.Series.mode(x)))).reset_index(name='dob_claim')
    print('post most common - dob: ' + str(df_dob.shape))

    print(df_dob['dob_claim'].head())
    
    if dataset not in ['ip_taf','ot_taf','ltc_taf','rx_taf']:
        #Sex
        cols = group_cols +['sex_claim']
        df_sex = cms1[cols].drop_duplicates()
        print('sex: ' + str(df_sex.shape))
        df_sex = df_sex.groupby(group_cols)['sex_claim'].agg(lambda x: most_common(list(pd.Series.mode(x)))).reset_index(name='sex_claim')
        print('post most common - sex: ' + str(df_sex.shape))
        #Race
        cols = group_cols +['race_claim']
        df_race = cms1[cols].drop_duplicates()
        print('race: ' + str(df_race.shape))
        df_race = df_race.groupby(group_cols)['race_claim'].agg(lambda x: most_common(list(pd.Series.mode(x)))).reset_index(name='race_claim')
        print('post most common - race: ' + str(df_race.shape))
        #Join sex and race dfs onto df_dob
        df_dob = pd.merge(df_dob, df_race, how ='left', on =group_cols)
        df_dob = pd.merge(df_dob, df_sex, how ='left', on =group_cols)

    else:
        df_dob['sex_claim']=np.nan
        df_dob['race_claim']=np.nan
    
    # date formatting
    if 'chia' in dataset:
        df_dob['dob_claim'] = df_dob['dob_claim'].astype(str)
        df_dob['dob_claim'] = pd.to_datetime(df_dob['dob_claim'])
        df_dob['dob_claim'] = df_dob['dob_claim'].dt.strftime('%d%b%Y').str.upper()
    #Create a column for dataset
    df_dob['sub_dataset']=dataset
    df_dob['year_id']=yr
    print(df_dob['dob_claim'].head())

    #Save out data
    # CHIA is just MA so no need to partition by state
    max_taf = ''
    partition_by = ['year_id','BENE_STATE_CD']
        
    if mdcd_mdcr == 'mdcd':
        partition_by = ['year_id','STATE_CD']
        if dataset in ['ip_max','ot_max']:
            max_taf = 'MAX'
        else:
            max_taf = 'TAF'
    if mdcd_mdcr == 'mdcd':
        outpath = 'FILEPATH'
    elif mdcd_mdcr == 'mdcr':
        outpath = 'FILEPATH'
        
    if 'chia' in dataset:
        outpath = 'FILEPATH'
        
    print(df_dob['dob_claim'].head())
    df_dob.to_parquet(outpath, 
                      partition_cols=partition_by,
                      basename_template=f"format_{dataset}_{partition}_{p_5}_{{i}}",
                      existing_data_behavior='overwrite_or_ignore')

#--------------------------------------------------------------------#
#-----------------Argument parser for jobmon-----------------#
#--------------------------------------------------------------------#
if __name__ == "__main__":
    # Argument parser, must specify dataset to run  
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mdcd_mdcr",
        type=str,
        required=True
        )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True
        )
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
        "--partition",
        type=str,
        required=True
        )
    
    args = vars(parser.parse_args())
    claim_demo_info(args['mdcd_mdcr'],args['dataset'],args['state'],args['yr'],args['partition']) 

