#####################################################################################
##PURPOSE: CMS Medicare Medpar Skilled Nursing Facility
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
#import packages
import pandas as pd
import numpy as np
import math
from datetime import date
from time import time
import psutil 
import gc
from pathlib import Path
import argparse
import sys

# adding this to make importing these easier if using git worktree
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.helpers import jobmon_submitter
from 00_data_prep.stage_2.cms.helpers import processing_functions as pf
from 00_data_prep.stage_2.cms.constants import paths as path
from 00_data_prep.stage_2.cms.constants import cms_dict as cd

cms_cols_base = ['AGE','SEX','RACE','MEDICARE_STATUS_CODE','STATE','DAY_OF_ADMISSION','DISCHARGE_STATUS','HMO_PAID_INDICATOR',
           'STAY_INDICATOR','NUMBER_OF_BILLS','ADMISSION_DATE','DISCHARGE_DATE','LENGTH_OF_STAY','OUTLIER_DAYS','COVERED_DAYS',
           'COINSURANCE_DAYS','LIFETIME_RESERVE_DAYS','COINSURANCE_AMOUNT','INPATIENT_DEDUCTIBLE','BLOOD_DEDUCTIBLE',
           'PRIMARY_PAYER_AMOUNT','PRIMARY_PAYER_CODE','REIMBURSEMENT_AMOUNT','OUTLIER_AMOUNT','TOTAL_CHARGES', 
           'ADMISSION_TO_DEATH_INTERVAL','SERVICE_CHARGES_22','BILL_TOTAL_PER_DIEM'
          ]
dx_10_14=['DIAGNOSIS_CODE_1','DIAGNOSIS_CODE_2','DIAGNOSIS_CODE_3','DIAGNOSIS_CODE_4','DIAGNOSIS_CODE_5',
           'DIAGNOSIS_CODE_6','DIAGNOSIS_CODE_7','DIAGNOSIS_CODE_8','DIAGNOSIS_CODE_9','DIAGNOSIS_CODE_10',
           'DIAGNOSIS_CODE_11','DIAGNOSIS_CODE_12','DIAGNOSIS_CODE_13','DIAGNOSIS_CODE_14','DIAGNOSIS_CODE_15',
           'DIAGNOSIS_CODE_16','DIAGNOSIS_CODE_17','DIAGNOSIS_CODE_18','DIAGNOSIS_CODE_19','DIAGNOSIS_CODE_20',
           'DIAGNOSIS_CODE_21','DIAGNOSIS_CODE_22','DIAGNOSIS_CODE_23','DIAGNOSIS_CODE_24','DIAGNOSIS_CODE_25',
           'DIAGNOSIS_E_CODE_1','DIAGNOSIS_E_CODE_2','DIAGNOSIS_E_CODE_3','DIAGNOSIS_E_CODE_4','DIAGNOSIS_E_CODE_5',
           'DIAGNOSIS_E_CODE_6','DIAGNOSIS_E_CODE_7','DIAGNOSIS_E_CODE_8','DIAGNOSIS_E_CODE_9','DIAGNOSIS_E_CODE_10',
           'DIAGNOSIS_E_CODE_11','DIAGNOSIS_E_CODE_12','NCH_CLAIM_TYPE_CODE']
dx_02_08 = ['DIAGNOSTIC_CODES_1','DIAGNOSTIC_CODES_2','DIAGNOSTIC_CODES_3','DIAGNOSTIC_CODES_4','DIAGNOSTIC_CODES_5',
              'DIAGNOSTIC_CODES_6','DIAGNOSTIC_CODES_7','DIAGNOSTIC_CODES_8','DIAGNOSTIC_CODES_9']


def cms_column_format(outpath, yr, df):#
    print(yr)
    print(df)
    #--------------------------------------------------------------------
    #Step 1 - read in pharma data, create unique list of bene ids 
    #--------------------------------------------------------------------
    #Set file paths
    if yr in [2002, 2004, 2006, 2008, 2010, 2012, 2014]:
        cms_path = 'FILEPATH'+str(yr) +'/FILEPATH' +str(yr) +'.dta'
    elif yr == 2016:
        cms_path = 'FILEPATH'
    
    #Set columns
    if yr in [2002, 2004, 2006, 2008]:
        cms_cols=cms_cols_base + dx_02_08
    elif yr in [2010, 2012, 2014, 2016]:
        cms_cols=cms_cols_base + dx_10_14

    cms1 = pd.read_stata(cms_path,
                        columns = cms_cols)

    ######################################################################
    #---------------------Step 2 - COLUMN FORMATTING---------------------#
    ######################################################################
    #---------------------------------------------------------------------------------------------------------------------------#
    #Step 2.1: Create date related columns-#
    #---------------------------------------------------------------------------------------------------------------------------#
    #Inclusion in the dataset is based on the year the stay ended and therefore there can be a claim with an admission date in an early year than the data year.
    cms1['year_id']=yr
    cms1['year_adm']=np.nan
    cms1['year_dchg']=yr
    cms1['year_clm']=yr
    cms1['STATE']=cms1['STATE'].astype(int)
    cms1.rename(columns = {'LENGTH_OF_STAY':'los'}, inplace = True) 

    #---------------------------------------------------------------------------------------------------------------------------#
    #--------------Step 2.2: Create demographic related columns - sex, race, location info--------------------------------------#
    #---------------------------------------------------------------------------------------------------------------------------#
    cms1.rename(columns = {'AGE':'age_bin'}, inplace = True)
    #if sex doesn't = 1 or 2 fill w -1
    # Sex
    cms1=pf.sex_format(dataset='MDCR', subdataset='nf_medpar', df=cms1)
    #Race
    cms1=pf.race_format(dataset='MDCR', subdataset='nf_medpar', df=cms1)

    ###-----Geographic Info-----##
    #state of residence, State is SSA state code 
    cms1 = pf.mdcr_state_format(subdataset='nf_medpar',df=cms1, resi_serv = 'resi')
    print("demographic formatting completed")

    #---------------------------------------------------------------------------#
    #-------------Step 2.3: Create primary_payer col--------------#
    #---------------------------------------------------------------------------#
    #PRIMARY_PAYER_CODE - primary code if not MDCR 
    cms1['pri_payer'] = cms1['PRIMARY_PAYER_CODE'].map(lambda x: cd.mdcr_payer_dict[x][0]).fillna(1)

    #if 2, b or c then 1 is MC #https://resdac.org/cms-data/variables/hmo-indicator
    cms1['hmo_ind']=np.where(cms1['HMO_PAID_INDICATOR'].isin(['2','B','C']),1,0)

    #---------------------------------------------------------------------------#
    #-------------Step 2.4: Create other columns TOC, Facility etc--------------#
    #---------------------------------------------------------------------------#
    #Type of care
    cms1['toc']="NF" #Hardcoded to NF for dataset
    #code_system_id: icd-9 or icd-10. On oct 1, 2015 transitioned to icd-10 #1=icd9, 2=icd10
    cms1['code_system']=np.where(yr<=2015,'icd9','icd10')

    #---------------------------------------------------------------------------#
    #--------Step 2.5: Rename columns that don't need other processing----------#
    #---------------------------------------------------------------------------#
    if yr in [2002, 2004, 2006, 2008]:
        cms1.columns = cms1.columns.str.replace('DIAGNOSTIC_CODES_' , 'dx_')
    elif yr in [2010, 2012, 2014, 2016]:
        cms1.columns = cms1.columns.str.replace('DIAGNOSIS_CODE_' , 'dx_')
        cms1.columns = cms1.columns.str.replace('DIAGNOSIS_E_CODE_' , 'ecode_')

    #------------------------------------------------------------------------------------#
    #--------Step 2.6: Change datatype of columns to mimize df size for storage----------#
    #------------------------------------------------------------------------------------#
    #Specific issues with these columns
    cms1['sex_id']=cms1['sex_id'].fillna(-1).astype('int8')
    cms1['age_bin']=cms1['age_bin'].fillna(-1)

    cms1=pf.datatype_format(dataset='MDCR', subdataset='nf_medpar', df=cms1)

    #------------------------------------------------------------------------------------#
    #--------Step 2.7: Save out processed data ----------#
    #------------------------------------------------------------------------------------#
    cms1.astype({
            'sex_id':"int8",
            'age_bin':"int16",
        }).to_parquet(outpath,
                     partition_cols=['year_id','st_resi'],
                  existing_data_behavior='overwrite_or_ignore')
      
#--------------------------------------------------------------------#
#-----------------Step 5: Argument parser for jobmon-----------------#
#--------------------------------------------------------------------#
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outpath",
        type=str,
        required=True
        )
    parser.add_argument(
        "--yr",
        type=int,
        required=True
        )
    parser.add_argument(
        "--df",
        type=str,
        required=True
        )

    args = vars(parser.parse_args())
    cms_column_format(args['outpath'],args['yr'],args['df']) #




