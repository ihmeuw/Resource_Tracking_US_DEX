#####################################################################################
##PURPOSE: CMS Medicare RX Stage 3 - Helper script
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
#Import packages
import pandas as pd
import re
import numpy as np
import math
import argparse
from datetime import date
# adding this to make importing these easier if using git worktree
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_3.cms.helpers import column_formatter as cf
from 00_data_prep.stage_3.cms.constants import paths

cols_base=['age_bin','los','year_adm','year_dchg','year_clm','race_cd_raw','race_cd_imp',
           'toc','code_system','sex_id','st_loc_id_resi','st_num_resi','st_resi','pri_payer', 
           'DISCHARGE_DATE', 'ADMISSION_DATE', #dates are in quarters
           'BLOOD_DEDUCTIBLE','COINSURANCE_AMOUNT','INPATIENT_DEDUCTIBLE','PRIMARY_PAYER_AMOUNT',
           'PRIMARY_PAYER_CODE','REIMBURSEMENT_AMOUNT','TOTAL_CHARGES','BILL_TOTAL_PER_DIEM'] 

dx_10_14=['dx_1','dx_2','dx_3','dx_4','dx_5',
          'dx_6','dx_7','dx_8','dx_9','dx_10',
          'dx_11','dx_12','dx_13','dx_14','dx_15',
          'dx_16','dx_17','dx_18','dx_19','dx_20',
          'dx_21','dx_22','dx_23','dx_24','dx_25',
          'ecode_1','ecode_2','ecode_3','ecode_4','ecode_5',
          'ecode_6','ecode_7','ecode_8','ecode_9','ecode_10',
          'ecode_11','ecode_12']
dx_02_08 = ['dx_1','dx_2','dx_3','dx_4','dx_5',
            'dx_6','dx_7','dx_8','dx_9']

years=[2002, 2004, 2006, 2008,2010, 2012, 2014, 2016]
for yr in years:
    print(yr)
    #-------Read in data--------------------#
    #Set columns
    if yr in [2002, 2004, 2006, 2008]:
        cms_cols=cols_base + dx_02_08
    elif yr in [2010, 2012, 2014, 2016]:
        cms_cols=cols_base + dx_10_14
    file_path = str(paths.mdcr_nf_medpar_stage_2_dir)+'/year_id='+str(yr)
    print(file_path)
    cms=pd.read_parquet(file_path,
                        columns = cms_cols)
    cms['year_id']=yr              
    print('data read in')

    #-------Stg3 column formatting--------------------#
    #reanme admin date to match marketscan - note these are codes to indicate the quarter of the year and not true dates
    cms.rename(columns={"ADMISSION_DATE":"service_date",
                       'DISCHARGE_DATE':'discharge_date'}, inplace=True)

    #Create claim id
    cms=cf.gen_uuid(cms)
    
    cms = cf.w2l(cms)
    print('wide to long done')
    
    #Remove rows w/ no dx or implausible los
    print(cms.shape)
    cms, df_dim=cf.drop_rows(df=cms, toc='NF', year=yr, subdataset='nf_medpar')
    print('rows dropped')
    print(cms.shape)

    cms['mc_ind']=0
    cms['dual_ind']=np.nan

    #-------Create Payer Columns--------------------#
    cms['mdcr_chg_amt']=np.nan
    cms['mdcd_chg_amt']=np.nan
    cms['priv_chg_amt']=np.nan
    cms['oop_chg_amt']=np.nan
    cms['oth_chg_amt']=np.nan
    cms['mdcr_pay_amt']=cms["REIMBURSEMENT_AMOUNT"]+cms["BILL_TOTAL_PER_DIEM"]
    cms['mdcd_pay_amt']=np.nan
    cms['priv_pay_amt']=np.nan
    cms["oop_pay_amt"]=cms['BLOOD_DEDUCTIBLE']+cms['INPATIENT_DEDUCTIBLE']+cms['COINSURANCE_AMOUNT']
    cms['oth_pay_amt']=np.nan

    cms['tot_pay_amt']= np.nan 
    cms.rename(columns={"TOTAL_CHARGES":"tot_chg_amt"}, inplace=True)

    #---payer/payer order - move to stage 2--------#
    cms['pmt_1']=cms['PRIMARY_PAYER_AMOUNT']

    #payer_2
    pay2_conditions = [(cms['pri_payer']!=1) & (cms['mdcr_pay_amt']>0),
                       (cms['tot_chg_amt']==0)]
    pay2_outputs=[1,20]
    cms['payer_2']=np.select(pay2_conditions, pay2_outputs, 21).astype('int8')
    cms['pmt_2']=np.where(cms['payer_2']==1,cms['mdcr_pay_amt'],np.nan)

    #-------Create NID--------------------#
    cms = cf.nid_create(df=cms, dataset='MDCR', year='year_id')

    #Make a list of dollar columns
    dollar_cols = ['mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
                   'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
                   'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt',
                   'BLOOD_DEDUCTIBLE','COINSURANCE_AMOUNT','INPATIENT_DEDUCTIBLE',
                  'PRIMARY_PAYER_AMOUNT',
                  'pmt_1','pmt_2']
    #Function columns into smaller datatypes, keeps dol cols as float
    cms= cf.col_dtype(cms, dollar_cols)
    
    # change categorical type to int type for sex and year
    cms['sex_id'] = pd.to_numeric(cms['sex_id'], errors='coerce')
    cms['year_id'] = pd.to_numeric(cms['year_id'], errors='coerce')
    #----redist ages-----#
    cms['age_bin']=np.where(cms['age_bin']==9,8,cms['age_bin'])#combine 90+ with 85
    cms['age_bin']=np.where(cms['sex_id']==-1,0,cms['age_bin'])
    cms = cf.add_age_start(cms) 
    print('age redist')
    
    cms.rename(columns={"claim_id":"encounter_id"}, inplace=True)
    
    #Save out dimensions
    df_dim['dataset']='MDCR'
    df_dim['sub_dataset']='nf_medpar'
    df_dim['year_id']=yr
    #Save out as parquet
    df_dim.to_parquet('FILEPATH', 
                  basename_template=f"{{i}}.parquet",
                  partition_cols=['dataset','sub_dataset','year_id'],
                    existing_data_behavior='overwrite_or_ignore')
    
    #####Save to parquet, partition by year, age, sex####
    cms.drop(columns=['age_bin'], axis=1, inplace=True)
    partition_by = ["year_id",  "age_group_years_start","sex_id"]#
    #fill NA as -1 for columns for partition
    cms[partition_by]=cms[partition_by].fillna(-1)
    
    cms.to_parquet(file_path, 
                      basename_template=f"nf_medpar_stg3_{str(yr)}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')
