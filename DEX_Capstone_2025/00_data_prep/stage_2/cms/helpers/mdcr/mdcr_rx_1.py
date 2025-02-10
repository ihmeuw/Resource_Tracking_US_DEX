#####################################################################################
##PURPOSE: CMS Medicare Pharma - Part D Event
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
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.helpers import jobmon_submitter
from 00_data_prep.stage_2.cms.helpers import processing_functions as pf
from 00_data_prep.stage_2.cms.constants import paths as path
from 00_data_prep.stage_2.cms.constants import cms_dict as cd

def stage_2(outpath,yr,file):
    
    cms_cols = ['BENE_ID','PDE_ID','RX_SRVC_RFRNC_NUM','DOB_DT','GNDR_CD',
            'SRVC_DT','PD_DT','TOT_RX_CST_AMT','PTNT_PAY_AMT','OTHR_TROOP_AMT','GDC_BLW_OOPT_AMT',
            'GDC_ABV_OOPT_AMT','CVRD_D_PLAN_PD_AMT','NCVRD_PLAN_PD_AMT',
            'PROD_SRVC_ID','PRCNG_EXCPTN_CD','QTY_DSPNSD_NUM','BN'] 
    #--------------------------------------------------------------------
    #Step 1 - read in pharma data, create unique list of bene ids 
    #--------------------------------------------------------------------
    if yr >= 2013:
        cms_cols = cms_cols +['PHRMCY_SRVC_TYPE_CD']
    print(file)
    part = file.split("part",1)[1] #use this parquet parition name
    data_schema = pf.replace_schema(file, partitioning="hive")
    
    cms1=pd.read_parquet(file,
                         use_legacy_dataset=False,
                        schema=data_schema,
                         columns=cms_cols
                        ).drop_duplicates()
    print("claims data read in")
    print(cms1.shape)
    unique_bene=cms1['BENE_ID'].unique()

    #Creat year_id
    cms1['year_id']=yr

    #--------------------------------------------------------------------
    #Step 2 - read in ps (aka denominator aka mbsf) data - filtering on bene 
    #-------------------------------------------------------------------
    path_denom=str(path.path_mdcr_denom)+'FILEPATH'+str(yr)

    data_schema = pf.replace_schema(path_denom, partitioning="hive")
    data_schema = pf.change_col_schema(data_schema, {"PTC_PLAN_TYPE_CD": "string"})
    ps=pd.read_parquet(path_denom,
                       use_legacy_dataset=False,
                        schema=data_schema,
                filters= [("BENE_ID", "in", unique_bene)]) 
    ps['ENROLLMT_REF_YR']=yr
    ps['sex_id']=ps['sex_id'].astype(int)
    ps['sex_id']=ps['sex_id'].fillna(-1)
    ps['cnty']=ps['cnty'].astype(str)
    ps=ps[ps['cnty']!='12025']
    
    print('demographic data read in')
    print(ps.shape)

    #------------------------------------------------------------------#
    #--Step 3: Merge MDCR claims and PS (denominator) dataframes-------#
    #------------------------------------------------------------------#
    #Extract service begin month to merge on
    cms1['SRVC_DT']=pd.to_datetime(cms1['SRVC_DT'], errors = 'coerce')
    cms1['serv_mo']=cms1['SRVC_DT'].dt.month

    print("cms1 shape before join: " + str(cms1.shape))
    print("ps shape before join: " + str(ps.shape))
    size_cms_b4_ps=cms1.shape[0] #save the row count of original cms df
    cms1=cms1.merge(ps, left_on=['BENE_ID','year_id','serv_mo'], 
                right_on=['BENE_ID','ENROLLMT_REF_YR','MONTH'],how="left").drop_duplicates()
    cms1.drop_duplicates(inplace=True)
    cms1.drop_duplicates(subset = cms_cols, keep = 'first', inplace = True)
    size_cms_ps=cms1.shape[0]#save the row count of cms df after join
    print("cms1 shape after join to ps: " + str(cms1.shape))
    #ensure there is no row duplication
    assert size_cms_b4_ps == size_cms_ps, "cms row count changed after merge to ps file"

    ######################################################################
    #---------------------Step 4 - COLUMN FORMATTING---------------------#
    ######################################################################
    #---------------------------------------------------------------------------------------------------------------------------
    #Step 4.1: Create date related columns. Convert date columns to date type so age/los can be calculated and year's extracted-
    #---------------------------------------------------------------------------------------------------------------------------
    #Convert date columns to datetime type
    date_cols = ['SRVC_DT', 'PD_DT', 'DOB_DT','BIRTH_DT']
    cms1[date_cols]=cms1[date_cols].apply(pd.to_datetime, errors='coerce')

    #-------AGE----------------------------#
    cms1 = pf.age_format(dataset='MDCR', df = cms1)

    #-------year_ids and los----------------------------#
    #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
    cms1['year_adm']=cms1['SRVC_DT'].dt.year

    cms1['year_adm']=cms1.year_adm.fillna(yr)
    #year_id - year of data
    cms1['year_id']=yr

    #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
    cms1['year_dchg']=cms1['PD_DT'].dt.year
    #year_id_clm/CLM_THRU_DT aka claim end year
    cms1['year_clm']=cms1['PD_DT'].dt.year

    #No los for RX
    cms1['los']=np.nan

    #---------------------------------------------------------------------------------------------------------------------------
    #--------------Step 4.2: Create demographic related columns - sex, race, location info--------------------------------------
    #---------------------------------------------------------------------------------------------------------------------------
    #sex
    cms1= pf.sex_format(dataset = 'MDCR', subdataset = '', df=cms1)
    #race
    cms1= pf.race_format(dataset = 'MDCR', subdataset = '', df=cms1)

    ###-----Geographic Info-----##
    #state of residence
    cms1= pf.mdcr_state_format(subdataset = 'rx', df =cms1, resi_serv = 'resi')
    #county of residence
    cms1 = pf.cnty_format(dataset='MDCR', df=cms1)

    print("demographic formatting completed")

    #---------------------------------------------------------------------------#
    #------------Step 4.3: Create payer and payment related columns-------------#
    #---------------------------------------------------------------------------#
    #'CVRD_D_PLAN_PD_AMT'= MDCR Pay amount
    #PTNT_PAY_AMT = OOP pay amount. OTHER OOP:

    #MDCR>OTH>PRIV>MDCD>OOP
    #Unless there is Worker's Comp. then: WC>MDCR>OTH>PRIV>MDCD>OOP
    #MDCR=1, PRIV=2, MDCD=3, NC=20, 21=ukn ,4=OOP, 6= oth_not_mdcr
    #oth_not_mdcd_mdcr_oop=15, OTH-oth_not_mdcd_mdcr_oop_priv=19
    #12 = oth_not_mdcr_oop
    #-------------------------------------PAYERS---------------------------------------------------#
    cms1.rename(columns = {'TOT_RX_CST_AMT':'tot_chg_amt'}, inplace = True)

    #payer_1
    pri_conditions = [cms1['CVRD_D_PLAN_PD_AMT']>0,
                      cms1['OTHR_TROOP_AMT']>0,
                      cms1['PTNT_PAY_AMT']>0,
                      cms1['GDC_BLW_OOPT_AMT']>0,
                      cms1['tot_chg_amt']==0]

    pri_outputs=[1,12,4,6,20]
    cms1['pri_payer']=np.select(pri_conditions, pri_outputs, -1)
    #If pri_payer =oop aka 4, set to MDCR
    cms1['pri_payer']= np.where(cms1['pri_payer']==4,1,cms1['pri_payer'])

    #payer_2
    pay2_conditions = [((cms1['pri_payer']==1) & (cms1['OTHR_TROOP_AMT']>0)),
                       ((cms1['pri_payer']==1) & (cms1['PTNT_PAY_AMT']>0)),
                       ((cms1['pri_payer']==1) & (cms1['GDC_BLW_OOPT_AMT']>0)),
                       (cms1['pri_payer']==12) & (cms1['PTNT_PAY_AMT']>0),
                       (cms1['pri_payer']==12) & (cms1['GDC_BLW_OOPT_AMT']>0),
                       (cms1['pri_payer']==4) & (cms1['GDC_BLW_OOPT_AMT']>0),
                       cms1['tot_chg_amt']==0]
    pay2_outputs=[12,4,6,4,6,6,20]
    cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, -1).astype('int8')

    #payer_3
    pay3_conditions = [((cms1['payer_2']==12) & (cms1['PTNT_PAY_AMT']>0)),
                       ((cms1['payer_2']==4) & (cms1['GDC_BLW_OOPT_AMT']>0)),
                       cms1['tot_chg_amt']==0]
    pay3_outputs=[4,6,20]
    cms1['payer_3']=np.select(pay3_conditions, pay3_outputs, -1).astype('int8')

    #payer_4
    pay4_conditions = [((cms1['payer_3']==4) & (cms1['GDC_BLW_OOPT_AMT']>0)),
                       cms1['tot_chg_amt']==0]
    pay4_outputs=[6,20]
    cms1['payer_4']=np.select(pay4_conditions, pay4_outputs, -1).astype('int8')

    #------------------------------------PAYMENTS--------------------------------------------------#
    #Medicare paid amount = To obtain the total amount paid by Medicare for the claim, the pass-through amount (which is the daily per diem amount) must be multiplied by the number of Medicare-covered days (i.e., multiply the CLM_PASS_THRU_PER_DIEM_AMT by the CLM_UTLZTN_DAY_CNT), and then added to the claim payment amount (this field).
    #PMT_1 - these become floats
    pmt1_conditions = [cms1['pri_payer']==1,
                       cms1['pri_payer']==4,
                       cms1['pri_payer']==6,
                       cms1['pri_payer']==12]
    pmt1_outputs=[cms1['CVRD_D_PLAN_PD_AMT'],cms1['PTNT_PAY_AMT'], cms1['GDC_BLW_OOPT_AMT'], cms1['OTHR_TROOP_AMT']]
    cms1['pmt_1']=np.select(pmt1_conditions, pmt1_outputs, np.nan)

    #pmt_2
    pmt2_conditions = [cms1['payer_2']==4,
                       cms1['payer_2']==6,
                       cms1['payer_2']==12]
    pmt2_outputs=[cms1['PTNT_PAY_AMT'], cms1['GDC_BLW_OOPT_AMT'], cms1['OTHR_TROOP_AMT']]
    cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)

    #pmt_3
    pmt3_conditions = [cms1['payer_2']==4,
                       cms1['payer_2']==6]
    pmt3_outputs=[cms1['PTNT_PAY_AMT'], cms1['GDC_BLW_OOPT_AMT']]
    cms1['pmt_3']=np.select(pmt3_conditions, pmt3_outputs, np.nan)


    cms1['pmt_4']=np.where(cms1['payer_4']==6, cms1['GDC_BLW_OOPT_AMT'], np.nan)
    cms1['is_primary']=np.where(cms1['pri_payer']==1,1,0)

    #Indicators for dual Medicare/Medicaid enrollment
    cms1 = pf.dual_format(dataset='MDCR', df=cms1)

    #--------------------------------------------------------------------------#
    #-------------Step 4.4: Create other columns TOC, Facility etc--------------#
    #---------------------------------------------------------------------------#
    #Type of care
    cms1['toc']="RX" #Hardcoded to IP for dataset
    cms1['discharge_id'] = 'Other'
    cms1['patient_id_part'] = cms1['PDE_ID'].str[-2:].astype(str)
    
    #Some data in MDCR RX is MC and some is FFS, use this column to tell: https://resdac.org/cms-data/variables/part-c-plan-type-code-january
    cms1.loc[cms1["PTC_PLAN_TYPE_CD"] =='', "PTC_PLAN_TYPE_CD"] = np.nan
    cms1['mc_ind'] = np.where(cms1["PTC_PLAN_TYPE_CD"].isnull(), 0, 1)
    
    #---------------------------------------------------------------------------#
    #--------Step 4.5: Rename columns that don't need other processing----------#
    #---------------------------------------------------------------------------#
    cms1.rename(columns = {'BENE_ID':'bene_id',
                        'PROD_SRVC_ID': 'ndc',
                           'QTY_DSPNSD_NUM':'ndc_qty',
                           'BN':'BRND_GNRC_CD',
                           "SRVC_DT":"service_date"}, inplace = True)

    #------------------------------------------------------------------------------------#
    #--------Step 4.6: Change datatype of columns to mimize df size for storage----------#
    #------------------------------------------------------------------------------------#
    #Convert from string to int
    cms1['ndc']=cms1['ndc'].astype(int)

    # apply days_supply map 
    cms1 = pf.apply_days_supply(cms1)
    if 'days_supply' not in cms1.columns:
        raise ValueError("That didn't work")
    cms1.loc[cms1['tot_chg_amt'] == "SIMVASTATIN", 'tot_chg_amt'] = np.nan
    cms1['tot_chg_amt']=cms1['tot_chg_amt'].astype(float)#Coming through as a string

    cms1=pf.datatype_format(dataset='MDCD', subdataset = 'rx', df = cms1)
    
    cms1.astype({
        'sex_id':"int8",
        'age':"int16",
    }).to_parquet(outpath,,
                  partition_cols=['year_id','st_resi'],
                  basename_template=f"format_{str(yr)}_{str(part)}_{{i}}.parquet",
                  existing_data_behavior='overwrite_or_ignore')
    print('data saved here:'+ str(path.path_mdcr_stg2) + '/RX/mdcr_rx_stg2_'+str(yr))

#--------------------------------------------------------------------#
#-----------------Step 5: Argument parser for jobmon-----------------#
#--------------------------------------------------------------------#
if __name__ == "__main__":
    # Argument parser, must specify dataset to run
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
        "--file",
        type=str,
        required=True
        ) 
    args = vars(parser.parse_args())
    stage_2(args['outpath'],args['yr'],args['file'])


