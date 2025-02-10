#####################################################################################
##PURPOSE: CMS Medicare Stage 3 - Helper script for all MDCR FFS subdatasets
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
#Import packages
import pandas as pd
import numpy as np
import math
import os
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

def stage_3(outpath, state, yr,sex, toc, subdataset, chia):
    print(subdataset)
    print(state)
    print(yr)
    print(sex)
    print(toc)
    
    today = str(date.today())
    
    if sex != 'all':
        sex=int(sex)
        
    # set stage 3 folder for chia
    stage_3_path = 'FILEPATH'
    if chia == 1:
        stage_3_path = 'FILEPATH'
    
    #--------------------------------------------------------------------
    #Step 0 - Get file paths and columns specific to each subdataset
    #--------------------------------------------------------------------
    cms_cols=['bene_id','claim_id','age','los','year_adm','year_clm','year_dchg',
             'race_cd_raw','race_cd_imp','cnty','cnty_loc_id_resi','mcnty_resi','zip_5_resi',
              'st_loc_id_resi','st_num_resi','dual_ind','tot_chg_amt','is_primary','toc',
              'code_system','sex_id','ENHANCED_FIVE_PERCENT_FLAG','pri_payer','payer_2','pmt_1','pmt_2',
              'CLM_FROM_DT','CLM_THRU_DT','CLM_PMT_AMT','DUAL_STUS_CD','RACE_CD','HMO_IND']
    e_cols = []
    
    if subdataset == 'carrier':
        if chia == 1:
            cms_yr = str(paths.chia_mdcr_carrier_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        else:
            cms_yr = str(paths.mdcr_carrier_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols = ['toc1','HCPCS_CD','CLM_BENE_PD_AMT','CARR_CLM_PRMRY_PYR_PD_AMT','payer_3','pmt_3']
        dx_cols = ["dx_" + str(x + 1) for x in range(12)]
        remove_cols =['toc']
    elif subdataset == 'hop':
        if chia == 1:
            cms_yr = str(paths.chia_mdcr_hop_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        else:
            cms_yr = str(paths.mdcr_hop_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols = ['toc1','HCPCS_CD','NCH_BENE_BLOOD_DDCTBL_LBLTY_AM','NCH_BENE_PTB_DDCTBL_AMT',
                      'NCH_BENE_PTB_COINSRNC_AMT','NCH_PRMRY_PYR_CLM_PD_AMT','NCH_PRMRY_PYR_CD']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        remove_cols =['toc']
    elif subdataset == 'ip':
        if chia == 1:
            cms_yr = str(paths.chia_mdcr_ip_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        else:
            cms_yr = str(paths.mdcr_ip_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols =  ['payer_3','pmt_3','CLM_ADMSN_DT','CLM_FAC_TYPE_CD','NCH_PRMRY_PYR_CLM_PD_AMT',
                      'NCH_PRMRY_PYR_CD','CLM_PASS_THRU_PER_DIEM_AMT','CLM_UTLZTN_DAY_CNT','NCH_IP_TOT_DDCTN_AMT',
                      'NCH_BENE_DSCHRG_DT']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        remove_cols =[]
    elif subdataset == 'hha':
        if chia == 1:
            cms_yr = str(paths.chia_mdcr_hha_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        else:
            cms_yr = str(paths.mdcr_hha_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols =  [ 'CLM_ADMSN_DT','CLM_FAC_TYPE_CD','CLM_SRVC_CLSFCTN_TYPE_CD','NCH_PRMRY_PYR_CLM_PD_AMT',
              'NCH_PRMRY_PYR_CD','CLM_HHA_TOT_VISIT_CNT'] 
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        remove_cols =[]
    elif subdataset == 'hosp':
        if chia == 1:
            cms_yr = str(paths.chia_mdcr_hosp_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        else:
            cms_yr = str(paths.mdcr_hosp_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols =  ['NCH_BENE_DSCHRG_DT','CLM_HOSPC_START_DT_ID','CLM_FAC_TYPE_CD','CLM_SRVC_CLSFCTN_TYPE_CD',
             'NCH_CLM_TYPE_CD','NCH_PRMRY_PYR_CD','NCH_PRMRY_PYR_CLM_PD_AMT']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        remove_cols =[]
    elif subdataset == 'nf':
        if chia == 1:
            cms_yr = str(paths.chia_mdcr_nf_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        else:
            cms_yr = str(paths.mdcr_nf_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols =  ['los1','CLM_ADMSN_DT','NCH_BENE_DSCHRG_DT','NCH_PRMRY_PYR_CLM_PD_AMT',
               'NCH_PRMRY_PYR_CD','NCH_IP_TOT_DDCTN_AMT']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        remove_cols =[]
    elif subdataset == 'rx':
        cms_yr = str(paths.mdcr_rx_stage_2_dir) +'/year_id=' +str(yr)+'/st_resi='+str(state)
        other_cols = ['PDE_ID', 'service_date', 'PTNT_PAY_AMT','OTHR_TROOP_AMT', 'CVRD_D_PLAN_PD_AMT',
                      'NCVRD_PLAN_PD_AMT', 'ndc', 'patient_id_part','discharge_id','ndc_qty',
                      'PTC_PLAN_TYPE_CD','BRND_GNRC_CD','payer_3','payer_4','pmt_3','pmt_4', 'mc_ind', 'days_supply']
        dx_cols = []
        e_cols = []
        remove_cols =['claim_id','service_date','zip_5_resi','code_system','CLM_FROM_DT','CLM_THRU_DT','CLM_PMT_AMT',
                     'cnty']
        if yr > 2013:
            if yr != 2018:
                other_cols = other_cols + ['PHRMCY_SRVC_TYPE_CD']
        else:
            other_cols = other_cols
        
    if toc == 'DV':
        dx_cols = []
        
    cms_cols = [item for item in cms_cols if item not in remove_cols]
    cms_cols = cms_cols +other_cols +dx_cols +e_cols
        
    #--------------------------------------------------------------------
    #Step 1 - Read in data
    #--------------------------------------------------------------------
    # ## get schema replace
    data_schema = cf.replace_schema(cms_yr, partitioning="hive")
    #set up filters
    if sex != 'all':
        filter_sex = [('sex_id', '=', sex)]
    else: 
        filter_sex = []
    
    if subdataset in ['carrier', 'hop']:
        filter_toc = [('toc1', '=', toc)]
    else:
        filter_toc = []
        
    filter1 = filter_sex + filter_toc 
    
    if len(filter1)==0: 
        cms=pd.read_parquet(cms_yr,
                            use_legacy_dataset=False,
                            schema=data_schema,
                            columns = cms_cols)
    else:
        cms=pd.read_parquet(cms_yr,
                        use_legacy_dataset=False,
                        schema=data_schema,
                        columns = cms_cols,
                       filters=filter1)
        
    print('data read in')
    cms['year_id']=yr
    cms['st_resi']=state
    
    if (toc in ['OTH','UNKNOWN'])and (cms.empty == True): 
        print('DataFrame is empty!')
        exit()
    if (sex ==-1)and (cms.empty == True): 
        print('DataFrame is empty!')
        exit()
    
    #--------------------------------------------------------------------
    # Step 2 - Misc processing
    # Note: rx is a different stage 2 script than the other subdatasets
    #--------------------------------------------------------------------
    #add any hotfixes here
    if subdataset in ['carrier','hop']:
        cms['toc']=cms['toc1']
    
    #rename admin date to match marketscan
    if subdataset in ['carrier','hop']:
        cms['service_date']=cms['CLM_FROM_DT']
        cms['discharge_date']=cms['CLM_THRU_DT']
    elif subdataset in ['ip','hha','nf']:
        cms.rename(columns={"CLM_ADMSN_DT":"service_date"}, inplace=True)
        if subdataset in ['ip','nf']:
            cms.rename(columns={"NCH_BENE_DSCHRG_DT":"discharge_date"}, inplace=True)
        if subdataset == 'hha':
            cms['discharge_date']=cms['CLM_THRU_DT']
    elif subdataset == 'hosp':
        cms.rename(columns = {'CLM_HOSPC_START_DT_ID':'service_date',
                             'NCH_BENE_DSCHRG_DT':'discharge_date'}, inplace = True)
    elif subdataset == 'rx':
        cms['discharge_date']=np.nan
        
    #RX specific prep
    if subdataset == 'rx':
        cms['patient_id_part']=cms['patient_id_part'].str.upper()
        
        #Add columns that may be needed for next step in pipeline
        cms['zip_5_resi']=np.nan
        cms['code_system']=np.nan
        cms['dx_level']=np.nan
        cms['dx']=np.nan
        cms['CLM_FROM_DT']=np.nan
        
        cms=cf.gen_uuid(cms)
        if 'PHRMCY_SRVC_TYPE_CD' in cms.columns:
            if yr > 2013:
                cms['mc_ind1']=np.where(cms['PHRMCY_SRVC_TYPE_CD']==7,1,0)
            else:
                cms['mc_ind1']=0
    print("step 2 done")
    #------------ step 3: Age Binning-------------------------------#
    cms = cf.age_bin(cms, age_col = 'age')
    cms.drop(columns=["age_group_years_end"], inplace=True)
    
    #----------------w2l conversion-----------------------------------------#
    if toc not in ['DV','RX']:
        cms = cf.w2l(cms)#cf
        print('wide to long done')
        print('shape after wide to long'+str(cms.shape))
        
    elif toc == 'DV':
        print(toc)
        cms['primary_cause']=1
        cms = cf.create_dv_acause(df=cms, dataset='MDCR')
        print('acause added for DV')
        
    elif toc== ['RX']:
        print('no wide to long or cause assignment in stage 3 for RX')
                     
    print('wide to long done')
    #Remove rows w/ no dx or implausible los
    print(cms.shape)
    cms, df_dim=cf.drop_rows(df = cms, toc = toc, year=yr)
    print('rows dropped')
    print(cms.shape)
    
    #-------Create Payer Columns--------------------#
    #Create dollar cols and mc_ind
    cms = cf.payer_cols(dataset='MDCR', subdataset=subdataset, df=cms) 

    #Create nid
    cms = cf.nid_create(df=cms, dataset='MDCR', year='year_id')
    print('nid done')
    
    #Make a list of dollar columns
    dollar_cols = ['mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
                   'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
                   'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt',
                  'pmt_1','pmt_2','CLM_PMT_AMT']
    if subdataset == 'carrier':
        dollar_cols = dollar_cols + ['CLM_BENE_PD_AMT','CARR_CLM_PRMRY_PYR_PD_AMT','pmt_3']
    elif subdataset == 'hop':
        dollar_cols = dollar_cols + ['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM','NCH_BENE_PTB_DDCTBL_AMT',
                                     'NCH_BENE_PTB_COINSRNC_AMT','NCH_PRMRY_PYR_CLM_PD_AMT']
    elif subdataset == 'ip':
        dollar_cols = dollar_cols + ['pmt_3','NCH_PRMRY_PYR_CLM_PD_AMT','CLM_PASS_THRU_PER_DIEM_AMT']
    elif subdataset in ['hha','hosp']:
        dollar_cols = dollar_cols +['NCH_PRMRY_PYR_CLM_PD_AMT']
    elif subdataset =='nf':
        dollar_cols = dollar_cols +['NCH_PRMRY_PYR_CLM_PD_AMT','NCH_IP_TOT_DDCTN_AMT']
    elif subdataset =='rx':
        dollar_cols = dollar_cols +['CVRD_D_PLAN_PD_AMT','PTNT_PAY_AMT','OTHR_TROOP_AMT',
                                    'NCVRD_PLAN_PD_AMT','pmt_3','pmt_4', 'days_supply']
        dollar_cols = [item for item in dollar_cols if item not in ['CLM_PMT_AMT']]
        
        
    #Function columns into smaller datatypes, keeps dol cols as float
    cms= cf.col_dtype(cms, dollar_cols) 
    
    #####Save to parquet, partition by year, age, sex####
    partition_by = ["year_id", "age_group_years_start", "sex_id"]

     # change categorical type to int type for sex and year
    cms.year_id = pd.to_numeric(cms.year_id, errors='coerce')
    cms.sex_id = pd.to_numeric(cms.sex_id, errors='coerce')

    #fill NA as -1 for age_group_years_start and sex_id for partition
    cms[partition_by].fillna(-1, inplace=True)
    cms['age_group_years_start']=cms.age_group_years_start.fillna(-1)
    cms['sex_id']=cms.sex_id.fillna(-1)
    
    #ensure no duplicates
    cms.drop_duplicates(inplace=True)
    
    if subdataset in ['carrier','hop']:
        partition_by = ['toc'] +partition_by
    
    
    required_cols = ['nid','year_id','year_adm','year_dchg','year_clm','bene_id','claim_id','age_group_years_start',
                     'age_group_id','sex_id','race_cd_raw','race_cd_imp','mcnty_resi','cnty_loc_id_resi','zip_5_resi','st_resi',
                     'code_system','service_date','toc','los','pri_payer','tot_pay_amt','mdcr_pay_amt','mdcd_pay_amt',
                     'priv_pay_amt','oop_pay_amt','oth_pay_amt','tot_chg_amt','mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt',
                     'oop_chg_amt','oth_chg_amt','mc_ind','dual_ind','CLM_FROM_DT','ENHANCED_FIVE_PERCENT_FLAG',
                    'service_date','discharge_date']
    if subdataset == 'rx':
        cms['service_date']=np.nan
    cf.check_cols(df = cms, required_cols=required_cols)
    
    if toc not in ['DV','RX']:
        cf.check_cols(df = cms, required_cols=['dx','dx_level'])
        
    if subdataset == 'rx':
        required_cols = ['ndc','ndc_qty',
                       'BRND_GNRC_CD', 'days_supply'] 
        # save PHRMCY_SRVC_TYPE_CD as string for causemap
        if yr > 2013:
            cms['PHRMCY_SRVC_TYPE_CD'] = cms['PHRMCY_SRVC_TYPE_CD'].astype(str)
        #Split RX data into mc and non-mc data because will be saved in different places
        cms_mc = cms[cms['mc_ind']==1].copy()
        cms = cms[cms['mc_ind']==0].copy()
        cms_mc.to_parquet(stage_3_path+'FILEPATH'+today+'.parquet', 
                      basename_template=f"mdcr_stg3_{str(state)}_{toc}_{sex}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')

    if toc != 'DV':
        ##Save out as parquet
        cms.to_parquet(outpath, 
                      basename_template=f"mdcr_stg3_{str(state)}_{toc}_{sex}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')
    else:
        if 'acause' not in cms.columns:
            raise RuntimeError("acause not created for DV")
            
        if subdataset == 'carrier':
            folder = 'dv_carrier'
        elif subdataset == 'hop':
            folder = 'dv_hop'
        elif subdataset == 'hop_part_c':
            folder = 'dv_hop_c'
        elif subdataset == 'carrier_part_c':
            folder = 'dv_carrier_c'
        #Save DV as separate dataset

        cms.to_parquet(stage_3_path+folder+'FILEPATH'+today+'.parquet', 
                      basename_template=f"mdcr_dv_stg3_{str(state)}_{toc}_{sex}_{{i}}.parquet",
                      partition_cols=["year_id", "age_group_years_start", "sex_id"], existing_data_behavior='overwrite_or_ignore')
        
    #Save out dimensions
    df_dim['dataset']='MDCR'
    df_dim['sub_dataset']=subdataset
    df_dim['year_id']=yr
    #Save out as parquet
    df_dim.to_parquet('FILEPATH'+today+'.parquet', 
                  basename_template=f"{state}_{sex}_{{i}}.parquet",
                  partition_cols=['dataset','sub_dataset','year_id'],
                    existing_data_behavior='overwrite_or_ignore')
    print('done!!')
    
#--------------------------------------------------------------------#
#-----------------Step 8: Argument parser for jobmon-----------------#
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
        "--sex",
        type=str,
        required=True
        )
    parser.add_argument(
        "--toc",
        type=str,
        required=True
        )
    parser.add_argument(
        "--subdataset",
        type=str,
        required=True
        )
    parser.add_argument(
        "--chia",
        type=int,
        required=True
        )
    
    args = vars(parser.parse_args())
    stage_3(args['outpath'],args['state'],args['yr'],args['sex'],args['toc'],args['subdataset'],args['chia']) #
    
    
    
