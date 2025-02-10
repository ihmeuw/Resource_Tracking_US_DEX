#####################################################################################
##PURPOSE: CMS Medicare Part C Stage 3 - Helper script
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
#Import packages
import pandas as pd
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

def stage_3(outpath, state, yr,sex, toc, subdataset):
    print(outpath)
    print(yr)
    print(state)
    print(sex)
    print(toc)
    print(subdataset)
    
    #--------------------------------------------------------------------
    #Step 1 - Get file paths and columns specific to each subdataset
    #--------------------------------------------------------------------
    #columns all datasets share:
    base_cols=['bene_id','age','los','year_adm','year_dchg','year_clm',
             'race_cd_raw', 'race_cd_imp','cnty','cnty_loc_id_resi','mcnty_resi','zip_5_resi',
              'st_loc_id_resi','st_num_resi','st_serv','dual_ind','code_system','sex_id','toc', 
              'ENHANCED_FIVE_PERCENT_FLAG','DUAL_STUS_CD','hmo_ind','RACE_CD','HMO_IND',
              'CLM_FROM_DT']
    
    if subdataset == 'ip_part_c':
        path = str(paths.mdcr_ip_partc_stage_2_dir)+'/year_id='+str(yr)+'/st_resi='+str(state)
        remove_cols = []
        other_cols = ['CLM_ADMSN_DT','BENE_DSCHRG_DT']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        edx_cols = ["ecode_" + str(x + 1) for x in range(10)]
    
    elif subdataset == 'hha_part_c':
        path = str(paths.mdcr_hha_partc_stage_2_dir) +'/year_id='+str(yr)+'/st_resi='+str(state)
        remove_cols = []
        other_cols = ['CLM_ADMSN_DT','BENE_DSCHRG_DT']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        edx_cols = ["ecode_" + str(x + 1) for x in range(3)]
        
    elif subdataset == 'nf_part_c':
        path = str(paths.mdcr_nf_partc_stage_2_dir)+'/year_id='+str(yr)+'/st_resi='+str(state)
        remove_cols = []
        other_cols = ['CLM_ADMSN_DT','BENE_DSCHRG_DT']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        edx_cols = ["ecode_" + str(x + 1) for x in range(10)]
        
    elif subdataset == 'carrier_part_c':
        path = str(paths.mdcr_carrier_partc_stage_2_dir)+'/year_id='+str(yr)+'/st_resi='+str(state)
        remove_cols = ['toc']
        other_cols = ['CLM_THRU_DT','toc1','HCPCS_CD']
        dx_cols = ["dx_" + str(x + 1) for x in range(13)]
        edx_cols = []
        
    elif subdataset == 'hop_part_c':
        path = str(paths.mdcr_hop_partc_stage_2_dir)+'/year_id='+str(yr)+'/st_resi='+str(state)
        remove_cols = ['toc']
        other_cols = ['CLM_THRU_DT','toc1','HCPCS_CD']
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        edx_cols = ["ecode_" + str(x + 1) for x in range(10)]
        
    if toc =='DV':
        dx_cols = []
        edx_cols = []
    
    base_cols = [item for item in base_cols if item not in remove_cols]
    cms_cols=base_cols+other_cols+dx_cols+edx_cols
    print(path)
    
    #--------------------------------------------------------------------
    #Step 2 - Read in data
    #--------------------------------------------------------------------
    
    data_schema = cf.replace_schema(path, partitioning="hive")
    if subdataset not in ['carrier_part_c','hop_part_c']:
        cms=pd.read_parquet(path,
                            use_legacy_dataset=False,
                            schema=data_schema,
                            columns = cms_cols,
                            filters=[('sex_id', '=', sex)])
    elif subdataset in ['carrier_part_c','hop_part_c']:
        cms=pd.read_parquet(path,
                            use_legacy_dataset=False,
                            schema=data_schema,
                            columns = cms_cols,
                            filters=[('sex_id', '=', sex),
                                  ('toc1', '=', toc)])
    print('data read in')
    cms['year_id']=yr
    cms['st_resi']=state
    print(cms.shape)
    
    if toc in ['OTH','UNK']:
        if cms.shape[0]==0:
            print("no data:" + str(cms.shape))
            quit()
        else:
            print("non empty df, continue")

    #--------------------------------------------------------------------
    #Step 3 - Misc formatting 
    #--------------------------------------------------------------------
    if subdataset in ['carrier_part_c','hop_part_c']:
        cms.rename(columns={'toc1':'toc'}, inplace=True)
        
    #rename create service_date and discharge_date to be used in c2e
    if subdataset in ['carrier_part_c','hop_part_c']:
        cms['service_date']=cms['CLM_FROM_DT']#want to retain this column to make a copy instead
        cms['discharge_dt']=cms['CLM_THRU_DT']#want to retain this column to make a copy instead
    elif subdataset in ['ip_part_c','hha_part_c','nf_part_c']:
        cms["service_date"]=cms["CLM_ADMSN_DT"]
        cms["discharge_dt"]=cms["BENE_DSCHRG_DT"]
    
    #--------------------------------------------------------------------
    #Step 4 - Age binning
    #--------------------------------------------------------------------
    cms = cf.age_bin(cms, age_col = 'age')
    cms.drop(columns=["age_group_years_end"], inplace=True)
    print('age bin done')
    
    #--------------------------------------------------------------------
    #Step 5 - Create claim_id
    #--------------------------------------------------------------------
    #Create claim id
    cms=cf.gen_uuid(cms)
    print('claim id generated')
    
    #--------------------------------------------------------------------
    #Step 6 - wide to long on dx (doesn't happen on RX or DV)
    #--------------------------------------------------------------------
    if toc not in ['DV','RX']:
        cms = cf.w2l(cms)#cf
        print('wide to long done')
        print('shape after wide to long'+str(cms.shape))
        #Remove rows w/ no dx or implausible los
    if toc == 'DV':
        print(toc)
        cms['primary_cause']=1
        cms = cf.create_dv_acause(df=cms, dataset='MDCR')
        print('acause added for DV')
            
    if toc== ['RX']:
        print('no wide to long or cause assignment in stage 3 for RX')
    
    #--------------------------------------------------------------------
    #Step 7 - Drop rows that are unsuable
    #--------------------------------------------------------------------
    print(cms.shape)
    cms, df_dim=cf.drop_rows(df=cms, toc=toc, year=yr)
    print('dropped missing dx rows')
    print(cms.shape)
    
    #-------Create Payer Columns--------------------#
    #Make a list of dollar columns
    dollar_cols = ['mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
                   'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
                   'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt']
    #Create mc_ind and payer columns
    cms = cf.payer_cols(dataset='MDCR', subdataset=subdataset, df= cms)
    cms['pri_payer']=1

    #------Create nid---------#
    cms = cf.nid_create(df=cms, dataset='MDCR', year='year_id')
    print('nid done')
    
    #Function columns into smaller datatypes, keeps dol cols as float
    cms= cf.col_dtype(cms, dollar_cols)
    
    #####Save to parquet, partition by year, age, sex####
    partition_by = ["year_id", "age_group_years_start", "sex_id"]
    if subdataset in ['carrier_part_c','hop_part_c']:
        partition_by = ['toc'] + partition_by 

     # change categorical type to int type for sex and year
    cms.year_id = pd.to_numeric(cms.year_id, errors='coerce')
    cms.sex_id = pd.to_numeric(cms.sex_id, errors='coerce')

    #fill NA as -1 for age_group_years_start and sex_id for partition
    cms[partition_by].fillna(-1, inplace=True)
    cms['age_group_years_start']=cms.age_group_years_start.fillna(-1)
    cms['sex_id']=cms.sex_id.fillna(-1)

    required_cols = ['nid','year_id','year_adm','year_dchg','year_clm','bene_id','claim_id','age_group_years_start','age_group_id','sex_id',
                     'race_cd_raw','race_cd_imp','mcnty_resi','cnty_loc_id_resi','zip_5_resi','st_resi','code_system','service_date','toc','los',
                     'pri_payer','tot_pay_amt','mdcr_pay_amt','mdcd_pay_amt','priv_pay_amt','oop_pay_amt','oth_pay_amt','tot_chg_amt',
                     'mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mc_ind','dual_ind',
                    'ENHANCED_FIVE_PERCENT_FLAG']
    cf.check_cols(df = cms, required_cols=required_cols)
    
    if toc not in ['DV','RX']:
        cf.check_cols(df = cms, required_cols=['dx','dx_level'])
    
    if toc != 'DV':
        #Save out as parquet
        cms.to_parquet(outpath, 
                      basename_template=f"stg3_{yr}_{sex}_{state}_{toc}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')
    elif toc =='DV':
        if 'acause' not in cms.columns:
            raise RuntimeError("Acause not created for DV") 
        if subdataset == 'carrier_part_c':
            dv_file = 'dv_carrier_c'
        elif subdataset == 'hop_part_c':
            dv_file = 'dv_hop_c'
        today = str(date.today())
        outpath = 'FILEPATH'+dv_file+'/dv_'+subdataset+ '_'+ today+'.parquet'
        cms.to_parquet(outpath, 
                      basename_template=f"stg3_{yr}_{sex}_{state}_{toc}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')
        print(outpath)
        
    #Save out dimensions
    df_dim['dataset']='MDCR'
    df_dim['sub_dataset']=subdataset
    df_dim['year_id']=yr
    df_dim['toc']=toc
    today = str(date.today())
    #Save out as parquet
    df_dim.to_parquet('FILEPATH'+today+'.parquet', 
                  basename_template=f"{state}_{sex}_{toc}-{{i}}.parquet",
                  partition_cols=['dataset','sub_dataset','year_id'],
                    existing_data_behavior='overwrite_or_ignore')
    
    print('Done')
   
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
        type=int,
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
    
    args = vars(parser.parse_args())
    stage_3(args['outpath'],args['state'],args['yr'],args['sex'],args['toc'],args['subdataset']) 