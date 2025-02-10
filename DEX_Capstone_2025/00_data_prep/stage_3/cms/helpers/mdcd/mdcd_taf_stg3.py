#####################################################################################
##PURPOSE: CMS Medicaid TAF Helper script
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
#Import packages
import pandas as pd
import numpy as np
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

def stage_3(outpath, state, yr, sex, toc, subdataset, partition):
    print(outpath)
    print(subdataset)
    print(yr)
    print(state)
    print(sex)
    print(toc)
    print(partition)
    #-----------------------------------------------------#
    #-------------Read in CMS DATA FORMATTING-------------#
    #-----------------------------------------------------#
    #Get columns
    cms_cols=['bene_id','MSIS_ID', 'claim_id','year_adm','year_clm','year_dchg',
               'SRVC_BGN_DT','SRVC_END_DT','los','age','sex_id','race_cd_raw', 'race_cd_imp', 
              'mcnty_resi', 'zip_5_resi','DUAL_ELGBL_CD', 
              'toc', 'BILLED_AMT','MDCD_PD_AMT','OTHR_INSRNC_PD_AMT',
              'TP_PD_AMT','COINSRNC_AMT','COPAY_AMT','DDCTBL_AMT',
              'MDCR_DDCTBL_PD_AMT','MDCR_COINSRNC_PD_AMT',
              'pri_payer','payer_2','payer_3','pmt_1','pmt_2','pmt_3',
              'mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
               'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
               'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt',
              'mc_ind','code_system', 'mc_status','dual_ind',
             'BENE_STATE_CD','SUBMTG_STATE_CD'] 

    if subdataset == 'ip_taf':
        cms_yr=str(paths.mdcd_ip_taf_stage_2_dir)
        d_code_cols = ["dx_" + str(x + 1) for x in range(11)]
        cms_cols = cms_cols + d_code_cols +  ['MDCR_PD_AMT','ADMSN_DT','DSCHRG_DT','MDCD_COPAY_AMT'] #
        
    elif subdataset =='ltc_taf':
        cms_yr = str(paths.mdcd_nf_taf_stage_2_dir)
        d_code_cols = ["dx_" + str(x + 1) for x in range(5)]
        cms_cols = cms_cols + d_code_cols + ['MDCR_PD_AMT','ADMSN_DT','DSCHRG_DT']
    elif subdataset=='rx_taf':
        cms_yr = str(paths.mdcd_rx_taf_stage_2_dir)
        other_cols = ['PRSCRBD_DT','RX_FILL_DT', 'NDC_line','BRND_GNRC_CD_line','DOSAGE_FORM_CD_line','NDC_QTY_line','days_supply']
        remove_cols =['SRVC_BGN_DT','SRVC_END_DT','MDCD_PD_DT'] 
        cms_cols = [item for item in cms_cols if item not in remove_cols]
        cms_cols = cms_cols + other_cols
    elif subdataset == 'ot_taf':
        cms_yr = str(paths.mdcd_ot_taf_stage_2_dir) 
        other_cols = ['MDCD_COPAY_AMT']
        if toc == 'DV':
                     other_cols = other_cols + ['acause']
                     d_code_cols= []
        elif toc != 'DV':
            d_code_cols = ["dx_" + str(x + 1) for x in range(2)]

        cms_cols = cms_cols + other_cols + d_code_cols
    
    
    #Read in cms after data/column formatting
    print(cms_yr)
    data_schema = cf.replace_schema(cms_yr, partitioning="hive")
    if subdataset in ['rx_taf','ot_taf']: #these two need extra partitioning bc so large
        cms=pd.read_parquet(cms_yr+'/year_id='+str(yr)+'/st_resi='+state,
                                use_legacy_dataset=False,
                                schema=data_schema,
                             columns = cms_cols,
                            filters = [("sex_id","=",sex),
                                          ("toc","=",toc),
                                      ("partition","=",partition)]).drop_duplicates()
    else: 
        cms=pd.read_parquet(cms_yr+'/year_id='+str(yr)+'/st_resi='+state,
                                use_legacy_dataset=False,
                                schema=data_schema,
                             columns = cms_cols,
                            filters = [("sex_id","=",sex),
                                          ("toc","=",toc)]).drop_duplicates()
    print('data read in')
    print(cms.shape)
    #Year_id coming in as category
    cms['year_id']=yr
    cms['st_resi']=state
    
    if toc in ['OTH','UNK']:
        if cms.shape[0]==0:
            print("no data:" + str(cms.shape))
            quit()
        else:
            print("non empty df, continue")
                     
    #columns for c2e
    cms['adm_flag']=np.where(cms['year_id']==cms['year_adm'],1,0)
    
    if subdataset !='rx_taf':
        cms['CLM_FROM_DT']=cms['SRVC_BGN_DT'] #Naming to match mdcr

    #rename admin date to match marketscan
    if subdataset in ['ip_taf','ltc_taf']:
        cms['ADMSN_DT1']=cms['ADMSN_DT'].fillna(cms['SRVC_BGN_DT'])
        cms['DSCHRG_DT1']=cms['DSCHRG_DT'].fillna(cms['SRVC_END_DT'])
        cms.rename(columns={'ADMSN_DT1':"service_date",
                           'DSCHRG_DT1':'discharge_date'}, inplace=True)
    elif subdataset == 'ot_taf':
        cms.rename(columns={"SRVC_BGN_DT":"service_date",
                           "SRVC_END_DT":"discharge_date"}, inplace=True)
    elif subdataset == 'rx_taf':
        cms.rename(columns={"RX_FILL_DT":"service_date",
                       "NDC_line": "ndc",
                        'NDC_QTY_line':'ndc_qty',
                       'BRND_GNRC_CD_line':'BRND_GNRC_CD',
                       'DUAL_ELGBL_CD_ps':'DUAL_ELGBL_CD'}, inplace=True)
        #Adjust pri_payer
        cms['pri_payer']=np.where(((cms['pri_payer']!=2)& 
                              ((cms['MDCR_DDCTBL_PD_AMT']>0) |(cms['MDCR_COINSRNC_PD_AMT']>0))), 1, cms['pri_payer'])

    if subdataset == 'rx_taf':
        cms['fac_prof_ind']='U'
    cms['fac_prof_ind'].fillna('U',inplace=True)

    #------------Age Binning-------------------------------#
    cms = cf.age_bin(cms, age_col = 'age')

    cms.drop(columns=["age_group_years_end",
                      ], inplace=True)
    print('age bin done')
                     
    #----------------w2l conversion-----------------------------------------#
    if toc not in ['DV','RX']:
        cms = cf.w2l(cms)#cf
        print('wide to long done')
        print('shape after wide to long'+str(cms.shape))
        #Remove rows w/ no dx or implausible los
    elif toc == ['DV']:
        print(toc)
        cms['primary_cause']=1
        
        #Assign acause for dental
        cms.rename(columns = {'acause':'acause_1'}, inplace = True)
        #Read in dental procedure code to acause map
        dv_map=pd.read_csv('FILEPATH',
                          usecols = ['code','acause'])
        #Merge map to data
        size_cms_b4_dv=cms.shape[0]
        cms=cms.merge(dv_map, left_on=['LINE_PRCDR_CD_line'], 
                           right_on=['code'],how="left")
        size_cms_dv=cms.shape[0]
        assert size_cms_b4_dv == size_cms_dv, "cms row count changed after merge to dv file"
        #Where there is no procedure code fill in w/ acause_1 determined from stage 2
        cms['acause'].fillna(cms['acause_1'],inplace=True)
        cms.drop(columns=["acause_1"], inplace=True)
    elif toc== ['RX']:
        print('no wide to long or cause assignment in stage 3 for RX')
                     
    print('wide to long done')
    #Remove rows w/ no dx or implausible los
    print(cms.shape)
    cms, df_dim=cf.drop_rows(df = cms, toc = toc, year=yr)
    print('rows dropped')
    print(cms.shape)

    cms = cf.nid_create(df = cms, dataset = 'MDCD_TAF')
    print('nid done')

    #Make a list of dollar columns
    dollar_cols=['BILLED_AMT','MDCD_PD_AMT','MDCR_PD_AMT','OTHR_INSRNC_PD_AMT',
                  'TP_PD_AMT','COINSRNC_AMT','COPAY_AMT','DDCTBL_AMT',
                  'MDCR_DDCTBL_PD_AMT','MDCR_COINSRNC_PD_AMT',
                  'pmt_1','pmt_2','pmt_3',
                  'mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
                   'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
                   'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt']
    if subdataset not in ['ltc_taf','ip_taf']:
        remove_cols =['MDCR_PD_AMT']
        dollar_cols = [item for item in dollar_cols if item not in remove_cols]
    #Function columns into smaller datatypes, keeps dol cols as float
    cms= cf.col_dtype(cms, dollar_cols)

    #####Save to parquet, partition by year, age, sex####
    partition_by = ["year_id", "age_group_years_start", "sex_id"]
                    
    #fill NA as -1 for columns for partition
    cms[partition_by]=cms[partition_by].fillna(-1)
    if subdataset == 'ot_taf':
        partition_by = ['toc'] +partition_by
    
    
    #Make sure expected columns exist
    #run time error will be raised if column missing
    required_cols = ['nid','year_id','year_adm','year_dchg','year_clm','bene_id','claim_id','age_group_years_start','age_group_id','sex_id',
                     'race_cd_raw','race_cd_imp','mcnty_resi','zip_5_resi','st_resi','code_system','service_date','toc','los',
                     'pri_payer','tot_pay_amt','mdcr_pay_amt','mdcd_pay_amt','priv_pay_amt','oop_pay_amt','oth_pay_amt','tot_chg_amt',
                     'mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mc_ind','dual_ind','fac_prof_ind']
    cf.check_cols(df = cms, required_cols=required_cols)
    
    if subdataset == 'rx_taf':
        required_cols = ['ndc','days_supply','ndc_qty',
                       'BRND_GNRC_CD']
        cf.check_cols(df = cms, required_cols=required_cols)
    
    if toc not in ['RX','DV']:
        cf.check_cols(df = cms, required_cols=['dx','dx_level','CLM_FROM_DT'])

    if toc != 'DV':
        ##Save out as parquet
        cms.to_parquet(outpath, 
                      basename_template=f"taf_stg3_{str(state)}_{toc}_{partition}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')
    else:
        #Save DV as separate dataset
        ##Save out as parquet
        today = str(date.today())
        cms.to_parquet('FILEPATH'+today+'.parquet', 
                      basename_template=f"taf_mdcd_dv_stg3_{str(state)}_{toc}_{partition}_{{i}}.parquet",
                      partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')
        
    #Save out dimensions
    df_dim['dataset']='MDCD'
    df_dim['sub_dataset']=subdataset
    df_dim['year_id']=yr
    today = str(date.today())
    #Save out as parquet
    df_dim.to_parquet('FILEPATH'+today+'.parquet', 
                  basename_template=f"{state}_{{i}}.parquet",
                  partition_cols=['dataset','sub_dataset','year_id'],
                    existing_data_behavior='overwrite_or_ignore')
    
#--------------------------------------------------------------------#
#----------------- Argument parser for jobmon-----------------#
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
    parser.add_argument(
        "--partition",
        type=int,
        required=True
        )
    
    args = vars(parser.parse_args())
    stage_3(args['outpath'],args['state'],args['yr'],args['sex'],args['toc'],args['subdataset'],args['partition']) 
