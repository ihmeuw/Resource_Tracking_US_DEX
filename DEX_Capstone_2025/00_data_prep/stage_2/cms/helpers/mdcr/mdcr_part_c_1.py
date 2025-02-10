#####################################################################################
##PURPOSE: CMS Medicare Part C Stage 2 - Data Processing - This is script 1
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################

#import packages
import pandas as pd
import numpy as np
import psutil 
import gc
import argparse
# adding this to make importing these easier if using git worktree
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.helpers import processing_functions as pf
from 00_data_prep.stage_2.cms.constants import paths as path
from 00_data_prep.stage_2.cms.constants import cms_dict as cd


def stage_2(outpath, state, yr, subdataset, test):
    print(outpath)
    print(yr)
    print(state)
    print(subdataset)
    print(test)
    #--------------------------------------------------------------------
    #Step 1 - Get file paths and columns specific to each subdataset
    #--------------------------------------------------------------------
    #columns all datasets share:
    base_cols= ['BENE_ID','CLM_FROM_DT','CLM_THRU_DT','SRVC_MONTH',
            'CLM_CNTL_NUM','CLM_ORIG_CNTL_NUM','CLM_BPRVDR_USPS_STATE_CD',
           'CLM_BPRVDR_ADR_ZIP_CD','CLM_SUBSCR_USPS_STATE_CD','CLM_SUBSCR_ADR_ZIP_CD','BENE_CNTY_CD',
           'BENE_STATE_CD','BENE_MLG_CNTCT_ZIP_CD','GNDR_CD','BENE_RACE_CD','DOB_DT','BENE_STATE']
    
    if subdataset == 'ip_part_c':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) +'/ip' + str(path.path_mdcr_part_c) + str(yr) + ".parquet" #path
        other_cols = ['CLM_DAY_CNT','CLM_ADMSN_DT','BENE_DSCHRG_DT']
        dx_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        edx_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(10)]
        
    elif subdataset == 'hha_part_c':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) +'/hha' + str(path.path_mdcr_part_c) + str(yr) + ".parquet" #path
        other_cols = ['CLM_ADMSN_DT','BENE_DSCHRG_DT']
        dx_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        edx_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(3)]
        
    elif subdataset == 'nf_part_c':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) +'/snf' + str(path.path_mdcr_part_c) + str(yr) + ".parquet"
        other_cols = ['CLM_DAY_CNT','CLM_ADMSN_DT','BENE_DSCHRG_DT']
        dx_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        edx_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(10)]
    
    elif subdataset == 'carrier_part_c':
        cms_yr = str(path.path_mdcr) + '/' +str(yr)  + str(path.path_mdcr_carrier_part_c) + str(yr) + ".parquet"
        other_cols = ['ENC_JOIN_KEY','CLM_PLACE_OF_SRVC_CD']
        dx_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(13)]
        edx_cols = []
    
    elif subdataset == 'hop_part_c':
        cms_yr = str(path.path_mdcr) + '/' +str(yr) + str(path.path_mdcr_hop_part_c) + str(yr) + ".parquet"
        other_cols = ['ENC_JOIN_KEY',]
        dx_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        edx_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(10)]
        if state != -1:
            if (subdataset == 'hop_part_c') & (yr == 2019):
                if len(str(state)) != 2: 
                    state = str(state).zfill(2)
                else:
                    state = str(state)
            else:
                state = state
        
    cms_cols=base_cols+other_cols+dx_cols+edx_cols
    print(cms_yr)
    
    #--------------------------------------------------------------------
    #Step 2 - read in claims data
    #--------------------------------------------------------------------
    if subdataset in ['ip_part_c','hha_part_c','nf_part_c']: #don't need to parallelize over state
        cms1=pd.read_parquet(cms_yr,columns=cms_cols).drop_duplicates()
    elif subdataset in ['carrier_part_c','hop_part_c']: #don't need to parallelize over state
        if state != -1:
            cms1=pd.read_parquet(cms_yr,columns=cms_cols,
                            filters = [("BENE_STATE_CD", "=", state)]).drop_duplicates()
        elif state == -1:
            if (subdataset == 'hop_part_c') & (yr == 2019):
                cms1=pd.read_parquet(cms_yr,columns=cms_cols,
                                filters = [("BENE_STATE_CD", "not in", ['1','2','3','4','5','6','7','8','9','10',
                                                                        '01','02','03','04','05','06','07','08','09',
                                                                     '11','12','13','14','15','16','17','18','19','20',
                                                                     '21','22','23','24','25','26','27','28','29','30',
                                                                     '31','32','33','34','35','36','37','38','39','40',
                                                                     '41','42','43','44','45','46','47','48','49','50',
                                                                     '51','52','53','54','55','56','57','58','59','60',
                                                                     '61','62','63','64','65','97','98','99'])]).drop_duplicates()
            else:
                cms1=pd.read_parquet(cms_yr,columns=cms_cols,
                                filters = [("BENE_STATE_CD", "not in", [1,2,3,4,5,6,7,8,9,10,
                                                                         11,12,13,14,15,16,17,18,19,20,
                                                                         21,22,23,24,25,26,27,28,29,30,
                                                                         31,32,33,34,35,36,37,38,39,40,
                                                                         41,42,43,44,45,46,47,48,49,50,
                                                                         51,52,53,54,55,56,57,58,59,60,
                                                                         61,62,63,64,65,97,98,99])]).drop_duplicates()
        
        
    print(cms1.shape)
    print("claims data read in")
    
    if (cms1.empty == True) and (state in [47,48,49,50,
             51,52,53,54,55,56,57,58,59,60,
             61,62,63,64,65,97,98,99,-1]): #these are either not-us states or are the histroical code for them
        print('DataFrame is empty!')
        exit()
    elif (cms1.empty == True) and (state in ['47','48','49','50',
         '51','52','53','54','55','56','57','58','59','60',
         '61','62','63','64','65','97','98','99','-1']): #these are either not-us states or are the histroical code for them
        print('DataFrame is empty!')
        exit()
    else:
        print('dataframe not empty!')
    
    if test==1:
        cms1=cms1.head(500)
    
    #year_id is year of data
    cms1['year_id']=yr
    unique_bene =cms1['BENE_ID'].to_list()
    
    #change empty strings to nans - first create a list of columns that are strings
    string_columns = [column for column in cms1.columns if cms1[column].dtype == 'object']
    cms1[string_columns] = cms1[string_columns].apply(lambda x: np.where(x == '', np.nan, x))
    #There are some empty string values that need to be changed to null
    cms1=cms1[cms1['BENE_RACE_CD']!='BENE_RACE_CD']
    cms1[['BENE_STATE_CD', 'CLM_SUBSCR_USPS_STATE_CD', 'BENE_STATE', 'BENE_CNTY_CD']] = cms1[['BENE_STATE_CD', 'CLM_SUBSCR_USPS_STATE_CD', 'BENE_STATE', 'BENE_CNTY_CD']].apply(lambda x: np.where(x == '', np.nan, x))
    cms1['BENE_RACE_CD']=cms1['BENE_RACE_CD'].astype(float)
    cms1['BENE_STATE_CD']=cms1['BENE_STATE_CD'].astype(float)
    cms1['BENE_CNTY_CD']=cms1['BENE_CNTY_CD'].astype(float)
    if 'CLM_DAY_CNT' in cms1.columns:
        cms1['CLM_DAY_CNT']=np.where(cms1['CLM_DAY_CNT']=='',np.nan, cms1['CLM_DAY_CNT'])
        cms1['CLM_DAY_CNT']=cms1['CLM_DAY_CNT'].astype(float)
        
    if subdataset in ['ip_part_c','nf_part_c','hha_part_c']:
        chia=0
        cms1=pf.fix_admin_date(dataset='MDCR', subdataset=subdataset, df=cms1, chia=chia)

    #--------------------------------------------------------------------
    #Step 3 - read in ps (aka denominator aka mbsf) data - filtering on bene 
    #--------------------------------------------------------------------
    ps_path = str(path.path_mdcr_denom)+'/ENROLLMT_REF_YR='+str(yr)
    data_schema = pf.replace_schema(ps_path, partitioning="hive")
    data_schema = pf.change_col_schema(data_schema, {"PTC_PLAN_TYPE_CD": "string"})
    ps=pd.read_parquet(ps_path,
                       use_legacy_dataset=False,
                        schema=data_schema,
                       filters= [("BENE_ID", "in", unique_bene)])
    ps['ENROLLMT_REF_YR'] = yr
    ps['sex_id']=ps['sex_id'].astype(int) #convert to int bc comes in as category
    ps['sex_id']=ps['sex_id'].fillna(-1).astype(int)
    print('demographic data read in')
    print(ps.shape)

    #------------------------------------------------------------------#
    #--Step 4: Merge MDCR claims and PS (denominator) dataframes-------#
    #------------------------------------------------------------------#
    if subdataset in ['ip_part_c','hha_part_c', 'nf_part_c']:
        cms1['CLM_ADMSN_DT1']=np.where(cms1['CLM_ADMSN_DT']=='',cms1['CLM_FROM_DT'],cms1['CLM_ADMSN_DT'])
        #Extract service begin month to merge on
        cms1['CLM_ADMSN_DT1']=pd.to_datetime(cms1['CLM_ADMSN_DT1'], errors = 'coerce')
        cms1['clm_from_mo']=cms1['CLM_ADMSN_DT1'].dt.month
    else:
        #Extract service begin month to merge on
        cms1['CLM_FROM_DT']=pd.to_datetime(cms1['CLM_FROM_DT'], errors = 'coerce')
        cms1['clm_from_mo']=cms1['CLM_FROM_DT'].dt.month
    #One is int the other is string
    if pd.api.types.is_string_dtype(cms1['BENE_STATE_CD'].dtype):
        cms1['BENE_STATE_CD']=np.where(cms1['BENE_STATE_CD']=='','-1',cms1['BENE_STATE_CD'])
        cms1['BENE_STATE_CD']=cms1['BENE_STATE_CD'].fillna('-1').astype(int)
    else:
        cms1['BENE_STATE_CD']=cms1['BENE_STATE_CD'].fillna(-1).astype(int)
        
    #Merge takes about 5min to run
    print("cms1 shape before join: " + str(cms1.shape))
    size_cms_b4_ps=cms1.shape[0]
    print("ps shape before join: " + str(ps.shape))
    cms1=cms1.merge(ps, left_on=['BENE_ID','year_id','clm_from_mo'], 
                   right_on=['BENE_ID','ENROLLMT_REF_YR','MONTH'],how="left")
    cms_shape2=cms1.shape
    print("cms1 shape after join to ps: " + str(cms1.shape))
    cms1.sort_values(by='cnty', inplace=True)
    cms1.drop_duplicates(subset = cms_cols, keep = 'first', inplace = True) 
    size_cms_ps=cms1.shape[0]
    print("cms1 shape after join to ps: " + str(cms1.shape))
    #ensure there is no row duplication
    assert size_cms_b4_ps == size_cms_ps, "cms row count changed after merge to ps file"

    ######################################################################
    #---------------------Step 6 - COLUMN FORMATTING---------------------#
    ######################################################################
    #---------------------------------------------------------------------------------------------------------------------------#
    #Step 6.1: Create date related columns. Convert date columns to date type so age/los can be calculated and year's extracted-#
    #---------------------------------------------------------------------------------------------------------------------------#
    # Convert date columns to datetime type
    if subdataset in ['ip_part_c','hha_part_c','nf_part_c']:
        date_cols = ['DOB_DT','CLM_FROM_DT','CLM_THRU_DT','CLM_ADMSN_DT','BIRTH_DT','BENE_DSCHRG_DT']
        cms1['BENE_DSCHRG_DT1']=cms1.BENE_DSCHRG_DT.fillna(cms1.CLM_THRU_DT)
    else:
        date_cols = ['DOB_DT','CLM_FROM_DT','CLM_THRU_DT','BIRTH_DT']
    cms1[date_cols]=cms1[date_cols].apply(pd.to_datetime, errors='coerce')
    
    # sometimes years are impalusible mixed up ie 2109 when it should be 2019, this function fixes that
    cms1 = pf.switch_year_digits(df=cms1, date_column = 'CLM_THRU_DT')
    cms1 = pf.switch_year_digits(df=cms1, date_column = 'CLM_FROM_DT')
    if "CLM_ADMSN_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'CLM_ADMSN_DT')
    if "BENE_DSCHRG_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'BENE_DSCHRG_DT')
        
    #rename create service_date and discharge_date to be used in c2e
    if subdataset in ['carrier_part_c','hop_part_c']:
        cms1['service_date']=cms1['CLM_FROM_DT']#want to retain this column to make a copy instead
        cms1['discharge_dt']=cms1['CLM_THRU_DT']#want to retain this column to make a copy instead
    elif subdataset in ['ip_part_c','hha_part_c','nf_part_c']:
        cms1["service_date"]=cms1["CLM_ADMSN_DT"]
        cms1["discharge_dt"]=cms1["BENE_DSCHRG_DT"]

    #-------AGE----------------------------#
    cms1=pf.age_format(dataset='MDCR',df=cms1)#pf

    #LOS #Division removes word days
    if subdataset in ['ip_part_c','hha_part_c','nf_part_c']:
        cms1['los']=(cms1['BENE_DSCHRG_DT']-cms1['CLM_ADMSN_DT'])/ np.timedelta64(1, 'D') 
    else:
        cms1['los']=(cms1['CLM_THRU_DT']-cms1['CLM_FROM_DT'])/ np.timedelta64(1, 'D')

    if subdataset in ['ip_part_c','hha_part_c','nf_part_c']:
        #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
        cms1['year_adm']=cms1['CLM_ADMSN_DT'].dt.year 
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
        cms1['year_dchg']=cms1['BENE_DSCHRG_DT'].dt.year
        #year_id_clm/CLM_THRU_DT aka claim end year
        cms1['year_clm']=cms1['CLM_THRU_DT'].dt.year
    else:
        #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
        cms1['year_adm']=cms1['CLM_FROM_DT'].dt.year 
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
        cms1['year_dchg']=cms1['CLM_THRU_DT'].dt.year
        #year_id_clm/CLM_THRU_DT aka claim end year
        cms1['year_clm']=cms1['CLM_THRU_DT'].dt.year
    print("date formatting completed")

    #---------------------------------------------------------------------------------------------------------------------------
    #--------------Step 6.2: Create demographic related columns - sex, race, location info--------------------------------------
    #---------------------------------------------------------------------------------------------------------------------------
    # Sex: #Sex - if missing from personal summary, take from claim
    #CMS sex: 0=unk, 1=male, 2=female
    cms1=pf.sex_format(dataset='MDCR', subdataset='', df =cms1)

    # Race
    cms1=pf.race_format(dataset='MDCR', subdataset=subdataset, df =cms1)

    ###-----Geographic Info-----##
    #state of residence
    cms1= pf.mdcr_state_format(subdataset='',df=cms1, resi_serv = 'resi')
    
    if 'CLM_BPRVDR_USPS_STATE_CD' in cms1.columns:
        cms1['st_serv']=cms1['CLM_BPRVDR_USPS_STATE_CD']
    #county of residence
    cms1 = pf.cnty_format(dataset='MDCR', df=cms1)
    #zip codes
    chia = 0
    cms1=pf.zip_format(dataset='MDCR', subdataset=subdataset, df=cms1, chia = chia)

    print("demographic formatting completed")

    # --------------------------------------------------------------------------#
    #-------------Step 6.5: Create other columns TOC, Facility etc--------------#
    #---------------------------------------------------------------------------#
    #Tocs that have DV will have toc and toc1 (toc1 is the column that will contain DV, 
    cms1=pf.toc_format(subdataset=subdataset, df=cms1, yr=yr, chia=chia)
    
    if subdataset in ['hop_part_c','carrier_part_c']:
        cms1= pf.dv_line(subdataset=subdataset, df_cms=cms1, unique_bene= unique_bene, yr =yr, chia=chia)
     
    if subdataset in ['ip_part_c','hha_part_c','nf_part_c']:
        cms1['code_system']=np.where(cms1['BENE_DSCHRG_DT']<pd.to_datetime('2015-10-01'),'icd9','icd10')
        if yr != 2015:
            cms1['code_system']=np.where(yr<2015,'icd9','icd10')
    else:
        cms1['code_system']=np.where(cms1['CLM_THRU_DT']<pd.to_datetime('2015-10-01'),'icd9','icd10')
        if yr != 2015:
            cms1['code_system']=np.where(yr<2015,'icd9','icd10')
        
    #Create dual_indicator
    cms1= pf.dual_format(dataset='MDCR', df=cms1)
    
    if subdataset == 'carrier_part_c':
        cms1['ENHANCED_FIVE_PERCENT_FLAG']='Y' #Some are null in the raw data, but should all be Y
    
    print("Misc formatting completed")

    #---------------------------------------------------------------------------#
    #--------Step 6.6: Rename columns that don't need other processing----------#
    #---------------------------------------------------------------------------#
    cms1.rename(columns = {'BENE_ID':'bene_id'}, inplace=True)
    cms1.columns = cms1.columns.str.replace('ICD_DGNS_CD' , 'dx_')
    cms1.columns = cms1.columns.str.replace('ICD_DGNS_E_CD' , 'ecode_')
    print("renaming completed")

    #------------------------------------------------------------------------------------#
    #--------Step 6.7: Change datatype of columns to mimize df size for storage----------#
    #------------------------------------------------------------------------------------#
    #Specific issues with these 4 columns
    cms1['sex_id']=cms1['sex_id'].fillna(2).astype('int8')
    cms1['age']=cms1['age'].fillna(-1)
    cms1['MONTH']=cms1['MONTH'].astype('string').fillna('-1')

    cms1=pf.datatype_format(dataset='MDCR', subdataset=subdataset, df=cms1)#pf

    parition_by=['year_id','st_resi']

    #save output
    cms1.astype({
        'sex_id':'int'
    }
    ).to_parquet(outpath,
                partition_cols=parition_by, 
                 basename_template=f"stg2_{state}_{{i}}.parquet",
                existing_data_behavior = "overwrite_or_ignore")

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
        type=int,
        #type=str,
        required=True
        )
    parser.add_argument(
        "--yr",
        type=int,
        required=True
        )
    parser.add_argument(
        "--subdataset",
        type=str,
        required=True
        )
    parser.add_argument(
        "--test",
        type=int,
        required=True
    )

    args = vars(parser.parse_args())
    stage_2(args['outpath'],args['state'],args['yr'],args['subdataset'],args['test']) 