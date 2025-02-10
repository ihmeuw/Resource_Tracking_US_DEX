#####################################################################################
##PURPOSE: CMS Medicare Carrier Stage 2 - Data Processing 
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#All MDCR FFS stage 2 processed by this script except RX
#####################################################################################

#import packages
import pandas as pd
import numpy as np
import psutil 
import gc
import argparse
import glob
from datetime import date

# adding this to make importing these easier if using git worktree
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.helpers import processing_functions as pf
from 00_data_prep.stage_2.cms.constants import paths as path
from 00_data_prep.stage_2.cms.constants import cms_dict as cd


def stage_2(outpath, state, yr, subdataset,sex,test,chia):
    print(state)
    print(yr)
    print(subdataset)
    print(sex)
    
    #--------------------------------------------------------------------
    #Step 1 - get columns and file paths
    #--------------------------------------------------------------------
    #List columns of interest from claims and ps 
    cms_cols = ['BENE_ID','CLM_ID','BENE_STATE_CD', 
                'NCH_CLM_TYPE_CD','CLM_FROM_DT','CLM_THRU_DT', 
              'CLM_PMT_AMT','DOB_DT','GNDR_CD','BENE_RACE_CD','BENE_CNTY_CD','BENE_MLG_CNTCT_ZIP_CD',
                #not in carrier or rx, but in all other files
                'NCH_PRMRY_PYR_CD','NCH_PRMRY_PYR_CLM_PD_AMT','CLM_FAC_TYPE_CD','CLM_SRVC_CLSFCTN_TYPE_CD',
                'CLM_TOT_CHRG_AMT','PTNT_DSCHRG_STUS_CD',
                #just in inpatiet, op, and snf
                'NCH_BENE_BLOOD_DDCTBL_LBLTY_AM'
               ]

    if subdataset == 'ip':
        other_cols = ['CLM_ADMSN_DT','PRVDR_STATE_CD','NCH_IP_TOT_DDCTN_AMT','CLM_SRVC_FAC_ZIP_CD',
                     'CLM_PASS_THRU_PER_DIEM_AMT','CLM_UTLZTN_DAY_CNT','CLM_NON_UTLZTN_DAYS_CNT',
                     'NCH_BENE_DSCHRG_DT'] 
        d_code_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        v_code_cols = []
        e_code_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(12)]
        remove_cols = []
        if chia == 0:
            #reuse files have a different file name
            if yr in [2000, 2010, 2014, 2015, 2016]:
                ip_path = path.path_mdcr_ip
            elif yr in [2008, 2009, 2011, 2012, 2013, 2017]:
                ip_path = path.path_mdcr_ip_reuse
            elif yr in [2019]:
                ip_path = path.path_mdcr_ip_2019
            print(path)
            if state != -1:
                if (yr == 2019):
                    state = str(state).rjust(2, '0') #The one digit # in 2015 are zero padded to be 2 digit
                else:
                    state = str(state)

            cms_yr = str(path.path_mdcr) + "/" + str(yr) + str(ip_path) + str(yr) + ".parquet"
        elif chia == 1:
            cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "inpatient_base_claims_*.parquet"))[0]

    elif subdataset == 'hosp':
        other_cols=['CLM_HOSPC_START_DT_ID','PRVDR_STATE_CD','NCH_BENE_DSCHRG_DT','CLM_SRVC_FAC_ZIP_CD'] 
        d_code_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        v_code_cols = []
        e_code_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(12)]
        remove_cols = ['PTNT_DSCHRG_STUS_CD','NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']
        if chia == 0:
            cms_yr = str(path.path_mdcr) + '/'+str(yr) + str(path.path_mdcr_hosp) + str(yr) + ".parquet"
        elif chia == 1:
            cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "hospice_base_claims_*.parquet"))[0]

    elif subdataset == 'nf':
        other_cols=['PRVDR_STATE_CD','CLM_ADMSN_DT','NCH_BENE_DSCHRG_DT','CLM_UTLZTN_DAY_CNT','NCH_IP_TOT_DDCTN_AMT',
                    'CLM_SRVC_FAC_ZIP_CD','NCH_PTNT_STATUS_IND_CD','BENE_TOT_COINSRNC_DAYS_CNT',
                    'CLM_NON_UTLZTN_DAYS_CNT'] 
        d_code_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        v_code_cols = []
        e_code_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(12)]
        remove_cols = ['CLM_FAC_TYPE_CD','CLM_SRVC_CLSFCTN_TYPE_CD','PTNT_DSCHRG_STUS_CD','NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']
        if chia == 0:
            cms_yr = str(path.path_mdcr)+'/'+str(yr)+'/'+str(path.path_mdcr_nf)+str(yr)+".parquet"
            if state != -1:
                if (yr == 2019):
                    state = str(state).rjust(2, '0') #The one digit # in 2015 are zero padded to be 2 digit
                else:
                    state = str(state)
            print(state)
        if chia == 1:
            cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "snf_base_claims_*.parquet"))[0]

    elif subdataset == 'hha':
        other_cols=['CLM_ADMSN_DT','PRVDR_STATE_CD','NCH_BENE_DSCHRG_DT',
                      'CLM_SRVC_FAC_ZIP_CD','CLM_HHA_TOT_VISIT_CNT'] 
        d_code_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        v_code_cols = []
        e_code_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(12)]
        remove_cols = ['PTNT_DSCHRG_STUS_CD','NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']
        if chia == 0:
            #New year of cms: 2019, has diff file name
            if yr in [2000, 2010, 2014, 2015, 2016]:
                hh_path = path.path_mdcr_hha
            elif yr in [2019]:
                hh_path = path.path_mdcr_hha_2019
            print(path)
            cms_yr = str(path.path_mdcr) + "/" + str(yr) + str(hh_path) + str(yr) + ".parquet"
        if chia == 1:
            cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH.parquet"))[0]

    elif subdataset == 'carrier':
        #columns
        other_cols = ['CLM_BENE_PD_AMT','NCH_CLM_PRVDR_PMT_AMT','NCH_CLM_BENE_PMT_AMT','CARR_CLM_PRMRY_PYR_PD_AMT',
                     'NCH_CARR_CLM_SBMTD_CHRG_AMT','NCH_CARR_CLM_ALOWD_AMT','CARR_CLM_CASH_DDCTBL_APLD_AMT']
        remove_cols = ['CLM_FAC_TYPE_CD','CLM_SRVC_CLSFCTN_TYPE_CD','CLM_TOT_CHRG_AMT','PTNT_DSCHRG_STUS_CD',
                      'NCH_BENE_BLOOD_DDCTBL_LBLTY_AM','NCH_PRMRY_PYR_CD','NCH_PRMRY_PYR_CLM_PD_AMT'] 
        d_code_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(12)]
        v_code_cols = ["ICD_DGNS_VRSN_CD" + str(x + 1) for x in range(12)]
        e_code_cols = []#carrier doesn't have ecodes
        #filepath
        if chia == 0:
            cms_yr = str(path.path_mdcr) + "/" + str(yr) + "/" +str(path.path_mdcr_carrier)+ str(yr) +'.parquet' 
        if chia == 1:
            cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH*.parquet"))[0]

    elif subdataset == 'hop':
        #columns
        other_cols = ['NCH_BENE_PTB_DDCTBL_AMT','NCH_BENE_PTB_COINSRNC_AMT','PRVDR_STATE_CD',
                     'CLM_SRVC_FAC_ZIP_CD','OT_PHYSN_SPCLTY_CD']
        remove_cols = []
        d_code_cols = ["ICD_DGNS_CD" + str(x + 1) for x in range(25)]
        #hop doesn't have version codes, but has ecodes
        v_code_cols = [] 
        e_code_cols = ["ICD_DGNS_E_CD" + str(x + 1) for x in range(12)] #ecodes also in haa,hosp,ip, and nf
        #filepath
        if chia == 0:
            if state != -1:
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
        if chia == 1:
            cms_yr = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH*.parquet"))[0]

    cms_cols = [item for item in cms_cols if item not in remove_cols]
    cms_cols = cms_cols + other_cols + d_code_cols + v_code_cols + e_code_cols
    print(cms_yr)
    
    #--------------------------------------------------------------------
    #Step 1 - read in claims data, create unique list of bene ids 
    #--------------------------------------------------------------------
    
    if sex == 'all':
        filter_sex = []
    elif sex not in ['all','-1']:
        if (subdataset == 'hop') & (yr in [2008, 2015]): #hop much larger and needs to be partitioned again
            sex=int(sex)
        filter_sex = [("GNDR_CD", "=", sex)]
    elif sex == '-1':
        filter_sex = [("GNDR_CD", "not in", ['1','2','0'])] 
        if (subdataset == 'hop') & (yr in [2008,2015]):
            filter_sex = [("GNDR_CD", "not in", [1,2,0])] 

    
    if state != -1:
        filter_state = [("BENE_STATE_CD", "in", [state])]
        if (subdataset == 'hop') & (yr == 2008): 
            filter_state = [("PRVDR_STATE_CD", "in", [state])]
    elif state == -1:
        filter_state = [("BENE_STATE_CD", "not in", [1,2,3,4,5,6,7,8,9,10,
                                                     11,12,13,14,15,16,17,18,19,20,
                                                     21,22,23,24,25,26,27,28,29,30,
                                                     31,32,33,34,35,36,37,38,39,41,
                                                     42,43,44,45,46,47,49,50,51,52,
                                                     53,67,68,69,70,71,72,73,74,80,99])]
            
        if subdataset in ['nf','hop','ip']:
            states_str = ['1','2','3','4','5','6','7','8','9','10',
                             '01','02','03','04','05','06','07','08','09',
                         '11','12','13','14','15','16','17','18','19','20',
                         '21','22','23','24','25','26','27','28','29','30',
                         '31','32','33','34','35','36','37','38','39',
                         '41','42','43','44','45','46','47','49','50',
                         '51','52','53','67','68','69','70','71','72','73','74','80','99']
            filter_state = [("BENE_STATE_CD", "not in", states_str)]
            if (subdataset == 'hop') & (yr == 2008): 
                filter_state = [("PRVDR_STATE_CD", "not in", states_str)]
            
    filter1 = filter_sex +filter_state
    
    #Read in data
    data_schema = pf.replace_schema(cms_yr, partitioning="hive")
    cms1=pd.read_parquet(cms_yr,
                             columns=cms_cols,
                             use_legacy_dataset=False,
                             schema=data_schema,
                             filters = filter1).drop_duplicates()
    print("claims data read in")
    print(cms1.shape)
    
    if (cms1.empty == True) and ((sex in ['0','-1'])|(sex in [0,-1])): 
        #this is unknown sexes and maybe empty #2015 sex is int
        print('DataFrame is empty!')
        exit()
    elif (cms1.empty == True) and (state in [48,54,55,56,57,58,59,60,
             61,62,63,64,65,97,98,99,-1,
            67,68,69,70,71,72,73,74,80]): #these are either not-us states or are the histroical code for them
        print('DataFrame is empty!') #
        exit()
    elif (cms1.empty == True) and (state in ['48','54','55','56','57','58','59','60',
         '61','62','63','64','65','97','98','99',
          '67','68','69','70','71','72','73','74','80']): #these are either not-us states or are the histroical code for them
        print('DataFrame is empty!')
        exit()
    else:
        print('dataframe not empty! (or is a US state)')
    if test==1:
        cms1=cms1.head(5000)
        
    #year_id is year of data
    cms1['year_id']=yr
    unique_bene =cms1['BENE_ID'].to_list()
    
    if (len(unique_bene)==0) and (state in [48,54,55,56,57,58,59,60,
             61,62,63,64,65,97,98,99,-1]): #these are either not-us states or are the histroical code for them
        print('DataFrame contains no bene_ids!')
        exit()
        
    if pd.api.types.is_string_dtype(cms1['BENE_STATE_CD']):
        cms1.loc[cms1['BENE_STATE_CD'].str.endswith('0320000'), 'BENE_STATE_CD'] = -1
        
    if subdataset in ['ip','nf','hha','hosp']:
        cms1=pf.fix_admin_date(dataset='MDCR', subdataset=subdataset, df=cms1, chia=chia)

    #--------------------------------------------------------------------
    #Step 2 - read in ps (aka denominator aka mbsf) data - filtering on bene 
    #--------------------------------------------------------------------
    if chia == 1:
        ps_path = str(path.path_chia_mdcr_denom)+'FILEPATH'+str(yr)
    else:
        ps_path = str(path.path_mdcr_denom)+'FILEPATH'+str(yr)

    data_schema = pf.replace_schema(ps_path, partitioning="hive")
    data_schema = pf.change_col_schema(data_schema, {"PTC_PLAN_TYPE_CD": "string"})
    ps=pd.read_parquet(ps_path,
                       use_legacy_dataset=False,
                        schema=data_schema,
                       filters= [("BENE_ID", "in", unique_bene)
                                ])
    ps['ENROLLMT_REF_YR'] = yr
    ps['sex_id']=ps['sex_id'].astype(int) #convert to int bc comes in as category
    ps['sex_id']=ps['sex_id'].fillna(-1).astype(int)
    print('demographic data read in')
    print(ps.shape)


    #------------------------------------------------------------------#
    #--Step 4: Merge MDCR claims and PS (denominator) dataframes-------#
    #------------------------------------------------------------------#
    #Extract service begin month to merge on
    cms1['CLM_FROM_DT'] = cms1['CLM_FROM_DT'].astype(str)
    cms1['CLM_FROM_DT']=pd.to_datetime(cms1['CLM_FROM_DT'], errors = 'coerce')
    if subdataset in ['hop','carrier']:
        cms1['clm_from_mo']=cms1['CLM_FROM_DT'].dt.month
    elif subdataset in ['ip','hha','nf']:
        cms1['CLM_ADMSN_DT'] = cms1['CLM_ADMSN_DT'].astype(str)
        cms1['CLM_ADMSN_DT']=pd.to_datetime(cms1['CLM_ADMSN_DT'], errors = 'coerce')
        cms1['CLM_ADMSN_DT1']=cms1.CLM_ADMSN_DT.fillna(cms1.CLM_FROM_DT) 
        cms1['clm_from_mo']=cms1['CLM_ADMSN_DT1'].dt.month
    elif subdataset == 'hosp':
        cms1['CLM_HOSPC_START_DT_ID'] = cms1['CLM_HOSPC_START_DT_ID'].astype(str)
        cms1['CLM_HOSPC_START_DT_ID']=pd.to_datetime(cms1['CLM_HOSPC_START_DT_ID'], errors = 'coerce')
        cms1['CLM_HOSPC_START_DT_ID1']=cms1.CLM_HOSPC_START_DT_ID.fillna(cms1.CLM_FROM_DT) #if this is na, can't merge w/ ps
        cms1['clm_from_mo']=cms1['CLM_HOSPC_START_DT_ID1'].dt.month

    #some of the columns are set to categories and some are wrong datatype for join, 
    #can't fill na if datatype is category
    num_cols=['year_id','clm_from_mo','BENE_STATE_CD']
    join_cols = ['BENE_ID','year_id','clm_from_mo','BENE_STATE_CD']
    cms1[join_cols] = cms1[join_cols].astype(str)
    cms1[num_cols] = cms1[num_cols].astype(float).fillna(-1).astype(int)

    num_cols=['ENROLLMT_REF_YR','MONTH','STATE_CODE']
    join_cols = ['BENE_ID','ENROLLMT_REF_YR','MONTH','STATE_CODE']
    ps[join_cols] = ps[join_cols].astype(str)
    ps[num_cols] = ps[num_cols].astype(float).fillna(-1).astype(int)
    print("cms1 shape before join: " + str(cms1.shape))
    print("ps shape before join: " + str(ps.shape))
    size_cms_b4_ps=cms1.shape[0] #save the row count of original cms df
    cms1=cms1.merge(ps, left_on=['BENE_ID','year_id','clm_from_mo'], 
                   right_on=['BENE_ID','ENROLLMT_REF_YR','MONTH'],how="left")

    cms1.drop_duplicates(inplace=True)
    cms1.drop_duplicates(subset = cms_cols, keep = 'first', inplace = True)
    size_cms_ps=cms1.shape[0]#save the row count of cms df after join
    print("cms1 shape after join to ps: " + str(cms1.shape))
    #ensure there is no row duplication
    assert size_cms_b4_ps == size_cms_ps, "cms row count changed after merge to ps file"
    
    ######################################################################
    #---------------------Step 6 - COLUMN FORMATTING---------------------#
    ######################################################################
    #---------------------------------------------------------------------------------------------------------------------------#
    #Step 6.1: Create date related columns. Convert date columns to date type so age/los can be calculated and year's extracted-#
    #---------------------------------------------------------------------------------------------------------------------------#

    # making sure the date is a string before converting bc it was causing problems in CHIA
    if 'CLM_THRU_DT' in cms1.columns:
        cms1['CLM_THRU_DT'] = cms1['CLM_THRU_DT'].astype(str)

    if 'NCH_BENE_DSCHRG_DT' in cms1.columns:
        if chia == 1:
            cms1['NCH_BENE_DSCHRG_DT']=cms1['NCH_BENE_DSCHRG_DT'].fillna(-1)
            cms1['NCH_BENE_DSCHRG_DT'] = cms1['NCH_BENE_DSCHRG_DT'].astype('Int64')
        cms1['NCH_BENE_DSCHRG_DT'] = cms1['NCH_BENE_DSCHRG_DT'].astype(str)  

    date_cols = ['DOB_DT','CLM_THRU_DT','BIRTH_DT']
    if subdataset in ['ip','hha','hosp','nf']:
        date_cols = date_cols + ['NCH_BENE_DSCHRG_DT']
    cms1[date_cols]=cms1[date_cols].apply(pd.to_datetime, errors='coerce')
    cms1['CLM_THRU_DT'].value_counts()

    # sometimes years are impalusible mixed up ie 2109 when it should be 2019, this function fixes that
    cms1 = pf.switch_year_digits(df=cms1, date_column = 'CLM_THRU_DT')
    if "CLM_ADMSN_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'CLM_ADMSN_DT')
    if "NCH_BENE_DSCHRG_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'NCH_BENE_DSCHRG_DT')
    if "CLM_HOSPC_START_DT_ID" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'CLM_HOSPC_START_DT_ID')

    if subdataset in ['ip','hha','hosp','nf']: #fill here just for ip 
        #fill in na's of columns w/ values from similar columns
        cms1['NCH_BENE_DSCHRG_DT1']=cms1.NCH_BENE_DSCHRG_DT.fillna(cms1.CLM_THRU_DT)
        cms1['CLM_THRU_DT1']=cms1.CLM_THRU_DT.fillna(cms1.NCH_BENE_DSCHRG_DT)

    #rename create service_date and discharge_date to be used in c2e
    if subdataset in ['carrier','hop']:
        cms1['service_date']=cms1['CLM_FROM_DT']#want to retain this column to make a copy instead
    elif subdataset in ['ip','hha','nf']:
        cms1["service_date"]=cms1["CLM_ADMSN_DT1"]
        if subdataset in ['ip','nf']:
            cms1["discharge_date"]=cms1["NCH_BENE_DSCHRG_DT1"]
    elif subdataset == 'hosp':
        cms1["service_date"]=cms1['CLM_HOSPC_START_DT_ID1']
        cms1["discharge_date"]=cms1['NCH_BENE_DSCHRG_DT1']

    print("starting age")

    #-------AGE----------------------------#
    cms1 = pf.age_format(dataset='MDCR',df=cms1)

    #LOS #Division removes word days
    if subdataset in ['carrier','hop']:
        cms1['los']=(cms1['CLM_THRU_DT']-cms1['CLM_FROM_DT'])/ np.timedelta64(1, 'D') 
    elif subdataset in ['ip','hha','nf']:
        #LOS #Division removes word days
        cms1['los']=(cms1['NCH_BENE_DSCHRG_DT']-cms1['CLM_ADMSN_DT'])/ np.timedelta64(1, 'D')
        cms1['los_hha']=(cms1['NCH_BENE_DSCHRG_DT1']-cms1['CLM_ADMSN_DT1'])/ np.timedelta64(1, 'D')
        cms1['los_c']=(cms1['CLM_THRU_DT']-cms1['CLM_FROM_DT'])/ np.timedelta64(1, 'D') 
        if subdataset in ['ip','nf']:
            cms1['los1']=cms1['CLM_NON_UTLZTN_DAYS_CNT']+cms1['CLM_UTLZTN_DAY_CNT']
            cms1['los']=cms1['los'].fillna(cms1['los1'])
            cms1['los']=cms1['los'].fillna(cms1['los_c'])
        elif subdataset == "hha":
            cms1['los']=cms1['los'].fillna(cms1['los_hha'])
    elif subdataset == 'hosp':
        cms1['los']=(cms1['NCH_BENE_DSCHRG_DT']-cms1['CLM_HOSPC_START_DT_ID'])/ np.timedelta64(1, 'D')
        cms1['los1']=(cms1['NCH_BENE_DSCHRG_DT1']-cms1['CLM_HOSPC_START_DT_ID1'])/ np.timedelta64(1, 'D')
        cms1['los_c']=(cms1['CLM_THRU_DT']-cms1['CLM_FROM_DT'])/ np.timedelta64(1, 'D') 
        cms1['los']=cms1['los'].fillna(cms1['los1'])
        cms1['los']=cms1['los'].fillna(cms1['los_c'])
    
    #year_id - year of data
    cms1['year_id']=yr
    if subdataset in ['carrier','hop']:
        #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
        cms1['year_adm']=cms1['CLM_FROM_DT'].dt.year 
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge if not available, use claim thru
        cms1['year_dchg']=cms1['CLM_THRU_DT'].dt.year
        #year_id_clm/CLM_THRU_DT aka claim end year
        cms1['year_clm']=cms1['CLM_THRU_DT'].dt.year
    elif subdataset in ['ip','hha','hosp','nf']:
        if subdataset != 'hosp':
            #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
            cms1['year_adm']=cms1['CLM_ADMSN_DT1'].dt.year
        elif subdataset == 'hosp':
            cms1['year_adm']=cms1['CLM_HOSPC_START_DT_ID1'].dt.year
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
        cms1['year_dchg']=cms1['NCH_BENE_DSCHRG_DT1'].dt.year
        #year_id_clm/CLM_THRU_DT aka claim end year
        cms1['year_clm']=cms1['CLM_THRU_DT1'].dt.year
    print("date formatting completed")

    #---------------------------------------------------------------------------------------------------------------------------
    #--------------Step 6.2: Create demographic related columns - sex, race, location info--------------------------------------
    #---------------------------------------------------------------------------------------------------------------------------
    # Sex: #Sex - if missing from personal summary, take from claim
    #CMS sex: 0=unk, 1=male, 2=female
    cms1= pf.sex_format(dataset='MDCR', subdataset='', df =cms1)

    # Race
    cms1=pf.race_format(dataset='MDCR', subdataset='', df =cms1)

    ###-----Geographic Info-----##
    #state of residence
    cms1= pf.mdcr_state_format(subdataset=subdataset,df=cms1, resi_serv = 'resi')
    #county of residence
    cms1 = pf.cnty_format(dataset='MDCR', df=cms1)
    #zip codes
    cms1=pf.zip_format(dataset='MDCR', subdataset=subdataset, df=cms1, chia=chia, yr=yr)

    print("demographic formatting completed")

    #---------------------------------------------------------------------------#
    #------------Step 6.4: Create payer and payment related columns-------------#
    #---------------------------------------------------------------------------#
    if subdataset in ['hop','ip','hha','hosp','nf']:
        #all have same logic for pri_payer
        #-----------------------------PAYERS----------------------------------------#
        #NCH_PRMRY_PYR_CD - primary code if not MDCR 
        cms1['pri_payer'] = cms1['NCH_PRMRY_PYR_CD'].map(lambda x: cd.mdcr_payer_dict[x][0])
        #PMT_1 - these become floats
        pmt1_conditions = [cms1['pri_payer']==1,
                           cms1['pri_payer']==2,
                           cms1['pri_payer']==19]
        pmt1_outputs=[(cms1['CLM_PMT_AMT']), 
                      cms1['NCH_PRMRY_PYR_CLM_PD_AMT'], cms1['NCH_PRMRY_PYR_CLM_PD_AMT']]
        cms1['pmt_1']=np.select(pmt1_conditions, pmt1_outputs, np.nan)
        
    if subdataset == 'carrier':
        #-----------------------------PAYERS----------------------------------------#
        #
        cms1['pri_payer'] = np.where(cms1['CARR_CLM_PRMRY_PYR_PD_AMT'] > 0, 6, 1)
        # payer_2
        pay2_conditions = [((cms1['pri_payer'] != 1) & (cms1['CLM_PMT_AMT'] > 0)),
                            ((cms1['pri_payer'] == 1) & (cms1['CARR_CLM_PRMRY_PYR_PD_AMT'] > 0))]

        pay2_outputs=[1,6]

        cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, np.nan)
        #payer_3
        cms1['payer_3']=np.where(((cms1['payer_2']!=4) & ((cms1['CLM_BENE_PD_AMT'])>0)),4,np.nan)

        #------------------------------------PAYMENTS--------------------------------------------------#
        #PMT_1 - these become floats
        pmt1_conditions = [cms1['pri_payer']==1,
                           cms1['pri_payer']==6]
        pmt1_outputs=[(cms1['CLM_PMT_AMT']), 
                      cms1['CARR_CLM_PRMRY_PYR_PD_AMT']]
        cms1['pmt_1']=np.select(pmt1_conditions, pmt1_outputs, np.nan)
        #pmt_2
        pmt2_conditions = [cms1['payer_2']==1,
                           cms1['payer_2']==6,
                           cms1['payer_2']==4]
        pmt2_outputs=[(cms1['CLM_PMT_AMT']), 
                      cms1['CARR_CLM_PRMRY_PYR_PD_AMT'], cms1['CLM_BENE_PD_AMT']]
        cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)
        #pmt_3
        cms1['pmt_3']=np.where(cms1['payer_3']==4, cms1['CLM_BENE_PD_AMT'], np.nan)
    
        
    elif subdataset == 'hop':
        #-----------------------------PAYERS----------------------------------------#
        #payer_2
        pay2_conditions = [((cms1['pri_payer']!= 1) & (cms1['CLM_PMT_AMT']>0)),
                            ((cms1['pri_payer']==1) & (cms1['NCH_PRMRY_PYR_CLM_PD_AMT']>0)),#OTH not anything
                            ((cms1['pri_payer']==1) & (((cms1['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+cms1['NCH_BENE_PTB_DDCTBL_AMT']+cms1['NCH_BENE_PTB_COINSRNC_AMT'])>0) & (cms1['NCH_PRMRY_PYR_CLM_PD_AMT']==0))),
                            ((cms1['pri_payer']==19) & ((cms1['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+cms1['NCH_BENE_PTB_DDCTBL_AMT'])>0)),
                            ((cms1['pri_payer']==2) & ((cms1['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+cms1['NCH_BENE_PTB_DDCTBL_AMT']+cms1['NCH_BENE_PTB_COINSRNC_AMT'])>0))]
        pay2_outputs=[1,6,4,4,4]
        cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, np.nan)
        #payer_3
        cms1['payer_3']=np.where(((cms1['payer_2']!=4) & ((cms1['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+cms1['NCH_BENE_PTB_DDCTBL_AMT'])>0)),4,np.nan)

        #------------------------------------PAYMENTS--------------------------------------------------#
        #Medicare paid amount = To obtain the total amount paid by Medicare for the claim, the pass-through amount (which is the daily per diem amount) must be multiplied by the number of Medicare-covered days (i.e., multiply the CLM_PASS_THRU_PER_DIEM_AMT by the CLM_UTLZTN_DAY_CNT), and then added to the claim payment amount (this field).
        #pmt_2
        pmt2_conditions = [cms1['payer_2']==1,
                           cms1['payer_2']==6,
                           cms1['payer_2']==4]
        pmt2_outputs=[(cms1['CLM_PMT_AMT']), 
                      cms1['NCH_PRMRY_PYR_CLM_PD_AMT'], (cms1['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+cms1['NCH_BENE_PTB_DDCTBL_AMT'])]
        cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)
        #pmt_3
        cms1['pmt_3']=np.where(cms1['payer_3']==4, (cms1['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+cms1['NCH_BENE_PTB_DDCTBL_AMT']), np.nan)
        
    elif subdataset == 'ip':
        #payer_2
        pay2_conditions = [((cms1['pri_payer']!= 1) & 
                            ((cms1['CLM_PMT_AMT']>0)| (cms1['CLM_PASS_THRU_PER_DIEM_AMT']*cms1['CLM_UTLZTN_DAY_CNT'])>0)),
                            ((cms1['pri_payer']==1) & (cms1['NCH_PRMRY_PYR_CLM_PD_AMT']>0))]

        pay2_outputs=[1,6]
        cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, np.nan)
        #payer_3
        cms1['payer_3']=np.where(((cms1['payer_2']!=4) & (cms1['NCH_IP_TOT_DDCTN_AMT']>0)),4,np.nan)
        #------------------------------------PAYMENTS--------------------------------------------------#
        #pmt_2
        pmt2_conditions = [cms1['payer_2']==1,
                        cms1['payer_2']==6,
                        cms1['payer_2']==4]
        pmt2_outputs=[(cms1['CLM_PMT_AMT']+(cms1['CLM_PASS_THRU_PER_DIEM_AMT']*cms1['CLM_UTLZTN_DAY_CNT'])), 
                    cms1['NCH_PRMRY_PYR_CLM_PD_AMT'], cms1['NCH_IP_TOT_DDCTN_AMT']]
        cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)
        #pmt_3
        cms1['pmt_3']=np.where(cms1['payer_3']==4, cms1['NCH_IP_TOT_DDCTN_AMT'], np.nan)
    elif subdataset in ['hha','hosp','nf']:
        #payer_2
        pay2_conditions = [((cms1['pri_payer']!= 1) & (cms1['CLM_PMT_AMT']>0)),
                            ((cms1['pri_payer']==1) & (cms1['NCH_PRMRY_PYR_CLM_PD_AMT']>0))]
        pay2_outputs=[1,6]
        cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, np.nan)
        #pmt_2
        pmt2_conditions = [cms1['payer_2']==1,
                           cms1['payer_2']==6]
        pmt2_outputs=[cms1['CLM_PMT_AMT'], 
                      cms1['NCH_PRMRY_PYR_CLM_PD_AMT']]
        cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)
        
        
    cms1['is_primary']=np.where(cms1['pri_payer']==1,1,0)
    print("Payer formatting completed")

    # --------------------------------------------------------------------------#
    #-------------Step 6.5: Create other columns TOC, Facility etc--------------#
    #---------------------------------------------------------------------------#
    #Pushing TOC and dischare info to stage 3 to use TOC map from denoms 
    cms1 = pf.dual_format(dataset='MDCR', df=cms1)

    if 'NCH_BENE_DSCHRG_DT1' in cms1.columns: 
        cms1['code_system']=np.where(cms1['NCH_BENE_DSCHRG_DT1']<pd.to_datetime('2015-10-01'),'icd9','icd10')
    else:
        cms1['code_system']=np.where(cms1['CLM_THRU_DT']<pd.to_datetime('2015-10-01'),'icd9','icd10')
        
     #-------------------TOC Assignment---------------------------#
    cms1=pf.toc_format(subdataset=subdataset, df=cms1, yr=yr, chia = chia)
        
    #pull out dental and save in toc1 column 
    if subdataset in ['carrier','hop']:
        
        cms1 = pf.dv_line(subdataset=subdataset,
                              df_cms=cms1 , unique_bene=unique_bene, yr=yr, chia = chia)
        #double check there is dv
        tocs = cms1['toc1'].unique()
        if ((state in [5,10,33]) & (yr != 2015)):
            assert 'DV' in tocs, "No claims assigned to DV! Check to see if this should be true"   
        
        
    print("Misc formatting completed")

    #---------------------------------------------------------------------------#
    #--------Step 6.6: Rename columns that don't need other processing----------#
    #---------------------------------------------------------------------------#
    cms1.rename(columns = {
        'BENE_ID':'bene_id',
        'CLM_ID': 'claim_id'
        }, inplace=True)
    if 'NCH_CARR_CLM_SBMTD_CHRG_AMT' in cms1.columns:
        cms1.rename(columns = {'NCH_CARR_CLM_SBMTD_CHRG_AMT':'tot_chg_amt'}, inplace=True)
    if 'CLM_TOT_CHRG_AMT' in cms1.columns:
        cms1.rename(columns = {'CLM_TOT_CHRG_AMT':'tot_chg_amt'}, inplace=True)
    cms1.columns = cms1.columns.str.replace('ICD_DGNS_CD' , 'dx_')
    cms1.columns = cms1.columns.str.replace('ICD_DGNS_E_CD' , 'ecode_')
    print("renaming completed")
    
    #Other processing stuff
    if subdataset == 'carrier':
        cms1['ENHANCED_FIVE_PERCENT_FLAG']='Y' #Some are null in the raw data, but should all be Y
    if subdataset == 'hha':
        cms1['CLM_HHA_TOT_VISIT_CNT'].fillna(1, inplace=True)
        cms1['CLM_HHA_TOT_VISIT_CNT']=np.where(cms1['CLM_HHA_TOT_VISIT_CNT']==0,1,cms1['CLM_HHA_TOT_VISIT_CNT'])

    #------------------------------------------------------------------------------------#
    #--------Step 6.7: Change datatype of columns to mimize df size for storage----------#
    #------------------------------------------------------------------------------------#
    #Specific issues with these 4 columns
    cms1['sex_id']=cms1['sex_id'].fillna(2).astype('int8')
    cms1['age']=cms1['age'].fillna(-1)
    cms1['MONTH']=cms1['MONTH'].astype('string').fillna('-1')

    cms1=pf.datatype_format(dataset='MDCR', subdataset=subdataset, df=cms1)
    

    #Check required columns are in dataset
    required_cols = ['bene_id','claim_id','age','los','year_adm','year_clm','year_dchg','year_id','st_resi',
             'race_cd_raw','race_cd_imp','cnty','cnty_loc_id_resi','mcnty_resi','zip_5_resi',
              'st_loc_id_resi','st_num_resi','dual_ind',
             'tot_chg_amt','is_primary','toc','code_system','sex_id', 
              'ENHANCED_FIVE_PERCENT_FLAG','pri_payer','payer_2','pmt_1','pmt_2',
              'CLM_FROM_DT','CLM_PMT_AMT','service_date']
    if subdataset == 'ip':
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        other_cols = ['NCH_IP_TOT_DDCTN_AMT','payer_3','pmt_3','NCH_PRMRY_PYR_CD','CLM_ADMSN_DT','NCH_BENE_DSCHRG_DT',
                     'discharge_date']
        
    elif subdataset == 'carrier':
        dx_cols = ["dx_" + str(x + 1) for x in range(12)]
        e_cols = []
        other_cols = ['toc1','HCPCS_CD','CLM_BENE_PD_AMT','CARR_CLM_PRMRY_PYR_PD_AMT','payer_3','pmt_3']
    
    elif subdataset == 'hop':
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        other_cols = ['toc1','HCPCS_CD','NCH_BENE_BLOOD_DDCTBL_LBLTY_AM','NCH_BENE_PTB_DDCTBL_AMT',
                      'NCH_BENE_PTB_COINSRNC_AMT','NCH_PRMRY_PYR_CLM_PD_AMT','NCH_PRMRY_PYR_CD']
        
    elif subdataset == 'hha':
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        other_cols = ['NCH_PRMRY_PYR_CD','CLM_ADMSN_DT','NCH_BENE_DSCHRG_DT']
    elif subdataset == 'hosp':
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        other_cols = ['NCH_PRMRY_PYR_CD','CLM_HOSPC_START_DT_ID','NCH_BENE_DSCHRG_DT',
                     'discharge_date']
    elif subdataset == 'nf':
        dx_cols = ["dx_" + str(x + 1) for x in range(25)]
        e_cols = ["ecode_" + str(x + 1) for x in range(12)]
        other_cols = ['NCH_PRMRY_PYR_CD','CLM_ADMSN_DT','NCH_BENE_DSCHRG_DT','NCH_IP_TOT_DDCTN_AMT',
                     'discharge_date']
    print("please")
    required_cols = required_cols + dx_cols + e_cols + other_cols
    pf.check_cols(df = cms1, required_cols=required_cols)

    #Save data out
    parition_by=['year_id','st_resi']

    # converting datetime formats
    for column in cms1.columns:
        if cms1[column].dtype == 'datetime64[ns]':
            cms1[column] = cms1[column].astype('datetime64[us]')
    
    #save output
    cms1.astype({
        'sex_id':'int',
    }
    ).to_parquet(outpath,
                basename_template=f"stg2_{state}_{sex}_{subdataset}_{{i}}.parquet",
                partition_cols=parition_by, existing_data_behavior = "overwrite_or_ignore") 
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
        "--sex",
        type=str,
        required=True
        )
    parser.add_argument(
        "--test",
        type=int,
        required=True
        )
    parser.add_argument(
        "--chia",
        type=int,
        required=True
        )
    
    args = vars(parser.parse_args())
    stage_2(args['outpath'], args['state'],args['yr'],args['subdataset'],args['sex'],args['test'],args['chia'])