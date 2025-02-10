#####################################################################################
##PURPOSE: CMS Medicaid TAF Stage 2 - Data Processing 
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################

#import packages
import pandas as pd
import numpy as np
import psutil 
import gc
import argparse
#Import helper functions and dictionaries

import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.helpers import processing_functions as pf
from 00_data_prep.stage_2.cms.constants import paths as path
from 00_data_prep.stage_2.cms.constants import cms_dict as cd


def stage_2(outpath, state, yr, subdataset, part):
    print(subdataset)
    print(state)
    print(yr)
    print(part)
    p_5 = 1 #set to 0 for the non-OT subdatasets, will get reassigned in step 0 for the ot datasets
    #--------------------------------------------------------------------
    #Step 0 - Get file paths and columns specific to each subdataset
    #--------------------------------------------------------------------
    #Columns both datasets share
    cms_cols=['BENE_ID','MSIS_ID','CLM_ID','STATE_CD','SUBMTG_STATE_CD','BIRTH_DT','SRVC_BGN_DT','SRVC_END_DT','MDCD_PD_DT',
          'CROSSOVER_CLM_IND','PGM_TYPE_CD','OTHR_INSRNC_IND',
          'BILLED_AMT','MDCD_PD_AMT','OTHR_INSRNC_PD_AMT','TP_PD_AMT','COINSRNC_AMT','COPAY_AMT','DDCTBL_AMT',
          'MDCR_DDCTBL_PD_AMT','MDCR_COINSRNC_PD_AMT','CLM_TYPE_CD','MC_PLAN_ID','BRDR_STATE_IND',
         'ADMTG_PRVDR_SPCLTY_CD']
    if subdataset == 'ip_taf':
        cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ip_taf)+ str(yr) + ".parquet"
        d_code_cols = ["DGNS_CD_" + str(x + 1) for x in range(11)]
        v_code_cols = ["DGNS_VRSN_CD_" + str(x + 1) for x in range(11)]
        cms_cols = cms_cols + d_code_cols + v_code_cols + ['MDCR_PD_AMT','PTNT_DSCHRG_STUS_CD','ADMSN_DT','DSCHRG_DT','MDCD_COPAY_AMT',
                                                    'NCVRD_DAYS','CVRD_DAYS']
        
    elif subdataset =='ltc_taf':
        cms_yr = str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ltc_taf)+ str(yr) + ".parquet"
        d_code_cols = ["DGNS_CD_" + str(x + 1) for x in range(5)]
        v_code_cols = ["DGNS_VRSN_CD_" + str(x + 1) for x in range(5)]
        cms_cols = cms_cols + d_code_cols + v_code_cols + ['MDCR_PD_AMT','PTNT_DSCHRG_STUS_CD','ADMSN_DT','DSCHRG_DT',
                                                    'NCVRD_DAYS','CVRD_DAYS_ICF_IID','CVRD_DAYS_NF','CVRD_DAYS_IP_PSYCH']
    elif subdataset=='rx_taf':
        cms_yr = str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_rx_taf)+ str(yr) +'/STATE_CD='+str(state) #no missing states in rx
        if state in ['CA','NY']:
            cms_yr = cms_yr +'/part_0_'+part+'.parquet'
        other_cols = ['PRSCRBD_DT','RX_FILL_DT']
        remove_cols =['STATE_CD','SRVC_BGN_DT','SRVC_END_DT','MDCD_PD_DT','ADMTG_PRVDR_SPCLTY_CD']
        cms_cols = [item for item in cms_cols if item not in remove_cols]
        cms_cols = cms_cols + other_cols
    elif subdataset == 'ot_taf':
        part = int(part)
        remove_cols =['ADMTG_PRVDR_SPCLTY_CD']
        cms_cols = [item for item in cms_cols if item not in remove_cols]
        other_cols = ['OT_FIL_DT','POS_CD','MDCD_COPAY_AMT','BILL_TYPE_CD',
          'HLTH_HOME_PRVDR_IND','BLG_PRVDR_TYPE_CD','BLG_PRVDR_SPCLTY_CD','FED_SRVC_CTGRY_CD']
        d_code_cols = ["DGNS_CD_" + str(x + 1) for x in range(2)]
        v_code_cols = ["DGNS_VRSN_CD_" + str(x + 1) for x in range(2)]
        cms_cols = cms_cols + other_cols + d_code_cols + v_code_cols
        #filepath
        if yr == 2019:
            cms_yr="FILEPATH"
        elif yr ==2016:
            cms_yr="FILEPATH"

        if state =='CA': 
            p_5=part+10
            l=list(range(part, p_5))
        else:
            p_5=part+50
            l=list(range(part, p_5))
            
    #--------------------------------------------------------------------
    #Step 1 - read in claims data, create unique list of bene ids 
    #--------------------------------------------------------------------
    if state != '-1':
        if subdataset in ['ip_taf','ltc_taf']:
            cms1=pd.read_parquet(cms_yr,
                                 columns=cms_cols,
                                 filters = [("STATE_CD", "in", [state]),
                                           ("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])]).drop_duplicates()
        elif subdataset == 'rx_taf': 
            cms1=pd.read_parquet(cms_yr,
                                 columns=cms_cols,
                                 filters = [("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])]).drop_duplicates()
            cms1['STATE_CD']=state
        elif subdataset == 'ot_taf':
            cms1=pd.read_parquet(cms_yr,
                                 columns=cms_cols,
                                 filters = [("STATE_CD", "in", [state]),
                                            ("partition",'in',l),
                                           ("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])]).drop_duplicates()
    elif state == '-1':
        states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 
            'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
            'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE',
            'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI',
            'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
        
        if subdataset in ['ip_taf','ltc_taf']:
            cms1=pd.read_parquet(cms_yr,
                                 columns=cms_cols,
                                 filters = [("STATE_CD", "not in", states),
                                           ("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])]).drop_duplicates()

        elif subdataset == 'ot_taf':
            cms1=pd.read_parquet(cms_yr,
                                 columns=cms_cols,
                                 filters = [("STATE_CD", "not in", states),
                                            ("partition",'in',l),
                                           ("CLM_TYPE_CD",'in',['1','3','A','C','U','W'])]).drop_duplicates()

    print("claims data read in")
    print(cms1.shape)
    size_cms_start=cms1.shape[0]


    if (cms1.empty == True) and (state == '-1'): 
        print('No unknown states!')
        exit()

    unique_bene=cms1['BENE_ID'].unique()
    unique_msis=cms1['MSIS_ID'].unique()
    unique_clm=cms1['CLM_ID'].unique()
    print('created unique lists')
    
    if (len(unique_bene) == 0) and (state == '-1'): 
        #If no bene_ids, can't join to the PS to get demographic info
        print('There are no bene_ids!')
        exit()
    
    if subdataset in ['ip_taf','ltc_taf']:
        chia = 0
        cms1=pf.fix_admin_date(dataset='MDCD_TAF', subdataset=subdataset, df=cms1, chia = chia)
    
    ##########################
    # Steps 1.20 and 1.21 for RX only
    # Steps 1.30 and 1.31 for OT only
    ##########################
    if subdataset == 'rx_taf':
        #--------------------------------------------------------------------
        #Step 1.20 - read in line file which has ndc
        #--------------------------------------------------------------------
        #Bring in line file  
        rx_line = pd.read_parquet(str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_rx_taf_line)+ str(yr)+'.parquet',
                                 columns = ['BENE_ID','MSIS_ID','CLM_ID','STATE_CD','SUBMTG_STATE_CD', 'RX_FILL_DT', 'NDC',
                                            'LINE_MDCD_PD_AMT','BRND_GNRC_CD','DOSAGE_FORM_CD','NDC_QTY','DAYS_SUPPLY'],
                             filters = [("BENE_ID", "in", unique_bene), ("MSIS_ID", "in", unique_msis),("CLM_ID", "in", unique_clm),("STATE_CD", "in", [state])]).add_suffix('_line').drop_duplicates()
        print('read in rx_line'+ str(rx_line.shape))
        rx_line.dropna(subset=['NDC_line'], inplace=True)
        rx_line.sort_values(by=['LINE_MDCD_PD_AMT_line'], inplace=True)
        print(rx_line.columns)
        #Drop duplicates
        rx_line.drop_duplicates(subset=['BENE_ID_line','MSIS_ID_line','CLM_ID_line','STATE_CD_line', 'SUBMTG_STATE_CD_line'], inplace=True)
        print('RX line shape after duplicate drop'+str(rx_line.shape))

        #--------------------------------------------------------------------
        #Step 1.21 - Merge line to main and use info to fill nas
        #--------------------------------------------------------------------
        #Merge line to main data
        #Merge non-null bene_id df to ps
        print("cms1 shape before line join: " + str(cms1.shape))
        size_cms_b4_line=cms1.shape[0] #save the row count of original cms df
        print("line shape before join: " + str(rx_line.shape))
        cms1=cms1.merge(rx_line, left_on=['CLM_ID'], 
                       right_on=['CLM_ID_line'],how="left").drop_duplicates()
        size_cms_line=cms1.shape[0]#save the row count of cms df after join to line
        print("cms1 shape after join to rx_line: " + str(cms1.shape))

        #ensure there is no row duplication
        assert size_cms_b4_line == size_cms_line, "cms row count changed after merge to line file"

        #Fill nas from cms with info from line
        cms1['BENE_ID']=cms1['BENE_ID'].fillna(cms1['BENE_ID_line'])
        cms1['RX_FILL_DT']=pd.to_datetime(cms1['RX_FILL_DT'], errors = 'coerce')
        cms1['RX_FILL_DT_line']=pd.to_datetime(cms1['RX_FILL_DT_line'], errors = 'coerce')
        cms1['RX_FILL_DT']=cms1['RX_FILL_DT'].fillna(cms1['RX_FILL_DT_line'])
        cms1['STATE_CD']=cms1['STATE_CD'].fillna(cms1['STATE_CD_line'])
        cms1['SUBMTG_STATE_CD']=cms1['SUBMTG_STATE_CD'].fillna(cms1['SUBMTG_STATE_CD_line'])
    
    elif subdataset == 'ot_taf':
        #--------------------------------------------------------------------
        #Step 1.30 - read in line file which has dental info
        #--------------------------------------------------------------------
        #Bring in line file  #Needs to get aggregated separetly
        if state !='-1':
            filter1 = [("STATE_CD", "=", state),("TOS_CD","in",cd.dv_tos),
                                        ("CLM_ID", "in", unique_clm),
                                       ]
        elif state == '-1':
            filter1 = [("TOS_CD","in",cd.dv_tos),("CLM_ID", "in", unique_clm)]
        ot_line = pd.read_parquet(str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ot_taf_line)+ str(yr)+'.parquet',
                                 columns = ['BENE_ID','MSIS_ID','CLM_ID','STATE_CD', 'SUBMTG_STATE_CD','TOS_CD','LINE_PRCDR_CD'],
                             filters = filter1).drop_duplicates().add_suffix('_line')
        print('read in line. Shape:'+str(ot_line.shape))

        #Create dental indicator and drop tos_cd and duplicates
        ot_line['dv_ind']=1
        #Create acause for dental claims
        ot_line['acause']=ot_line['TOS_CD_line'].map(cd.mdcd_dv_acause).fillna('_gc')
        #Drop duplicates
        ot_line.drop_duplicates(subset=['BENE_ID_line','MSIS_ID_line','CLM_ID_line','STATE_CD_line', 'SUBMTG_STATE_CD_line','dv_ind'], inplace=True)
        print('OT line shape after dental indicator and duplicate drop filter'+str(ot_line.shape))

        #--------------------------------------------------------------------
        #Step 1.31 - Merge line to main and use info to fill nas
        #--------------------------------------------------------------------
        #Merge line to main data
        #Merge non-null bene_id df to ps
        size_cms_b4_line=cms1.shape[0] #save the row count of original cms df
        print("cms1 shape before join: " + str(cms1.shape))
        print("line shape before join: " + str(ot_line.shape))
        cms1=cms1.merge(ot_line, left_on=['CLM_ID','BENE_ID','SUBMTG_STATE_CD','MSIS_ID'], 
                       right_on=['CLM_ID_line','BENE_ID_line','SUBMTG_STATE_CD_line','MSIS_ID_line'],how="left").drop_duplicates()
        size_cms_line=cms1.shape[0]#save the row count of cms df after join to line
        cms1['dv_ind'].fillna(0,inplace=True)
        print("cms1 shape after join to ot_line: " + str(cms1.shape))

        #ensure there is no row duplication
        assert size_cms_b4_line == size_cms_line, "cms row count changed after merge to line file"

    #--------------------------------------------------------------------
    #Step 2 - read in ps (aka denominator aka mbsf) data - filtering on bene 
    #--------------------------------------------------------------------

    if state !='-1':
        mdcd_path = str(path.path_mdcd_taf_denom) +'/year_id='+str(yr)+'/STATE_CD='+state
        filter1 = [("BENE_ID", "in", unique_bene), ("MSIS_ID", "in", unique_msis)]
    elif state == '-1':
        mdcd_path = str(path.path_mdcd_taf_denom) +'/year_id='+str(yr)
        filter1 = [("BENE_ID", "in", unique_bene)] #Sometimes has no MSIS_CODES
    
    ps=pd.read_parquet(mdcd_path,
                       filters = filter1).drop_duplicates()
    ps.rename(columns={'BIRTH_DT':'BIRTH_DT_ps'}, inplace=True)
    ps['year_id']=yr
    if state !='-1':
        ps['STATE_CD']=state
    print('demographic data read in')
    print(ps.shape)

    #--------------------------------------------------#
    #--Step 3-  Merge MDCD taf and PS dataframes-------#
    #--------------------------------------------------#
    if subdataset != 'rx_taf':
        #Extract service begin month
        cms1['SRVC_BGN_DT']=pd.to_datetime(cms1['SRVC_BGN_DT'], errors = 'coerce')
        if "ADMSN_DT" in cms1.columns:
            cms1['ADMSN_DT']=pd.to_datetime(cms1['ADMSN_DT'], errors = 'coerce')
            cms1['SRVC_BGN_DT']=cms1['SRVC_BGN_DT'].fillna(cms1['ADMSN_DT'])
        cms1=pf.switch_year_digits(df=cms1, date_column = 'SRVC_BGN_DT')
        cms1['srv_mo']=cms1['SRVC_BGN_DT'].dt.month

    elif subdataset == 'rx_taf':
        #Extract service begin month
        cms1['RX_FILL_DT']=pd.to_datetime(cms1['RX_FILL_DT'], errors = 'coerce')
        cms1['PRSCRBD_DT']=pd.to_datetime(cms1['PRSCRBD_DT'], errors = 'coerce')
        cms1['RX_FILL_DT']=cms1.RX_FILL_DT.fillna(cms1.PRSCRBD_DT)
        cms1['srv_mo']=cms1['RX_FILL_DT'].dt.month
        
    elif subdataset == 'ot_taf':
        #Extract service begin month
        cms1['SRVC_BGN_DT']=pd.to_datetime(cms1['SRVC_BGN_DT'], errors = 'coerce')
        cms1['srv_mo']=cms1['SRVC_BGN_DT'].dt.month
        
    cms1['year_id']=yr
    #Split cms data into null bene id and non-null bene-id
    cms1_null=cms1.loc[cms1['BENE_ID'].isnull()].copy()
    cms1.dropna(subset=['BENE_ID'], inplace= True)

    #Merge non-null bene_id df to ps #If not joining on MSIS_id there is duplication
    print("non-null cms1 shape before join: " + str(cms1.shape))
    size_cms_b4_ps=cms1.shape[0]
    print("ps shape before join: " + str(ps.shape))
    cms1=cms1.merge(ps, left_on=['BENE_ID','MSIS_ID','year_id','srv_mo','STATE_CD'], 
                   right_on=['BENE_ID','MSIS_ID','year_id','mnth','STATE_CD'],how="left").drop_duplicates() #state
    #Sort df by mcnty and take the first so the non-null mcnty is selected
    cms1.sort_values(by='mcnty', inplace=True)
    cms1.drop_duplicates(subset = cms_cols, keep = 'first', inplace = True) 
    size_cms_ps=cms1.shape[0]
    print("cms1 shape after join to ps: " + str(cms1.shape))
    #ensure there is no row duplication
    assert size_cms_b4_ps == size_cms_ps, "cms row count changed after merge to ps file"

    #Merge null bene_id df to ps
    print("Null cms1 shape before join: " + str(cms1_null.shape))
    size_cms_null_b4_ps=cms1_null.shape[0]
    print("ps shape before join: " + str(ps.shape))
    cms1_null=cms1_null.merge(ps, left_on=['MSIS_ID','STATE_CD','year_id','srv_mo'], 
                   right_on=['MSIS_ID','STATE_CD','year_id','mnth'],how="left").drop_duplicates()
    #Sort df by mcnty and take the first so the non-null mcnty is selected
    cms1_null.sort_values(by='mcnty', inplace=True)
    cms1_null.drop_duplicates(subset = cms_cols, keep = 'first', inplace = True) 
    cms_shape3=cms1_null.shape
    size_cms_null_ps=cms1_null.shape[0]
    print("cms1 shape after join to ps: " + str(cms_shape3))

    #ensure there is no row duplication
    assert size_cms_null_b4_ps == size_cms_null_ps, "Null cms row count changed after merge to ps file"

    #Concat both cms tables together!
    print("Shape cms1 table" + str(cms1.shape))
    print("Shape cms1_null table" + str(cms1_null.shape))
    cms1=pd.concat([cms1,cms1_null], axis=0)
    print(cms1.shape)

    ######################################################################
    #---------------------Step 4 - COLUMN FORMATTING---------------------#
    ######################################################################
    #---------------------------------------------------------------------------------------------------------------------------#
    #Step 4.1: Create date related columns. Convert date columns to date type so age/los can be calculated and year's extracted-#
    #---------------------------------------------------------------------------------------------------------------------------#
    #Convert date columns to datetime type
    #Note - some birth dates from claims are off by 100 years
    if subdataset in ['ip_taf','ltc_taf']:
        date_cols=['BIRTH_DT','BIRTH_DT_ps','DSCHRG_DT','SRVC_BGN_DT','SRVC_END_DT','ADMSN_DT','MDCD_PD_DT']
    elif subdataset == 'rx_taf':
        date_cols=['BIRTH_DT','BIRTH_DT_ps','PRSCRBD_DT','RX_FILL_DT']
    elif subdataset == 'ot_taf':
        date_cols=['BIRTH_DT','BIRTH_DT_ps','SRVC_BGN_DT','SRVC_END_DT','MDCD_PD_DT','OT_FIL_DT']
    cms1[date_cols]=cms1[date_cols].replace('31DEC9999', np.nan).apply(pd.to_datetime, errors = 'coerce')
    
    if "ADMSN_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'ADMSN_DT')
    if "DSCHRG_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'DSCHRG_DT')
    if "SRVC_BGN_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'SRVC_BGN_DT')
    if "SRVC_END_DT" in cms1.columns:
        cms1=pf.switch_year_digits(df=cms1, date_column = 'SRVC_END_DT')

    #fill in na's of columns w/ values from similar columns
    if subdataset in ['ip_taf','ltc_taf']:
        cms1['DSCHRG_DT1']=cms1.DSCHRG_DT.fillna(cms1.SRVC_END_DT)
        cms1['ADMSN_DT1']=cms1.ADMSN_DT.fillna(cms1.SRVC_BGN_DT)
    elif subdataset == 'rx_taf':
        cms1['RX_FILL_DT']=cms1.RX_FILL_DT.fillna(cms1.PRSCRBD_DT)
        

    #-------AGE----------------------------#
    cms1=pf.age_format(dataset='MDCD', df=cms1)

    #LOS #Division removes word days 
    if subdataset in ['ip_taf','ltc_taf']: 
        cms1['los']=((cms1['DSCHRG_DT']-cms1['ADMSN_DT'])/ np.timedelta64(1, 'D')).fillna(-1).astype(int)
        cms1['los_service']=((cms1['SRVC_END_DT']-cms1['SRVC_BGN_DT'])/ np.timedelta64(1, 'D')).fillna(-1).astype(int)
        if subdataset == 'ip_taf':
            cms1['los_days']=cms1['NCVRD_DAYS']+cms1['CVRD_DAYS']
        elif subdataset == 'ltc_taf':
            cms1['los_days']=cms1['NCVRD_DAYS']+cms1['CVRD_DAYS_ICF_IID']+cms1['CVRD_DAYS_NF']+cms1['CVRD_DAYS_IP_PSYCH']
        cms1['los']=cms1['los'].fillna(cms1['los_days'])
        cms1['los']=cms1['los'].fillna(cms1['los_service'])
    elif subdataset == 'rx_taf':
        cms1['los']=np.nan
    elif subdataset in ['ot_taf']:
        cms1['los']=((cms1['SRVC_END_DT']-cms1['SRVC_BGN_DT'])/ np.timedelta64(1, 'D')).fillna(-1).astype(int)

    cms1['year_id']=yr
    if subdataset in ['ip_taf','ltc_taf']:
        #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
        cms1['year_adm']=cms1['ADMSN_DT'].dt.year
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
        cms1['year_dchg']=cms1['DSCHRG_DT1'].dt.year
        #year claim end year
        cms1['year_clm']=cms1['SRVC_END_DT'].dt.year
    elif subdataset == 'rx_taf':
        #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
        cms1['year_adm']=cms1['RX_FILL_DT'].dt.year
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
        cms1['year_dchg']=cms1['RX_FILL_DT'].dt.year
        #year claim end year
        cms1['year_clm']=cms1['RX_FILL_DT'].dt.year
    elif subdataset == 'ot_taf':
        #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
        cms1['year_adm']=cms1['SRVC_BGN_DT'].dt.year
        #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
        cms1['year_dchg']=cms1['SRVC_END_DT'].dt.year
        #year claim end year
        cms1['year_clm']=cms1['SRVC_BGN_DT'].dt.year
    print("date formatting completed") 

    #---------------------------------------------------------------------------------------------------------------------------
    #--------------Step 4.2: Create demographic related columns - sex, race, location info--------------------------------------
    #---------------------------------------------------------------------------------------------------------------------------
    #CMS sex: 0=unk, 1=male, 2=female
    cms1=pf.sex_format(dataset = 'MDCD_TAF', subdataset = '', df=cms1)

    # Race
    cms1=pf.race_format(dataset = 'MDCD_TAF',subdataset='', df=cms1)

    ###-----Geographic Info-----##
    if subdataset in ['ot_taf','rx_taf']: #st_resi and st_serv come from PS
        cms1['st_num_serv']=cms1['SUBMTG_STATE_CD']
    cms1=pf.fips_state_format(cms1, ps_fips_col = 'BENE_STATE_CD', claim_fips_col = 'SUBMTG_STATE_CD', claim_abv_col = 'state')
    cms1.rename(columns = {'mcnty': 'mcnty_resi'}, inplace = True)

    #4) Zip codes - some US zip codes have leading 0, so need to use str dtype and zero pad left side
    chia = 0
    cms1=pf.zip_format(chia = chia, dataset='MDCD_TAF', subdataset='', df=cms1)

    print("demographic formatting completed")

    #---------------------------------------------------------------------------#
    #------------Step 4.3: Create payer and payment related columns-------------#
    #---------------------------------------------------------------------------#
    #-------------------------------------PAYERS---------------------------------------------------#
    #turn spaces into nan
    dollar_cols=['BILLED_AMT','MDCD_PD_AMT','MDCR_PD_AMT','OTHR_INSRNC_PD_AMT','TP_PD_AMT','COINSRNC_AMT','COPAY_AMT','DDCTBL_AMT',
              'MDCR_DDCTBL_PD_AMT','MDCR_COINSRNC_PD_AMT']
    if subdataset in ['rx_taf','ot_taf']:
        dollar_cols.remove('MDCR_PD_AMT')
    cms1[dollar_cols]=cms1[dollar_cols].replace('', np.nan).astype(float)

    #-------Create Payer Columns--------------------#
    cms1['mdcr_chg_amt']=np.nan
    cms1['mdcd_chg_amt']=np.nan
    cms1['priv_chg_amt']=np.nan
    cms1['oop_chg_amt']=np.nan
    cms1['oth_chg_amt']=np.nan
    cms1['tot_chg_amt']= cms1['BILLED_AMT']
    
    if subdataset in ['ip_taf','ltc_taf']:
        cms1['mdcr_pay_amt']=cms1['MDCR_PD_AMT']
    elif subdataset in ['rx_taf','ot_taf']:
        cms1['mdcr_pay_amt']=np.nan
    cms1['mdcd_pay_amt']=cms1['MDCD_PD_AMT']
    cms1['priv_pay_amt']=np.nan
    cms1['oop_pay_amt']=cms1['COINSRNC_AMT']+cms1['COPAY_AMT']+cms1['DDCTBL_AMT']
    cms1['oth_pay_amt']=np.nan
    cms1['tot_pay_amt']= np.nan 

    #-------Create Payer Order--------------------#
    pay1_conditions = [(cms1['mdcr_pay_amt']>0)|(cms1['CROSSOVER_CLM_IND']==1), #MDCR
                       (cms1['OTHR_INSRNC_IND']==1) | (cms1['TP_PD_AMT']>0), #OTH
                       cms1['mdcd_pay_amt']>0, 
                        cms1['tot_chg_amt']==0] #NC
    pay1_outputs = [1,15,3,20]
    cms1['pri_payer']=np.select(pay1_conditions, pay1_outputs, 21).astype('int8')

    #PMT_1 - these become floats
    pmt1_conditions = [cms1['pri_payer']==1,
                       cms1['pri_payer']==3,
                       cms1['pri_payer']==15,
                       cms1['pri_payer']==20]
    pmt1_outputs=[cms1['mdcr_pay_amt'], cms1['mdcd_pay_amt'],cms1['TP_PD_AMT'],0]
    cms1['pmt_1']=np.select(pmt1_conditions, pmt1_outputs, np.nan)

    #payer_2
    pay2_conditions = [(cms1['pri_payer']==1) & (cms1['OTHR_INSRNC_IND']==1) | (cms1['TP_PD_AMT']>0), 
                       (cms1['pri_payer']==15) &cms1['mdcd_pay_amt']>0, #OTH
                        cms1['tot_chg_amt']==0] #NC
    pay2_outputs = [15,3,20]
    cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, 21).astype('int8')

    #PMT_2 - these become floats
    pmt2_conditions = [cms1['payer_2']==3,
                       cms1['payer_2']==15,
                       cms1['payer_2']==20]
    pmt2_outputs=[cms1['mdcd_pay_amt'],cms1['TP_PD_AMT'],0]
    cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)

    #payer_3
    pay3_conditions = [(cms1['payer_2']==15) &cms1['mdcd_pay_amt']>0, #OTH
                        cms1['tot_chg_amt']==0] #NC
    pay3_outputs = [3,20]
    cms1['payer_3']=np.select(pay3_conditions, pay3_outputs, 21).astype('int8')

    #PMT_3 - these become floats
    pmt3_conditions = [cms1['payer_2']==3,
                       cms1['payer_2']==20]
    pmt3_outputs=[cms1['mdcd_pay_amt'],0]
    cms1['pmt_3']=np.select(pmt3_conditions, pmt3_outputs, np.nan)

    #Create managed care indicator
    cms1= pf.mc_format_taf(dataset='MDCD_TAF', df=cms1, state=state, clm_type_col = 'CLM_TYPE_CD') 

    print('pay info completed')

    #---------------------------------------------------------------------------#
    #---------------Step 5: Create other columns TOC,  etc--------------#
    #---------------------------------------------------------------------------#
    #Type of care
    if subdataset not in 'ot_taf':
        cms1=pf.toc_taf_format(cms1, subdataset=subdataset, dv_col='ADMTG_PRVDR_SPCLTY_CD') 
        
    if subdataset in 'ot_taf': 
        #Type of care - SPECIAL TOC FOR OT AND DENTAL!!
        #Create type of care mapping
        cms1['POS_CD']=cms1['POS_CD'].astype(str)
        toc_keys = list(cd.mdcd_ot_toc.keys())#cd.
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('[A-Za-z]', '0')
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('-', '0')
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('?', '0')
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('#', '0')
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('*', '0')
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('\\', '0') #'1\\'
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('0`', '0') 
        cms1['POS_CD'] = cms1['POS_CD'].str.replace('_', '0') 
        cms1['POS_CD'] = cms1['POS_CD'].str.replace(']', '0') 
        cms1['POS_CD']=np.where(cms1['POS_CD']=='', '-1',cms1['POS_CD'])
        cms1['POS_CD']=cms1['POS_CD'].astype(int)
        toc_keys = list(cd.mdcd_ot_toc.keys())#cd.
        cms1['toc'] = np.where(cms1['POS_CD'].isin(toc_keys), cms1['POS_CD'],-1) 
        cms1['toc']=cms1['toc'].map(lambda x: cd.mdcd_ot_toc[x][0]).fillna("UNK")
        #Add dentist
        cms1['toc']=np.where(cms1['dv_ind']==1, 'DV', cms1['toc'])
        #HH
        cms1['toc']=np.where(cms1['HLTH_HOME_PRVDR_IND']==1, 'HH', cms1['toc'])

        cms1['toc']=np.where((cms1['toc']=='OTH')|(cms1['toc']=='UNK'),cms1['BLG_PRVDR_TYPE_CD'].map(lambda x: cd.taf_ot_type_cd_dict[x][0]).fillna("OTH"), cms1['toc'])
        cms1['toc']=np.where((cms1['toc']=='OTH')|(cms1['toc']=='UNK'),cms1['BLG_PRVDR_SPCLTY_CD'].map(lambda x: cd.taf_ot_spc_cd_dict[x][0]).fillna("OTH"), cms1['toc'])

        cms1['BILL_TYPE_CD'].replace('', pd.NA, inplace=True)
        # #extract 2nd and 3rd digits for toc mapping
        cms1['BILL_TYPE_CD_23']=cms1['BILL_TYPE_CD'].str[1:3].fillna('-1').astype(str)
        cms1['toc']=np.where((cms1['toc']=='UNK')|(cms1['toc']=='OTH'),cms1['BILL_TYPE_CD_23'].map(cd.taf_ot_bill_cd_dict).fillna('UNK'), cms1['toc'])
        cms1.drop(columns=['BILL_TYPE_CD_23'], axis=1, inplace=True)   

    #code_system_id: icd-9 or icd-10. 2015 transitioned to icd-10 #1=icd9, 2=icd10
    if subdataset in ['ip_taf','ltc_taf']:
        cms1= pf.code_system_format(cms1, date_col='DSCHRG_DT') 
    elif subdataset == 'rx_taf':
        cms1=pf.code_system_format(cms1, date_col='PRSCRBD_DT')
    elif subdataset == 'ot_taf':
        cms1=pf.code_system_format(cms1, date_col='SRVC_END_DT')
    
    cms1 = pf.dual_format(dataset='MDCD_TAF', df=cms1)
    
    #add in facility vs professional indicator
    if subdataset in ['ip_taf','ltc_taf']:
        cms1['fac_prof_ind']='F'
    elif subdataset == 'rx_taf':
        cms1['fac_prof_ind']=np.nan
    elif subdataset == 'ot_taf':
        #The table that gets pulled in is created in stage_2/cms/mdcd_fac_prof/mdcd_fac_prof.R
        fp_path = "FILEPATH"
        df_fp = pd.read_parquet(fp_path,
                               filters = [('CLM_ID','in',unique_clm),
                                         ('BENE_ID','in',unique_bene),
                                         ('year','=',yr)],
                               columns = ['BENE_ID','CLM_ID','fac_prof_ind']).drop_duplicates()
        
        #Join df_fp to main dataset
        #Merge non-null bene_id df to ps #If not joining on MSIS_id there is duplication
        print("cms1 shape before fp join: " + str(cms1.shape))
        size_cms_b4_fp=cms1.shape[0]
        print("fp shape before join: " + str(df_fp.shape))
        cms1=cms1.merge(df_fp, left_on=['BENE_ID','CLM_ID'], 
                       right_on=['BENE_ID','CLM_ID'],how="left").drop_duplicates() #state 
        size_cms_fp=cms1.shape[0]
        print("cms1 shape after join to ps: " + str(cms1.shape))
        #ensure there is no row duplication
        assert size_cms_b4_fp == size_cms_fp, "cms row count changed after merge to ps file"

    print('Misc formatting completed')
        

    #---------------------------------------------------------------------------#
    #--------Step 7: Rename columns that don't need other processing----------#
    #---------------------------------------------------------------------------#
    cms1.rename(columns = {'BENE_ID':'bene_id',
                        'CLM_ID': 'claim_id'}, inplace = True)
    if subdataset in 'rx_taf':
        cms1.rename(columns = {'DAYS_SUPPLY_line': 'days_supply'}, inplace = True)
    cms1.columns = cms1.columns.str.replace('DGNS_CD_' , 'dx_')

    #------------------------------------------------------------------------------------#
    #--------Step 8: Change datatype of columns to mimize df size for storage----------#
    #------------------------------------------------------------------------------------#
    #Create parition column to split up big datasets even more
    cms1['partition']=np.random.randint(1, 6, cms1.shape[0]) #adds value between 1 and 5 roughly evenly
    dollar_cols = dollar_cols + ['mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
                       'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
                       'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt',
                                'pmt_1','pmt_2','pmt_3']

    #Specific issues with these columns
    cms1['sex_id']=cms1['sex_id'].fillna(-1).astype('int8')
    cms1['age']=cms1['age'].fillna(-1)
    cms1['year_id']=yr
    cms1['tot_chg_amt']=cms1['tot_chg_amt'].astype(float)#Coming through as a string

    cms1=pf.datatype_format(dataset='MDCD_TAF', subdataset=subdataset, df=cms1)
    
    size_cms_end=cms1.shape[0]
    assert size_cms_start -5 <= size_cms_end <= size_cms_start +5, "CMS row count at the end is more than +/- the row count from the beginning"

    #Save out parquet
    cms1.astype({
        'sex_id':"int8",
        'age':"int32",
    }).to_parquet(outpath, 
                  partition_cols=['year_id','st_resi'],
                  basename_template=f"format_{yr}_{state}_{subdataset}_{part}_{p_5}_{{i}}",
                  existing_data_behavior='overwrite_or_ignore')
    print('done')

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
        "--subdataset",
        type=str,
        required=True
        )
    parser.add_argument(
        "--part",
        type=str,
        required=True
        )
    
    args = vars(parser.parse_args())
    stage_2(args['outpath'],args['state'],args['yr'],args['subdataset'],args['part']) 

