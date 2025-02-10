#####################################################################################
##PURPOSE: CMS Medicaid MAX Stage 2 - Data Processing 
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

def stage_2(outpath, state, yr, subdataset):
    print(subdataset)
    print(state)
    print(yr)
    #Columns both datasets share
    cms_cols=['MSIS_ID','BENE_ID','YR_NUM','STATE_CD','SRVC_BGN_DT','SRVC_END_DT', 'EL_DOB',
          'PYMT_DT','TYPE_CLM_CD','PHP_VAL','MDCD_PYMT_AMT','TP_PYMT_AMT',
          'MDCR_COINSUR_PYMT_AMT','MDCR_DED_PYMT_AMT','CHRG_AMT','MSIS_TOS',
          'EL_RACE_ETHNCY_CD', 'EL_SEX_CD','MSIS_TOP','MAX_TOS',
          'PHP_TYPE','PHP_ID','TAXONOMY','NPI']
    #add dataset specific columns and list paths
    if subdataset == 'ip_max':
        cms_yr=str(path.path_mdcd)+ "/" +str(yr)+str(path.path_mdcd_ip)+ str(yr) + ".parquet"
        dx_cols = ["DIAG_CD_" + str(x + 1) for x in range(9)]
        cms_cols = cms_cols + dx_cols + ['ADMSN_DT','MDCD_CVRD_IP_DAYS']
        
    elif subdataset =='ot_max':
        cms_yr = str(path.path_mdcd) + "/" + str(yr) + str(path.path_mdcd_ot) + str(state.lower()) +'_ot_'+ str(yr) + ".parquet"
        cms_cols = cms_cols + ['DIAG_CD_1','DIAG_CD_2','PLC_OF_SRVC_CD','CLTC_FLAG','PRCDR_CD',
                              'SRVC_PRVDR_SPEC_CD']
        
    #--------------------------------------------------------------------
    #Step 1 - read in claims data, create unique list of bene ids 
    #--------------------------------------------------------------------
    if state != '-1':
        cms1=pd.read_parquet(cms_yr,
                         columns=cms_cols,
                         filters = [("STATE_CD", "in", [state])]).drop_duplicates()
    elif state == '-1':
        states=['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL',
              'GA','HI','IA','ID','IL','IN','KS','KY','LA','MA',
              'MD','ME','MI','MN','MO','MS','MT','NC','ND','NE',
              'NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI',
              'SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY']
        cms1=pd.read_parquet(cms_yr,
                         columns=cms_cols,
                         filters = [("STATE_CD", "not in", states)]).drop_duplicates()
    print("claims data read in")
    print(cms1.shape)
    cms1["TYPE_CLM_CD"]=cms1["TYPE_CLM_CD"].astype(str)
    cms1=cms1[cms1["TYPE_CLM_CD"].isin(['1','3','A','C','U','W'])]
    
    if (cms1.empty == True) and (state == '-1'): 
        print('No unknown states!')
        exit()

    # naming dob different from incoming ps file
    cms1.rename(columns={'EL_DOB': 'EL_DOB_claims'}, inplace=True)
    unique_bene=cms1['BENE_ID'].unique()
    unique_msis=cms1['MSIS_ID'].unique()
    
    if (len(unique_bene) == 0) and (state == '-1'): 
        #If no bene_ids, can't join to the PS to get demographic info
        print('There are no bene_ids!')
        exit()
    
    if subdataset == 'ip_max':
        chia = 0
        cms1=pf.fix_admin_date(dataset='MDCD', subdataset=subdataset, df=cms1, chia = chia)

    #--------------------------------------------------------------------
    #Step 2 - read in ps (aka denominator aka mbsf) data - filtering on bene 
    #--------------------------------------------------------------------
    if state !='-1':
        mdcd_path = str(path.path_mdcd_denom) + '/year_id='+str(yr)+ '/STATE_CD='+str(state)
    if state == '-1':
        mdcd_path = str(path.path_mdcd_denom) + '/year_id='+str(yr)
    ps=pd.read_parquet(mdcd_path,
                       filters = [[("BENE_ID", "in", unique_bene)],
                                            [("MSIS_ID", "in", unique_msis)]],
                       thrift_string_size_limit = 2147483647).drop_duplicates()
    ps['year_id']=yr
    ps['mnth'] = pd.to_numeric(ps['mnth'], errors='coerce').astype('Int64')
    if state != '-1':
        ps['STATE_CD']=state
    print('demographic data read in')
    print(ps.shape)
    print(ps.columns)

    #---------------------------------------------#
    #------Merge MDCD max and PS dataframes-------#
    #---------------------------------------------#
    #Extract service begin month

    cms1['srv_mo']=cms1['SRVC_BGN_DT'].dt.month.fillna(-1).astype('Int64')
    if subdataset == 'ip_max': 
        cms1['ADMSN_DT']=pd.to_datetime(cms1['ADMSN_DT'], format = "%Y%m%d", errors = 'coerce')
        cms1['admin_mo']=pd.to_numeric(cms1['ADMSN_DT'].dt.month, errors='coerce').astype('Int64')
        cms1['srv_mo']=cms1['admin_mo'].fillna(cms1['srv_mo'])
    
    print("cms1 shape before join: " + str(cms1.shape))
    size_cms_b4_ps=cms1.shape[0]
    print("ps shape before join: " + str(ps.shape))
    cms1=cms1.merge(ps, left_on=['BENE_ID','MSIS_ID','STATE_CD','YR_NUM','srv_mo'], 
                   right_on=['BENE_ID','MSIS_ID','STATE_CD','year_id','mnth'],how="left").drop_duplicates()
    print("cms1 shape after join to ps: " + str(cms1.shape))
    #Sort df by mcnty and take the first so the non-null mcnty is selected
    cms1.sort_values(by='mcnty', inplace=True)
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
    #Convert date columns to datetime type
    cms1['EL_DOB']=pd.to_datetime(cms1['EL_DOB'])
    cms1['EL_DOB_claims']=pd.to_datetime(cms1['EL_DOB_claims'], format = "%Y%m%d", errors = 'coerce')
    if subdataset == 'ip_max':
        cms1['ADMSN_DT']=cms1['ADMSN_DT'].fillna(cms1['SRVC_BGN_DT'])
        service_begin_col = 'ADMSN_DT'
    elif subdataset == 'ot_max':
        service_begin_col = 'SRVC_BGN_DT'
    cms1['SRVC_END_DT']=pd.to_datetime(cms1['SRVC_END_DT'], format = "%Y%m%d", errors = 'coerce')
    
    #Calculate age
    cms1=pf.age_format(dataset = 'MDCD', df = cms1)  

    #LOS 
    cms1['los']=(cms1['SRVC_END_DT']-cms1[service_begin_col])/ np.timedelta64(1, 'D')
    if subdataset == 'ip_max':
        cms1['los']=cms1['los'].fillna(cms1['MDCD_CVRD_IP_DAYS'])

    cms1['year_id']=yr
    #year_id_adm/CLM_ADMSN_DT aka year of admission, extract year from admission date
    cms1['year_adm']=cms1[service_begin_col].dt.year
    #year_id/NCH_BENE_DSCHRG_DT aka year of discharge
    cms1['year_dchg']=cms1['SRVC_END_DT'].dt.year
    #year claim end year
    cms1['year_clm']=cms1['SRVC_END_DT'].dt.year

    #---------------------------------------------------------------------------------------------------------------------------#
    #--------------Step 6.2: Create demographic related columns - sex, race, location info--------------------------------------#
    #---------------------------------------------------------------------------------------------------------------------------#
    cms1.rename(columns = {'mcnty': 'mcnty_resi',
                          'state': 'st_num_resi',
                          'EL_RSDNC_CNTY_CD_LTST':'cnty_loc_id_resi'}, inplace = True)

    #Create 3) st_location_id_resi = 3 digit fips id for state
    #Read in state csv and turn into dictionary - all fips info
    state_csv = pd.read_csv("FILEPATH")
    # Create dictionary for location_ids
    state_dict_location_id = dict(zip(state_csv.abbreviation, state_csv.location_id))
    cms1['st_loc_id_resi'] = cms1['st_resi'].fillna(-1).map(state_dict_location_id)
    # Introduced chia as an argument to this function when processing CHIA  MDCR data
    chia = 0
    cms1=pf.zip_format(dataset='MDCD_MAX', subdataset='', df=cms1, chia=chia)
                                         

    print("demographic formatting completed")

    #---------------------------------------------------------------------------#
    #--------------Step 4: Create payer and payment related columns-------------#
    #---------------------------------------------------------------------------#
    #Charge Amount
    cms1['tot_chg_amt']=np.where(cms1['CHRG_AMT']>cms1['PHP_VAL'],cms1['CHRG_AMT'],cms1['PHP_VAL'])

    #Create mdcd, mdcr and private insurance indicators
    cms1['priv_ind']=np.where(cms1['EL_PVT_INS_CD'].isin(['2','3','4']),1,0)
    cms1['mdcr_ind']=np.where(((cms1['MDCR_COINSUR_PYMT_AMT']>0)| (cms1['MDCR_DED_PYMT_AMT']>0)),1,
                    np.where(cms1['EL_MDCR_DUAL_MO'].isin(['1','2','3']),1,0))
    cms1['mdcd_ind']=np.where(((cms1['MDCD_PYMT_AMT']>0)|(cms1['MDCR_COINSUR_PYMT_AMT']>0)| (cms1['MDCR_DED_PYMT_AMT']>0) | (cms1['MAX_ELG_CD_MO'].isin(cd.mdcd_elg))),1,0)
    cms1 = pf.dual_format(dataset='MDCD', df=cms1)

    #-------------------------------------PAYERS---------------------------------------------------#
    pay1_conditions = [cms1['mdcr_ind']==1, #MDCR
                       (cms1['TP_PYMT_AMT']>0) & (cms1['priv_ind']==0), #OTH not anything
                       cms1['priv_ind']==1, #private
                       cms1['mdcd_ind']==1, #MDCD
                        cms1['tot_chg_amt']==0] #NC
    pay1_outputs = [1,19,2,3,20]
    cms1['pri_payer']=np.select(pay1_conditions, pay1_outputs, 21).astype('int8') 

    #payer_2
    pay2_conditions = [(cms1['pri_payer']==1) & (cms1['TP_PYMT_AMT']>0) & (cms1['priv_ind']==0),#OTH not anything
                       (cms1['pri_payer']==1) & (cms1['priv_ind']==1),
                       (cms1['pri_payer']==1) & (cms1['mdcd_ind']==1),
                       (cms1['pri_payer']==19) & (cms1['mdcd_ind']==1),
                       (cms1['pri_payer']==2) & (cms1['mdcd_ind']==1),
                       cms1['tot_chg_amt']==0]
    pay2_outputs=[19,2,3,3,3,20]
    cms1['payer_2']=np.select(pay2_conditions, pay2_outputs, 21).astype('int8')

    #payer_3
    pay3_conditions = [(cms1['payer_2']==2) & (cms1['mdcd_ind']==1),
                       (cms1['payer_2']==19) & (cms1['mdcd_ind']==1),
                       cms1['tot_chg_amt']==0]
    pay3_outputs=[3,3,20]
    cms1['payer_3']=np.select(pay3_conditions, pay3_outputs, 21).astype('int8')

    #Flag if none of the payers are MDCD
    cms1['no_mdcd_ind']=np.where((cms1['pri_payer']!=3) & (cms1['payer_2']!=3) & (cms1['payer_3']!=3), 1,0)

    #------------------------------------PAYMENTS--------------------------------------------------#
    #PMT_1 
    pmt1_conditions = [cms1['pri_payer']==1,
                       cms1['pri_payer']==19,
                       cms1['pri_payer']==2,
                       cms1['pri_payer']==3,
                       cms1['pri_payer']==20]
    pmt1_outputs=[np.nan, cms1['TP_PYMT_AMT'], cms1['TP_PYMT_AMT'],cms1['MDCD_PYMT_AMT'],0]
    cms1['pmt_1']=np.select(pmt1_conditions, pmt1_outputs, np.nan)

    #PMT_2
    pmt2_conditions = [cms1['payer_2']==19, 
                       cms1['payer_2']==2,
                       cms1['payer_2']==3,
                       cms1['payer_2']==20]
    pmt2_outputs=[cms1['TP_PYMT_AMT'], cms1['TP_PYMT_AMT'],cms1['MDCD_PYMT_AMT'],0]
    cms1['pmt_2']=np.select(pmt2_conditions, pmt2_outputs, np.nan)

    #PMT_3
    pmt3_conditions = [cms1['payer_3']==3,
                       cms1['payer_3']==20]
    pmt3_outputs=[cms1['MDCD_PYMT_AMT'],0]
    cms1['pmt_3']=np.select(pmt3_conditions, pmt3_outputs, np.nan)

    #---------------------------------------------------------------------------#
    #---------------Step 5: Create other columns TOC, Facility etc--------------#
    #---------------------------------------------------------------------------#
    #Create managed care indicator
    cms1= pf.mc_format_taf(dataset = 'MDCD_MAX', df = cms1, state=state)

    #Type of care and facility vs. professional indicator
    if subdataset == 'ip_max':
        cms1['toc']="IP" #Hardcoded to IP for dataset
        cms1['fac_prof_ind']='F'
    elif subdataset == 'ot_max':
        #Create type of care mapping
        toc_keys = list(cd.mdcd_ot_toc.keys())
        cms1['toc'] = np.where(cms1['PLC_OF_SRVC_CD'].isin(toc_keys), cms1['PLC_OF_SRVC_CD'],-1)
        cms1['toc']=cms1['toc'].map(lambda x: cd.mdcd_ot_toc[x][0]).fillna("OTH")
        #When TOS columns = 9, then DV
        cms1['toc']=np.where(cms1['MSIS_TOS']==9, 'DV', cms1['toc'])
        cms1['toc']=np.where(cms1['MAX_TOS']==9, 'DV', cms1['toc'])
        cms1['fac_prof_ind']=cms1['MAX_TOS'].map(lambda x: cd.msis_dict[x][2]).fillna("U")


    #code_system_id: icd-9 or icd-10. In 2015 transitioned to icd-10 #1=icd9, 2=icd10
    #need to use discharge date instead of admission date
    cms1['code_system']=np.where(cms1['SRVC_END_DT']<pd.to_datetime('2015-10-01'),"icd9","icd10")

    #---------------------------------------------------------------------------#
    #----------Step 6: Rename columns that don't need other processing----------#
    #---------------------------------------------------------------------------#
    cms1.rename(columns = {'BENE_ID':'bene_id'}, inplace = True) 
    if subdataset == 'ip_max':
        cms1['service_date']=cms1['ADMSN_DT']
    elif subdataset == 'ot_max':
        cms1['service_date']=cms1['SRVC_BGN_DT']
    cms1['discharge_date']=cms1['SRVC_END_DT']
    cms1.columns = cms1.columns.str.replace('DIAG_CD_' , 'dx_')

    #------------------------------------------------------------------------------------#
    #----------Step 7: Change datatype of columns to mimize df size for storage----------#
    #------------------------------------------------------------------------------------#
    cms1= pf.datatype_format(dataset='MDCD_MAX', subdataset='', df=cms1) 

    print(cms1.columns)
    print('data types complete')

    cms1.astype({
        'sex_id':"int8",
        'age':"int32",
        'st_num_resi':"float",
        'MDCR_COINSUR_PYMT_AMT':"float",
        'PHP_VAL':'float'

    }).to_parquet(outpath, 
                  partition_cols=['year_id','st_resi'],
                  basename_template=f"format_{yr}_{state}_{subdataset}_{{i}}",
                  existing_data_behavior='overwrite_or_ignore')

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
        "--subdataset",
        type=str,
        required=True
        )
    
    args = vars(parser.parse_args())
    stage_2(args['outpath'],args['state'],args['yr'],args['subdataset']) 