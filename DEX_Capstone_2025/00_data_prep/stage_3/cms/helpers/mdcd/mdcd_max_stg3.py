#####################################################################################
##PURPOSE: CMS Medicaid MAX Stage 3 - Helper script
##AUTHOR(S): Meera Beauchamp, Drew DeJarnatt
#####################################################################################
#Import packages
import pandas as pd
import numpy as np
import pyarrow.dataset as ds
import pyarrow as py
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
from 00_data_prep.stage_3.cms.constants import cms_dict as cd


def stage_3(outpath, state, yr,sex,toc, subdataset):
    print(outpath)
    print(subdataset)
    print(yr)
    print(state)
    print(sex)
    print(toc)

    cms_cols=['bene_id','MSIS_ID','year_adm','year_clm','year_dchg','los', 
                  'age','race_cd_raw','race_cd_imp','sex_id','st_num_resi','st_loc_id_resi','mcnty_resi',
                  'cnty_loc_id_resi','zip_5_resi',
                  'tot_chg_amt','toc', 'priv_ind','mdcr_ind','mdcd_ind','no_mdcd_ind','MDCD_PYMT_AMT','TP_PYMT_AMT',
                  'pri_payer','payer_2','payer_3','pmt_1','pmt_2','pmt_3','mc_ind', 'mc_status',
                  'PHP_TYPE','EL_MDCR_DUAL_MO','dual_ind','part_dual_ind', 'full_dual_ind','SRVC_BGN_DT',
                  'SRVC_END_DT','service_date','discharge_date',
              'MDCR_COINSUR_PYMT_AMT','MDCR_DED_PYMT_AMT','code_system','fac_prof_ind'] 

    #add dataset specific columns and list paths
    if subdataset == 'ip_max':
        path1= str(paths.mdcd_ip_max_stage_2_dir )
        dx_cols = ["dx_" + str(x + 1) for x in range(9)]
        cms_cols = cms_cols + dx_cols 
        
    elif subdataset =='ot_max':
        path1= str(paths.mdcd_ot_max_stage_2_dir)
        cms_cols = cms_cols + ['PLC_OF_SRVC_CD','CLTC_FLAG','MSIS_TOS','MAX_TOS']
        if toc != 'DV':
            cms_cols = cms_cols + ['dx_1','dx_2']
        elif toc == 'DV':
            cms_cols = cms_cols + ['PRCDR_CD']
    
    path1 = path1+ '/year_id='+str(yr)+'/st_resi='+state
    
    # ## get schema replace
    data_schema = ds.dataset(path1, partitioning="hive").schema

    # Replacing any null-column schemas with strings. The default schema is interpreted from the first file,
    # Which can result in improper interpretations of column types. These are usually handled by pyarrow
    # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
    # On later files, which will fail if any values in the column are not null. Interpreting as string
    # changes all nulls to None types.
    null_idxs = [idx for idx, datatype in enumerate(data_schema.types) if datatype == py.null()]
    
    if subdataset == 'ip_max':
        null_idxs=null_idxs+[34,69,75] #Add this bc race_cd read in as a dict
        for idx in null_idxs:
            data_schema = data_schema.set(idx, data_schema.field(idx).with_type(py.string()))

    #-----------------------------------------------------#
    #-------------Read in CMS DATA FORMATTING-------------#
    #-----------------------------------------------------#
    #Read in cms after data/column formatting 
    cms1=pd.read_parquet(path1,
                        use_legacy_dataset=False,
                         schema=data_schema,
                         columns = cms_cols,
                        filters = [("sex_id", "=", sex),("toc", "=", toc)],
                        )
    print('data read in')
    print(cms1.shape)
    #If there is no data (some state, toc combos don't have sex =-1, quit script to prevent errors and reruns)
    if cms1.shape[0]==0:
        print("no data:" + str(cms1.shape))
        quit()
    else:
        print("non empty df, continue")
    #columns for c2e
    cms1['year_id']=yr
    cms1['st_resi']=state
    cms1['adm_flag']=np.where(cms1['year_id']==cms1['year_adm'],1,0)
    
    cms1['CLM_FROM_DT']=cms1['SRVC_BGN_DT'] #Naming to match mdcr
    
    print(cms1.shape)
    
    if subdataset == 'ot_max':

        cms1['CLTC_FLAG'].fillna(-1, inplace=True)
        cms1['cltc_ind']=np.where(cms1['CLTC_FLAG'].isin(cd.cltc_list),1,0)
        print("managed care and dual status done")

    #------------Age Binning-------------------------------#
    cms1 = cf.age_bin(cms1, age_col = 'age')
    print('age bin done')
    
    #-----------Create claim id----------------------------#
    cms1 = cf.gen_uuid(cms1)
    #Create a unique id bc bene_id can be null
    cms1['bene_id']=cms1.bene_id.fillna('-1')
    print('uuid/encounter is done') 

    cms1.drop(columns=["age_group_years_end"], inplace=True)

    #----------------w2l conversion-----------------------------------------#
    if toc != 'DV':
        cms1 = cf.w2l(cms1)
        print('shape after wide to long'+str(cms1.shape))
        print('wide to long done')
    elif toc == 'DV':
        print(toc)
        cms1['primary_cause']=1
        
        #Read in dental procedure code to acause map
        dv_map=pd.read_csv('FILEPATH',
                          usecols = ['code','acause'])
        #Merge map to data
        size_cms_b4_dv=cms1.shape[0]
        cms1['PRCDR_CD']=cms1['PRCDR_CD'].astype(str)
        cms1=cms1.merge(dv_map, left_on=['PRCDR_CD'], 
                           right_on=['code'],how="left")
        size_cms_dv=cms1.shape[0]
        assert size_cms_b4_dv == size_cms_dv, "cms row count changed after merge to dv file"
        #If acause is null fill randomly w/ _oral or exp_well_dental
        cms1['acause1']=np.nan
        cms1['acause1']=cms1['acause1'].apply(lambda l: np.random.choice(['_oral', 'exp_well_dental']))
        cms1['acause'].fillna(cms1['acause1'], inplace=True)
        cms1.drop(columns=["acause1"], inplace=True)
    #Remove rows w/ no dx or implausible los
    cms1, df_dim=cf.drop_rows(df = cms1,toc=toc, year=yr )#cf
    print('shape after dropping rows:'+str(cms1.shape))

    #-------Create Payer Columns--------------------#
    #Charge amounts
    cms1['mdcr_chg_amt']=np.nan
    cms1['mdcd_chg_amt']=np.nan
    cms1['priv_chg_amt']=np.nan
    cms1['oop_chg_amt']=np.nan
    cms1['oth_chg_amt']=np.nan
    #Paid amounts
    cms1['mdcr_pay_amt']=np.nan
    cms1.rename(columns = {'MDCD_PYMT_AMT':'mdcd_pay_amt'}, inplace = True)
    cms1['priv_pay_amt']=np.nan
    cms1['oop_pay_amt']=np.nan
    cms1['oth_pay_amt']=np.nan
    #tot_chg_amt - already exists
    cms1['tot_pay_amt']=np.nan
    print('pay done')

    #Create nid
    cms1 = cf.nid_create(df = cms1, dataset = 'MDCD_MAX')
    print('nid done')

    #####Save to parquet, partition by year, age, sex####
    partition_by = ["year_id", "age_group_years_start", "sex_id"]
    
    if subdataset == 'ot_max':
        partition_by = ['toc'] + partition_by
    

    # # change categorical type to int type for sex and year
    cms1['age_group_years_start']=cms1.age_group_years_start.fillna(-1)
    cms1['sex_id']=cms1.sex_id.fillna(-1)

    cms1.year_id = pd.to_numeric(cms1.year_id, errors='coerce')
    cms1.sex_id = pd.to_numeric(cms1.sex_id, errors='coerce')

    #fill NA as -1 for age_group_years_start and sex_id for partition
    cms1[partition_by].fillna(-1, inplace=True)

    #Reduce columns to smaller data type
    float_col=(cms1.select_dtypes(include=[float])).columns.values.tolist()
    pmt_col = ['mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
           'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
           'pmt_1','pmt_2','pmt_3','MDCR_COINSUR_PYMT_AMT','MDCR_DED_PYMT_AMT',
           'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt','TP_PYMT_AMT']
    float_col = [x for x in float_col if x not in pmt_col]

    cms1[float_col]=cms1[float_col].fillna(-1).astype('int')
    
    #Make sure expected columns exist
    #run time error will be raised if column missing
    required_cols = ['nid','year_id','year_adm','year_dchg','year_clm','bene_id','claim_id','age_group_years_start','age_group_id','sex_id',
                     'race_cd_raw','race_cd_imp','mcnty_resi','cnty_loc_id_resi','zip_5_resi','st_resi','code_system','service_date','discharge_date','toc',
                     'los','pri_payer','tot_pay_amt','mdcr_pay_amt','mdcd_pay_amt','priv_pay_amt','oop_pay_amt','oth_pay_amt','tot_chg_amt',
                     'mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mc_ind','dual_ind','fac_prof_ind','CLM_FROM_DT']
    cf.check_cols(df = cms1, required_cols=required_cols)

    if toc != 'DV':
        outpath = outpath
        cf.check_cols(df = cms1, required_cols=['dx','dx_level'])
    elif toc =='DV':
        if 'acause' not in cms1.columns:
            raise RuntimeError("acause not created for DV")
        today = str(date.today())
        outpath = 'FILEPATH'+ today+'.parquet'
        print(outpath)
        
    #Save out as parquet
    cms1.to_parquet(outpath, 
                  basename_template=f"mdcd_max_stg3_{yr}_{state}_{str(sex)}_{toc}_{{i}}.parquet",
                  partition_cols=partition_by, existing_data_behavior='overwrite_or_ignore')

    print(cms1.shape)
    
    #Save out dimensions
    df_dim['dataset']='MDCD'
    df_dim['subdataset']=subdataset
    df_dim['year_id']=yr
    #Save out as parquet
    today = str(date.today())
    df_dim.to_parquet('FILEPATH'+today+'.parquet', 
                  basename_template=f"{state}_{str(sex)}_{toc}_{{i}}.parquet",
                  partition_cols=['dataset','subdataset','year_id'],
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