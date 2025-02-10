#Functions
# Author(s): Meera Beauchamp, Drew DeJarnatt
import numpy as np
import pandas as pd
from numpy.random import choice
from datetime import datetime
import pyarrow.dataset as ds
import pyarrow as py
import glob

# adding this to make importing these easier if using git worktree
import os
current_dir = os.getcwd()
repo_dir = current_dir.split('00_data_prep')[0]
import sys
sys.path.append(repo_dir)
from 00_data_prep.stage_2.cms.constants import cms_dict as cd

# -------------------------------------
# Replace schema
# -------------------------------------
def replace_schema(filepath, partitioning=None):

        # Loading in interpreted dataset schema from Pyarrow
        data_schema = ds.dataset(filepath, partitioning=partitioning).schema

        # Replacing any null-column schemas with strings. The default schema is interpreted from the first file,
        # Which can result in improper interpretations of column types. These are usually handled by pyarrow
        # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
        # On later files, which will fail if any values in the column are not null. Interpreting as string
        # changes all nulls to None types.
        null_idxs = [idx for idx, datatype in enumerate(data_schema.types) if datatype == py.null()]
        for idx in null_idxs:
            data_schema = data_schema.set(idx, data_schema.field(idx).with_type(py.string()))

        return data_schema

# ---------------------------
# Change schema
# ---------------------------
def change_col_schema(data_schema, col_type_dict):
    
    pa_types = {"null" : py.null(),
                "bool" : py.bool_(),
                "int" : py.int32(),
                "int8" : py.int8(),
                "int16" : py.int16(),
                "int32" : py.int32(),
                "int64" : py.int64(),
                "float" : py.float32(),
                "float32" : py.float32(),
                "float64" : py.float64(),
                "date" : py.date32(),
                "date32" : py.date32(),
                "date64" : py.date64(),
                "string" : py.string()}
    
    if any(v not in pa_types.keys() for v in col_type_dict.values()):
        raise ValueError("Invalid column type provided. Please provide valid column to cast to.")

    idx_type_dict = {data_schema.get_field_index(k):v for k,v in col_type_dict.items()}
    for idx, new_type in idx_type_dict.items():
        data_schema = data_schema.set(idx, data_schema.field(idx).with_type(pa_types[new_type]))
    
    return data_schema

# ----------------------------------------------------------------------------------------------------
#this function fixes wacky admission dates by replacing the yr with the year from the claim/service begin year
# ----------------------------------------------------------------------------------------------------
def fix_admin_date(dataset, subdataset, df, chia): #this function only works before the columns are converted to datetime format
    if dataset in ['MDCD','MDCD_TAF']:
        clm_col = 'SRVC_BGN_DT'
        admin_col= 'ADMSN_DT'
    elif dataset == 'MDCR':
        clm_col = 'CLM_FROM_DT'
        if subdataset in ['ip','nf','hha','ip_part_c','nf_part_c','hha_part_c']:
            admin_col= 'CLM_ADMSN_DT'
        elif subdataset == 'hosp':
            admin_col= 'CLM_HOSPC_START_DT_ID'
    if dataset == 'MDCD_TAF':
    #take just the year characters from the admission date
        df['admin_yr']=df[admin_col].str[5:9].fillna('-1')
        df['clm_yr']=df[clm_col].str[5:9].fillna('-1')
    elif dataset in ['MDCD','MDCR']:
        df[admin_col]=df[admin_col].astype(str)
        df[clm_col]=df[clm_col].astype(str)
        if dataset == 'MDCD' or chia == 1:
            df['admin_yr']=df[admin_col].str[0:4].fillna('-1')
            df['clm_yr']=df[clm_col].str[0:4].fillna('-1')
        elif dataset == 'MDCR' and chia == 0:
            df['admin_yr']=df[admin_col].str[5:9].fillna('-1')
            df['clm_yr']=df[clm_col].str[5:9].fillna('-1')

    df.loc[df["admin_yr"] == "", "admin_yr"] = -1
    df.loc[df["admin_yr"] == "nan", "admin_yr"] = -1
    df['admin_yr']=df['admin_yr'].fillna('-1').astype(int)

    #take just the year characters from the claim/service begin date
    df.loc[df["clm_yr"] == "", "clm_yr"] = -1
    df.loc[df["clm_yr"] == "nan", "clm_yr"] = -1
    df['clm_yr']=df['clm_yr'].fillna('-1').astype(int)
    this_yr = datetime.now().year
    df.loc[(df["admin_yr"] > this_yr) &(df["clm_yr"] != -1) , "admin_yr"] = df['clm_yr']
    if dataset == 'MDCD_TAF':
        df['admin_yr']=df['admin_yr'].astype(str)
        df[admin_col]=df[admin_col].str[0:5]+df["admin_yr"]
    elif dataset in ['MDCD', 'MDCR']:
        df['admin_yr']=df['admin_yr'].astype(str)
        if dataset == "MDCD" or chia == 1:
            df[admin_col]=np.where(df["admin_yr"]!='-1',df["admin_yr"]+df[admin_col].str[4:8],df[admin_col])
            print(df[admin_col].head())
            df.loc[df[admin_col] == "nan", admin_col] = '-1'
            df[admin_col]=df[admin_col].fillna('-1').astype(int)
            df[clm_col]=df[clm_col].fillna('-1').astype(int)
        else:
            df.loc[df[admin_col] == "nan", admin_col] = '-1'
    
    return df

# ----------------------------------------------------------------------------------------------------
#this function is to fix implausible years such as 2109 which should be 2019
# ----------------------------------------------------------------------------------------------------
def switch_year_digits(df, date_column):
    # Remove microseconds by using floor to reduce the precision to seconds 
    df[date_column] = df[date_column].dt.floor('S')
    # Extract the year from the date column
    years = df[date_column].dt.year
    # Convert years to string to manipulate digits
    years_str = years.astype(str)
    #remove .0 from 2016.0
    years_str= years_str.str.replace(r'\.0$', '', regex=True)
    # Identify rows where the second digit is 1
    mask = years_str.str[1] == '1'
    # Switch the second and third digits for those rows
    new_years_str = years_str[mask].str[0] + years_str[mask].str[2] + years_str[mask].str[1] + years_str[mask].str[3:]
    # Update the DataFrame with the new years where the mask is True
    df.loc[mask, date_column] = pd.to_datetime(new_years_str + df[date_column].dt.strftime('-%m-%d'))
    
    return df
# ----------------------------------------------------------------------------------------------------
# DATE/AGE: function to calc age, age is comes from ps, but maybe missing, if we have info to calculate in stage 2, we do and then
# fill the ps age column where missing with this new calculated age
# ----------------------------------------------------------------------------------------------------
def age_format(dataset, df):
    #claim_col should be los_end
    if dataset == 'MDCR':
        age_col = 'AGE'
        dob_col = 'BIRTH_DT'
    elif dataset == 'MDCD':
        if 'age_ps' in df.columns:
            age_col = 'age_ps'
        else: 
            age_col = 'age'
            
    df.rename(columns = {age_col:'age'}, inplace = True)
    
    return df

# --------------------------------------------------
# SEX: function to fill in missing sex info
# --------------------------------------------------
def sex_format(dataset, subdataset, df): 
    if dataset == 'MDCR':
        ps_sex_col = 'sex_id'
        claim_sex_col = 'GNDR_CD'
        if subdataset == 'nf_medpar':
            claim_sex_col='SEX'
    elif dataset == 'MDCD_MAX':
        ps_sex_col = 'sex_id'
        claim_sex_col = 'EL_SEX_CD'
    elif dataset == 'MDCD_TAF':
        if 'sex_id' in df.columns:
            ps_sex_col = 'sex_id'
        elif 'sex_id_ps' in df.columns:
            ps_sex_col = 'sex_id_ps'
        df[ps_sex_col] = pd.to_numeric(df[ps_sex_col], errors = 'coerce')
        claim_sex_col = ps_sex_col #no demographic info in claims
    
    if subdataset == 'nf_medpar': #doesn't have a way to join to ps, so use claim info
        df[claim_sex_col]=df[claim_sex_col].fillna(-1)
        df.rename(columns = {claim_sex_col:'sex_id'}, inplace = True)
        df['sex_id']=np.where((df['sex_id']==1 )| (df['sex_id']==2), df['sex_id'],-1)
    else:
        df[ps_sex_col]=np.where(df[ps_sex_col]=='',-1, df[ps_sex_col])
        df[ps_sex_col]=np.where(df[ps_sex_col]==' ',-1, df[ps_sex_col])
        df['sex_id']=np.where((df[ps_sex_col]==1 )| (df[ps_sex_col]==2), df[ps_sex_col],-1)
        if dataset == 'MDCD_TAF':
            df['sex_id'] = df['sex_id'].astype(int)
    return df

# --------------------------------------------------
# RACE: function to fill in missing race info and apply dict
# --------------------------------------------------
def race_format(dataset, subdataset, df): 
    if dataset == 'MDCR':
        ps_race_raw_col='race_cd_raw'
        ps_race_imp_col='race_cd_imp'
        race_dict=cd.race_dict_mdcr
        claim_race_col='BENE_RACE_CD' 
        ps_self_report_race='RACE_CD'
        if subdataset == 'nf_medpar':
            claim_race_col = 'RACE'
    elif dataset == 'MDCD_MAX':
        ps_race_raw_col='race_cd_raw'
        ps_race_imp_col='race_cd_imp'
        claim_race_col= 'EL_RACE_ETHNCY_CD'
        race_dict=cd.race_dict_mdcd
        ps_self_report_race=''
    elif dataset == 'MDCD_TAF':
        ps_race_col='race_cd'
        ps_race_raw_col='race_cd_raw'
        ps_race_imp_col='race_cd_imp'
        claim_race_col= ''#no demographic info in TAF
        race_dict=cd.race_dict_mdcd_taf
        ps_self_report_race=''
        
    if ps_race_raw_col in df.columns:
        df['race_cd_raw']=df['race_cd_raw'].fillna('UNK')
        df['race_cd_raw']=np.where(df['race_cd_raw']=='0.0','UNK',df['race_cd_raw'])
    if ps_race_imp_col in df.columns:
        df['race_cd_imp']=df['race_cd_imp'].fillna('UNK')
        df['race_cd_imp']=np.where(df['race_cd_imp']=='0.0','UNK',df['race_cd_imp'])
        
    elif (claim_race_col in df.columns) & (ps_race_raw_col not in df.columns): #in NF medpar there is no PS, so we use claim info!
        df[claim_race_col]=df[claim_race_col].fillna(0).astype(int) #2019 came through as string, so set to int
        # No imputed race for medpar, just copy race column from claims to keep format the same as other files
        # We can't impute race w zip imputation function like the sample denoms bc we don't have zip coes in our medpar data
        df['race_cd_raw'] = df[claim_race_col].map(race_dict).fillna('UNK')
        df['race_cd_imp'] = df['race_cd_raw']
        
    return df

# --------------------------------------------------
# Medicaid state function
# --------------------------------------------------
def fips_state_format(df, ps_fips_col = 'BENE_STATE_CD', claim_fips_col = 'SUBMTG_STATE_CD', claim_abv_col = 'STATE_CD'):
    #Read in DEX State map
    print(ps_fips_col)
    state_csv = pd.read_csv('FILEPATH')
    df[ps_fips_col] = df[ps_fips_col].fillna('-1').astype(float).astype(int)
    #fips state # : [state name, state location id, state abv, ssa state #]
    #Create a dictionary from csv
    state_dict = state_csv.set_index('state').T.to_dict('list')
    dict_na = {-1 : ['-1',-1,'-1',-1],
               60 : ['American Samoa',999,'American Samoa',999],
               66 : ['Guam',999,'-1',999],
               69 : ['Northern Mariana Islands',999,'Northern Mariana Islands',999],
               72 : ['Puerto Rico',999,'PR',999],
               74 : ['U.S. Minor Outlying Islands', 999, 'U.S. Minor Outlying Islands',999],
               78 : ['Virgin Islands',999,'Virgin Islands', 999],
               94 : ['-1',-1,'-1',-1],
               93 : ['-1',-1,'-1',-1],
               97 : ['-1',-1,'-1',-1]}
    state_dict.update(dict_na)
    
    #Fill missing ps fips state w/ claim
    df[claim_fips_col]=df[claim_fips_col].astype(float).fillna(-1).astype(int)
    df[ps_fips_col]=np.where(df[ps_fips_col]==-1,np.nan,df[ps_fips_col])

    #Create state fips numbers
    df['st_num_resi'] = df[ps_fips_col].fillna(-1)
    #state location id number from state map
    df['st_loc_id_resi'] = df[ps_fips_col].fillna(-1).map(lambda x: state_dict[x][1])
    
    return df


# --------------------------------------------------
# Medicaid state function
# --------------------------------------------------
def mdcr_state_format(subdataset, df, resi_serv = 'resi'):

    #state input cols are SSA code
    if resi_serv == 'resi':
        a = 'st_resi'
        b = 'st_num_resi'
        c = 'st_loc_id_resi'
    elif resi_serv == 'serv':
        a = 'st_serv'
        b = 'st_num_serv'
        c = 'st_loc_id_serv'
        
    ps_st_col= 'STATE_CODE'
    claim_st_col='BENE_STATE_CD' 

    if subdataset == 'nf_medpar':
        ps_st_col = 'STATE'
    if resi_serv == 'resi':
        claim_abv_st_col_oth = 'CLM_SUBSCR_USPS_STATE_CD'
    else:
        claim_abv_st_col_oth = 'CLM_BPRVDR_USPS_STATE_CD'
        
    #Some datasets have a state columns in claims data, some don't
     #From denom/ps - STATE_CODE = SSA state
    
    # ##Geographic Info##
    #Read in state csv and turn into dictionary - all fips info
    state_csv = pd.read_csv("FILEPATH")

    #move abbreviation column and drop state_name
    # shift column 'C' to first position
    first_column = state_csv.pop('abbreviation')

    # insert column using insert(position,column_name,first_column) function
    state_csv.insert(0, 'abbreviation', first_column)
    state_csv.drop(['state_name'],inplace=True,axis=1)
    state_dict=state_csv.set_index('ssa_state').T.to_dict('list')

    #Add list of non-US locations from constants folder
    state_dict.update(cd.ssa_state_dict) #cd.ssa_state_dict
    #Turn dict keys into list
    state_dict_keysList = list(state_dict.keys())

    if claim_st_col in df.columns:
        #fill in state code from ps with state values from claims when missing
        #Sometime claim state column comes in as a category, sometimes as float
        #To address all possibilities turn to a float first, then int
        df[claim_st_col]=df[claim_st_col].astype(float).fillna(-1).astype(int)
        #If value not a key in dictionary set to -1
        df[ps_st_col]=np.where(~df[ps_st_col].isin(state_dict_keysList) ==True, -1, df[ps_st_col])

        #Create state abbreviations
        df[a] = df[ps_st_col].fillna(-1).map(lambda x: state_dict[x][0])
        #Create state fips numbers
        df[b] = df[ps_st_col].fillna(-1).map(lambda x: state_dict[x][2])
        #state location id number from state map
        df[c] = df[ps_st_col].fillna(-1).map(lambda x: state_dict[x][1])
    else:
        #If value not a key in dictionary set to -1
        df[ps_st_col]=np.where(~df[ps_st_col].isin(state_dict_keysList) ==True, -1, df[ps_st_col])

        #Create state abbreviations
        df[a] = df[ps_st_col].fillna(-1).map(lambda x: state_dict[x][0])
        #Create state fips numbers
        df[b] = df[ps_st_col].fillna(-1).map(lambda x: state_dict[x][2])
        #state location id number from state map
        df[c] = df[ps_st_col].fillna(-1).map(lambda x: state_dict[x][1])
        
    return df

#----------------------------
# Function to create county - only used for MDCR
#----------------------------
def cnty_format(dataset, df, cnty_col = 'cnty', cnty_mcnty='cnty'):
    if dataset =='MDCR': 
        cnty_col='cnty'
            
    #From denom - cnty = fips state(2 digit) + fips county(3 digit)
    #Change None to -1, so we can change column to int
    df[cnty_col] = df[cnty_col].fillna(value=-1).astype(int)

    #Read in mcnty csv
    mcnty_csv = pd.read_csv('FILEPATH')
    #Filter to current mcnty
    mcnty_csv = mcnty_csv[mcnty_csv['current']==1][['mcnty','cnty','location_id']]
    #rename columns to match dex columns
    mcnty_csv.rename(columns = {'mcnty':'mcnty_resi',
                        'location_id': 'cnty_loc_id_resi'
                        }, inplace = True)
    #Merge mcnty to main df
    df=df.merge(mcnty_csv, left_on=[cnty_col], right_on=[cnty_mcnty],how="left")
    df[cnty_mcnty] = df[cnty_mcnty].fillna(value=-1).astype(int)

    return df
    
#----------------------------
# Function to make zip-codes
#----------------------------
def zip_format(chia, dataset, subdataset, df, yr =''):
    if dataset == 'MDCR':
        if 'CLM_SRVC_FAC_ZIP_CD' in df.columns:
            df['CLM_SRVC_FAC_ZIP_CD']=np.where(df['CLM_SRVC_FAC_ZIP_CD'].isin(['C','J']), np.nan, df['CLM_SRVC_FAC_ZIP_CD'])
        if 'BENE_MLG_CNTCT_ZIP_CD' in df.columns:
            df['BENE_MLG_CNTCT_ZIP_CD']=np.where(df['BENE_MLG_CNTCT_ZIP_CD'].isin(['T1W1L6','',' ']), np.nan, df['BENE_MLG_CNTCT_ZIP_CD'])
        if 'CLM_SUBSCR_ADR_ZIP_CD' in df.columns:
            df['CLM_SUBSCR_ADR_ZIP_CD']=np.where(df['CLM_SUBSCR_ADR_ZIP_CD']=='T1W1L6', np.nan, df['CLM_SUBSCR_ADR_ZIP_CD'])
        if subdataset == 'nf':
            df['BENE_MLG_CNTCT_ZIP_CD'] = pd.to_numeric(df['BENE_MLG_CNTCT_ZIP_CD'], errors='coerce')
        df['zip_5_resi']=df['BENE_MLG_CNTCT_ZIP_CD'].astype(float).fillna(-1).round().astype(int).astype(str)
        df['zip_5_resi']=df['zip_5_resi'].str.pad(9, side='left', fillchar='0')
        df['zip_5_resi']=df['zip_5_resi'].astype(str).str[:5]
        if subdataset in ['ip_part_c', 'nf_part_c', 'hha_part_c', 'carrier_part_c','hop_part_c']:
            df['zip_5_resi']=df['BENE_MLG_CNTCT_ZIP_CD'].astype(float).fillna(-1).round().astype(int).astype(str)
            df['zip_5_resi']=df['zip_5_resi'].str.pad(9, side='right', fillchar='0')
            df['zip_5_resi']=df['zip_5_resi'].astype(str).str[:5]
        
        if subdataset in ['hop','hha']: 
            df['zip_5_serv']=df['CLM_SRVC_FAC_ZIP_CD'].astype(float).fillna(-1).astype(int).astype(str)
            df['zip_5_serv']=np.where((df['zip_5_serv']=='-1'),np.nan,df['zip_5_serv'])
            df['zip_5_serv']=df['zip_5_serv'].str.pad(9, side='left', fillchar='0')
            df['zip_5_serv']=df['zip_5_serv'].astype(str).str[:5]
        if subdataset == 'ip':
            if chia ==0:
                if yr in [2000, 2010, 2014, 2015, 2016,2008, 2009, 2011, 2012, 2013, 2017]:
                    df['zip_5_resi']=df['BENE_MLG_CNTCT_ZIP_CD'].round().astype(str)
                elif yr in [2019]:
                    df['zip_5_resi']=df['BENE_MLG_CNTCT_ZIP_CD'] #This 2019 col in ps is string, others is int/double
            elif chia == 1:
                df['zip_5_resi']=df['BENE_MLG_CNTCT_ZIP_CD'].round().astype(int).astype(str)
            df['zip_5_resi']=df['zip_5_resi'].str.pad(9, side='left', fillchar='0')
            df['zip_5_resi']=df['zip_5_resi'].astype(str).str[:5]
            #Service
            df['zip_5_serv']=df['CLM_SRVC_FAC_ZIP_CD'].fillna(-1).astype(int).astype(str)
            df['zip_5_serv']=np.where((df['zip_5_serv']=='-1'),np.nan,df['zip_5_serv'])
            df['zip_5_serv']=df['zip_5_serv'].str.pad(9, side='left', fillchar='0')
            df['zip_5_serv']=df['zip_5_serv'].astype(str).str[:5]
            
    if dataset == 'MDCD_MAX':
        #4) Zip codes - some US zip codes have leading 0, so need to use str dtype and zero pad left side
        df['zip_5_resi']=df['EL_RSDNC_ZIP_CD_LTST'].astype(str).str[0:5]
        df['zip_5_resi']=df['zip_5_resi'].str.pad(5, side='left', fillchar='0')
    elif dataset == 'MDCD_TAF':
        #Zip codes - some US zip codes have leading 0, so need to use str dtype and zero pad left side
        if 'BENE_ZIP_CD' in df.columns:
            df['zip_5_resi']=df['BENE_ZIP_CD'].str[0:5]
        if 'BENE_ZIP_CD_ps' in df.columns:
            df['zip_5_resi']=df['BENE_ZIP_CD_ps'].str[0:5]
            
    return df


#----------------------------------------------------
# Function to make dual enrollement indicators for MDCR-MDCD
#----------------------------------------------------
def dual_format(dataset, df):
    all_dual = cd.all_dual
    part_dual = cd.part_dual
    full_dual = cd.full_dual
        
    if dataset == 'MDCR':
        col = 'DUAL_STUS_CD'
    elif dataset == 'MDCD':
        col = 'EL_MDCR_DUAL_MO'
        all_dual = cd.max_all_dual
        part_dual = cd.max_part_dual
        full_dual = cd.max_full_dual
    elif dataset == 'MDCD_TAF':
        if 'DUAL_ELGBL_CD' in df.columns:
            col = 'DUAL_ELGBL_CD'
        elif 'DUAL_ELGBL_CD_ps' in df.columns:
            col = 'DUAL_ELGBL_CD_ps'
        
    #Indicators for dual Medicare/Medicaid enrollment
    if pd.api.types.is_string_dtype(df[col].dtype):
        df.loc[df[col] == 'NA', col] = '-1'
        df[col]=df[col].fillna('-1').astype(int)
    elif pd.api.types.is_numeric_dtype(df[col].dtype):
        df[col]= df[col].fillna(-1).astype(int)
    df['dual_ind']=np.where(df[col].isin(all_dual),1,0)
    df['part_dual_ind']=np.where(df[col].isin(part_dual),1,0)
    df['full_dual_ind']=np.where(df[col].isin(full_dual),1,0)
    if dataset == 'MDCR':
        df['hmo_ind']=np.where(df['HMO_IND'].isin(cd.mdcr_hmo_ind),1,0)
    
    return df


#------------------------------------------
# Function to create the toc column - right now just written for MDCR
#-----------------------------------------

def toc_format(subdataset, df, yr, chia):
    #TOC datasets are assigned the TOC of that dataset
    if subdataset in ['ip','ip_part_c']:
        df['toc']='IP'
    elif subdataset in ['hha','hha_part_c']:
        df['toc']='HH'
    elif subdataset in ['nf','nf_part_c', 'hospice','hosp','nf_medpar','nf_saf']:
        df['toc']='NF'
    elif subdataset in ['rx']:
        df['toc']='RX'
    
    #MDCR Carrier data can have any toc, its mapping is done in a separate script that is merged in here
    elif subdataset in ['carrier','carrier_part_c']:
        if subdataset =='carrier_part_c':
            path = 'FILEPATH'
            join_cols = ['BENE_ID','ENC_JOIN_KEY','year_id']
        elif subdataset =='carrier' and chia != 1:
            path = 'FILEPATH'
            join_cols = ['BENE_ID','CLM_ID','year_id']
        elif subdataset == 'carrier' and chia == 1:
            path = 'FILEPATH'
            join_cols = ['BENE_ID','CLM_ID','year_id']
        #Read in carrier toc table
        df_toc = pd.read_parquet(path,
                            ).drop_duplicates()
        df_toc['year_id']=yr
        
        #Merge toc table onto main table
        #Get row nums before merge to ensure no duplication from merge
        print("df_cms shape before join: " + str(df.shape))
        size_cms_b4_df_toc=df.shape[0]
        print("df_toc shape before join: " + str(df_toc.shape))
        df=df.merge(df_toc, left_on=join_cols, 
                        right_on=join_cols,how="left")
        print('post merge to toc' + str(df.shape))
        size_cms_df_toc=df.shape[0]
        print("df_cms shape after join to df_toc: " + str(df.shape))
        #ensure there is no row duplication
        assert size_cms_b4_df_toc == size_cms_df_toc, "cms row count changed after merge to df_toc file"
        
        #fill in NAs 
        df['toc']=df['toc'].fillna('UNK')
        
    #Outpatient files, identify ED claims, everything else is AM
    elif subdataset in ['hop_part_c','hop']:
        ed_path = "FILEPATH" +str(yr) + "FILEPATH"
        if chia == 1:
            ed_path = "FILEPATH" +str(yr) + "FILEPATH"
        if subdataset == 'hop_part_c':
            ed_path = ed_path + '1'
            cols = ['BENE_ID','ENC_JOIN_KEY']
        elif subdataset == 'hop':
            ed_path = ed_path + '0'
            cols = ['BENE_ID','CLM_ID']
        df_ed = pd.read_parquet(ed_path,
                               columns = cols).drop_duplicates()
        df_ed['year_id']=yr
        #Anything in this table is an ED claim
        df_ed['toc']='ED'

        #Merge ed table onto main table
        #Get row nums before merge to ensure no duplication from merge
        print("df_cms shape before join: " + str(df.shape))
        size_cms_b4_df_ed=df.shape[0]
        print("df_ed shape before join: " + str(df_ed.shape))
        cols = cols +['year_id']
        df=df.merge(df_ed, left_on=cols, 
                    right_on=cols,how="left")
        print('post merge to ed' + str(df.shape))
        size_cms_df_ed=df.shape[0]
        print("df_cms shape after join to df_ed: " + str(df.shape))
        #ensure there is no row duplication
        assert size_cms_b4_df_ed == size_cms_df_ed, "cms row count changed after merge to df_ed file"
        
        #Any row not in the ED table is an AM claims
        df['toc']=df.toc.fillna('AM')
    
    if subdataset not in ['rx']:
        #Any RX in non-RX files should be mapped to AM
        df['toc']=np.where(df['toc']=='RX','AM', df['toc'])
    #all hospice data should be toc = NF
    df['toc']=np.where(df['toc']=='HOSPICE','NF', df['toc'])
    
    if subdataset == 'carrier_part_c':
        df['toc']=np.where((df['toc']== 'OTH')|(df['toc']== 'UNKNOWN'), df['CLM_PLACE_OF_SRVC_CD'].map(lambda x: cd.carrier_c_toc[x][0]).fillna('UNK'), df['toc'])#
    elif subdataset == 'carrier':
        df['toc']=np.where((df['toc']== 'OTH')|(df['toc']== 'UNKNOWN'), df['NCH_CLM_TYPE_CD'].map(lambda x: cd.carrier_toc[x][0]).fillna('UNK'), df['toc'])
        
    df.loc[df['toc'] == 'UNKNOWN', 'toc'] = 'UNK'
    df['toc']=np.where(df['toc']=='HOSPICE','NF',df['toc'])
        
    return df

#-----------------------------------------------#
# function to identify dental toc 
#-----------------------------------------------#
def dv_line(subdataset, df_cms, unique_bene, yr, chia):
    path = ""
    if subdataset == 'carrier_part_c':
        path = "FILEPATH"
    elif subdataset == 'hop_part_c':
        path = "FILEPATH"
    elif subdataset == 'hop':
        if chia == 1:
            path = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
        else:
            if yr in [2000,2010,2014,2015,2016]:
                path = "FILEPATH"
            elif yr in [2008,2009,2011,2012,2013, 2017]:
                path = "FILEPATH"
            elif yr == 2019:
                path= "FILEPATH"
            path = "FILEPATH"

    elif subdataset == 'carrier':
        if chia == 1:
            path = glob.glob(os.path.join(path.path_chia , str(yr), "FILEPATH"))[0]
        else:
            path = "FILEPATH"

    print(path)

    if subdataset in ['carrier_part_c', 'hop_part_c']:
        join_key = ['ENC_JOIN_KEY']
        col_filters = [('BENE_ID','in', unique_bene),('LINE_LTST_CLM_IND','=','Y')]
    elif subdataset in ['carrier', 'hop']:
        join_key = ['CLM_ID']
        col_filters = [('BENE_ID','in', unique_bene)]
    ##if duplicate rows, take the first one
    cms_cols = df_cms.columns
    #--read in line data and filter to just dental codes
    cms_line = pd.read_parquet(path,
                      filters = col_filters,
                      columns = ['BENE_ID','HCPCS_CD']+join_key).drop_duplicates()#
    print('cms_line read in')
    print(cms_line.shape)

    cms_line['HCPCS_CD'] = cms_line['HCPCS_CD'].astype(str)
    cms_line = cms_line.dropna(subset=['HCPCS_CD'])
    cms_line = cms_line[cms_line['HCPCS_CD'].str.startswith('D')]#dental codes start w/ D
    #most common Dx on dental codes - R69, R6889, Z0120, K006
    cols = ['BENE_ID']+join_key 
    cms_line.drop_duplicates(subset=cols, keep='first', inplace = True) #can have multiple D codes

    #--merge to main df
    #Get row nums before merge to ensure no duplication from merge
    print("df_cms shape before join: " + str(df_cms.shape))
    size_cms_b4_df_line=df_cms.shape[0]
    print("df_line shape before join: " + str(cms_line.shape))

    #merge time
    df_cms=df_cms.merge(cms_line,
                  left_on=cols, 
                  right_on= cols,
                  how="left").drop_duplicates()
    print("df_cms shape after join to df_ps: " + str(df_cms.shape))

    ##if duplicate rows, take the first one
    df_cms.drop_duplicates(subset=cms_cols, keep = 'first', inplace = True)
    size_cms_df_line=df_cms.shape[0]
    print("df_cms shape after join to df_ps + drops: " + str(df_cms.shape))
    #ensure there is no row duplication
    assert size_cms_b4_df_line == size_cms_df_line, "cms row count changed after merge to df_ps file"
    
    #where there is a dental claim, change toc to DV 
    df_cms['HCPCS_CD'].fillna('-1',inplace=True)
    df_cms['toc1']=np.where(df_cms['HCPCS_CD']!='-1','DV',df_cms['toc'])
        
    if 'OT_PHYSN_SPCLTY_CD' in df_cms.columns:
        if pd.api.types.is_string_dtype(df_cms['OT_PHYSN_SPCLTY_CD']):
                df_cms['toc1']=np.where(df_cms['OT_PHYSN_SPCLTY_CD'].isin(['C5','19']),'DV',df_cms['toc1'])
        else:
            df_cms['toc1']=np.where(df_cms['OT_PHYSN_SPCLTY_CD'].isin([19]),'DV',df_cms['toc1'])
    
    
    return df_cms

#----------------------------------------------------
# Function to convert columns to the correct datatype
#----------------------------------------------------
def datatype_format(dataset, subdataset, df):
    dollar_cols = []
    if (dataset == 'MDCR') & (subdataset not in ['carrier_part_c','hha_part_c','hop_part_c','ip_part_c','nf_part_c','ip_part_c', 'nf_medpar']):
        dollar_cols = dollar_cols +['pmt_1','pmt_2','CLM_PMT_AMT','tot_chg_amt']
        if subdataset in ['carrier']:
            dollar_cols = dollar_cols + ['CLM_BENE_PD_AMT','NCH_CLM_PRVDR_PMT_AMT','NCH_CLM_BENE_PMT_AMT',
                                         'CARR_CLM_PRMRY_PYR_PD_AMT',
                     'NCH_CARR_CLM_ALOWD_AMT','CARR_CLM_CASH_DDCTBL_APLD_AMT','pmt_3'] 
        if subdataset != 'carrier':
            dollar_cols = dollar_cols +['NCH_PRMRY_PYR_CLM_PD_AMT']
        if subdataset == 'ip':
            dollar_cols = dollar_cols + ['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM','NCH_IP_TOT_DDCTN_AMT','CLM_PASS_THRU_PER_DIEM_AMT','pmt_3']
        if subdataset in ['hha','hosp']:
            dollar_cols = dollar_cols 
        if subdataset in ['hop']:
            dollar_cols = dollar_cols + ['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM','NCH_BENE_PTB_DDCTBL_AMT','NCH_BENE_PTB_COINSRNC_AMT','pmt_3']
        if subdataset == 'rx':
            dollar_cols = dollar_cols + ['PTNT_PAY_AMT','OTHR_TROOP_AMT','GDC_BLW_OOPT_AMT','GDC_ABV_OOPT_AMT',
    'CVRD_D_PLAN_PD_AMT','NCVRD_PLAN_PD_AMT','pmt_4']
        if subdataset == 'nf':
            dollar_cols = dollar_cols + ['NCH_IP_TOT_DDCTN_AMT']
        if subdataset == 'nf_medpar':
                dollar_cols = dollar_cols + ['COINSURANCE_AMOUNT','INPATIENT_DEDUCTIBLE','BLOOD_DEDUCTIBLE',
               'PRIMARY_PAYER_AMOUNT','REIMBURSEMENT_AMOUNT','OUTLIER_AMOUNT','TOTAL_CHARGES','SERVICE_CHARGES_22','BILL_TOTAL_PER_DIEM']
    if dataset == 'MDCD_MAX':
        dollar_cols = dollar_cols +['tot_chg_amt','pmt_1','pmt_2','pmt_3','CHRG_AMT','PHP_VAL','MDCD_PYMT_AMT',
                                    'MDCR_COINSUR_PYMT_AMT','MDCR_DED_PYMT_AMT','TP_PYMT_AMT']
    if dataset == 'MDCD_TAF':
        dollar_cols = dollar_cols + ['BILLED_AMT','MDCD_PD_AMT','OTHR_INSRNC_PD_AMT','TP_PD_AMT','COINSRNC_AMT',
              'COPAY_AMT','DDCTBL_AMT', 'MDCR_DDCTBL_PD_AMT','MDCR_COINSRNC_PD_AMT'] +['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt',
              'oop_chg_amt','oth_chg_amt','tot_chg_amt','mdcr_pay_amt','mdcd_pay_amt',
               'priv_pay_amt','oop_pay_amt','oth_pay_amt','tot_pay_amt','pmt_1','pmt_2']
        if subdataset not in ['nf_taf','ltc_taf', 'rx_taf']: 
            dollar_cols = dollar_cols + ['MDCD_COPAY_AMT']
        df[dollar_cols]=np.where(df[dollar_cols]=='',np.nan,df[dollar_cols])

        #there is no dollar amounts
    #Specific issues with these 3 columns
    if subdataset != 'nf_medpar':
        df['age']=df['age'].fillna(-1)
    df['sex_id']=df['sex_id'].fillna(-1).astype('int8')
    df['st_resi']=df['st_resi'].fillna('-1')
    
    #Create list of int and float columns
    float_int=(df.select_dtypes(include=['int8','int16','int32','int64','int','float'])).columns.values.tolist()
    #using just int and float wasn't grabbing everything
    #fill nas to enable float conversion to int and for parquet paritioning can't have NA values
    float_int = [x for x in float_int if x not in dollar_cols]
    df[float_int]=df[float_int].fillna(-1) 
    #Convert floats to Int
    floats=(df.select_dtypes(include=[float])).columns.values.tolist()
    #Remove $ columns from float list, want to leave as floats
    floats = [x for x in floats if x not in dollar_cols]
    df[dollar_cols]=df[dollar_cols].astype(float).round(2)
    df[floats]=df[floats].astype(np.int64, errors='ignore')
   
    return df

def mc_format_taf(dataset, df, state, clm_type_col = 'CLM_TYPE_CD'): 
    if state in ['AL','AK','CT','ID','ME','MT','NC','OK','SD','VT','WY']:
        #Some states don't have MC, force mc_ind to be 0 to match denom
        df['mc_ind']=0
        df['mc_ind_clm']=0
    else:
        if dataset == 'MDCD_TAF':
            clm_type_col = 'CLM_TYPE_CD'
        elif dataset == 'MDCD_MAX':
            clm_type_col = 'TYPE_CLM_CD'
        df['mc_ind_clm']=np.where(df[clm_type_col].isin(cd.mc_list),1,0) 
        if 'mc_status_ps' in df.columns:
            df['mc_ind']=np.where(df['mc_status_ps']=='mc_only',1,
                      np.where(df['mc_status_ps']=='partial_mc',2,0))
        else:
            df['mc_ind']=np.where(df['mc_status']=='mc_only',1,
                      np.where(df['mc_status']=='partial_mc',2,0))
    df['mc_ind'].fillna(df['mc_ind_clm'],inplace=True)
            
        
    return df

def toc_taf_format(df, subdataset, dv_col='ADMTG_PRVDR_SPCLTY_CD'):
    if subdataset == 'ip_taf':
        toc = 'IP'
    elif subdataset == 'ltc_taf':
        toc = 'NF'
    elif subdataset == 'rx_taf':
        toc = 'RX'
    if dv_col in df.columns:
        df['toc']=toc #Hardcoded to dataset toc
        df['toc_1'] = np.where(df[dv_col]=='19','DV',toc)
    else:
        df['toc']=toc 
    return df

def code_system_format(df, date_col):
    #code_system_id: icd-9 or icd-10. On oct 1, 2015 transitioned to icd-10 #1=icd9, 2=icd10
    df['code_system']=np.where(df[date_col]<pd.to_datetime('2015-10-01'),'icd9','icd10')
    return df


#---------------------------------
# - Function to check all all columns are in data
#-----------------------------------
def check_cols(df,required_cols):
    if not set(required_cols).issubset(set(df.columns)):
        print('columns in df'+str(df.columns))
        raise RuntimeError(f"{' and '.join(set(required_cols).difference(df.columns))} are not available in the dataframe")
    return "All columns are available in the dataframe"


#---------------------------------
# - Function to apply days supply map
#---------------------------------#
def apply_days_supply(df):

    # read in days supply map
    # matches days_suply values by ndc/age_cat/year_id including missing age and missing year
    ds_map = pd.read_parquet("FILEPATH")
    print("map read in")
    ds_map['ndc'] = pd.to_numeric(ds_map['ndc'], errors='coerce').astype('Int64').fillna(-1)
    ds_map['year_id'] = pd.to_numeric(ds_map['year_id'], errors='coerce').astype('Int64').fillna(-1)
    ds_map['days_supply'] = pd.to_numeric(ds_map['days_supply'], errors = 'coerce').astype('float64').fillna(-1)
    ds_map['age_cat'] = ds_map['age_cat'].astype('string').fillna("-1")
    
    # if no match on age/year impute with all-age all-year aggregate 
    # sometimes there is no match for an ndc/age/year but there is for ndc/all-age/all-year
    ds_map_na = ds_map[(ds_map['age_cat'] == '-1') & (ds_map['year_id'] == -1)][['ndc', 'days_supply']]
    ds_map_na['days_supply_x'] = ds_map_na['days_supply']
    ds_map_na = ds_map_na.drop(columns = ['days_supply'])
    ds_map_age_na = ds_map[(ds_map['age_cat'] == '-1')& (ds_map['year_id'] != -1)][['ndc', 'year_id','days_supply']]
    ds_map_age_na['days_supply_y'] = ds_map_age_na['days_supply']
    ds_map_age_na = ds_map_age_na.drop(columns = ['days_supply'])

    ds_map['days_supply_z'] = ds_map['days_supply']
    ds_map = ds_map.drop(columns = ['days_supply'])
                       
    conditions = [
        (df["age"].lt(20)),
        (df["age"].ge(20) & df["age"].lt(45)),
        (df["age"].ge(45) & df["age"].lt(65)),
        (df["age"].ge(65)),
    ]
    choices = ["0-19", "20-44", "45-64", "65+"]
    
    df["age_cat"] = np.select(conditions, choices, default = pd.NA)
    df['age_cat'] = df['age_cat'].astype('string').fillna("-1")

    # merge on ndc first for imputed value
    # then merge on ndc/age/year
    print(df.shape)
    df = pd.merge(df, ds_map_na, on = ['ndc'], how = 'left')
    print(df.shape)
    df = pd.merge(df, ds_map_age_na, on = ['ndc', 'year_id'], how = 'left')
    print(df.shape)
    df = pd.merge(df, ds_map, on = ['age_cat','ndc','year_id'], how = 'left')
    print(df.shape)
    print("merged!")


    print(df['days_supply_x'].shape, df['days_supply_y'].shape, df['days_supply_z'].shape)
    
    # First, check if 'days_supply_z' is not NA. If so, use 'days_supply_z'.
    # If 'days_supply_z' is NA, then check if 'days_supply_y' is not NA, and use 'days_supply_y'.
    # If both 'days_supply_z' and 'days_supply_y' are NA, use 'days_supply_x'.
    df['days_supply'] = np.where(
        ~df['days_supply_z'].isna(), df['days_supply_z'],
        np.where(~df['days_supply_y'].isna(), df['days_supply_y'], df['days_supply_x'])
    )
    # convert from float to int
    df["days_supply"] = df["days_supply"].astype(float).round().astype("Int32")
    print(df.shape)

    # drop cols that aren't needed
    df = df.drop(columns = ['age_cat', 'days_supply_x', 'days_supply_y', 'days_supply_z','N'])
    
    return df