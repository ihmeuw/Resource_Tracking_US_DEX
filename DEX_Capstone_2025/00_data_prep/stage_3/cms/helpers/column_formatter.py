import pandas as pd
from numpy.random import choice
import numpy as np
import uuid
import re
import pyarrow.dataset as ds
import pyarrow as py
from dex_us_county.00_data_prep.stage_3.cms.constants import paths as paths

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



# --------------------------------------------------
# function to rename date columns
# --------------------------------------------------
def date_rename(dataset, subdataset, df):
    #identify cols by subdataset
    if subdataset in ['carrier_part_c','hop_part_c','carrier','hop']:
        serv_date = "CLM_FROM_DT"
        dchg_date = "CLM_THRU_DT"
    #rename cols    
    df.rename(columns={serv_date:"service_date",
                         dchg_date:"discharge_dt"}, inplace=True)
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

#----------------------------------------------------
# Function to make mc_ind that matches denominator
#----------------------------------------------------
def mc_format(dataset, df, state):
    if dataset == 'MDCD':
        if state in ['AL','AK','CT','ID','ME','MT','NC','OK','SD','VT','WY']:
            #Some states don't have MC, force mc_ind to be 0 to match denom
            df['mc_ind']=0
            df['mc_ind_clm']=0
        else:
            df['mc_ind_clm']=df['mc_ind']
            if 'mc_status_ps' in df.columns:
                df['mc_ind']=np.where(df['mc_status_ps']=='mc_only',1,
                              np.where(df['mc_status_ps']=='partial_mc',2,0)) 
            elif 'mc_status' in df.columns:
                df['mc_ind']=np.where(df['mc_status']=='mc_only',1,
                              np.where(df['mc_status']=='partial_mc',2,0)) 
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
# function to create age bins
# --------------------------------------------------
def age_bin(df,age_col = 'age'):

    # Getting age bin data and reordering
    ages = pd.read_csv('FILEPATH').reset_index()

    # Creating age bins, includes final closed interval from last age_group_years_end (age 125)
    age_bins = ages["age_group_years_start"].to_list() + [ages["age_group_years_end"].to_list()[-1]]

    # Binning data in new mapper column
    df["age_mapper"] = pd.cut(df[age_col], bins=age_bins, right=False, labels=False)

    # Mapping all columns of interest to dataframe
    age_data_cols = ["age_group_id", "age_group_years_start", "age_group_years_end"]
    for column in age_data_cols:
        df[column] = df['age_mapper'].map(ages[column].to_dict())

    # Dropping mapper column
    df.drop(columns="age_mapper", inplace=True)
    
    return df

#Function to redistribute ages for MDCR NF SAF and MEDPAR
def add_age_start(data):
    
    map1 = pd.read_csv("FILEPATH")
    identifier_box = data[['claim_id', 'sex_id', 'age_bin']].drop_duplicates()
    new_data = []

    # loop through sex_id + age bin combos
    for index, row in data[['sex_id', 'age_bin']].drop_duplicates().T.iteritems():
        # find relevant rows of map and claim_ids
        sub_map = map1.loc[(map1['sex_id'] == row['sex_id'])  & (map1['age_bin'] == row['age_bin'])]
        sub_data = identifier_box.loc[(identifier_box['sex_id'] == row['sex_id'])  & (identifier_box['age_bin'] == row['age_bin'])].copy()
        # add age group on to each claim
        sub_data['age_group_years_start'] = choice(list(sub_map['age_group_years_start']), len(sub_data),
                  p=list(sub_map['prop']))
        new_data.append(sub_data)
    
    new_data = pd.concat(new_data)
    assert(len(identifier_box) == len(new_data))

    # merge identifier box back onto data
    og_len = len(data)
    data = pd.merge(data, new_data, on = ['claim_id', 'sex_id', 'age_bin'], how = 'left')
    assert(len(data)==og_len)
    
    return(data)


# --------------------------------------------------
# function to create unique identifier
# --------------------------------------------------
def gen_uuid(df):
    col = [str(uuid.uuid4()) for i in range(len(df))]
    df.insert(0, 'claim_id', col)
    if len(df) != df.claim_id.unique().size:
        diff = df[df.duplicated(keep=False)]
        raise ValueError(f"The claim_id column is not actually unique {diff}")
    return df   
                    
# --------------------------------------------------
# function to transform data from wide to long on dx
# --------------------------------------------------
def w2l(df, pivot_cols=[r"^dx_\d*$", r"^ecode_\d*$"]):
    matched_cols = [list(filter(re.compile(i).match, df.columns)) for i in pivot_cols]
    long_cols = [item for sublist in matched_cols for item in sublist]
    
    # Finding all columns that will not be transformed
    id_cols = [col for col in df.columns if col not in long_cols]
    df = df.melt(id_vars=id_cols, value_vars=long_cols, var_name="dx_level", value_name="dx")
    df.dropna(axis=0, subset=['dx', 'dx_level'], inplace=True)
    df=df[(df['dx']!='-1')|(df['dx']!='')]
    df = df[df['dx'].notna()]
    df["dx_level"].replace("ecode", "ex", regex=True, inplace=True)
    return df

                       
# -------------------------------------------------------
# function to create acause from procedure code in DV claims
# -------------------------------------------------------
def create_dv_acause(df, dataset):
    if dataset in ['MDCD_MAX','MDCD_TAF']:
        proc_col = 'PRCDR_CD'
    elif dataset == 'MDCR':
        proc_col = 'HCPCS_CD'
    df['primary_cause']=1

    #Read in dental procedure code to acause map 
    dv_map=pd.read_csv('FILEPATH',
                      usecols = ['code','acause'])
    #Merge map to data
    size_cms_b4_dv=df.shape[0]
    df=df.merge(dv_map, left_on=[proc_col], 
                       right_on=['code'],how="left")
    size_cms_dv=df.shape[0]
    assert size_cms_b4_dv == size_cms_dv, "cms row count changed after merge to dv file"
    #If acuase is null fill randomly w/ _oral or exp_well_dental
    df['acause1']=np.nan
    df['acause1']=df['acause1'].apply(lambda l: np.random.choice(['_oral', 'exp_well_dental']))
    df['acause'].fillna(df['acause1'], inplace=True)
    df.drop(columns=["acause1"], inplace=True)

    return df

# -------------------------------------------------------
# Drop rows that are implausible (admitted > 100yrs before data year negative los) 
# or are missing dx (if not RX or DV), and locations not in US
# -------------------------------------------------------
#Save df dimensions before and after drop
#And states that we know
def drop_rows(df, toc, year, subdataset=''):
    if 'dx_level' in df.columns:
        df1=df[df['dx_level']=='dx_1']
        shape_before=str(df1.shape)
    else:
        shape_before=str(df.shape)
    df['st_resi']=df['st_resi'].astype(str).fillna('-1')
    if 'year_adm' in df.columns:
        df['year_adm'].fillna(-1, inplace = True)
        df['year_adm']=df['year_adm'].astype(int)
    elif 'year_id_adm' in df.columns:
        df['year_id_adm'].fillna(-1, inplace = True)
        df['year_id_adm']=df['year_id_adm'].astype(int)
    #ensure 'plausible' admission year and location is in US
    if subdataset != 'nf_medpar':
        if 'year_adm' in df.columns:
            df=df[(df['year_adm']>= year - 100)&(df['st_resi'].isin(cd.states))]
        elif 'year_id_adm' in df.columns:
            df=df[(df['year_id_adm']>= year - 100)&(df['st_resi'].isin(cd.states))]
          
    if subdataset == 'nf_medpar':
        df=df[df['st_resi'].isin(cd.states)] 
                       
    if toc not in ['RX','DV']:
        #drop missing dx's
        df=df[df['dx'].notna()]
        df['dx'] = df['dx'].astype(str)
        df=df[(df['dx']!='-1')&(df['dx']!='')&(df['dx']!=' ')]
        
    if toc in ['IP','NF']:
        #Drop negative los, except -1
        df['los'].fillna(-1, inplace = True)
        df['los']=df['los'].astype(int)
        df=df[df['los']>= -1]
        
    if 'dx_level' in df.columns:
        df1=df[df['dx_level']=='dx_1']
        shape_after=str(df1.shape)
    else:
        shape_after=str(df.shape)
    df_dim = pd.DataFrame({'before_drop': [shape_before],
                        'after_drop': [shape_after]})
                       
    return df, df_dim

                       
# --------------------------------------------------
# function to payer columns
# --------------------------------------------------
def payer_cols(dataset, subdataset, df):
    #add in pri_payer
    df['pri_payer']=1
    
    if (subdataset not in ['rx','carrier_part_c','hha_part_c','hop_part_c','ip_part_c','nf_part_c']) & (dataset == 'MDCR'):
        df['mc_ind']=0
    if subdataset in ['carrier_part_c','hha_part_c','hop_part_c','ip_part_c','nf_part_c']:
        dollar_cols = ['mdcd_chg_amt','mdcd_pay_amt','mdcr_chg_amt','mdcr_pay_amt',
                   'oop_chg_amt','oop_pay_amt','oth_chg_amt','oth_pay_amt',
                   'priv_chg_amt','priv_pay_amt','tot_chg_amt','tot_pay_amt']
        df[dollar_cols]=np.nan
        df['mc_ind']=1
    elif subdataset == 'carrier':
        df['mdcr_pay_amt']=df['CLM_PMT_AMT']
        df['oop_pay_amt']=df['CLM_BENE_PD_AMT']
        df['tot_chg_amt']=df['tot_chg_amt']
        cols =['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mdcd_pay_amt',
               'priv_pay_amt','oth_pay_amt','tot_pay_amt']
        df[cols]=np.nan
    elif subdataset == 'hop':
        df['mdcr_pay_amt']=df['CLM_PMT_AMT']
        df['oop_pay_amt']=df['NCH_BENE_BLOOD_DDCTBL_LBLTY_AM']+df['NCH_BENE_PTB_DDCTBL_AMT']+df['NCH_BENE_PTB_COINSRNC_AMT']
        df['tot_chg_amt']=df['tot_chg_amt']
        cols =['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oth_chg_amt','oop_chg_amt','mdcd_pay_amt',
               'priv_pay_amt','oth_pay_amt','tot_pay_amt']
        df[cols]=np.nan
        
    elif subdataset == 'ip':
        df['CLM_PMT_AMT']=df['CLM_PMT_AMT'].astype(float)
        df['CLM_PASS_THRU_PER_DIEM_AMT']=df['CLM_PASS_THRU_PER_DIEM_AMT'].astype(float)
        df['CLM_UTLZTN_DAY_CNT']=df['CLM_UTLZTN_DAY_CNT'].astype(float)
        df['mdcr_pay_amt']=df['CLM_PMT_AMT']+(df['CLM_PASS_THRU_PER_DIEM_AMT']*df['CLM_UTLZTN_DAY_CNT'])
        df.rename(columns = {'NCH_IP_TOT_DDCTN_AMT':'oop_pay_amt'}, inplace = True)
        cols = ['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mdcd_pay_amt',
               'priv_pay_amt','oth_pay_amt','tot_pay_amt']
        df[cols]=np.nan
    
    elif subdataset in ['hha','hosp']:
        df['mdcr_pay_amt']=df['CLM_PMT_AMT']
        df.rename(columns = {'NCH_IP_TOT_DDCTN_AMT':'oop_pay_amt'}, inplace = True)
        cols = ['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mdcd_pay_amt',
               'priv_pay_amt','oop_pay_amt','oth_pay_amt','tot_pay_amt']
        df[cols]=np.nan
        
    elif subdataset == 'nf':
        df['mdcr_pay_amt']=df['CLM_PMT_AMT']
        df['oop_pay_amt']=df['NCH_IP_TOT_DDCTN_AMT']
        cols = ['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mdcd_pay_amt',
               'priv_pay_amt','oth_pay_amt','tot_pay_amt']
        df[cols]=np.nan
        
    elif subdataset == 'rx':
        df['mdcr_pay_amt']=df['CVRD_D_PLAN_PD_AMT']
        df['oop_pay_amt']=df['PTNT_PAY_AMT']
        cols = ['mdcr_chg_amt','mdcd_chg_amt','priv_chg_amt','oop_chg_amt','oth_chg_amt','mdcd_pay_amt',
               'priv_pay_amt','oth_pay_amt','tot_pay_amt']
        df[cols]=np.nan  
    print(df.columns)

    return df


def col_dtype(df, dollar_cols):
    #Reduce columns to smaller data type
    float_col=(df.select_dtypes(include=[float])).columns.values.tolist()
    #Remove the dollar columns
    float_col = [x for x in float_col if x not in dollar_cols]
    df[dollar_cols]=df[dollar_cols].replace(-1, np.nan)
    df[dollar_cols]=df[dollar_cols].astype(float).round(2)
    df[float_col]=df[float_col].fillna(-1).astype('int')
    
    return df

def nid_create(df, dataset, year='year_id'):
    if dataset=='MDCR':
        mdcr_nid_dict = { 
            1999 : 472912,
            2000 : 471277,
            2001 : 472913,
            2002 : 472915,
            2004 : 472917,
            2006 : 472919,
            2008 : 508807,
            2009 : 508806,
            2010 : 471267,
            2011 : 508804,
            2012 : 508803,
            2013 : 508802,
            2014 : 471258,
            2015 : 470991,
            2016 : 470990,
            2017 : 508798,
            2019 : 501474,
                        }
        
        df['nid'] = df[year].map(mdcr_nid_dict).fillna(-1)

    elif dataset in ['MDCD_MAX', 'MDCD_TAF']:
        mdcd_nid_dict = { 
            2000 : 471320,
            2010 : 471319,
            2014 : 471318,
            2016 : 511590,
            2019 : 511589,
                        }
        
        df['nid'] = df[year].map(mdcd_nid_dict).fillna(-1)
    return df
                   
#Create enrollee_id
def gen_encounter_id(df, group_cols):
    
    # Grouping columns and assigning a temporary unique id
    df["temp_id"] = df.groupby(group_cols, dropna=False).ngroup()
    
    # Getting all temporary ids as list
    groups = df["temp_id"].unique().tolist()

    # Creating a uuid for each unique id and mapping
    unique_id_map = {i:str(uuid.uuid4()) for i in groups}
    df["encounter_id"] = df["temp_id"].map(unique_id_map)

    # Dropping temporary id column
    df.drop(columns=["temp_id"], inplace=True)
    
    # Ensuring all encounter ids are actually unique
    if len(df["encounter_id"].unique()) != len(df.groupby(group_cols, dropna=False)):
        raise RuntimeError("Some created ids are not unique. Please re-run this step.")
    
    return df


#---------------------------------
# - Function to check all all columns are in data
#-----------------------------------
def check_cols(df,required_cols):
    if not set(required_cols).issubset(set(df.columns)):
        print('columns in df'+str(df.columns))
        raise RuntimeError(f"{' and '.join(set(required_cols).difference(df.columns))} are not available in the dataframe")
    return "All columns are available in the dataframe"