## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to assist with transforming MarketScan data.
## ==================================================

from pyspark.sql import DataFrame, functions as F
import re


def order_cols(df: DataFrame):
    """Reorder columns of a tranformed dataframe into a pre-determined order.

    Args:
    df -- Pandas dataframe
    """

    # List of all stage 3 columns and typing in a somewhat sensible order
    stage_3_cols = {
        "nid": "string",  ## Basic info
        "year_id": "int",
        "year_adm": "int",
        "year_dchg": "int",
        "year_clm": "int",
        "bene_id": "string",
        "claim_id": "string",
        "age_group_years_start": "int",  ## Demographic info
        "age_group_id": "int",
        "sex_id": "int",
        "race_cd": "string",
        "mcnty_resi": "string",  ## Residence loc info
        "cnty_loc_id_resi": "string",
        "zip_3_resi": "string",
        "zip_5_resi": "string",
        "st_resi": "string",
        "st_loc_id_resi": "string",
        "mcnty_serv": "string",  ## Service loc info
        "cnty_loc_id_serv": "string",
        "zip_3_serv": "string",
        "zip_5_serv": "string",
        "st_serv": "string",
        "st_loc_id_serv": "string",
        "code_system": "string",  ## Encounter info
        "dx_level": "string",
        "dx": "string",
        "ndc": "string",
        "days_supply": "int",
        "discharge_date": "date",
        "service_date": "date",
        "fac_prof_ind": "string",
        "toc": "string",
        "los": "int",
        "pri_payer": "string",
        "tot_pay_amt": "float",  ## Payment info
        "mdcr_pay_amt": "float",
        "mdcd_pay_amt": "float",
        "priv_pay_amt": "float",
        "oop_pay_amt": "float",
        "oth_pay_amt": "float",
        "tot_chg_amt": "float",  ## Charge info
        "mdcr_chg_amt": "float",
        "mdcd_chg_amt": "float",
        "priv_chg_amt": "float",
        "oop_chg_amt": "float",
        "oth_chg_amt": "float",
        "survey_wt": "float",
    }  ## Other info

    # Re-ordering and re-typing dataframe columns
    existing_cols = {k: v for k, v in stage_3_cols.items() if k in df.columns}
    df = df.select([F.col(c).cast(t) for c, t in existing_cols.items()])

    return df


def w2l_transform(
    df: DataFrame,
    value_cols: list,
    id_cols: list = None,
    col_name: str = "dx_level",
    value_col_name: str = "dx",
):
    """Transform PySpark dataframe from wide to long format. Adapted from:
    https://stackoverflow.com/questions/41670103/how-to-melt-spark-dataframe

    df -- PySpark dataframe to transform
    value_cols -- List of columns that will be transformed from wide to long
    id_cols -- Optional list of columns to keep that are not transformed
    col_name -- Name of new column for storing value_vars names
    value_col_name -- Name of new column for storing value_vars values
    """

    # If id_cols not specified, all columns besides value_cols are used
    if not id_cols:
        id_cols = df.drop(*value_cols).columns

    # Creating an array containing each value_var name and its value
    vars_and_vals = F.array(
        *(
            F.struct(F.lit(c).alias(col_name), F.col(c).alias(value_col_name))
            for c in value_cols
        )
    )

    # Adding array column to temp dataframe
    tmp = df.withColumn("_tmp_", F.explode(vars_and_vals))

    # Gathering all columns, expanding array columns into final format
    cols = id_cols + [F.col("_tmp_")[x].alias(x) for x in [col_name, value_col_name]]

    # Creating dataframe from expanded columns, dropping any null values in new columns
    df = tmp.select(*cols).na.drop(subset=[col_name, value_col_name])

    return df


def dx_rename(df: DataFrame, regex_dx: list, dx_format: str = "dx_{num}"):
    """Rename DX columns in PySpark dataframe using regex matching. Expects
    To find a set of columns using regex_dx, with each column containing a
    Numerical value. This number is then used in the renaming to the specified
    Format, dx_format.

    df -- PySpark dataframe
    regex_dx -- List of DX columns regex expressions to match in dataframe
    dx_format -- New DX column format
    """

    # Getting old column names by using regex search
    old_dx_cols = [list(filter(re.compile(i).match, df.columns)) for i in regex_dx]

    # Defining regex to parse number from string
    num_regex = re.compile(r"\d+")

    # Creating a dictionary of old columns to rename to new format
    dx_renames = {
        old_col: dx_format.format(num=num_regex.findall(old_col)[0])
        for old_col in old_dx_cols[0]
    }

    # Renaming columns using dx_renames as a map
    df = df.select([F.col(i).alias(dx_renames.get(i, i)) for i in df.columns])

    return df
