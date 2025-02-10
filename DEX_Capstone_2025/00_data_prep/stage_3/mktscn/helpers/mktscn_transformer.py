## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to transform MarketScan data.
## ==================================================

import argparse
from pyspark.sql import SparkSession, DataFrame
from functools import reduce, partial
import re
import os

import mktscn_transformer_helper


def run_transformer(df, year, rx_proc):

    if not rx_proc:
        # Renaming diagnoses columns to standard format
        df = mktscn_transformer_helper.dx_rename(df, regex_dx=["^DX\d*$"])

        # Getting new dx column names
        dx_cols = list(filter(re.compile("^dx_\d*$").match, df.columns))

        # Transforming diagnosis columns into long format
        df = mktscn_transformer_helper.w2l_transform(df, value_cols=dx_cols)

    # Re-ordering columns and casting to appropriate types
    df = mktscn_transformer_helper.order_cols(df)

    return df


def get_spark_connection(session_name):

    # Searching for PySpark master connection file
    if os.path.isfile("FILEPATH"):
        master = open("FILEPATH").read().strip()
    else:
        raise RuntimeError("Could not locate .Master-spark-uri file - quitting")

    # Connecting to PySpark master
    print(f"Attempting to connect to master at {master}")
    spark = (
        SparkSession.builder.master(master)
        .appName(session_name)
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "5g")
        .config("spark.cores.max", "200")
        .getOrCreate()
    )

    return spark


def spark_read_filepaths(spark_session, filepaths: list):

    # Reading in all filepaths as separate dataframes
    df_list = []
    for path in filepaths:
        df_list.append(spark_session.read.parquet(path))

    # Creating union function and merging all elements in df_list
    # Into a single spark dataframe
    union_func = partial(DataFrame.unionByName, allowMissingColumns=True)
    df = reduce(union_func, df_list)

    return df


def write_parquet(
    df: DataFrame,
    outpath: str,
    partition_cols: list = ["age_group_years_start", "sex_id"],
):

    # Nulls values are automatically dropped for partition columns
    # Assigning a flag of -1 for any nulls in partition columns to prevent data loss
    df = df.fillna(-1, subset=partition_cols)

    # Writing out parquet file with partitions. Directory is written to by multiple tasks,
    # Resulting in appending data. Index used in name to ensure unique filename across all
    # Tasks (preventing overwriting), as long these condtions must be met:
    # 1) MUST BE PARTIONED BY SOMETHING
    # 2) basename_template must be unique
    # 3) existing_data_behavior must be "overwrite_or_ignore" (this is the default)
    df.write.option("maxRecordsPerFile", 1000000).partitionBy(partition_cols).mode(
        "overwrite"
    ).parquet(outpath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Must specify marketscan files to process
    parser.add_argument(
        "-f",
        "--filepaths",
        nargs="+",
        required=True,
        help="Filepaths of MarketScan stage 2 data to process.",
    )

    # Must specify marketscan years to process
    parser.add_argument(
        "-y",
        "--year",
        type=int,
        required=True,
        help="Year of MarketScan stage 2 data being processed.",
    )

    # Must specify outpath
    parser.add_argument(
        "-o",
        "--outpath",
        type=str,
        required=True,
        help="Outpath to write final parquet file to.",
    )

    # Specifying if processing RX or IP/OP
    parser.add_argument(
        "-r",
        "--rx_proc",
        required=True,
        choices=[1, 0],
        type=int,
        help="Flag of whether to process RX or not (processed separately from IP/OP).",
    )

    # Getting args from parser
    args = vars(parser.parse_args())
    filepaths = args["filepaths"]
    year = args["year"]
    outpath = args["outpath"]
    rx_proc = args["rx_proc"]

    # Creating name for current spark session and connecting
    session_name = f"dex_MKTSCN_transform_{year}"
    spark = get_spark_connection(session_name)

    # Getting dataframe from filepaths
    df = spark_read_filepaths(spark, filepaths)

    # Formatting dataframe for stage 3
    transformed_df = run_transformer(df, year, rx_proc)

    # Saving out dataframe to outpath
    write_parquet(transformed_df, outpath)
