## ==================================================
## Author(s): Sawyer Crosby
## Date: Jan 31, 2025
## Purpose: Helper functions that read and write data
## ==================================================

## --------------------------------------------------------------------------------------------
## Import modules
## --------------------------------------------------------------------------------------------
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow as pa

## function to replace null schemas with string schemas
def replace_null_schema(filepath, partitioning="hive"):
    
    # Loading in interpreted dataset schema from Pyarrow
    data_schema = ds.dataset(filepath, partitioning=partitioning).schema

    # Replacing any null-column schemas with strings. The default schema is interpreted from the first file,
    # Which can result in improper interpretations of column types. These are usually handled by pyarrow
    # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
    # On later files, which will fail if any values in the column are not null. Interpreting as string
    # changes all nulls to None types.
    null_idxs = [idx for idx, datatype in enumerate(data_schema.types) if datatype == pa.null()]
    for idx in null_idxs:
        data_schema = data_schema.set(idx, data_schema.field(idx).with_type(pa.string()))
    
    return data_schema

## function to read the formatted data
def read_formatted(filepath, schema, source, year, age, sex, state):

    if source in ["MDCR", "MDCD", "CHIA_MDCR"]:
        if state != "all":
            parquet_file = pq.ParquetDataset(
                filepath, 
                filters=[
                    ("year_id", "=", year),
                    ("age_group_years_start", "=", age), 
                    ("sex_id", "=", sex),
                    ("st_resi", "=", state)
                ], 
                schema=schema
            )
        else:
            parquet_file = pq.ParquetDataset(
                filepath, 
                filters=[
                    ("year_id", "=", year),
                    ("age_group_years_start", "=", age), 
                    ("sex_id", "=", sex)
                ],
                schema=schema
            )
    else:
        if state != "all":
            parquet_file = pq.ParquetDataset(
                filepath, 
                filters=[
                    ("age_group_years_start", "=", age), 
                    ("sex_id", "=", sex),
                    ("st_resi", "=", state)
                ], 
                schema=schema
            )
        else:
            parquet_file = pq.ParquetDataset(
                filepath, 
                filters=[
                    ("age_group_years_start", "=", age), 
                    ("sex_id", "=", sex)
                ],
                schema=schema
            )
    formatted = parquet_file.read().to_pandas()
    formatted.reset_index(inplace = True, drop = True)

    return formatted

## function to write out the data from causemap
def write_parquet(df, sub_dataset, outpath, state = False, carrier = False):

    ## clear index
    df.reset_index(inplace = True, drop = True)

    ## determine naming/partitions
    partition_cols = ["toc", "year_id", "age_group_years_start", "sex_id"]
    if carrier:
        partition_cols = ["carrier"] + partition_cols
    if state: 
        partition_cols.insert(partition_cols.index("age_group_years_start"), "st_resi") ## insert state before age
    if sub_dataset == "none":
        name = f"part{{i}}.parquet"  ## only one file per partition
    else:
        name = f"{sub_dataset}_part{{i}}.parquet" ## everything besides sub_dataset already in partition

    ## write
    df.to_parquet(outpath, basename_template=name, partition_cols=partition_cols, existing_data_behavior="overwrite_or_ignore")
