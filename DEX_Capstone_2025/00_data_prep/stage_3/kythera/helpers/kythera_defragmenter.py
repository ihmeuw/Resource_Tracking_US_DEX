## ==================================================
## Author(s): Max Weil
## Purpose: This script is used to defragment parquet files.
## ==================================================

import argparse
import pyarrow.dataset as ds
from pathlib import Path
import pyarrow as pa
import shutil


def run_defragmenter(filepath, table):

    # Creating temporary location to save partition data
    temp_filepath = Path(str(filepath).replace(table, f"TEMP_{table}"))

    # Reading in partition data
    data_schema, data_partitioning = replace_schema(filepath, None)

    # Reading in partition data with adjusted data schema
    data = ds.dataset(filepath, partitioning=data_partitioning, schema=data_schema)

    # Saving partition out to temporary filepath in files with 1M rows each
    ds.write_dataset(
        data,
        temp_filepath,
        format="parquet",
        partitioning=data.partitioning,
        max_rows_per_file=1000000,
        min_rows_per_group=1000000,
        max_rows_per_group=1000000,
        existing_data_behavior="delete_matching",
    )

    # Removing fragmented dataset and replacing with defragmented dataset
    shutil.rmtree(filepath)
    shutil.move(temp_filepath, filepath)


def replace_schema(filepath, partitioning=None):

    # Loading in interpretted dataset schema from Pyarrow
    data = ds.dataset(filepath, partitioning=partitioning)
    data_schema = data.schema
    data_partitioning = data.partitioning

    # Replacing any null-column schemas with strings. The default_schema is interpretted from the first file,
    # Which can result in improper interpretations of column types. These are usually handled by pyarrow
    # But can fail when a null schema is detected for a column. Pyarrow tries to apply null-column schemas
    # On later files, which will fail if any values in the column are not null. Interpretting as string
    # changes all nulls to None types.
    null_idxs = [
        idx for idx, datatype in enumerate(data_schema.types) if datatype == pa.null()
    ]
    for idx in null_idxs:
        data_schema = data_schema.set(
            idx, data_schema.field(idx).with_type(pa.string())
        )

    return data_schema, data_partitioning


if __name__ == "__main__":

    # Requesting input and output path arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--filepath",
        type=str,
        required=True,
        help="Filepath of .parquet file or partition to defragment.",
    )

    parser.add_argument(
        "--table", type=str, required=True, help="Name of table being defragmented."
    )

    # Assigning input args to variables
    args = vars(parser.parse_args())
    filepath = Path(args["filepath"])
    table = args["table"]

    # Running defragmenter
    run_defragmenter(filepath, table)
