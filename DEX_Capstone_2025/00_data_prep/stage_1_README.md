# Stage 1

### Purpose
In the data processing pipeline, stage 1 handle conversion of raw data files to the parquet file format. This is done to ensure efficient use of storage space, improve file reading speed by allowing for filters/partitions, and to have data in a consistent format across multiple sources.

### Code
Because stage 1 code primarily consists of interal filepaths and other sensitive information, it has not been included in this repository. Stage 1 data is identical to the raw input data, except for changes in file format.