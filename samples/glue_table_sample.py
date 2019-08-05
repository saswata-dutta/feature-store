import os

from featurestore.clients.aws_glue import add_partitions, register_table
from featurestore.clients.aws_s3 import ls_files

os.environ["SPARK_LOCAL_IP"] = "localhost"
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

s3 = "s3://data-lake/feature_store/cli/app/test_ent/data/1.1/y=2009/m=09/d=30/part-00000-15f1b712-9b13-4b3f-bd9e-e01071d45ec9-c000.snappy.parquet"

db = "feature_store"
tbl = "test"

# any arbitrary db, tbl and s3
register_table(db, tbl, s3)

# get all files in s3
bucket = "data-lake"
prefix = "feature_store/cli/app/test_ent/data/1.1/"
file_keys = ls_files(bucket, prefix)
folder_keys = set(map(lambda it: os.path.dirname(it), file_keys))
s3_paths = list(map(lambda it: f"s3://{bucket}/{it}/", folder_keys))

# will fail if any of the partitions exist
add_partitions(db, tbl, s3_paths)
