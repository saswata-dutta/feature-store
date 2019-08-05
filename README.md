## Introduction 

Implementation of a simple AWS backed feature-store using AWS S3 for storage, Glue for data catalog, and Athena as SQL engine.

## Installation 

`pip3 install -v --upgrade featurestore --extra-index-url http://pypi.featurestore.com --trusted-host pypi.featurestore.com`. 

Alternately use `pipenv` instead of pip

Current version = `0.1.3`

Note: Requires `Python >= 3.5`

## Feature Store Architecture
Feature store is build as using server less technologies.

* Lambda - To trigger Glue and Athena and S3 buckets for final and intermediate data storage
* Glue - Its based on hive meta store. It has pretty nice [UI](https://ap-south-1.console.aws.amazon.com/glue/home?region=ap-south-1#catalog) to discover existing features.
* Athena - Its a server less presto engine which can be used to query any data available on Glue meta store.

DB name for feature store is **feature_store**.

#### Tables names follows convention:
**<client>_<application>_<feature>_<version>**

Example : `feature_sample_consignment_v0001`

## Support for Nested Schema

Feature store supports nested schema in data. Pandas DF with nested schema can be submitted to featurestore
and it will internally handle schema management by discovering schema of DF and storing schema information with data


## API Documentation and Usages

Sample usage to create, upload and query features, present in : 
`featurestore/samples/fg_sample.py`.

```
# create the feature group table

success1, response1 = create_fg(    
client="business",    
app="user",    
entity="activity",    
version="v0001",    
time_col="created_at",    
time_col_unit="ms",    
pandas_df=sample_data)

# upload data to aws s3

success2, response2 = upload_fg(    
client="business",     
app="user",     
entity="activity",    
version="v0001",    
pandas_df=input_data)

# dump query data into s3

success3, query_id, s3path, response3 = dump_fg(        
"select * from feature_store.business_user_activity_v0001;")

# parse back data into a DataFrame

out_df, out_schema = read_fg(query_id)

```

There are additional utils in `featurestore/clients/aws_*` for general AWS services interaction like S3 ls, 
Glue Table creation, etc. Samples can again be found under `featurestore/samples/*`.

P.S. Setup the env vars properly as shown in sample if running from ipython/jupyter notebooks.

Ensure AWS credentials are configured in `~/.aws/credentials`,
and region is set to "ap-south-1", `~/.aws/config`.

#### Registers table in Glue
Registers a glue table using pre-existing data from s3.
s3_data_path must be "s3://" path to a spark compatible parquet/csv file.
db must exist and table non existent
```
from featurestore.clients.aws_glue import register_table
register_table(<db-name>, <table-name>, partition)

Eg:- 
register_table("customer", "demand_attributes", "s3://data-lake/processed/business/publication/demand/demand_attributes/y=2019/m=07/d=01/part-00000-1168e71c-dc92-4f73-8c1a-4fc067086fa4-c000.snappy.parquet")

```
#### Add Partitions in Glue Table
Add partitions present in s3 to preexisting table in Glue,
Add 99 partitions at a time to sidestep boto3 restriction.
Partition paths must contain the "y=1111/m=11/d=11" substring.
```
from featurestore.clients.aws_glue import add_partitions
add_partitions(<db-name>, <table-name>, <list-of-partitions>)

Eg:-
add_partitions("customer", "demand_attributes", ["y=2019/m=07/d=01/"])

```

## Developer Guide 

Following are the details if any one wants to contribute to this repository 

#### How to Build and Package 

`make init` will install dev dependencies, (assumes pipenv preinstalled).

`make build` will create the dist folder containing the wheel and tar.gz of the client code (assumes wheel, setuptool preinstalled).

`make upload-pypi` will upload current build to the s3 hosted PyPi (assumes s3pypi preinstalled).

#### Coding Guidelines  

`make lint` will run tests, check for PEP8 style guides and perform static analysis. (assumes mypy, pytype, black, flake8, pytest preinstalled)

`make tools` will install all the required tools.

* always apply type hints to top level functions
* keep functions limited to 10 to 15 lines
* all interactions with feature_store prod s3, Athena, Glue should be carried out through the Aws Lambda handler.
