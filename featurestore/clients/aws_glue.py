import logging
import re
from typing import Any, Dict, Sequence, Tuple

import boto3

from . import spark_utils

Schema = Dict[str, Any]

PARTITION_RE = re.compile(r"/y=(\d{4})/m=(\d{2})/d=(\d{2})")

glueClient = boto3.client("glue")

logging.basicConfig(
    format="%(asctime)s - %(message)s", level=logging.INFO, datefmt="%d-%b-%y %H:%M:%S"
)


def register_table(db: str, table: str, s3_file_path: str) -> Dict[str, Any]:
    """
    Registers a glue table using pre-existing data from s3.
    s3_data_path must be "s3://" path to a spark compatible parquet/csv file.
    db must exist and table non existent
    :param db:
    :param table:
    :param s3_file_path:
    :return:
    """
    parsed = extract_y_m_d(s3_file_path)
    s3_data_folder = parsed[0]

    df = spark_utils.read(s3_file_path)
    schema = spark_utils.get_schema(df)

    response = create_table(db, table, s3_data_folder, schema)
    return response


def create_table(
    db: str, table: str, s3_data_path: str, schema: Schema
) -> Dict[str, Any]:
    """
    Create a table in Glue,
    user must have credentials to create in Glue.
    Schema is the hive schema obtained by say "describe table",
    must provide the S3 base folder path of the data.
    :param db:
    :param table:
    :param s3_data_path:
    :param schema:
    :return:
    """
    params = create_table_params(db, table, s3_data_path, schema)
    response = glueClient.create_table(**params)
    logging.info(response)
    return response


GLUE_PARTITION_ADD_BATCH_SIZE: int = 99


def add_partitions(
    db: str, table: str, s3_data_paths: Sequence[str], schema: Schema = None
) -> Sequence[Dict[str, Any]]:
    """
    Adds partitions present in s3 to preexisting table in Glue,
    Add 99 partitions at a time to sidestep boto3 restriction.
    Partition paths must contain the "y=1111/m=11/d=11" substring.
    :param db:
    :param table:
    :param s3_data_paths:
    :param schema:
    :return:
    """
    responses = []
    count = len(s3_data_paths)
    for i in range(0, count, GLUE_PARTITION_ADD_BATCH_SIZE):
        paths = s3_data_paths[i : i + GLUE_PARTITION_ADD_BATCH_SIZE]  # noqa: E203
        params = add_partitions_params(db, table, paths, schema)
        response = glueClient.batch_create_partition(**params)
        responses.append(response)

    return responses


def create_table_params(
    db: str, table: str, s3_data_path: str, schema: Schema
) -> Dict[str, Any]:
    cols = _extract_cols(schema)

    params = {
        "DatabaseName": db,
        "TableInput": {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": _storage_descriptor(cols, s3_data_path),
            "PartitionKeys": [
                {"Name": "y", "Type": "string"},
                {"Name": "m", "Type": "string"},
                {"Name": "d", "Type": "string"},
            ],
        },
    }

    return params


def add_partitions_params(
    db: str, table: str, s3_data_paths: Sequence[str], schema: Schema = None
) -> Dict[str, Any]:
    cols = [] if schema is None else _extract_cols(schema)
    partitions = list(map(lambda it: _partitions(cols, it), s3_data_paths))
    params = {"DatabaseName": db, "TableName": table, "PartitionInputList": partitions}

    return params


def extract_y_m_d(s3_data_path: str) -> Tuple[str, str, Sequence[str]]:
    assert s3_data_path.startswith("s3://"), "Not a valid s3 uri"

    matches = PARTITION_RE.search(s3_data_path)
    if matches:
        ymd = [matches.group(1), matches.group(2), matches.group(3)]
        data_folder = s3_data_path[: matches.start()].rstrip("/") + "/"
        partition_folder = s3_data_path[: matches.end()].rstrip("/") + "/"
        return data_folder, partition_folder, ymd

    raise ValueError("Not a valid s3 partition path")


def _partitions(cols: Sequence[Dict[str, str]], s3location: str):
    parsed = extract_y_m_d(s3location)
    partition_values = parsed[2]
    partition_folder = parsed[1]
    return {
        "Values": partition_values,
        "StorageDescriptor": _storage_descriptor(cols, partition_folder),
    }


def _storage_descriptor(
    cols: Sequence[Dict[str, str]], s3location: str
) -> Dict[str, Any]:
    return {
        "Columns": cols,
        "Location": s3location,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "Name": "SERDE",
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"},
        },
        "Parameters": {"classification": "parquet", "typeOfData": "file"},
    }


def _extract_cols(schema: Schema) -> Sequence[Dict[str, str]]:
    return [{"Name": k, "Type": v} for k, v in schema.items()]
