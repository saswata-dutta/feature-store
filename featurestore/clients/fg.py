# -*- coding: utf-8 -*-

import glob
import logging
import time
import uuid
from json import dumps as json_ser, loads as json_dser
from os.path import basename
from typing import Any, Dict, List, Optional, Sequence, Tuple

from botocore.exceptions import ClientError
from pandas import DataFrame as Pandas_df
from pyspark.sql.dataframe import DataFrame as Spark_df

from . import (
    aws_glue,
    aws_lambda,
    aws_s3,
    ist_utils,
    schema_utils,
    spark_utils,
    str_utils,
)

S3_BUCKET: str = "data-lake"
S3_ROOT: str = "feature_store"

S3_STAGE_BUCKET: str = S3_BUCKET
S3_STAGE_ROOT: str = "feature_store_stage"
S3_STAGE_UPLOAD_FOLDER: str = f"{S3_STAGE_ROOT}/uploads"
S3_STAGE_QUERY_FOLDER: str = f"{S3_STAGE_ROOT}/queries"

S3_SCHEMA_FOLDER: str = "schema"
SCHEMA_FILE: str = "schema.json"
S3_DATA_FOLDER: str = "data"

ACTION_KEY: str = "action"
ARGS_KEY: str = "args"
PARAMS_KEY: str = "params"
PATHS_KEY: str = "paths"
SCHEMA_KEY: str = "schema"
STATUS_KEY: str = "status"
PAYLOAD_KEY: str = "payload"
QUERY_ID_KEY: str = "query_id"
QUERY_STATUS_KEY: str = "query_status"
S3_PATH_KEY: str = "s3_path"

ACTION_CREATE: str = "CREATE"
ACTION_CREATE_PARTITION: str = "CREATE_PARTITION"
ACTION_UPLOAD: str = "UPLOAD"
ACTION_DUMP: str = "DUMP"
ACTION_DUMP_STATUS: str = "DUMP_STATUS"

STATUS_OK = "OK"
STATUS_ERROR = "ERROR"

LAMBDA_ENDPOINT: str = (
    "arn:aws:lambda:ap-south-1:906474297797:function:feature-store-lambda:$LATEST"
)

GLUE_DB_NAME = "feature_store"

Schema = Dict[str, Any]
Lambda_params = Dict[str, Any]
Lambda_response = Tuple[bool, Dict[str, Any]]
Paths = List[Tuple[str, str]]


logging.basicConfig(
    format="%(asctime)s - %(message)s", level=logging.INFO, datefmt="%d-%b-%y %H:%M:%S"
)


def create_fg(
    client: str,
    app: str,
    entity: str,
    version: str,
    time_col: str,
    time_col_unit: str = "ms",
    pandas_df: Pandas_df = None,
    spark_df: Spark_df = None,
) -> Lambda_response:
    """Create a Glue table, queryable via Athena,
     based on schema inferred for the supplied pandas/spark data-frame.
     The data-frame must be representative of the data types of all the columns,
     for schema inference.
     Client might be name of business unit for better organisation;
     App might be name of application like crm, payments, compass;
     Entity might be name of the features collection like user_profile;
     Version might be something like v0001,
     Time_col must be a field in the DF, containing epoch,
     based on which data will be partitioned in s3.
     Time_col_unit is the unit of the epoch, must be one of s, ms, us, ns
     Exactly one of pandas_df or spark_df must be present.
     """

    try:
        if pandas_df is not None:
            schema = _get_pandas_schema(pandas_df)
        elif spark_df is not None:
            schema = spark_utils.get_schema(spark_df)
        else:
            raise ValueError("No DataFrame supplied")

        sane_time_col = str_utils.sanitise(time_col)
        schema_utils.validate(schema, sane_time_col, time_col_unit)
        params = _glue_create_table_params(client, app, entity, version, schema)

        schema[schema_utils.SCHEMA_TIME_COL] = sane_time_col
        schema[schema_utils.SCHEMA_TIME_UNIT] = time_col_unit

        rel_path = _s3_schema_rel_path(client, app, entity, version)

        prod_path = _s3_abs_path(S3_ROOT, rel_path)
        _assert_absent_s3(aws_s3.handle(S3_BUCKET, prod_path))

        stage_path = _s3_abs_path(
            f"{S3_STAGE_UPLOAD_FOLDER}/{S3_SCHEMA_FOLDER}", rel_path
        )
        _upload_schema(schema, aws_s3.handle(S3_STAGE_BUCKET, stage_path))

        response = _invoke_lambda(
            ACTION_CREATE, params, schema, [(stage_path, prod_path)]
        )
        return response

    except Exception:
        logging.exception("Failed to create FG")
        return False, {}


def add_fg_partition(
    client: str, app: str, entity: str, version: str, partition_suffixes: Sequence[str]
) -> Lambda_response:
    """Adds the provided list of suffixes in 'y=%Y/m=%m/d=%d' format,
     to the Glue table as partitions in s3"""
    try:
        params = _glue_add_partition_params(
            client, app, entity, version, partition_suffixes
        )
        response = _invoke_lambda(ACTION_CREATE_PARTITION, params)
        return response
    except Exception:
        logging.exception("Failed to upload FG")
        return False, {}


def upload_fg(
    client: str, app: str, entity: str, version: str, pandas_df: Pandas_df
) -> Lambda_response:
    """Uploads the pandas Df data,
     and links the newly added partitions into the Glue table.
     Must match schema specified during create FG call."""

    try:
        schema = _get_pandas_schema(pandas_df)
        # TODO move download and match schema to lambda,
        #  need to pass expected schema path as well
        expected_schema = _download_schema(client, app, entity, version)
        schema_utils.match(expected_schema, schema)

        time_col = expected_schema[schema_utils.SCHEMA_TIME_COL]
        time_col_unit = expected_schema[schema_utils.SCHEMA_TIME_UNIT]
        df_groups = _groupby_time(pandas_df, time_col, time_col_unit)

        # for each folder in s3 need to add the partitions
        paths: Paths = []
        time_suffixes = []
        for time_suffix in df_groups.groups:
            group_paths = _upload_df(
                df_groups.get_group(time_suffix),
                client,
                app,
                entity,
                version,
                time_suffix,
            )

            paths.extend(group_paths)
            time_suffixes.append(time_suffix)

        params = _glue_add_partition_params(
            client, app, entity, version, time_suffixes, schema
        )
        response = _invoke_lambda(ACTION_UPLOAD, params, schema, paths)
        return response

    except Exception:
        logging.exception("Failed to upload FG")
        return False, {}


def dump_fg(sql_query: str,) -> Tuple[bool, str, str, Dict[str, Any]]:
    """Invokes User defined sql query on Athena,
     and dumps csv result in S3.
     Returns the Query-Id which can be used to poll execution status,
     and the S3 dump folder"""

    try:
        result_path = _s3_query_result_path(S3_STAGE_BUCKET, S3_STAGE_QUERY_FOLDER)
        params = _athena_query_params(GLUE_DB_NAME, sql_query, result_path)
        success, response = _invoke_lambda(ACTION_DUMP, params)
        query_id = _lambda_exec_response(response)["QueryExecutionId"]
        return success, query_id, result_path, response
    except Exception:
        logging.exception("Failed to dump FG")
        return False, "", "", {}


def read_fg(query_id: str) -> Optional[Tuple[Optional[Pandas_df], Dict[str, Any]]]:
    """Loads result of 'dump_fg' query into a Pandas-Df.
    Might need to poll this if query is long running.
    Might fail, if result is too large to fetch,
    user must download CSV data from S3 in that case,
    using the query result meta-data provided
    """
    try:
        query_status = _poll_query_status(query_id)
        if _query_success(query_status[QUERY_STATUS_KEY]):
            tmp_file = _download_from_s3(query_status[S3_PATH_KEY])
            spark_df = spark_utils.csv2spark(tmp_file)
            return spark_df.toPandas(), query_status
        else:
            return None, query_status
    except Exception:
        logging.exception("Failed to read FG")
        return None


def get_versions(client: str, app: str, entity: str) -> Sequence[str]:
    """
    Returns list of all available versions of the given inputs.
    The versions are not sorted, and upto the user to find latest;
    as version numbers are not standardised yet.
    :param client:
    :param app:
    :param entity:
    :return:
    """
    version = "???"
    path = _s3_schema_path(S3_ROOT, client, app, entity, version)
    prefix = path.split(f"/{version}")[0]

    keys = aws_s3.ls_files(S3_BUCKET, prefix)
    return _extract_versions(keys, prefix + "/", "/" + SCHEMA_FILE)


def _extract_versions(keys: Sequence[str], prefix: str, suffix: str) -> Sequence[str]:
    items = map(lambda s: str_utils.strip(s, prefix, suffix), keys)
    return list(filter(lambda s: "/" not in s, items))


def _pause(sec: int) -> None:
    logging.info(f"Pause for {sec} sec ...")
    time.sleep(sec)


def _query_completed(status: str) -> bool:
    return status in {"SUCCEEDED", "FAILED", "CANCELLED"}


def _query_success(status: str) -> bool:
    return "SUCCEEDED" == status


def _poll_query_status(query_id: str) -> Dict[str, Any]:
    response: Dict[str, Any] = {}
    for timeout in [5, 10, 15]:
        # prevent spamming abuse
        _pause(timeout)
        response = _query_status(query_id)
        if _query_completed(response[QUERY_STATUS_KEY]):
            break
    else:
        logging.warning(f"Query {query_id} not yet completed, try later")

    return response


def _query_status(query_id: str) -> Dict[str, Any]:
    success, response = _invoke_lambda(ACTION_DUMP_STATUS, {QUERY_ID_KEY: query_id})
    if success:
        return _lambda_exec_response(response)
    else:
        return {QUERY_STATUS_KEY: "UNKNOWN"}


def _s3_query_result_path(bucket: str, root: str) -> str:
    return f"s3://{bucket}/{root}/{_uuid()}"


def _download_from_s3(s3_url: str) -> str:
    bucket, key = aws_s3.parse_url(s3_url)
    fname = basename(key)
    tmp_file = f"/tmp/{fname}"
    aws_s3.save_as(bucket, key, tmp_file)
    return tmp_file


def _athena_query_params(db_name: str, sql_query: str, s3_output_folder: str):
    return {
        "QueryString": sql_query,
        "QueryExecutionContext": {"Database": db_name},
        "ResultConfiguration": {"OutputLocation": s3_output_folder},
    }


def _get_pandas_schema(pandas_df: Pandas_df) -> Schema:
    spark_df = spark_utils.pandas2spark(pandas_df)
    return spark_utils.get_schema(spark_df)


def _s3_schema_rel_path(client: str, app: str, entity: str, version: str) -> str:
    return "/".join([client, app, entity, S3_SCHEMA_FOLDER, version, SCHEMA_FILE])


def _s3_schema_path(root: str, client: str, app: str, entity: str, version: str) -> str:
    return "/".join([root, _s3_schema_rel_path(client, app, entity, version)])


def _s3_data_file(
    root: str,
    client: str,
    app: str,
    entity: str,
    version: str,
    time_suffix: str,
    fname: str,
) -> str:
    return "/".join(
        [_s3_data_partition(root, client, app, entity, version, time_suffix), fname]
    )


def _s3_data_partition(
    root: str, client: str, app: str, entity: str, version: str, time_suffix: str
) -> str:
    return "/".join([_s3_data_folder(root, client, app, entity, version), time_suffix])


def _s3_data_folder(root: str, client: str, app: str, entity: str, version: str) -> str:
    return "/".join([root, client, app, entity, S3_DATA_FOLDER, version])


def _upload_df(
    pandas_df: Pandas_df,
    client: str,
    app: str,
    entity: str,
    version: str,
    time_suffix: str,
) -> Paths:

    prod_prefix = _s3_data_partition(S3_ROOT, client, app, entity, version, time_suffix)
    # abort in case that partitions data is already present in prod
    _assert_folder_absent_s3(S3_BUCKET, prod_prefix)

    ts = ist_utils.current_epoch_millis()
    pq_name = f"_{client}_{app}_{entity}_{ts}"
    local_pq_dir = f"/tmp/{pq_name}"
    spark_df = spark_utils.pandas2spark(pandas_df)
    parquet_paths = _save_parquet_local(spark_df, local_pq_dir)

    # avoid clobbering on concurrent/multiple updates
    folder_id = _uuid()
    stage_root = f"{S3_STAGE_UPLOAD_FOLDER}/{S3_DATA_FOLDER}/{folder_id}"
    stage_prefix = _s3_data_partition(
        stage_root, client, app, entity, version, time_suffix
    )

    paths: Paths = []
    for pq in parquet_paths:
        fname = basename(pq)
        stage_path = "/".join([stage_prefix, fname])
        prod_path = "/".join([prod_prefix, fname])

        s3obj = aws_s3.handle(S3_STAGE_BUCKET, stage_path)
        s3obj.upload_file(pq)
        paths.append((stage_path, prod_path))

    return paths


def _uuid() -> str:
    return str(uuid.uuid4())


def _upload_schema(schema: Schema, s3obj) -> None:
    _assert_absent_s3(s3obj)

    schema_json = json_ser(schema)
    s3obj.put(Body=schema_json, ContentType="application/json; charset=utf-8")


def _s3_abs_path(root: str, rel_path: str) -> str:
    return f"{root}/{rel_path}"


def _assert_folder_absent_s3(bucket: str, prefix: str) -> None:
    items = aws_s3.ls_files(bucket, prefix)
    assert len(items) < 1, f"Some objects exist in S3 under {prefix} : {items}"


def _assert_absent_s3(s3obj) -> None:
    not_found = False
    try:
        # list object is costly
        s3obj.load()
    except ClientError as ex:
        not_found = ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    assert not_found, f"file {s3obj.key} already exists in s3 {s3obj.bucket_name}"


def _download_schema(client: str, app: str, entity: str, version: str) -> Schema:
    path = _s3_schema_path(S3_ROOT, client, app, entity, version)
    stream = aws_s3.get_stream(S3_BUCKET, path)
    schema_json = json_dser(str_utils.stream2str(stream))
    return schema_json


def _get_epoch_secs(
    pandas_df: Pandas_df, index: int, time_col: str, time_col_unit: str
) -> int:
    div = {"s": 1, "ms": 1000, "us": 1000000, "ns": 1000000000}
    return int(pandas_df[time_col].loc[index] / div[time_col_unit])


def _get_record_s3_folder(
    pandas_df: Pandas_df, index: int, time_col: str, time_col_unit: str
) -> str:
    epoch_secs = _get_epoch_secs(pandas_df, index, time_col, time_col_unit)
    return ist_utils.to_partition(epoch_secs)


def _groupby_time(pandas_df: Pandas_df, time_col: str, time_col_unit: str):
    def s3_folder(i: int) -> str:
        return _get_record_s3_folder(pandas_df, i, time_col, time_col_unit)

    return pandas_df.groupby(s3_folder)


def _save_parquet_local(spark_df: Spark_df, fpath: str) -> Sequence[str]:
    spark_df.coalesce(1).write.parquet(fpath)
    return glob.glob(f"{fpath}/*.parquet")


def _glue_table_props(
    client: str, app: str, entity: str, version: str
) -> Tuple[str, str]:
    table_name = str_utils.sanitise(f"{client}_{app}_{entity}_{version}")
    db_name = GLUE_DB_NAME

    return db_name, table_name


def _glue_s3_partition(bucket: str, s3_folder: str) -> str:
    return f"s3://{bucket}/{s3_folder}/"


def _glue_create_table_params(
    client: str, app: str, entity: str, version: str, schema: Schema
) -> Lambda_params:
    db_name, table_name = _glue_table_props(client, app, entity, version)
    s3_data_path = _glue_s3_partition(
        S3_BUCKET, _s3_data_folder(S3_ROOT, client, app, entity, version)
    )

    params = aws_glue.create_table_params(db_name, table_name, s3_data_path, schema)
    return params


def _glue_add_partition_params(
    client: str,
    app: str,
    entity: str,
    version: str,
    time_suffixes: Sequence[str],
    schema: Schema = None,
) -> Lambda_params:
    db_name, table_name = _glue_table_props(client, app, entity, version)
    partitions = list(
        map(
            lambda it: _glue_partition_paths(client, app, entity, version, it),
            time_suffixes,
        )
    )
    return aws_glue.add_partitions_params(db_name, table_name, partitions, schema)


def _glue_partition_paths(
    client: str, app: str, entity: str, version: str, time_suffix: str
) -> str:
    prod_location = _glue_s3_partition(
        S3_BUCKET,
        _s3_data_partition(S3_ROOT, client, app, entity, version, time_suffix),
    )
    return prod_location


def _invoke_lambda(
    action: str, params: Lambda_params, schema: Schema = None, rel_paths: Paths = None
) -> Lambda_response:
    params = {
        ACTION_KEY: action,
        ARGS_KEY: {SCHEMA_KEY: schema, PARAMS_KEY: params, PATHS_KEY: rel_paths},
    }

    response = aws_lambda.invoke(LAMBDA_ENDPOINT, params)

    logging.info(f"Lambda response: \n{response}")
    if _http_ok(response):
        payload = json_dser(str_utils.stream2str(response["Payload"]))
        logging.info(f"Lambda response Payload: \n{payload}")
        return _lambda_status_ok(payload), payload
    else:
        return False, {}


def _http_ok(response) -> bool:
    return response["ResponseMetadata"]["HTTPStatusCode"] == 200


def _lambda_status_ok(payload: Dict[str, Any]) -> bool:
    return payload[STATUS_KEY] == STATUS_OK


def _lambda_exec_response(response: Dict[str, Any]) -> Dict[str, Any]:
    return response[PAYLOAD_KEY]
