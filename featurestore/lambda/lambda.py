import logging
from typing import Any, Dict, List, Sequence, Tuple

import boto3

Args = Dict[str, Any]
Response = Dict[str, Any]
Schema = Dict[str, Any]
Lambda_params = Dict[str, Any]
Paths = List[Tuple[str, str]]

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

STATUS_OK = "OK"
STATUS_ERROR = "ERROR"

ACTION_CREATE: str = "CREATE"
ACTION_CREATE_PARTITION: str = "CREATE_PARTITION"
ACTION_UPLOAD: str = "UPLOAD"
ACTION_DUMP: str = "DUMP"
ACTION_DUMP_STATUS: str = "DUMP_STATUS"

S3_BUCKET: str = "data-lake"
S3_STAGE_BUCKET: str = S3_BUCKET


logging.basicConfig(
    format="%(asctime)s - %(message)s", level=logging.INFO, datefmt="%d-%b-%y %H:%M:%S"
)


# Dont change the name of this function,
# its registered as the entry point in AWS Lambda config
def handler(event, context) -> Response:
    # unused
    del context

    switcher = {
        ACTION_CREATE: _create_fg,
        ACTION_CREATE_PARTITION: _add_partition,
        ACTION_UPLOAD: _upload_fg,
        ACTION_DUMP: _dump_fg,
        ACTION_DUMP_STATUS: _dump_fg_status,
    }
    action_name = event.get(ACTION_KEY)
    action = switcher.get(action_name)

    if not action:
        return _error_response(f"Illegal Action in {event}")

    args = event.get(ARGS_KEY)
    if not args:
        return _error_response(f"Missing Args {event}")

    result = action(args)
    return result


def _glue() -> Any:
    return boto3.client("glue")


def _athena() -> Any:
    return boto3.client("athena")


def _extract_glue_params(args: Dict[str, Any]) -> Tuple[Lambda_params, Schema, Paths]:
    params = args[PARAMS_KEY]
    schema = args[SCHEMA_KEY]
    paths = args[PATHS_KEY]

    logging.info(f"{params} | {schema} | {paths}")
    return params, schema, paths


def _create_fg(args: Dict[str, Any]) -> Response:
    try:
        params, schema, paths = _extract_glue_params(args)
        _s3_copy(*paths[0])
        result = _glue().create_table(**params)
        return _action_status(result)
    except Exception as ex:
        message = f"Failed to create table : Bad Args {args}"
        return _error_response(message, ex)


def _upload_fg(args: Dict[str, Any]) -> Response:
    try:
        params, schema, paths = _extract_glue_params(args)
        for path in paths:
            _s3_copy(*path)

        result = _glue().batch_create_partition(**params)
        return _action_status(result)
    except Exception as ex:
        message = f"Failed to update partition : Bad Args {args}"
        return _error_response(message, ex)


def _add_partition(args: Dict[str, Any]) -> Response:
    params = args.get(PARAMS_KEY)
    try:
        result = _glue().batch_create_partition(**params)
        return _action_status(result)
    except Exception as ex:
        message = f"Failed to update partition : Bad Args {args}"
        return _error_response(message, ex)


def _dump_fg(args: Dict[str, Any]) -> Response:
    params = args.get(PARAMS_KEY)
    try:
        result = _athena().start_query_execution(**params)
        return _action_status(result)
    except Exception as ex:
        message = f"Failed to start athena query : Bad Args {params}"
        return _error_response(message, ex)


def _dump_fg_status(args: Dict[str, Any]) -> Response:
    try:
        params = args[PARAMS_KEY]
        result = _query_results(params[QUERY_ID_KEY])
        return {STATUS_KEY: STATUS_OK, PAYLOAD_KEY: result}
    except Exception as ex:
        message = f"Failed to get athena query status: Bad Args {args}"
        return _error_response(message, ex)


def _error_response(message, ex=None) -> Response:
    logging.exception(message)
    error_message = " ".join(ex.args) if ex else ""
    return {
        STATUS_KEY: STATUS_ERROR,
        "message": f"{message} \n {error_message}",
        "exception": ex,
    }


def _s3_copy(stage_path: str, prod_path: str) -> None:
    s3 = boto3.resource("s3")
    stage = {"Bucket": S3_STAGE_BUCKET, "Key": stage_path}
    prod = s3.Bucket(S3_BUCKET)
    prod.copy(stage, prod_path)

    logging.info(f"Copied {stage} to {prod}")


def _http_ok(response: Dict[str, Any]) -> bool:
    return response["ResponseMetadata"]["HTTPStatusCode"] == 200


def _action_status(response: Dict[str, Any]) -> Response:
    status = STATUS_OK if _http_ok(response) else STATUS_ERROR
    return {STATUS_KEY: status, PAYLOAD_KEY: response}


def _query_results(query_id: str) -> Dict[str, Any]:
    status_response = _query_status(query_id)
    query_status = status_response[QUERY_STATUS_KEY]
    metadata_response = (
        _query_metadata(query_id) if _query_success(query_status) else {}
    )
    return {
        QUERY_ID_KEY: query_id,
        QUERY_STATUS_KEY: query_status,
        S3_PATH_KEY: status_response.get(S3_PATH_KEY),
        SCHEMA_KEY: metadata_response.get(SCHEMA_KEY),
    }


def _query_success(status: str) -> bool:
    return "SUCCEEDED" == status


def _query_status(query_id: str) -> Dict[str, str]:
    response = _athena().get_query_execution(QueryExecutionId=query_id)

    logging.info(response)
    if _http_ok(response):
        return {
            QUERY_STATUS_KEY: response["QueryExecution"]["Status"]["State"],
            S3_PATH_KEY: response["QueryExecution"]["ResultConfiguration"][
                "OutputLocation"
            ],
        }
    else:
        return _error_response(response)


def _query_metadata(query_id: str) -> Dict[str, Any]:
    response = _athena().get_query_results(QueryExecutionId=query_id, MaxResults=1)

    logging.info(response)
    if _http_ok(response):
        schema = _get_result_schema(
            response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        )
        return {SCHEMA_KEY: schema}
    else:
        return _error_response(response)


def _col_def(col: Dict[str, str]) -> Dict[str, str]:
    return {
        "name": col.get("Label", col["Name"]),
        "type": col["Type"],
        "nullable": col.get("Nullable", "UNKNOWN"),
    }


def _get_result_schema(cols: Sequence[Dict[str, str]]) -> Sequence[Dict[str, str]]:
    return list(map(_col_def, cols))
