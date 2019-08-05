#!/usr/bin/python3

import logging
import time

import boto3

logging.basicConfig(
    format="%(asctime)s - %(message)s", level=logging.INFO, datefmt="%d-%b-%y %H:%M:%S"
)

PAUSE_TIME_SEC = 5


def _pause(sec):
    logging.info(f"Pause for {sec} sec ...")
    time.sleep(5)


def _athena():
    return boto3.client("athena")


def _run_query(database: str, query: str, s3_output_path: str):
    response = _athena().start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output_path},
    )

    logging.info(response)
    if _http_ok(response):
        qid = response["QueryExecutionId"]
        _pause(PAUSE_TIME_SEC)
        return _query_results(qid)
    else:
        return _bad_response(response)


def _query_results(query_id: str):
    status_response = _query_status(query_id)
    query_status = status_response.get("query_status")
    metadata_response = (
        _query_metadata(query_id) if _query_success(query_status) else {}
    )
    return {
        "query_id": query_id,
        "query_status": query_status,
        "s3_path": status_response.get("s3_path"),
        "schema": metadata_response.get("schema"),
    }


def _query_success(status: str) -> bool:
    return "SUCCEEDED" == status


def _query_status(qid: str):
    response = _athena().get_query_execution(QueryExecutionId=qid)

    logging.info(response)
    if _http_ok(response):
        return {
            "query_status": response["QueryExecution"]["Status"]["State"],
            "s3_path": response["QueryExecution"]["ResultConfiguration"][
                "OutputLocation"
            ],
        }
    else:
        return _bad_response(response)


def _query_metadata(qid: str):
    response = _athena().get_query_results(QueryExecutionId=qid, MaxResults=1)

    logging.info(response)
    if _http_ok(response):
        schema = _get_result_schema(
            response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        )
        return {"schema": schema}
    else:
        return _bad_response(response)


def _col_def(col):
    return {
        "name": col.get("Label", col.get("Name")),
        "type": col.get("Type"),
        "nullable": col.get("Nullable", "UNKNOWN"),
    }


def _get_result_schema(cols):
    return list(map(_col_def, cols))


def _http_ok(response) -> bool:
    return response["ResponseMetadata"]["HTTPStatusCode"] == 200


def _bad_response(response):
    return {"status": "ERROR", "message": f"{response}"}


result = _run_query(
    "feature_store",
    "select * from feature_store.cli_app_test_ent_1_1;",
    "s3://data-lake/feature_store_stage/query/dump/cli_app_test_ent",
)
