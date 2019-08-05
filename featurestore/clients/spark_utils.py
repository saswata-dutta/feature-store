# -*- coding: utf-8 -*-

import logging
import shutil
from typing import Any, Dict

from pandas import DataFrame as Pandas_df
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as Spark_df

from . import aws_s3, str_utils

Schema = Dict[str, Any]

TMP_TABLE_NAME: str = "tmp_tbl"
SPARK_WAREHOUSE: str = "/tmp/tmp-spark"


def sparkSession() -> SparkSession:
    return (
        SparkSession.builder.config("spark.sql.warehouse.dir", SPARK_WAREHOUSE)
        .master("local")
        .appName("featurestore")
        .getOrCreate()
    )


def get_schema(spark_df: Spark_df) -> Schema:
    try:
        # remove leftover local spark table if present
        shutil.rmtree(f"{SPARK_WAREHOUSE}/{TMP_TABLE_NAME}")
    except Exception:
        logging.info(
            f"Failed to clear local spark table {SPARK_WAREHOUSE}/{TMP_TABLE_NAME}"
        )

    spark_df.write.saveAsTable(TMP_TABLE_NAME, mode="overwrite")
    spark = sparkSession()
    desc_tbl = spark.sql(f"DESCRIBE {TMP_TABLE_NAME}")

    schema = [
        (str_utils.sanitise(r["col_name"]), r["data_type"])
        for r in desc_tbl.rdd.collect()
    ]

    cols = list(map(lambda x: x[0], schema))
    assert len(cols) == len(
        set(cols)
    ), f"DF has duplicate column names after sanitization {cols}"

    return dict(schema)


def parquet2spark(fname: str) -> Spark_df:
    spark = sparkSession()
    return spark.read.format("parquet").load(fname)


def csv2spark(fname: str) -> Spark_df:
    spark = sparkSession()
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quoteMode", "ALL")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(fname)
    )


def pandas2spark(pandas_df: Pandas_df) -> Spark_df:
    spark = sparkSession()
    spark_df = spark.createDataFrame(
        pandas_df.rename(str_utils.sanitise, axis="columns"), samplingRatio=1.0
    )
    return spark_df


def read(fname: str) -> Spark_df:
    file = _get_file(fname)

    if fname.endswith(".csv"):
        return csv2spark(file)
    elif fname.endswith(".parquet"):
        return parquet2spark(file)
    else:
        raise ValueError("Unsupported File Format")


def _get_file(fname: str) -> str:
    """
    downloads fname from s3, to local dir,
    avoids s3 file system class load error in spark
    :param fname:
    :return:
    """
    if fname.startswith("s3"):
        bucket, key = aws_s3.parse_url(fname)
        tmp_file = "/tmp/" + key.replace("/", "_")
        aws_s3.save_as(bucket, key, tmp_file)
        return tmp_file
    else:
        return fname
