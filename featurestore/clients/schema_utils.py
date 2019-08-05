# -*- coding: utf-8 -*-

from typing import Any, Dict

Schema = Dict[str, Any]
SCHEMA_TIME_COL: str = "__time_col__"
SCHEMA_TIME_UNIT: str = "__time_col_unit__"


def validate(schema: Schema, time_col: str, time_col_unit: str) -> bool:
    assert time_col, "missing time_col"
    assert sum(c.isalpha() for c in time_col) > 0, "time_col has no alphabets"

    assert time_col_unit in {
        "s",
        "ms",
        "us",
        "ns",
    }, f"unknown time_col_unit {time_col_unit}"

    headers = set(schema.keys())
    assert time_col in headers, f"{time_col} absent from data-frame"
    assert schema[time_col] in {"int", "bigint"}, f"non numeric epoch in frame"
    for c in ["y", "m", "d"]:
        assert c not in headers, f"Data-Frame has a column named {c}"

    return True  # for easy testing


def match(expected_schema: Schema, actual_schema: Schema) -> bool:
    expected_cols = set(expected_schema.keys()).difference(
        {SCHEMA_TIME_COL, SCHEMA_TIME_UNIT}
    )
    actual_cols = set(actual_schema.keys())

    common_cols = expected_cols & actual_cols
    assert len(common_cols) == len(expected_cols), "Missing cols in current schema"
    assert len(common_cols) == len(actual_cols), "Extra cols in current schema"

    for col in expected_cols:
        assert col in actual_schema, f"Missing col {col}"
        assert (
            expected_schema[col] == actual_schema[col]
        ), f"Type mis-match in schema : {col}"

    # can happen if schema is altered in aws directly
    time_col = expected_schema[SCHEMA_TIME_COL]
    assert time_col in actual_schema, f"Missing {time_col} in schema"

    return True  # for easy testing
