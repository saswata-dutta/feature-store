import pytest

from featurestore.clients import schema_utils


def test_validate():
    assert schema_utils.validate({"x": "int"}, "x", "s")


def _validate_fail(schema, time_col, time_unit, message):
    with pytest.raises(AssertionError) as err:
        schema_utils.validate(schema, time_col, time_unit)

    assert err.value.args[0] == message


def test_validate_no_time_col():
    _validate_fail(None, None, "s", "missing time_col")


def test_validate_empty_time_col():
    _validate_fail(None, "  1 ", "s", "time_col has no alphabets")


def test_validate_bad_time_unit():
    _validate_fail(None, "x", "xxx", "unknown time_col_unit xxx")


def test_validate_absent_time_col():
    _validate_fail({"x": "int"}, "xx", "s", "xx absent from data-frame")


def test_validate_non_numeric_time_col():
    _validate_fail({"x": "string"}, "x", "s", "non numeric epoch in frame")


def test_validate_ymd_in_schema():
    _validate_fail(
        {"x": "int", "y": "string"}, "x", "s", "Data-Frame has a column named y"
    )
    _validate_fail(
        {"x": "int", "m": "int"}, "x", "s", "Data-Frame has a column named m"
    )
    _validate_fail(
        {"x": "int", "d": "float"}, "x", "s", "Data-Frame has a column named d"
    )


expected_schema = {
    "name": "string",
    "ts": "bigint",
    "__time_col__": "ts",
    "__time_col_unit__": "s",
}


def test_match():
    assert schema_utils.match(expected_schema, {"name": "string", "ts": "bigint"})


def _match_fail(expected, actual, message):
    with pytest.raises(AssertionError) as err:
        schema_utils.match(expected, actual)

    assert err.value.args[0] == message


def test_match_extra_cols():
    _match_fail(
        expected_schema,
        {"name": "string", "ts": "bigint", "a": "int"},
        "Extra cols in current schema",
    )


def test_match_fewer_cols():
    _match_fail(expected_schema, {"ts": "bigint"}, "Missing cols in current schema")


def test_match_renamed_cols():
    _match_fail(
        expected_schema,
        {"name1": "string", "ts": "bigint"},
        "Missing cols in current schema",
    )


def test_match_type_mismatch():
    _match_fail(
        expected_schema,
        {"name": "int", "ts": "bigint"},
        "Type mis-match in schema : name",
    )


def test_match_missing_time_col():
    _match_fail(
        {"ts": "bigint", "__time_col__": "ts1"},
        {"ts": "bigint"},
        "Missing ts1 in schema",
    )
