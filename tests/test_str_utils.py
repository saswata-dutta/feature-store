from featurestore.clients import str_utils


def test_sanitise():
    result = str_utils.sanitise("  A,.-_b12-  dd__  ")
    assert result == "a_b12_dd"


def test_strip():
    prefix = "s3://bucket/client/app/entity/schema"
    version = "v0000"
    suffix = "/schema.json"

    result = str_utils.strip(f"{prefix}{version}{suffix}", prefix, suffix)
    assert result == version
