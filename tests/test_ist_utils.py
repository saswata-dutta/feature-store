from featurestore.clients import ist_utils


def test_to_partition():
    result = ist_utils.to_partition(1562956200)

    assert result == "y=2019/m=07/d=13"
