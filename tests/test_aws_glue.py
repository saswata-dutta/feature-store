from featurestore.clients import aws_glue


def test_extract_y_m_d():
    url = "s3://bucket/a/b/c/d/y=2019/m=01/d=21/some.file"
    data_folder, partition_folder, ymd = aws_glue.extract_y_m_d(url)

    assert data_folder == "s3://bucket/a/b/c/d/"
    assert partition_folder == "s3://bucket/a/b/c/d/y=2019/m=01/d=21/"
    assert ymd == ["2019", "01", "21"]
