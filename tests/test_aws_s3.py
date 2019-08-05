from featurestore.clients import aws_s3


def test_parse_url():
    url = "s3://bucket/a/b/c/d/y=2019/m=01/d=21/some.file"
    bucket, key = aws_s3.parse_url(url)

    assert bucket == "bucket"
    assert key == "a/b/c/d/y=2019/m=01/d=21/some.file"
