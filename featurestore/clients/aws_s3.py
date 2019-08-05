# -*- coding: utf-8 -*-

from typing import Optional, Sequence, Tuple
from urllib.parse import urlparse

import boto3

s3resource = boto3.resource("s3")


def handle(bucket: str, path: str):
    return s3resource.Object(bucket, path)


def ls_files(bucket: str, prefix: Optional[str] = None) -> Sequence[str]:
    s3bucket = s3resource.Bucket(bucket)

    if prefix is not None:
        items = s3bucket.objects.filter(Prefix=prefix)
    else:
        items = s3bucket.objects.all()

    rel_paths = map(lambda x: x.key, items)
    files = filter(lambda x: not x.endswith("/"), rel_paths)

    return list(files)


def parse_url(s3url: str) -> Tuple[str, str]:
    """
    splits s3 url into bucket and key
    :param s3url:
    :return:
    """
    assert s3url.startswith("s3://"), f"{s3url} doesnt start with 's3'"
    parsed = urlparse(s3url, allow_fragments=False)
    return parsed.netloc, parsed.path.lstrip("/")


def get_stream(bucket: str, key: str):
    obj = handle(bucket, key)
    return obj.get()["Body"]


def save_as(bucket: str, key: str, fname: str):
    obj = handle(bucket, key)
    obj.download_file(fname)
