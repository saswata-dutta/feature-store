# -*- coding: utf-8 -*-

from json import dumps as json_ser
from typing import Any, Dict

import boto3

lambdaClient = boto3.client("lambda")


def invoke(endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = lambdaClient.invoke(
        FunctionName=endpoint,
        InvocationType="RequestResponse",
        Payload=json_ser(payload),
    )

    return response
