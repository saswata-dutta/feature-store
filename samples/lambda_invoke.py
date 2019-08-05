#!/usr/bin/python3

import json

import boto3


def invoke_lambda(params):
    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName="arn:aws:lambda:ap-south-1:906474297797:function:featurestore-lambda",
        InvocationType="RequestResponse",
        Payload=json.dumps(params),
    )

    print(response)
    payload = response["Payload"].read().decode("utf-8")
    print(payload)

invoke_lambda({"action": "CREATE", "params": {"att": "create", "v": 1}})
invoke_lambda({"action": "UPLOAD", "params": {"att": "upload", "v": 2}})
invoke_lambda({"action": "blah", "params": {"att": "blah", "v": 3}})
