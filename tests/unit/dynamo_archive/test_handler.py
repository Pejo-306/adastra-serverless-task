import os
import json
from typing import Dict, Any

import pytest
import boto3

from dynamo_archive import app


@pytest.fixture()
def ddb_stream_event() -> Dict[str, Any]:
    return {
        "Records": [
            {
                "eventID": "3",
                "eventName": "REMOVE",
                "eventVersion": "1.0",
                "eventSource": "aws:dynamodb",
                "awsRegion": "{region}",
                "dynamodb": {
                    "Keys": {
                        "Id": {
                            "N": "101"
                        }
                    },
                    "OldImage": {
                        "message": {
                            "S": "This item has changed"
                        },
                        "id": {
                            "S": "101"
                        }
                    },
                    "SequenceNumber": "333",
                    "SizeBytes": 38,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                },
                "eventSourceARN": "arn:{partition}:dynamodb:{region}:account-id:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899"
            }
        ]
    }


@pytest.fixture()
def destination_bucket() -> str:
    bucket = os.environ.get('DESTINATION_BUCKET')
    if bucket is None:
        raise KeyError("Missing required environmental variable 'DESTINATION_BUCKET'")
    return bucket


def count_objects_in_s3_bucket(bucket: 'boto3.resources.factory.s3.Bucket') -> int:
    count = 0
    for _ in bucket.objects.all():
        count += 1
    return count


def test_lambda_handler(ddb_stream_event: Dict[str, Any], destination_bucket: str) -> None:
    # Connect to the destination test bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(destination_bucket)
    initial_object_count = count_objects_in_s3_bucket(bucket)

    # Call the lambda handler with the appropriate DynamoDB streams event
    response = app.lambda_handler(ddb_stream_event, None)
    data = json.loads(response['body'])

    try:
        assert response['statusCode'] == 200
        assert 'message' in response['body']
        assert data['message'] == f"Successfully archived to s3://{destination_bucket}"
        assert 'records' in response['body']
        assert len(data['records']) == 1
        assert count_objects_in_s3_bucket(bucket) == initial_object_count + 1
    except AssertionError:
        raise
    finally:
        # Delete generated objects from S3 bucket
        delete_list = [{'Key': record_key} for record_key in data['records']]
        bucket.delete_objects(Delete={'Objects': delete_list})
