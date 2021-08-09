import json
import os
import logging
from datetime import datetime
from typing import Dict, Any

import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context: 'LambdaContext') -> Dict[str, Any]:
    # Connect to the destination S3 bucket
    destination_bucket_name = os.environ.get('DESTINATION_BUCKET')
    s3 = boto3.resource('s3')
    destination_bucket = s3.Bucket(destination_bucket_name)

    record_keys = []
    for record in event['Records']:  # archive each record in batch
        if record['eventName'] != 'REMOVE':  # invalid DynamoDB streams event
            response = {
                'statusCode': 500,
                'body': json.dumps({
                    'message': f"Invalid DynamoDB streams event passed ({record['eventName']})"
                })
            }
            break

        old_image = record['dynamodb']['OldImage']
        record_id = list(old_image['id'].values())[0]
        record_key = f'{record_id}_{datetime.now().strftime("%Y-%m-%d %H.%M.%S.%f")}.json'
        record_body = json.dumps(old_image)
        destination_bucket.put_object(Key=record_key, Body=record_body)
        record_keys.append(record_key)
    else:
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'message': f"Successfully archived to s3://{destination_bucket_name}",
                'records': record_keys
            })
        }

    logger.info(response)
    return response
