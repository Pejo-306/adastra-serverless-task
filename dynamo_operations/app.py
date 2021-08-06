import os
import json
from typing import Dict, Any

import boto3


def lambda_handler(event, context) -> Dict[str, Any]:
    # Connect to the test DynamoDB table
    table_name = os.environ.get('TABLE_NAME')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Insert the item into the database table
    data = json.loads(event['body'])
    payload = data['payload']['Item']
    table.put_item(Item=payload)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "table": table_name,
            'item_primary_key': payload['id']
        }),
    }
