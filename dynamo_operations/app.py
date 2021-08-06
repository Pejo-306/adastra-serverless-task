import os
import json
from typing import Dict, Any

import boto3


def read_from_db(table: 'boto3.resources.factory.dynamodb.Table',
                 event: Dict[str, Any]) -> Dict[str, Any]:
    item_pk = event['queryStringParameters']['id']
    table_response = table.get_item(Key={'id': item_pk})
    if 'Item' not in table_response.keys():  # return not found response
        return {
            'statusCode': 404,
            'body': json.dumps({
                'table': table.table_name,
                'item': None
            }),
        }

    return {
        'statusCode': 200,
        'body': json.dumps({
            'table': table.table_name,
            'item': table_response['Item']
        }),
    }


def insert_into_db(table: 'boto3.resources.factory.dynamodb.Table',
                   event: Dict[str, Any]) -> Dict[str, Any]:
    payload = json.loads(event['body'])['payload']['Item']
    table.put_item(Item=payload)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'table': table.table_name,
            'item_primary_key': payload['id']
        }),
    }


def lambda_handler(event, context) -> Dict[str, Any]:
    operations = {
        'read': read_from_db,
        'insert': insert_into_db,
    }

    # Connect to the test DynamoDB table
    table_name = os.environ.get('TABLE_NAME')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Insert the item into the database table
    data = json.loads(event['body'])
    response = operations[data['operation']](table, event)
    return response
