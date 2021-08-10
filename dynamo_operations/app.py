import os
import json
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

import boto3
import botocore.exceptions


def lambda_handler(event: Dict[str, Any], context: 'LambdaContext') -> Dict[str, Any]:
    """AWS Lambda function to interact with a DynamoDB table

    The following DynamoDB operations are supported: READ, INSERT, DELETE.
    The operation type must be specified in the Lambda event's body. If
    an invalid operation is parsed, a 400 Bad Request response is returned.

    An HTTP status response is always returned with the appropriate item
    details. The HTTP response's details are formed by the appropriate
    operation processing function.

    :param event: deserialized Lambda function event
    :type event: dict
    :param context: Lambda function context
    :type context: LambdaContext

    :raises KeyError: environment variable 'TABLE_NAME' is not defined

    :return: HTTP status response
    :rtype: dict
    """
    operations = {
        'read': read_from_db,
        'insert': insert_into_db,
        'delete': delete_from_db
    }

    # Deserialize the event's body
    data = json.loads(event['body'])
    if data['operation'] not in operations.keys():
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': f"Invalid DynamoDB operation specified; Valid operations: {list(operations.keys())}"
            }),
        }

    # Connect to the test DynamoDB table
    table_name = os.environ.get('TABLE_NAME')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Insert the item into the database table
    response = operations[data['operation']](table, event)
    return response


def read_from_db(table: 'boto3.resources.factory.dynamodb.Table',
                 event: Dict[str, Any]) -> Dict[str, Any]:
    """Read an item from the DynamoDB table

    The DynamoDB table stores items with a primary key named 'id' which must be
    provided as an API Gateway's query string parameter.

    The item is retrieved from the DynamoDB table and returned with status
    code 200. If the item is not found in the table, a 404 Not Found status
    is returned.

    :param table: boto3 DynamoDB table instance
    :type: boto3.resources.factory.dynamodb.Table
    :param event: deserialized API Gateway event
    :type: dict

    :return: HTTP response with retrieved item
    :rtype: dict
    """
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
    """Insert an item into the DynamoDB table

    If the item with the specified primary key already exists, the former
    is overridden. In any case a 200 Success HTTP status is returned.

    :param table: boto3 DynamoDB table instance
    :type: boto3.resources.factory.dynamodb.Table
    :param event: deserialized API Gateway event
    :type: dict

    :return: HTTP success response
    :rtype: dict
    """
    expiry_delta = timedelta(minutes=2)
    payload = json.loads(event['body'])['payload']['Item']
    # TODO: document ttl
    expiration_time = (datetime.now() + expiry_delta).replace(tzinfo=timezone.utc).timestamp()
    payload['expiration_time'] = Decimal(expiration_time)
    table.put_item(Item=payload)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'table': table.table_name,
            'item': {
                'id': payload['id']
            }
        }),
    }


def delete_from_db(table: 'boto3.resources.factory.dynamodb.Table',
                   event: Dict[str, Any]) -> Dict[str, Any]:
    """Delete an item from the DynamoDB table

    The item's primary key must be provided in the API Gateway's event body.
    Upon deletion a 200 Success response is provided with the deleted item's
    primary key value. If the item does not exist in the table, a 404 Not Found
    response is returned.

    :param table: boto3 DynamoDB table instance
    :type: boto3.resources.factory.dynamodb.Table
    :param event: deserialized API Gateway event
    :type: dict

    :raises botocore.exceptions.ClientError: boto3 client error when attempting to delete item

    :return: HTTP status response with deleted item primary key
    :rtype: dict
    """
    payload = json.loads(event['body'])['payload']['Key']
    try:
        table.delete_item(Key=payload, ConditionExpression='attribute_exists(id)')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'table': table.table_name,
                    'item': None
                }),
            }
        else:
            raise e
    else:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'table': table.table_name,
                'item': {
                    'id': payload['id']
                }
            }),
        }
