import json
import os
from typing import Dict, Any

import pytest
import boto3

from dynamo_operations import app


@pytest.fixture()
def apigw_insert_event() -> Dict[str, Any]:
    body = {
        "operation": "insert",
        "payload": {
            "Item": {
                "id": "1234567890",
                "ts": "2021-08-06 14:43:23.687000",
                "name": "test_item"
            }
        }
    }
    return {
        "body": json.dumps(body),
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
        },
        "queryStringParameters": {"foo": "bar"},
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "POST",
        "stageVariables": {"baz": "qux"},
        "path": "/records",
    }


@pytest.fixture()
def apigw_read_event() -> Dict[str, Any]:
    query_string_parameters = {
        'id': '1234567890'
    }
    return {
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
        },
        "queryStringParameters": query_string_parameters ,
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "GET",
        "stageVariables": {"baz": "qux"},
        "path": "/records",
    }


@pytest.fixture()
def apigw_delete_event() -> Dict[str, Any]:
    body = {
        "operation": "delete",
        "payload": {
            "Key": {
                "id": "1234567890"
            }
        }
    }
    return {
        "body": json.dumps(body),
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
        },
        "queryStringParameters": {"foo": "bar"},
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "DELETE",
        "stageVariables": {"baz": "qux"},
        "path": "/records",
    }


@pytest.fixture()
def apigw_invalid_operation_event() -> Dict[str, Any]:
    body = {
        "operation": "put",  # invalid operation
        "payload": {
            "Item": {
                "id": "1234567890",
                "ts": "2021-08-06 14:43:23.687000",
                "name": "test_item"
            }
        }
    }
    return {
        "body": json.dumps(body),
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
        },
        "queryStringParameters": {"foo": "bar"},
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "POST",
        "stageVariables": {"baz": "qux"},
        "path": "/records",
    }


@pytest.fixture()
def table_name() -> str:
    name = os.environ.get('TABLE_NAME')
    if name is None:
        raise KeyError("Missing required environmental variable 'TABLE_NAME'")
    return name


def test_lambda_handler_with_insert_event(apigw_insert_event: Dict[str, Any],
                                          table_name: str) -> None:
    # Connect to the test DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    # Make sure the test item does not already exist in the table
    table_response = table.get_item(Key={'id': '1234567890'})
    assert 'Item' not in table_response.keys()

    try:
        # Execute the Lambda handler with the test event
        response = app.lambda_handler(apigw_insert_event, None)
        data = json.loads(response['body'])
        assert response['statusCode'] == 200
        assert 'table' in response['body']
        assert data['table'] == table_name
        assert 'item' in response['body']
        assert 'id' in data['item'].keys()
        assert data['item']['id'] == '1234567890'

        # Make sure the test item has been written
        table_response = table.get_item(Key={'id': '1234567890'})
        assert 'Item' in table_response.keys()
        item = table_response['Item']
        assert 'id' in item.keys()
        assert item['id'] == '1234567890'
        assert 'ts' in item.keys()
        assert item['ts'] == '2021-08-06 14:43:23.687000'
        assert 'name' in item.keys()
        assert item['name'] == 'test_item'
        assert 'expiration_time' in item.keys()
    finally:
        # Ensure the test item is deleted
        table.delete_item(
            Key={'id': '1234567890'},
            ConditionExpression='attribute_exists(id)'
        )


def test_lambda_handler_with_read_event(apigw_read_event: Dict[str, Any],
                                        table_name: str) -> None:
    # Connect to the test DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    try:
        # Preemptively create the test item
        table.put_item(Item={
            'id': '1234567890',
            'name': 'test_item'
        })
        table_response = table.get_item(Key={'id': '1234567890'})
        assert 'Item' in table_response.keys()
        item = table_response['Item']
        assert 'id' in item.keys()
        assert item['id'] == '1234567890'
        assert 'name' in item.keys()
        assert item['name'] == 'test_item'

        response = app.lambda_handler(apigw_read_event, None)
        data = json.loads(response['body'])
        assert response['statusCode'] == 200
        assert 'table' in response['body']
        assert data['table'] == table_name
        assert 'item' in response['body']
        assert 'id' in data['item'].keys()
        assert data['item']['id'] == '1234567890'
        assert 'name' in data['item'].keys()
        assert data['item']['name'] == 'test_item'
    finally:
        # Ensure the test item is deleted
        table.delete_item(
            Key={'id': '1234567890'},
            ConditionExpression='attribute_exists(id)'
        )


def test_lambda_handler_with_invalid_read_event(apigw_read_event: Dict[str, Any],
                                                table_name: str) -> None:
    # Connect to the test DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    # Make sure the test item does not already exist in the table
    table_response = table.get_item(Key={'id': '1234567890'})
    assert 'Item' not in table_response.keys()

    response = app.lambda_handler(apigw_read_event, None)
    data = json.loads(response['body'])
    assert response['statusCode'] == 404
    assert 'table' in response['body']
    assert data['table'] == table_name
    assert 'item' in response['body']
    assert data['item'] is None


def test_lambda_handler_with_delete_event(apigw_delete_event: Dict[str, Any],
                                          table_name: str) -> None:

    # Connect to the test DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    try:
        # Preemptively create the test item
        table.put_item(Item={
            'id': '1234567890',
            'name': 'test_item'
        })
        table_response = table.get_item(Key={'id': '1234567890'})
        assert 'Item' in table_response.keys()
        item = table_response['Item']
        assert 'id' in item.keys()
        assert item['id'] == '1234567890'

        response = app.lambda_handler(apigw_delete_event, None)
        data = json.loads(response['body'])
        assert response['statusCode'] == 200
        assert 'table' in response['body']
        assert data['table'] == table_name
        assert 'item' in response['body']
        assert 'id' in data['item'].keys()
        assert data['item']['id'] == '1234567890'
    finally:
        # Ensure the test item is deleted
        table_response = table.get_item(Key={'id': '1234567890'})
        if 'Item' in table_response.keys():
            table.delete_item(
                Key={'id': '1234567890'},
                ConditionExpression='attribute_exists(id)'
            )


def test_lambda_handler_with_invalid_delete_event(apigw_delete_event: Dict[str, Any],
                                                  table_name: str) -> None:

    # Connect to the test DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    # Make sure the test item does not already exist in the table
    table_response = table.get_item(Key={'id': '1234567890'})
    assert 'Item' not in table_response.keys()

    response = app.lambda_handler(apigw_delete_event, None)
    data = json.loads(response['body'])
    assert response['statusCode'] == 404
    assert 'table' in response['body']
    assert data['table'] == table_name
    assert 'item' in response['body']
    assert data['item'] is None


def test_lambda_handler_with_invalid_operation_event(apigw_invalid_operation_event: Dict[str, Any],
                                                     table_name: str) -> None:

    # Connect to the test DynamoDB table
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    # Make sure the test item does not already exist in the table
    table_response = table.get_item(Key={'id': '1234567890'})
    assert 'Item' not in table_response.keys()

    response = app.lambda_handler(apigw_invalid_operation_event, None)
    data = json.loads(response['body'])
    assert response['statusCode'] == 400
    assert 'message' in response['body']
    assert data['message'] == "Invalid DynamoDB operation specified; Valid operations: ['read', 'insert', 'delete']"

    # Make sure the test item was not inserted into the table
    table_response = table.get_item(Key={'id': '1234567890'})
    assert 'Item' not in table_response.keys()
