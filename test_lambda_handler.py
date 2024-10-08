import pytest
from unittest.mock import MagicMock, patch, mock_open
import boto3
from src.lambda_handler import (
    lambda_handler, load_yaml, get_job_groups, query_dynamo_db_job_status,
    check_mandatory_jobs_success, check_optional_jobs_status, trigger_step_function,
    reschedule_lambda, cleanup_cloudwatch_rule
)
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

# Sample config data for testing
config_data = """
QA:
  JOB_GROUP_MANDATORY:
    IS_MANDATORY: true
    GROUP_1:
      DATASETS:
        enterprise_customer_account_relationships:
          ID: "1234567"
    GROUP_2:
      DATASETS:
        EC_CUST_ACCT_BC:
          ID: "8901010"
  JOB_GROUP_OPTIONAL:
    IS_MANDATORY: false
    CUT_OFF_TIME: "12:00:00ZUTC"
    GROUP_3:
      DATASETS:
        Card_Product_Registry_OneLake:
          ID: "10001"
    GROUP_4:
      DATASETS:
        Dapp_Batch:
          ID: "0001"
"""

@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_load_yaml(mock_file):
    config_data = load_yaml("dummy_path")
    assert "QA" in config_data
    assert "JOB_GROUP_MANDATORY" in config_data["QA"]
    assert "GROUP_1" in config_data["QA"]["JOB_GROUP_MANDATORY"]

@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_get_job_groups(mock_file):
    config_data = load_yaml("dummy_path")
    mandatory, optional, cut_off_time = get_job_groups(config_data)
    assert mandatory == ["GROUP_1", "GROUP_2"]
    assert optional == {"GROUP_3": "12:00:00ZUTC", "GROUP_4": "12:00:00ZUTC"}
    assert cut_off_time == "12:00:00ZUTC"

@patch("boto3.resource")
def test_query_dynamo_db_job_status(mock_boto_resource):
    dynamodb_mock = mock_boto_resource.return_value
    table_mock = dynamodb_mock.Table.return_value
    table_mock.query.return_value = {
        'Items': [
            {'job_id': 'GROUP_1|2024-08-02', 'job_status': 'SUCCEEDED'}
        ]
    }
    table_name = "batch_job_status"
    job_id = "GROUP_1|2024-08-02"
    response = query_dynamo_db_job_status(table_name, job_id)
    assert len(response['Items']) > 0
    assert response['Items'][0]['job_status'] == 'SUCCEEDED'

@patch("src.lambda_handler.query_dynamo_db_job_status")
def test_check_mandatory_jobs_success(mock_query):
    mock_query.return_value = {'Items': [{'job_id': 'GROUP_1|2024-08-02', 'job_status': 'SUCCEEDED'}]}
    mandatory_job_groups = ["GROUP_1", "GROUP_2"]
    all_successful, responses, failed_jobs, yet_to_trigger_jobs = check_mandatory_jobs_success(mandatory_job_groups)
    assert all_successful
    assert "GROUP_1|2024-08-02" in responses
    assert failed_jobs == []
    assert yet_to_trigger_jobs == []

@patch("src.lambda_handler.query_dynamo_db_job_status")
def test_check_optional_jobs_status(mock_query):
    mock_query.return_value = {'Items': [{'job_id': 'GROUP_3|2024-08-02', 'job_status': 'SUCCEEDED'}]}
    optional_job_groups = {"GROUP_3": "12:00:00ZUTC", "GROUP_4": "12:00:00ZUTC"}
    all_successful, responses, failed_jobs, yet_to_trigger_jobs = check_optional_jobs_status(optional_job_groups)
    assert all_successful
    assert "GROUP_3|2024-08-02" in responses
    assert failed_jobs == []
    assert yet_to_trigger_jobs == []

@patch("boto3.client")
def test_trigger_step_function(mock_boto_client):
    client_mock = mock_boto_client.return_value
    client_mock.start_execution.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    context = "account"
    response = trigger_step_function(context)
    assert 'executionArn' in response

@patch("boto3.client")
def test_reschedule_lambda(mock_boto_client):
    cloudwatch_mock = mock_boto_client.return_value
    lambda_arn = "arn:aws:lambda:region:account-id:function:function-name"
    reschedule_lambda(lambda_arn)
    cloudwatch_mock.put_rule.assert_called()
    cloudwatch_mock.put_targets.assert_called()

@patch("boto3.client")
def test_cleanup_cloudwatch_rule(mock_boto_client):
    cloudwatch_mock = mock_boto_client.return_value
    cloudwatch_mock.describe_rule.return_value = {'Name': 'RescheduleLambdaRule'}
    cleanup_cloudwatch_rule()
    cloudwatch_mock.remove_targets.assert_called()
    cloudwatch_mock.delete_rule.assert_called()

@patch("boto3.client")
@patch("src.lambda_handler.reschedule_lambda")
@patch("src.lambda_handler.cleanup_cloudwatch_rule")
@patch("src.lambda_handler.trigger_step_function")
@patch("src.lambda_handler.check_optional_jobs_status", return_value=(True, {}, []))
@patch("src.lambda_handler.check_mandatory_jobs_success", return_value=(True, {}, [], []))
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_all_success(mock_file, mock_mandatory, mock_optional, mock_trigger, mock_cleanup, mock_reschedule, mock_lambda_client):
    mock_trigger.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    event = {}
    context = MagicMock()
    context.invoked_function_arn = "arn:aws:lambda:region:account-id:function:function-name"
    response = lambda_handler(event, context)
    body = response['body']
    assert response['statusCode'] == 200
    assert body['status'] == "completed"

@patch("boto3.client")
@patch("src.lambda_handler.reschedule_lambda")
@patch("src.lambda_handler.cleanup_cloudwatch_rule")
@patch("src.lambda_handler.trigger_step_function")
@patch("src.lambda_handler.check_optional_jobs_status", return_value=(False, {}, [], []))
@patch("src.lambda_handler.check_mandatory_jobs_success", return_value=(True, {}, [], []))
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_pending(mock_file, mock_mandatory, mock_optional, mock_trigger, mock_cleanup, mock_reschedule, mock_lambda_client):
    mock_trigger.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    event = {}
    context = MagicMock()
    context.invoked_function_arn = "arn:aws:lambda:region:account-id:function:function-name"
    response = lambda_handler(event, context)
    body = response['body']
    assert response['statusCode'] == 200
    assert body['status'] == "pending"

@patch("boto3.client")
@patch("src.lambda_handler.reschedule_lambda")
@patch("src.lambda_handler.cleanup_cloudwatch_rule")
@patch("src.lambda_handler.trigger_step_function")
@patch("src.lambda_handler.check_optional_jobs_status", return_value=(False, {}, [], []))
@patch("src.lambda_handler.check_mandatory_jobs_success", return_value=(False, {}, [], []))
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_failure(mock_file, mock_mandatory, mock_optional, mock_trigger, mock_cleanup, mock_reschedule, mock_lambda_client):
    event = {}
    context = MagicMock()
    context.invoked_function_arn = "arn:aws:lambda:region:account-id:function:function-name"
    response = lambda_handler(event, context)
    body = response['body']
    assert response['statusCode'] == 500
    assert body['status'] == "failure"
