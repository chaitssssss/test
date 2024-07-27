import pytest
from unittest.mock import MagicMock, patch
import boto3
from src.lambda_handler import lambda_handler, load_yaml, get_mandatory_job_groups, query_dynamo_db_job_status, check_mandatory_jobs_success

def test_load_yaml(monkeypatch):
    mock_yaml_data = {
        'QA': {
            'JOB_GROUP_MANDATORY': {
                'IS_MANDATORY': True,
                'GROUP_1': {
                    'DATASETS': {
                        'alexendria_feature_library_group_77': {'ID': '1234567'},
                        'alexendria_feature_library_group_2032': {'ID': '891010'}
                    }
                }
            }
        }
    }
    monkeypatch.setattr('builtins.open', lambda *args, **kwargs: MagicMock(return_value=mock_yaml_data))
    data = load_yaml('config.yaml')
    assert data == mock_yaml_data

def test_get_mandatory_job_groups():
    config_data = {
        'QA': {
            'JOB_GROUP_MANDATORY': {
                'IS_MANDATORY': True,
                'GROUP_1': {
                    'DATASETS': {
                        'alexendria_feature_library_group_77': {'ID': '1234567'},
                        'alexendria_feature_library_group_2032': {'ID': '891010'}
                    }
                }
            }
        }
    }
    job_groups = get_mandatory_job_groups(config_data)
    assert job_groups == ['GROUP_1']

def test_query_dynamo_db_job_status(monkeypatch):
    mock_table = MagicMock()
    mock_table.query.return_value = {'Items': [{'job_status': 'SUCCESS'}]}
    mock_dynamodb_resource = MagicMock()
    mock_dynamodb_resource.Table.return_value = mock_table
    monkeypatch.setattr(boto3, 'resource', lambda *args, **kwargs: mock_dynamodb_resource)
    response = query_dynamo_db_job_status('job_status_table', 'GROUP_1-2024-07-27')
    assert response == {'Items': [{'job_status': 'SUCCESS'}]}

def test_check_mandatory_jobs_success(monkeypatch):
    mock_table = MagicMock()
    mock_table.query.return_value = {'Items': [{'job_status': 'SUCCESS'}]}
    mock_dynamodb_resource = MagicMock()
    mock_dynamodb_resource.Table.return_value = mock_table
    monkeypatch.setattr(boto3, 'resource', lambda *args, **kwargs: mock_dynamodb_resource)
    
    mandatory_job_groups = ['GROUP_1']
    all_successful, responses = check_mandatory_jobs_success(mandatory_job_groups)
    
    assert all_successful
    assert responses == {
        'GROUP_1-2024-07-27': [{'job_status': 'SUCCESS'}]
    }
