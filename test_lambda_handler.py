import pytest
from unittest.mock import patch, MagicMock,mock_open
from lambda_handler import lambda_handler, load_yaml, get_other_groups_in_job_group, query_dynamo_db_job_status

@pytest.fixture
def mock_event():
    return {
        "Records": [
            {
                "dynamodb": {
                    "Keys": {
                        "job_id": {"S": "group1-20230701"},
                        "job_start_utc_timestamp": {"N": "1685635200"}
                    },
                    "NewImage": {
                        "job_status": {"S": "SUCCESS"},
                        "job_end_utc_timestamp": {"N": "1685635300"},
                        "failure_message": {"S": ""},
                        "is_failed_job_retryable": {"BOOL": False},
                        "data_s3_paths_associated": {"L": [{"S": "s3://bucket/path1"}, {"S": "s3://bucket/path2"}]}
                    }
                }
            },
            {
                "dynamodb": {
                    "Keys": {
                        "job_id": {"S": "group2-20230701"},
                        "job_start_utc_timestamp": {"N": "1685635400"}
                    },
                    "NewImage": {
                        "job_status": {"S": "FAIL"},
                        "job_end_utc_timestamp": {"N": "1685635500"},
                        "failure_message": {"S": "Job failed"},
                        "is_failed_job_retryable": {"BOOL": True},
                        "data_s3_paths_associated": {"L": [{"S": "s3://bucket/path3"}, {"S": "s3://bucket/path4"}]}
                    }
                }
            }
        ]
    }

def test_lambda_handler_success(mock_event):
    with patch('lambda_handler.load_yaml', return_value={'QA': {'group1': ['group1', 'group2'], 'group2': ['group1', 'group2']}}), \
         patch('lambda_handler.query_dynamo_db_job_status', return_value={'Items': [{'job_status': 'success'}]}):
        lambda_handler(mock_event, None)

def test_lambda_handler_fail(mock_event):
    mock_event['Records'][0]['dynamodb']['NewImage']['job_status'] = {'S': 'FAIL'}
    with patch('lambda_handler.load_yaml', return_value={'QA': {'group1': ['group1', 'group2'], 'group2': ['group1', 'group2']}}):
        lambda_handler(mock_event, None)

def test_lambda_handler_in_progress(mock_event):
    mock_event['Records'][0]['dynamodb']['NewImage']['job_status'] = {'S': 'IN_PROGRESS'}
    lambda_handler(mock_event, None)

def test_lambda_handler_empty_records(mock_event):
    mock_event['Records'] = []
    with pytest.raises(ValueError) as exc:
        lambda_handler(mock_event, None)
    assert str(exc.value) == "Event contains empty 'Records'"

def test_lambda_handler_no_records(mock_event):
    del mock_event['Records']
    with pytest.raises(ValueError) as exc:
        lambda_handler(mock_event, None)
    assert str(exc.value) == "Event does not contain 'Records'"

def test_load_yaml():
    with patch('builtins.open', mock_open(read_data='{"QA": {"group1": ["group1", "group2"], "group2": ["group1", "group2"]}}')) as mock_file:
        data = load_yaml('/path/to/config.yaml')
        assert data == {'QA': {'group1': ['group1', 'group2'], 'group2': ['group1', 'group2']}}

def test_get_other_groups_in_job_group():
    data = {'QA': {'group1': ['group1', 'group2'], 'group2': ['group1', 'group2']}}
    other_groups = get_other_groups_in_job_group(data, 'group1')
    assert other_groups == ['group2']

def test_query_dynamo_db_job_status(monkeypatch):
    mock_table = MagicMock()
    mock_table.query.return_value = {'Items': [{'job_status': 'success'}]}
    monkeypatch.setattr('lambda_handler.boto3.resource', lambda x, region_name=None: MagicMock(table=lambda y: mock_table))
    response = query_dynamo_db_job_status('job_status_table', 'job-id-1')
    assert response == {'Items': [{'job_status': 'success'}]}
