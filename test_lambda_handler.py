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
    mandatory, optional = get_job_groups(config_data)
    assert mandatory == ["GROUP_1", "GROUP_2"]
    assert optional == {"GROUP_3": "12:00:00ZUTC", "GROUP_4": "12:00:00ZUTC"}

@patch("boto3.resource")
def test_query_dynamo_db_job_status(mock_boto_resource):
    dynamodb_mock = mock_boto_resource.return_value
    table_mock = dynamodb_mock.Table.return_value
    table_mock.query.return_value = {
        'Items': [
            {'job_id': 'GROUP_1-2024-07-29', 'job_status': 'SUCCESS'}
        ]
    }
    table_name = "batch_job_status"
    job_id = "GROUP_1-2024-07-29"
    response = query_dynamo_db_job_status(table_name, job_id)
    assert len(response['Items']) > 0
    assert response['Items'][0]['job_status'] == 'SUCCESS'

@patch("query_dynamo_db_job_status")
def test_check_mandatory_jobs_success(mock_query):
    mock_query.return_value = {'Items': [{'job_id': 'GROUP_1-2024-07-29', 'job_status': 'SUCCESS'}]}
    mandatory_job_groups = ["GROUP_1", "GROUP_2"]
    all_successful, responses, failed_jobs = check_mandatory_jobs_success(mandatory_job_groups)
    assert all_successful
    assert "GROUP_1-2024-07-29" in responses
    assert failed_jobs == []

@patch("query_dynamo_db_job_status")
def test_check_optional_jobs_status(mock_query):
    mock_query.return_value = {'Items': [{'job_id': 'GROUP_3-2024-07-29', 'job_status': 'SUCCESS'}]}
    optional_job_groups = {"GROUP_3": "12:00:00ZUTC", "GROUP_4": "12:00:00ZUTC"}
    all_successful, responses, failed_jobs = check_optional_jobs_status(optional_job_groups)
    assert all_successful
    assert "GROUP_3-2024-07-29" in responses
    assert failed_jobs == []

@patch("boto3.client")
def test_trigger_step_function(mock_boto_client):
    client_mock = mock_boto_client.return_value
    client_mock.start_execution.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    response = trigger_step_function()
    assert 'executionArn' in response

@patch("check_mandatory_jobs_success", return_value=(True, {}, []))
@patch("check_optional_jobs_status", return_value=(True, {}, []))
@patch("trigger_step_function")
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_success(mock_file, mock_trigger, mock_optional, mock_mandatory):
    mock_trigger.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    event = {}
    context = {}
    response = lambda_handler(event, context)
    body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert body['status'] == "success"
    assert body['step_function']['status'] == "triggered"

@patch("check_mandatory_jobs_success", return_value=(True, {}, []))
@patch("check_optional_jobs_status", return_value=(False, {}, ["GROUP_3-2024-07-29"]))
@patch("trigger_step_function")
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_partial_success(mock_file, mock_trigger, mock_optional, mock_mandatory):
    mock_trigger.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    event = {}
    context = {}
    response = lambda_handler(event, context)
    body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert body['status'] == "partial_success"
    assert body['step_function']['status'] == "triggered"

@patch("check_mandatory_jobs_success", return_value=(False, {}, ["GROUP_1-2024-07-29"]))
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_fail(mock_file, mock_mandatory):
    event = {}
    context = {}
    response = lambda_handler(event, context)
    body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert body['status'] == "fail"

@patch("builtins.open", side_effect=Exception("Error loading YAML"))
def test_lambda_handler_exception(mock_file):
    event = {}
    context = {}
    response = lambda_handler(event, context)
    assert response['statusCode'] == 500
    assert "Error loading YAML" in response['body']

if __name__ == '__main__':
    unittest.main()
