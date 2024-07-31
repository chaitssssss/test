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

# Fixture for loading YAML configuration
@pytest.fixture
def mock_load_yaml(mocker):
    mocker.patch("builtins.open", mocker.mock_open(read_data=config_data))
    return load_yaml("dummy_path")

def test_load_yaml(mock_load_yaml):
    config_data = mock_load_yaml
    assert "QA" in config_data
    assert "JOB_GROUP_MANDATORY" in config_data["QA"]
    assert "GROUP_1" in config_data["QA"]["JOB_GROUP_MANDATORY"]

def test_get_job_groups(mock_load_yaml):
    config_data = mock_load_yaml
    mandatory, optional = get_job_groups(config_data)
    assert mandatory == ["GROUP_1", "GROUP_2"]
    assert optional == {"GROUP_3": "12:00:00ZUTC", "GROUP_4": "12:00:00ZUTC"}

# Mock DynamoDB query response
@pytest.fixture
def mock_dynamo_response(mocker):
    mocker.patch("boto3.resource")
    dynamodb_mock = boto3.resource.return_value
    table_mock = dynamodb_mock.Table.return_value
    table_mock.query.return_value = {
        'Items': [
            {'job_id': 'GROUP_1-2024-07-29', 'job_status': 'SUCCESS'}
        ]
    }
    return table_mock

def test_query_dynamo_db_job_status(mock_dynamo_response):
    table_name = "batch_job_status"
    job_id = "GROUP_1-2024-07-29"
    response = query_dynamo_db_job_status(table_name, job_id)
    assert len(response['Items']) > 0
    assert response['Items'][0]['job_status'] == 'SUCCESS'

def test_check_mandatory_jobs_success(mock_load_yaml, mock_dynamo_response):
    config_data = mock_load_yaml
    mandatory_job_groups, _ = get_job_groups(config_data)
    all_successful, responses, failed_jobs = check_mandatory_jobs_success(mandatory_job_groups)
    assert all_successful == True
    assert "GROUP_1-2024-07-29" in responses
    assert failed_jobs == []

def test_check_optional_jobs_status(mock_load_yaml, mock_dynamo_response):
    config_data = mock_load_yaml
    _, optional_job_groups = get_job_groups(config_data)
    all_successful, responses, failed_jobs = check_optional_jobs_status(optional_job_groups)
    assert all_successful == True
    assert "GROUP_3-2024-07-29" in responses
    assert failed_jobs == []

# Mock Step Function response
@pytest.fixture
def mock_step_function(mocker):
    mocker.patch("boto3.client")
    client_mock = boto3.client.return_value
    client_mock.start_execution.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    return client_mock

def test_trigger_step_function(mock_step_function):
    response = trigger_step_function()
    assert 'executionArn' in response

def test_lambda_handler_success(mocker, mock_load_yaml, mock_dynamo_response, mock_step_function):
    event = {}
    context = {}
    mocker.patch("my_lambda_function.check_mandatory_jobs_success", return_value=(True, {}, []))
    mocker.patch("my_lambda_function.check_optional_jobs_status", return_value=(True, {}, []))
    response = lambda_handler(event, context)
    body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert body['status'] == "success"
    assert body['step_function']['status'] == "triggered"

def test_lambda_handler_partial_success(mocker, mock_load_yaml, mock_dynamo_response, mock_step_function):
    event = {}
    context = {}
    mocker.patch("my_lambda_function.check_mandatory_jobs_success", return_value=(True, {}, []))
    mocker.patch("my_lambda_function.check_optional_jobs_status", return_value=(False, {}, ["GROUP_3-2024-07-29"]))
    response = lambda_handler(event, context)
    body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert body['status'] == "partial_success"
    assert body['step_function']['status'] == "triggered"

def test_lambda_handler_fail(mocker, mock_load_yaml, mock_dynamo_response):
    event = {}
    context = {}
    mocker.patch("my_lambda_function.check_mandatory_jobs_success", return_value=(False, {}, ["GROUP_1-2024-07-29"]))
    response = lambda_handler(event, context)
    body = json.loads(response['body'])
    assert response['statusCode'] == 200
    assert body['status'] == "fail"

def test_lambda_handler_exception(mocker):
    event = {}
    context = {}
    mocker.patch("my_lambda_function.load_yaml", side_effect=Exception("Error loading YAML"))
    response = lambda_handler(event, context)
    assert response['statusCode'] == 500
    assert "Error loading YAML" in response['body']
