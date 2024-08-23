# Sample event and context
event = {}
context = MagicMock()
context.invoked_function_arn = "arn:aws:lambda:region:123456789012:function:test-function"

# Mock environment variables
@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    monkeypatch.setenv("ENV", "qa")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("STEP_FUNCTION_ARN", "arn:aws:states:us-west-2:123456789012:stateMachine:testStateMachine")

# Mock the current time
@pytest.fixture
def mock_datetime_now(monkeypatch):
    fixed_now = datetime(2023, 8, 22, 16, 0, 0)
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now.replace(tzinfo=tz)

    monkeypatch.setattr("src.lambda_handler.datetime", FixedDateTime)

# Mock the load_yaml function
@pytest.fixture
def mock_load_yaml(monkeypatch):
    mock_data = {
        "qa": {
            "JOB_GROUP_MANDATORY": {
                "IS_MANDATORY": True,
                "CUT_OFF_TIME": "18:00:00ZUTC",
                "group_card_population.customer": {},
                "group_ra_source_2.customer": {}
            },
            "JOB_GROUP_OPTIONAL": {
                "CUT_OFF_TIME": "19:00:00ZUTC",
                "group_suppressions_3.account": {},
                "group_suppressions.customer": {}
            }
        }
    }
    monkeypatch.setattr("src.lambda_handler.load_yaml", lambda _: mock_data)

# Mock the get_job_groups function
@pytest.fixture
def mock_get_job_groups(monkeypatch):
    mock_response = (
        ["group_card_population.customer", "group_ra_source_2.customer"],  # Mandatory jobs
        {"group_suppressions_3.account": "19:00:00ZUTC", "group_suppressions.customer": "19:00:00ZUTC"},  # Optional jobs
        "18:00:00ZUTC",  # Mandatory cut-off time
        "19:00:00ZUTC"  # Optional cut-off time
    )
    monkeypatch.setattr("src.lambda_handler.get_job_groups", MagicMock(return_value=mock_response))

# Mock DynamoDB response for successful job status
@pytest.fixture
def mock_dynamo_db_response_success(monkeypatch):
    mock_response = {
        'Items': [
            {
                'job_id': 'group_card_population.customer|2023-08-22',
                'job_status': 'SUCCEEDED'
            }
        ]
    }
    mock_query = MagicMock(return_value=mock_response)
    monkeypatch.setattr("src.lambda_handler.query_dynamo_db_job_status", mock_query)

# Test case when all mandatory and optional jobs are successful
@patch("src.lambda_handler.trigger_step_function", return_value={"executionArn": "arn:aws:states:us-west-2:123456789012:execution:test"})
@patch("src.lambda_handler.reschedule_lambda")
@patch("src.lambda_handler.cleanup_cloudwatch_rule")
def test_lambda_handler_all_successful(mock_cleanup, mock_reschedule, mock_trigger, mock_get_job_groups, mock_load_yaml, mock_dynamo_db_response_success, mock_datetime_now):
    response = lambda_handler(event, context)
    assert response["mandatory_jobs_status"]["all_successful"] is True
    assert response["optional_jobs_status"]["all_successful"] is True
    assert response["status"] == "completed"
    assert "step_function" in response

# Test case when mandatory jobs are yet to trigger
@pytest.fixture
def mock_dynamo_db_response_yet_to_trigger(monkeypatch):
    mock_response = {'Items': []}
    mock_query = MagicMock(return_value=mock_response)
    monkeypatch.setattr("src.lambda_handler.query_dynamo_db_job_status", mock_query)

@patch("src.lambda_handler.reschedule_lambda")
def test_lambda_handler_mandatory_yet_to_trigger(mock_reschedule, mock_get_job_groups, mock_load_yaml, mock_dynamo_db_response_yet_to_trigger, mock_datetime_now):
    response = lambda_handler(event, context)
    assert response["mandatory_jobs_status"]["all_successful"] is False
    assert response["status"] == "waiting"
    mock_reschedule.assert_called_once()

# Test case when mandatory jobs have failed
@pytest.fixture
def mock_dynamo_db_response_failed(monkeypatch):
    mock_response = {
        'Items': [
            {
                'job_id': 'group_card_population.customer|2023-08-22',
                'job_status': 'FAILED'
            }
        ]
    }
    mock_query = MagicMock(return_value=mock_response)
    monkeypatch.setattr("src.lambda_handler.query_dynamo_db_job_status", mock_query)

@patch("src.lambda_handler.cleanup_cloudwatch_rule")
def test_lambda_handler_mandatory_failed(mock_cleanup, mock_get_job_groups, mock_load_yaml, mock_dynamo_db_response_failed, mock_datetime_now):
    response = lambda_handler(event, context)
    assert response["mandatory_jobs_status"]["all_successful"] is False
    assert response["status"] == "failed"
    mock_cleanup.assert_called_once()
