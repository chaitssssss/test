@patch('src.lambda_handler.boto3.client')
def test_trigger_step_function_failure(mock_boto_client):
    mock_step_functions = MagicMock()
    mock_boto_client.return_value = mock_step_functions
    mock_step_functions.start_execution.side_effect = Exception("Step Function failed")
    
    with pytest.raises(Exception):
        trigger_step_function("dummy_context")

@patch('src.lambda_handler.query_dynamo_db_job_status')
def test_check_mandatory_jobs_mixed_status(mock_query):
    mock_query.side_effect = [
        {'Items': [{'job_status': 'SUCCEEDED'}]},
        {'Items': [{'job_status': 'FAILED'}]}
    ]
    mandatory_jobs = ['group1', 'group2']
    all_successful, responses, failed, yet_to_trigger = check_mandatory_jobs_success(mandatory_jobs)

    assert all_successful is False
    assert len(failed) == 1  # One job failed
    assert len(yet_to_trigger) == 0

@patch('src.lambda_handler.query_dynamo_db_job_status')
def test_check_optional_jobs_mixed_status(mock_query):
    mock_query.side_effect = [
        {'Items': [{'job_status': 'SUCCEEDED'}]},
        {'Items': [{'job_status': 'FAILED'}]}
    ]
    optional_jobs = {'group3': 'ZUTC12:00:00', 'group4': 'ZUTC12:00:00'}
    all_successful, responses, failed, yet_to_trigger = check_optional_jobs_status(optional_jobs)

    assert all_successful is False
    assert len(failed) == 1  # One job failed
    assert len(yet_to_trigger) == 0

def test_load_yaml_invalid_path():
    with pytest.raises(FileNotFoundError):
        load_yaml("invalid_path")
