@patch('src.lambda_handler.boto3.client')
def test_trigger_step_function_failure(mock_boto_client):
    mock_step_functions = MagicMock()
    mock_boto_client.return_value = mock_step_functions
    mock_step_functions.start_execution.side_effect = Exception("Step Function failed")
    
    with pytest.raises(Exception):
        trigger_step_function("dummy_context")

@patch('lambda_function.load_yaml')
def test_get_job_groups_empty():
    empty_yaml = {}
    mandatory, optional, cut_off = get_job_groups(empty_yaml)
    assert mandatory == []
    assert optional == {}
    assert cut_off is None
