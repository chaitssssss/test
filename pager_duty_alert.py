@patch('src.lambda_handler.load_yaml')
@patch('src.lambda_handler.check_mandatory_jobs_success')
def test_lambda_handler_success(mock_check_mandatory_jobs_success, mock_load_yaml):
    mock_load_yaml.return_value = config_data
    mock_check_mandatory_jobs_success.return_value = (True, {})
    
    event = {}
    context = {}
    response = lambda_handler(event, context)
assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['status'] == 'success'

@patch('src.lambda_handler.load_yaml')
@patch('src.lambda_handler.check_mandatory_jobs_success')
def test_lambda_handler_failure(mock_check_mandatory_jobs_success, mock_load_yaml):
    mock_load_yaml.return_value = config_data
    mock_check_mandatory_jobs_success.return_value = (False, {})
event = {}
    context = {}
    response = lambda_handler(event, context)
assert response['statusCode'] == 200
    body = json.loads(response['body'])
    assert body['status'] == 'fail'
