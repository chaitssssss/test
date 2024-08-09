@patch("src.lambda_handler.check_mandatory_jobs_success", return_value=(True, {}, [], []))
@patch("src.lambda_handler.check_optional_jobs_status", return_value=(True, {}, [], []))
@patch("src.lambda_handler.trigger_step_function")
@patch("src.lambda_handler.cleanup_cloudwatch_rule")
def test_lambda_handler_success(mock_cleanup, mock_trigger, mock_optional, mock_mandatory):
    mock_trigger.return_value = {
        "executionArn": "arn:aws:states:region:account-id:execution:state-machine-name:execution-id"
    }
    event = {}
    context = MagicMock()
    context.invoked_function_arn = "arn:aws:lambda:region:account-id:function:function-name"
    
    response = lambda_handler(event, context)
    assert response["statusCode"] == 200
    assert response["body"]["status"] == "completed"













@patch("boto3.client")
@patch("src.lambda_handler.reschedule_lambda")
@patch("src.lambda_handler.cleanup_cloudwatch_rule")
@patch("src.lambda_handler.trigger_step_function")
@patch("src.lambda_handler.check_optional_jobs_status", return_value=(True, {}, [], []))
@patch("src.lambda_handler.check_mandatory_jobs_success", return_value=(True, {}, [], []))
@patch("builtins.open", new_callable=mock_open, read_data=config_data)
def test_lambda_handler_success(mock_file, mock_mandatory, mock_optional, mock_trigger, mock_cleanup, mock_reschedule, mock_lambda_client, mock_cloudwatch_client):
    mock_trigger.return_value = {
        'executionArn': 'arn:aws:states:region:account-id:execution:state-machine-name:execution-id'
    }
    event = {}
    context = MagicMock()
    context.invoked_function_arn = "arn:aws:lambda:region:account-id:function:function-name"

    try:
        response = lambda_handler(event, context)
    except Exception as e:
        print(f"Exception occurred: {e}")
        raise e

    assert response['statusCode'] == 200

    body = json.loads(response['body'])  # Ensure the body is parsed correctly
    assert body['status'] == "completed"
    assert body['step_function']['status'] == "triggered"
