@when('the lambda "{lambda_name}" ran as per the cron schedule')
def invoke_lambda(context, lambda_name):
    lambda_client = boto3.client('lambda', region_name=os.getenv('AWS_REGION'))
    response = lambda_client.invoke(
        FunctionName=lambda_name,
        InvocationType='RequestResponse'
    )

    context.response_payload = json.loads(response['Payload'].read())
    context.lambda_status_code = response['StatusCode']

@then('the lambda runs with status "{expected_status}"')
def check_lambda_status(context, expected_status):
    status_code = context.lambda_status_code
    response_body = context.response_payload

    if expected_status == "True":
        assert status_code == 200, f"Expected status code 200 but got {status_code}"
        assert response_body['status'] in ['completed', 'pending'], f"Expected status to be 'completed' or 'pending', but got {response_body['status']}"
        context.is_successful = True
    else:
        assert status_code == 500, f"Expected status code 500 but got {status_code}"
        assert response_body['status'] == 'failed', f"Expected status to be 'failed', but got {response_body['status']}"
        context.is_successful = False

@then('the "step_function_triggered_status" will be {triggered}')
def check_step_function(context, triggered):
    response_body = context.response_payload

    if context.is_successful:  # Check only if the response was successful
        if triggered == "True":
            assert 'step_function_account' in response_body and response_body['step_function_account']['status'] == 'triggered'
            assert 'step_function_consumer' in response_body and response_body['step_function_consumer']['status'] == 'triggered'
        else:
            assert 'step_function_account' not in response_body
            assert 'step_function_consumer' not in response_body
    else:
        print("The Lambda function did not execute successfully; skipping step function checks.")
