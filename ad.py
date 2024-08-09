@patch("boto3.client")
def test_reschedule_lambda(mock_boto_client):
    # Create mock instances for CloudWatch and Lambda clients
    cloudwatch_mock = mock_boto_client.return_value
    cloudwatch_mock.put_rule.return_value = {}
    cloudwatch_mock.put_targets.return_value = {}

    lambda_arn = "arn:aws:lambda:region:account-id:function:function-name"
    
    # Call the function
    reschedule_lambda(lambda_arn)

    # Validate CloudWatch put_rule call
    cloudwatch_mock.put_rule.assert_called_once()
    rule_args = cloudwatch_mock.put_rule.call_args[1]
    assert rule_args['Name'] == "RescheduleLambdaRule"
    assert 'ScheduleExpression' in rule_args
    assert rule_args['State'] == 'ENABLED'

    # Validate CloudWatch put_targets call
    cloudwatch_mock.put_targets.assert_called_once()
    target_args = cloudwatch_mock.put_targets.call_args[1]
    assert target_args['Rule'] == "RescheduleLambdaRule"
    assert len(target_args['Targets']) == 1
    assert target_args['Targets'][0]['Arn'] == lambda_arn
    
@patch("boto3.client")
def test_cleanup_cloudwatch_rule(mock_boto_client):
    # Mock CloudWatch client
    cloudwatch_mock = mock_boto_client.return_value
    
    # Mock responses for describe_rule
    cloudwatch_mock.describe_rule.return_value = {
        'Name': 'RescheduleLambdaRule'
    }

    # Call the function
    cleanup_cloudwatch_rule()

    # Assert that the rule was described, targets were removed, and rule was deleted
    cloudwatch_mock.describe_rule.assert_called_once_with(Name='RescheduleLambdaRule')
    cloudwatch_mock.remove_targets.assert_called_once_with(Rule='RescheduleLambdaRule', Ids=['1'])
    cloudwatch_mock.delete_rule.assert_called_once_with(Name='RescheduleLambdaRule')
