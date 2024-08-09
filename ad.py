@patch("boto3.client")
def test_reschedule_lambda(mock_boto_client):
    cloudwatch_mock = mock_boto_client.return_value
    lambda_arn = "arn:aws:lambda:region:account-id:function:lambda-function-name"
    reschedule_lambda(lambda_arn)
    cloudwatch_mock.put_rule.assert_called_once()
    cloudwatch_mock.put_targets.assert_called_once()
    cloudwatch_mock.add_permission.assert_called_once()

@patch("boto3.client")
def test_cleanup_cloudwatch_rule(mock_boto_client):
    cloudwatch_mock = mock_boto_client.return_value
    cloudwatch_mock.describe_rule.return_value = {'Name': 'RescheduleLambdaRule'}
    cleanup_cloudwatch_rule()
    cloudwatch_mock.remove_targets.assert_called_once()
    cloudwatch_mock.delete_rule.assert_called_once()
