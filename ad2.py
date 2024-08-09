@patch('src.lambda_handler.boto3.client')
def test_cleanup_cloudwatch_rule(mock_boto_client):
    mock_cloudwatch = MagicMock()
    mock_boto_client.return_value = mock_cloudwatch
    mock_cloudwatch.describe_rule.return_value = {'Name': 'RescheduleLambdaRule'}

    cleanup_cloudwatch_rule()
    mock_cloudwatch.remove_targets.assert_called_once_with(Rule='RescheduleLambdaRule', Ids=['1'])
    mock_cloudwatch.delete_rule.assert_called_once_with(Name='RescheduleLambdaRule')
