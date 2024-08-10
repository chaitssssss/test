def test_reschedule_lambda(mock_boto_clients):
    # Arrange
    mock_cloudwatch = MagicMock()
    mock_lambda = MagicMock()
    mock_boto_clients.side_effect = [mock_cloudwatch, mock_lambda]

    lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
    
    # Mock the get_policy method to return a policy document
    mock_lambda.get_policy.return_value = {
        'Policy': json.dumps({
            'Statement': [
                {
                    'Sid': 'SomeOtherPermission',
                    'Effect': 'Allow',
                    'Action': 'lambda:InvokeFunction',
                    'Resource': lambda_arn
                }
            ]
        })
    }

    # Act
    reschedule_lambda(lambda_arn)

    # Assert
    mock_cloudwatch.put_rule.assert_called_once()
    mock_cloudwatch.put_targets.assert_called_once()
    mock_lambda.get_policy.assert_called_once_with(FunctionName=lambda_arn)
    mock_lambda.add_permission.assert_called_once_with(
        FunctionName=lambda_arn,
        StatementId='RescheduleLambdaPermission',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com',
        SourceArn=f'arn:aws:events:us-east-1:592273541233:rule/RescheduleLambdaRule'
    )
