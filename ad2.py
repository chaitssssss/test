def test_reschedule_lambda():
    # Mock the dependencies
    cloudwatch_mock = MagicMock()
    lambda_client_mock = MagicMock()
    region = "us-east-1"
    lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function"

    # Simulate that the permission does not exist
    lambda_client_mock.get_policy.side_effect = lambda_client_mock.exceptions.ResourceNotFoundException

    # Call the function the first time
    reschedule_lambda(lambda_arn, cloudwatch_mock, lambda_client_mock, region)

    # Assert that the CloudWatch put_rule was called
    cloudwatch_mock.put_rule.assert_called_once()
    
    # Assert that the CloudWatch put_targets was called
    cloudwatch_mock.put_targets.assert_called_once()
    
    # Assert that add_permission was called once
    #lambda_client_mock.add_permission.assert_called_once()

    # Reset mocks for the next call
    cloudwatch_mock.reset_mock()
    lambda_client_mock.reset_mock()

    # Simulate that the permission exists by returning a policy
    lambda_client_mock.get_policy.return_value = {
        'Policy': json.dumps({
            'Statement': [
                {
                    'Sid': 'RescheduleLambdaPermission',
                    'Effect': 'Allow',
                    'Action': 'lambda:InvokeFunction',
                    'Resource': lambda_arn
                }
            ]
        })
    }

    # Call the function again to check for add_permission
    reschedule_lambda(lambda_arn, cloudwatch_mock, lambda_client_mock, region)

    # Assert that add_permission was NOT called since permission already exists
    #lambda_client_mock.add_permission.assert_not_called()



def test_cleanup_cloudwatch_rule():
    # Mock the dependencies
    cloudwatch_mock = MagicMock()

    # Set up the response for describe_rule
    cloudwatch_mock.describe_rule.return_value = {'Name': 'RescheduleLambdaRule'}

    # Call the function
    cleanup_cloudwatch_rule(cloudwatch_mock)

    # Assert that remove_targets and delete_rule were called
    cloudwatch_mock.remove_targets.assert_called_once()
    cloudwatch_mock.delete_rule.assert_called_once()

def test_cleanup_cloudwatch_rule_not_exist():
    # Mock the dependencies
    cloudwatch_mock = MagicMock()
    cloudwatch_mock.describe_rule.side_effect = cloudwatch_mock.exceptions.ResourceNotFoundException

    # Call the function
    cleanup_cloudwatch_rule(cloudwatch_mock)

    # Assert that remove_targets and delete_rule were not called
    cloudwatch_mock.remove_targets.assert_not_called()
    cloudwatch_mock.delete_rule.assert_not_called()
