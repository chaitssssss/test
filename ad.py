
@pytest.fixture
def mock_boto_client():
    with patch('src.lambda_handler.boto3.client') as mock:
        yield mock

def test_reschedule_lambda(mock_boto_client):
    # Arrange
    mock_cloudwatch = MagicMock()
    mock_lambda = MagicMock()
    mock_boto_client.side_effect = [mock_cloudwatch, mock_lambda]
    
    lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-function"
    
    # Act
    reschedule_lambda(lambda_arn)

    # Assert
    mock_cloudwatch.put_rule.assert_called_once()
    mock_cloudwatch.put_targets.assert_called_once()
    mock_lambda.get_policy.assert_called_once()
    mock_lambda.add_permission.assert_called_once()

def test_cleanup_cloudwatch_rule(mock_boto_client):
    # Arrange
    mock_cloudwatch = MagicMock()
    mock_boto_client.return_value = mock_cloudwatch
    
    # Mock the describe_rule method to return a rule
    mock_cloudwatch.describe_rule.return_value = {'Name': 'RescheduleLambdaRule'}

    # Act
    cleanup_cloudwatch_rule()

    # Assert
    mock_cloudwatch.remove_targets.assert_called_once_with(Rule='RescheduleLambdaRule', Ids=['1'])
    mock_cloudwatch.delete_rule.assert_called_once_with(Name='RescheduleLambdaRule')

def test_cleanup_cloudwatch_rule_not_found(mock_boto_client):
    # Arrange
    mock_cloudwatch = MagicMock()
    mock_boto_client.return_value = mock_cloudwatch
    
    # Mock the describe_rule method to raise ResourceNotFoundException
    mock_cloudwatch.describe_rule.side_effect = mock_cloudwatch.exceptions.ResourceNotFoundException

    # Act
    cleanup_cloudwatch_rule()

    # Assert
    mock_cloudwatch.remove_targets.assert_not_called()
    mock_cloudwatch.delete_rule.assert_not_called()
