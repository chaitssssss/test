def test_send_alert_to_pagerduty():
    with patch('urllib3.PoolManager') as mock_pool_manager:
        # Arrange
        mock_response = mock_pool_manager.return_value.request.return_value
        mock_response.status = 202
        mock_response.data = json.dumps({"message": "Event processed"}).encode('utf-8')

        event = {"key": "value"}  # Sample event data
        expected_payload = {
            "routing_key": "YOUR_PAGERDUTY_INTEGRATION_KEY",
            "event_action": "trigger",
            "payload": {
                "summary": "Example alert from AWS Lambda",
                "severity": "error",
                "source": "AWS Lambda",
                "component": "Lambda Function",
                "custom_details": {
                    "error": str(event)
                }
            }
        }

        # Act
        result = send_alert_to_pagerduty(event)

        # Assert
        assert result['statusCode'] == 202
        assert json.loads(result['body']) == {"message": "Event processed"}
        mock_pool_manager.return_value.request.assert_called_once_with(
            'POST',
            'https://events.pagerduty.com/v2/enqueue',
            body=json.dumps(expected_payload),
            headers={'Content-Type': 'application/json'}
        )
