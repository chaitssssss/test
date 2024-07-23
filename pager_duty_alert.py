import urllib3

def send_alert_to_pagerduty(event):
    integration_key = 'YOUR_PAGERDUTY_INTEGRATION_KEY'
    url = 'https://events.pagerduty.com/v2/enqueue'
    
    headers = {
        'Content-Type': 'application/json',
    }

    payload = {
        "routing_key": integration_key,
        "event_action": "trigger",
        "payload": {
            "summary": "Example alert from AWS Lambda",
            "severity": "error",
            "source": "AWS Lambda",
            "component": "Lambda Function",
            "custom_details": {
                "error": str(event)  # Add relevant event details
            }
        }
    }

    http = urllib3.PoolManager()
    response = http.request('POST', url, body=json.dumps(payload), headers=headers)

    return {
        'statusCode': response.status,
        'body': response.data.decode('utf-8')  # Decode response for readability
    }
