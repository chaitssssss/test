def reschedule_lambda(lambda_arn, cloudwatch, lambda_client, region):
    """Schedule a Lambda function to run 15 minutes later."""
    current_time = datetime.utcnow()
    reschedule_time = current_time + timedelta(minutes=15)
    
    rule_name = "RescheduleLambdaRule"
    schedule_expression = f"cron({reschedule_time.minute} {reschedule_time.hour} * * ? *)"

    # Create or update CloudWatch Event rule
    cloudwatch.put_rule(
        Name=rule_name,
        ScheduleExpression=schedule_expression,
        State='ENABLED'
    )
    
    # Add Lambda function as the target
    cloudwatch.put_targets(
        Rule=rule_name,
        Targets=[
            {
                'Id': '1',
                'Arn': lambda_arn
            }
        ]
    )
    
    permission_exists = False
    try:
        policy = lambda_client.get_policy(FunctionName=lambda_arn)
        policy_doc = json.loads(policy['Policy'])
        for statement in policy_doc['Statement']:
            if statement['Sid'] == 'RescheduleLambdaPermission':
                permission_exists = True
                break
    except lambda_client.exceptions.ResourceNotFoundException:
        pass
    
    if not permission_exists:
        lambda_client.add_permission(
            FunctionName=lambda_arn,
            StatementId='RescheduleLambdaPermission',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com',
            SourceArn=f'arn:aws:events:{region}:592273541233:rule/{rule_name}'
        )

def cleanup_cloudwatch_rule(cloudwatch):
    """Clean up the CloudWatch rule."""
    rule_name = "RescheduleLambdaRule"
    
    # Check if the rule exists
    try:
        response = cloudwatch.describe_rule(Name=rule_name)
        # If the rule exists, proceed with removal
        if 'Name' in response and response['Name'] == rule_name:
            # Remove all targets from the rule
            cloudwatch.remove_targets(
                Rule=rule_name,
                Ids=['1']
            )
            # Delete the rule
            cloudwatch.delete_rule(
                Name=rule_name
            )
            print(f"CloudWatch rule '{rule_name}' removed successfully.")
    except cloudwatch.exceptions.ResourceNotFoundException:
        # If the rule does not exist, skip removal
        print(f"CloudWatch rule '{rule_name}' does not exist. Skipping cleanup.")
