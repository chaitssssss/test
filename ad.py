def cleanup_cloudwatch_rule():
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
