Feature: Job Listener Lambda Function

  Scenario: All Mandatory jobs and optional jobs succeeded before the cut off time
    Given all the "mandatory_and_optional_jobs" are present in dynamo db with job_status as SUCCEEDED
    When the lambda "job-listener-lambda-qa" ran as per the cron schedule
    Then the lambda runs with status "True"
    And the "step_function_triggered_status" will be True

  Scenario: Failed execution of the Lambda function
    When the lambda "my_lambda_function" ran as per the cron schedule
    Then the lambda runs with status "False"
    And the "step_function_triggered_status" will be False

  Scenario: Lambda function returns unexpected response structure
    When the lambda "my_lambda_function" ran as per the cron schedule
    Then the lambda runs with status "True"
    And the response should have a valid structure

  Scenario: Lambda function times out
    When the lambda "my_lambda_function" ran as per the cron schedule
    Then the lambda runs with status "False"
    And the error message should indicate a timeout

  Scenario: Lambda function returns an error message
    When the lambda "my_lambda_function" ran as per the cron schedule
    Then the lambda runs with status "False"
    And the error message should be "An error occurred"

