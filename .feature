Feature: Job Listener Lambda Function

  Scenario: All Mandatory jobs and optional jobs succeeded before the cut off time
    Given all the "mandatory_and_optional_jobs" are present in dynamo db with job_status as SUCCEEDED
    When the lambda "job-listener-lambda-qa" ran as per the cron schedule
    Then the lambda runs with status "True"
    And the "step_function_triggered_status" will be True

  Scenario: Some of the Mandatory jobs failed when the lambda is triggered based on the cron schedule
    Given some of the "mandatory_jobs" are present in dynamo db with job_status as FAILED
    When the lambda "job-listener-lambda-qa" ran as per the cron schedule
    Then the lambda runs with status "False"
    And the "step_function_triggered_status" will be False
