Environment Setup

Retrieves environment variables for configuration, including ENV and AWS_REGION.
Initializes clients for AWS services using the boto3 library, specifically for CloudWatch Events and Lambda.
Configuration Management

Loads configuration settings from a YAML file (config.yaml) located in the same directory as the script.
Extracts mandatory and optional job groups, including their associated cut-off times.
DynamoDB Integration

Uses the boto3 DynamoDB resource to query the batch_job_status table.
Defines a function (query_dynamo_db_job_status) that retrieves job status based on a unique job ID.
Job Status Verification

Implements two primary functions:
check_mandatory_jobs_success: Validates the success of mandatory jobs by querying their status.
check_optional_jobs_status: Checks the status of optional jobs, comparing their execution time against predefined cut-off times.
Step Functions Triggering

If all mandatory jobs are successful, the function triggers an AWS Step Function using trigger_step_function, passing necessary input data for further processing.
Lambda Function Rescheduling

If jobs are not successful, the function reschedules itself to run after a specified delay (2 minutes) using CloudWatch Events.
Implements reschedule_lambda to create or update a CloudWatch rule for triggering the Lambda function.
CloudWatch Rule Management

Manages CloudWatch rules to ensure that scheduled executions are handled appropriately.
Contains a cleanup function (cleanup_cloudwatch_rule) to remove existing rules and targets when no longer needed.
