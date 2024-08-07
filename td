
I have mandatory jobs which contains both account and customer and I also have optional jobs which only contains account at the moment. As per our previous conversations , there should be only input
{
        "job_name": "c-3po-cmd-batch-copy",
        "job_id": f"publish_{datetime.now().strftime('%Y%m%d')}",
        "job_start_utc_timestamp": start_ts,
        "job_args": {
            "--destination": "account|consumer",
            "--skip_file_puller": "true"
        }
    }
 that I have to be passing for the group of mandatory jobs post checking their respective job status , currently my lambda code will check if all the mandatory and optional jobs are successful but my point is that we have both account and consumer level in mandatory job groups. How can we differentiate the account and consumer level jobs while passing the input to the step functio ?



















Technical Description
This AWS Lambda function is designed to validate the status of jobs listed in a DynamoDB table
against a configuration file (config.yaml). 
The function independently checks both mandatory and optional job groups 
to determine their success status. 
If all mandatory jobs are successful, it triggers a Step Function with specific input parameters.

Key Components
Configuration File (config.yaml):

The YAML configuration file contains job groups categorized as mandatory and optional. Each group has datasets associated with it and an optional cut-off time for the optional groups.
Environment Variables:

ENV: Specifies the environment in which the Lambda function is running.
AWS_REGION: Specifies the AWS region for the DynamoDB and Step Function services.
Loading Configuration Data:

load_yaml(path): Loads and parses the YAML configuration file.
Extracting Job Groups:

get_job_groups(config_data): Extracts mandatory and optional job groups from the configuration data. For optional groups, it also extracts the cut-off time.
Querying DynamoDB:

query_dynamo_db_job_status(table_name, job_id): Queries the DynamoDB table (batch_job_status) to retrieve the status of a specific job based on the job_id.
Checking Job Status:

check_mandatory_jobs_success(mandatory_job_groups): Checks if all mandatory jobs are successful by querying DynamoDB. Returns the status, responses, and list of failed jobs.
check_optional_jobs_status(optional_job_groups): Checks if all optional jobs are successful before their respective cut-off times by querying DynamoDB. Returns the status, responses, and list of failed jobs.
Triggering Step Function:

trigger_step_function(): Triggers a Step Function execution if all mandatory jobs are successful. The input parameters for the Step Function are hardcoded within the function.
Lambda Handler:

lambda_handler(event, context): Main entry point for the Lambda function. It orchestrates the following steps:
Loads the configuration data.
Extracts job groups.
Checks the status of mandatory and optional jobs.
Constructs the response based on the job status.
Triggers the Step Function if all mandatory jobs are successful.
Returns the final status and details in the response.
Response Structure
The response from the Lambda function includes:

Status of mandatory and optional jobs.
Responses from the DynamoDB queries for each job.
List of failed jobs (if any).
Step Function execution status (if triggered).
Overall status message.
