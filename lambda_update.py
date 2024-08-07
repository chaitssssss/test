
cloudwatch = boto3.client('events', region_name=region)
lambda_client = boto3.client('lambda', region_name=region)

def load_yaml(path):
    with open(path, 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
        return data

def get_job_groups(config_data):
    try:
        mandatory_job_groups = []
        optional_job_groups = {}

        # Extract mandatory job groups
        job_groups = config_data['QA']['JOB_GROUP_MANDATORY']
        for group, datasets in job_groups.items():
            if group == 'IS_MANDATORY':
                continue
            mandatory_job_groups.append(group)

        # Extract optional job groups
        job_groups = config_data['QA']['JOB_GROUP_OPTIONAL']
        for group, group_info in job_groups.items():
            if group == 'IS_MANDATORY' or group == 'CUT_OFF_TIME':
                continue
            cut_off_time = job_groups.get('CUT_OFF_TIME')  # Get the cut-off time for all optional groups
            optional_job_groups[group] = cut_off_time  # Assign the cut-off time to each group

    except Exception as e:
        print(e)
        raise e 

    return mandatory_job_groups, optional_job_groups

def query_dynamo_db_job_status(table_name, job_id):
    dynamodb_con = boto3.resource("dynamodb", region_name=region)
    table = dynamodb_con.Table(table_name)
    key_condition_expression = Key('job_id').eq(job_id)

    response = table.query(
        KeyConditionExpression=key_condition_expression
    )
    return response

def check_mandatory_jobs_success(mandatory_job_groups):
    all_successful = True
    responses = {}
    failed_mandatory_jobs = []
    yet_to_trigger_mandatory_jobs = []
    today_date_str = datetime.now().strftime('%Y-%m-%d')

    for group_name in mandatory_job_groups:
        job_id = f"{group_name}-{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']
        if len(response['Items']) == 0:
            yet_to_trigger_mandatory_jobs.append(job_id)
            all_successful = False
        elif response['Items'][0]['job_status'] != 'SUCCESS':
            all_successful = False
            failed_mandatory_jobs.append(job_id)
    
    return all_successful, responses, failed_mandatory_jobs, yet_to_trigger_mandatory_jobs

def check_optional_jobs_status(optional_job_groups):
    all_successful = True
    responses = {}
    failed_optional_jobs = []
    yet_to_trigger_optional_jobs = []
    today_date_str = datetime.now().strftime('%Y-%m-%d')

    for group_name, cut_off_time_str in optional_job_groups.items():
        job_id = f"{group_name}-{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']

        time_str = cut_off_time_str.replace('ZUTC', '')
        time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
        today_date = datetime.today().date()
        datetime_obj = datetime.combine(today_date, time_obj)
        datetime_with_tz = datetime_obj.replace(tzinfo=ZoneInfo("UTC"))
        now = datetime.now(ZoneInfo("UTC")).time()

        if now <= datetime_with_tz.time():
            if len(response['Items']) == 0:
                yet_to_trigger_optional_jobs.append(job_id)
                all_successful = False
            elif response['Items'][0]['job_status'] != 'SUCCESS':
                all_successful = False
                failed_optional_jobs.append(job_id)
    
    return all_successful, responses, failed_optional_jobs, yet_to_trigger_optional_jobs

def trigger_step_function():
    client = boto3.client('stepfunctions', region_name=region)
    state_machine_arn = "arn:aws:states:region:account-id:stateMachine:state-machine-name"
    
    start_ts = datetime.utcnow().isoformat() + "Z"
    input_data = {
        "job_name": "c-3po-cmd-batch-copy",
        "job_id": f"publish_{datetime.now().strftime('%Y%m%d')}",
        "job_start_utc_timestamp": start_ts,
        "job_args": {
            "--destination": "account|consumer",
            "--skip_file_puller": "true"
        }
    }
    
    response = client.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(input_data)
    )
    
    return response

def reschedule_lambda():
    # Schedule Lambda to run 15 minutes later
    current_time = datetime.utcnow()
    reschedule_time = current_time + timedelta(minutes=15)
    
    rule_name = "ReschedulejobstatusLambdaRule"
    lambda_arn = os.getenv('AWS_LAMBDA_FUNCTION_NAME')

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

def cleanup_cloudwatch_rule():
    rule_name = "RescheduleLambdaRule"
    # Remove all targets from the rule
    cloudwatch.remove_targets(
        Rule=rule_name,
        Ids=['1']
    )
    # Delete the rule
    cloudwatch.delete_rule(
        Name=rule_name
    )

def lambda_handler(event, context):
    try:
        config_data = load_yaml(file_path)
        mandatory_job_groups, optional_job_groups = get_job_groups(config_data)
        
        all_mandatory_successful, mandatory_responses, failed_mandatory_jobs, yet_to_trigger_mandatory_jobs = check_mandatory_jobs_success(mandatory_job_groups)
        print("Mandatory jobs:", mandatory_responses)
        
        all_optional_successful, optional_responses, failed_optional_jobs, yet_to_trigger_optional_jobs = check_optional_jobs_status(optional_job_groups)
        print("Optional jobs:", optional_responses)

        response = {
            "mandatory_jobs_status": {
                "all_successful": all_mandatory_successful,
                "responses": mandatory_responses,
                "failed_jobs": failed_mandatory_jobs,
                "yet_to_trigger_jobs": yet_to_trigger_mandatory_jobs
            },
            "optional_jobs_status": {
                "all_successful": all_optional_successful,
                "responses": optional_responses,
                "failed_jobs": failed_optional_jobs,
                "yet_to_trigger_jobs": yet_to_trigger_optional_jobs
            },
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        current_time = datetime.utcnow()
        cut_off_time_str = optional_job_groups.get('CUT_OFF_TIME')
        cut_off_time = datetime.strptime(cut_off_time_str.replace('ZUTC', ''), '%H:%M:%S').replace(tzinfo=ZoneInfo("UTC"))

        if all_mandatory_successful:
            if current_time.time() < cut_off_time.time():
                if all_optional_successful:
                    response["status"] = "completed"
                    response["message"] = "All mandatory and optional jobs are successful."
                    step_function_response = trigger_step_function()
                    response["step_function"] = {
                        "status": "triggered",
                        "executionArn": step_function_response["executionArn"]
                    }
                    cleanup_cloudwatch_rule()
                else:
                    response["status"] = "pending"
                    response["message"] = "All mandatory jobs are successful, but some optional jobs are still pending."
                    reschedule_lambda()
            else:
                response["status"] = "completed"
                response["message"] = "All mandatory jobs are successful. Cut-off time reached, triggering step function."
                step_function_response = trigger_step_function()
                response["step_function"] = {
                    "status": "triggered",
                    "executionArn": step_function_response["executionArn"]
                }
                cleanup_cloudwatch_rule()
        else:
            response["status"] = "fail"
            response["message"] = "Some mandatory jobs failed."
            cleanup_cloudwatch_rule()

        return {
            'statusCode': 200,
            'body': response
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': str(e)
        }
