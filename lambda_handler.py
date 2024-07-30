def load_yaml(path):
    with open(path, 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
        return data

def get_job_groups(config_data):
    try:
        mandatory_job_groups = []
        optional_job_groups = {}
        
        job_groups = config_data['QA']['JOB_GROUP_MANDATORY']
        for group, datasets in job_groups.items():
            if group == 'IS_MANDATORY':
                continue
            mandatory_job_groups.append(group)
        
        job_groups = config_data['QA']['JOB_GROUP_OPTIONAL']
        for group, group_info in job_groups.items():
            if group == 'IS_MANDATORY':
                continue
            optional_job_groups[group] = group_info.get('CUT_OFF_TIME')
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
    today_date_str = datetime.now().strftime('%Y-%m-%d')

    for group_name in mandatory_job_groups:
        job_id = f"{group_name}-{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']
        if len(response['Items']) == 0 or response['Items'][0]['job_status'] != 'SUCCESS':
            all_successful = False
            failed_mandatory_jobs.append(job_id)
    
    return all_successful, responses, failed_mandatory_jobs

def check_optional_jobs_status(optional_job_groups):
    all_successful = True
    responses = {}
    failed_optional_jobs = []
    today_date_str = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow().time()

    for group_name, cut_off_time_str in optional_job_groups.items():
        job_id = f"{group_name}-{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']
        cut_off_time = datetime.strptime(cut_off_time_str, '%H:%M:%S%ZUTC').time()

        if now <= cut_off_time and (len(response['Items']) == 0 or response['Items'][0]['job_status'] != 'SUCCESS'):
            all_successful = False
            failed_optional_jobs.append(job_id)
    
    return all_successful, responses, failed_optional_jobs

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

def lambda_handler(event, context):
    try:
        config_data = load_yaml(file_path)
        mandatory_job_groups, optional_job_groups = get_job_groups(config_data)
        
        all_mandatory_successful, mandatory_responses, failed_mandatory_jobs = check_mandatory_jobs_success(mandatory_job_groups)
        print("Mandatory jobs:", mandatory_responses)
        
        all_optional_successful, optional_responses, failed_optional_jobs = check_optional_jobs_status(optional_job_groups)
        print("Optional jobs:", optional_responses)

        response = {
            "mandatory_jobs_status": {
                "all_successful": all_mandatory_successful,
                "responses": mandatory_responses,
                "failed_jobs": failed_mandatory_jobs
            },
            "optional_jobs_status": {
                "all_successful": all_optional_successful,
                "responses": optional_responses,
                "failed_jobs": failed_optional_jobs
            },
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        if all_mandatory_successful:
            step_function_response = trigger_step_function()
            response["step_function"] = {
                "status": "triggered",
                "executionArn": step_function_response["executionArn"]
            }
            if all_optional_successful:
                response["status"] = "success"
                response["message"] = "All mandatory and optional jobs are successful."
            else:
                response["status"] = "partial_success"
                response["message"] = "All mandatory jobs are successful, but some optional jobs failed."
        else:
            response["status"] = "fail"
            response["message"] = "Some mandatory jobs failed."

        return {
            'statusCode': 200,
            'body': json.dumps(response)
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': str(e)
        }
