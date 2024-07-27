def load_yaml(path):
    with open(path, 'r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
        return data

"""
get all the mandatory job groups from the config file
"""

def get_mandatory_job_groups(config_data):
    mandatory_job_groups = []
    job_groups = config_data['QA']['JOB_GROUP_MANDATORY']
    
    for group, datasets in job_groups.items():
        if group == 'IS_MANDATORY':
            continue
        mandatory_job_groups.append(group)
    
    return mandatory_job_groups


"""
Interface to connect to dynamo db and retrieve the data based on job id
"""
def query_dynamo_db_job_status(table_name, job_id):
    dynamodb_con = boto3.resource("dynamodb", region_name=region)
    table = dynamodb_con.Table(table_name)
    key_condition_expression = Key('job_id').eq(job_id)

    response = table.query(
        KeyConditionExpression=key_condition_expression
    )
    return response

"""
    checking each group in the job_group_mandatory is successful or not
"""
def check_mandatory_jobs_success(mandatory_job_groups):
    all_successful = True
    responses = {}
    today_date_str = datetime.now().strftime('%Y-%m-%d')

    for group_name in mandatory_job_groups:
        job_id = f"{group_name}-{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']
        if len(response['Items']) == 0 or response['Items'][0]['job_status'] != 'SUCCESS':
            all_successful = False
    
    return all_successful, responses

def lambda_handler(event, context):
    try:
        config_data = load_yaml(file_path)
        mandatory_job_groups = get_mandatory_job_groups(config_data)
        
        all_successful, responses = check_mandatory_jobs_success(mandatory_job_groups)

        if all_successful:
            print("All mandatory jobs are successful.")
        else:
            print("Some mandatory jobs are not successful.")

        return {
            'statusCode': 200,
            'body': json.dumps(responses)
        }

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': str(e)
        }
