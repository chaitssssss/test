import json
import yaml
import os
import boto3
from boto3.dynamodb.conditions import Key



region = os.environ.get("AWS_REGION")
current_dir = os.getcwd()
print(current_dir)
file_path = os.path.join(current_dir, 'config.yaml')
#file_path = '/Users/kchaitan/Desktop/SENGRID_API_INTEGRATION/config.yaml'


def load_yaml(path):
    with open(path,'r',encoding='utf-8') as file:
        data = yaml.safe_load(file)
        print(data)
        return data


def get_other_groups_in_job_group(data,group_name):
    other_groups = []
    for job_group, groups in data['QA'].items():
        if group_name in groups:
            for group in groups:
                if group != group_name:
                    other_groups.append(group)
            break
    return other_groups



def query_dynamo_db_job_status(table_name,job_id):
    dynamodb_con = boto3.resource("dynamodb",region_name=region)
    table = dynamodb_con.table('job_status_table')
    key_condition_expression = Key('job_id').eq('value')

    # Perform the query
    response = table.query(
        TableName=table_name,
        KeyConditionExpression=key_condition_expression
    )
    return response



def lambda_handler(event, context):
    try:
        responses = {}
        # Check if 'Records' is in the event
        if 'Records' not in event:
            raise ValueError("Event does not contain 'Records'")

        # Check if 'Records' is empty
        if not event['Records']:
            raise ValueError("Event contains empty 'Records'")

        for record in event['Records']:

            keys = record['dynamodb']['Keys']
            job_id = keys['job_id']['S']
            job_id_meta = job_id.split("-")
            group_name  = job_id_meta[0].upper
            date_str = job_id_meta[1]

            job_start_utc_timestamp = keys['job_start_utc_timestamp']['N']
            new_image = record['dynamodb']['NewImage']
            job_status = new_image['job_status']['S']


            if job_status == 'SUCCESS':
                data = load_yaml(file_path)
                group_names = get_other_groups_in_job_group(data,group_name)
                job_id_key_list = []
                for group in group_names:
                    job_id_partition_key = f"{group}-{date_str}"
                    job_id_key_list.append(job_id_partition_key)
                
                for i in job_id_key_list:
                    response = query_dynamo_db_job_status(i)
                    responses[job_id] = response['Items']
                all_successful = all(len(items) > 0 and items[0]['job_status'] == 'success' for items in responses.values())

                if all_successful:
                    print("All job IDs are successful")
                else:
                    print("Some job IDs are not successful")

            elif job_status == 'FAIL':
                print('PAGERDUTY ALERT WILL BE TRIGGERED')

            elif job_status == 'IN_PROGRESS':
                print('skipped')
            # job_end_utc_timestamp = new_image['job_end_utc_timestamp']['N']
            # failure_message = new_image['failure_message']['S']
            # is_failed_job_retryable = new_image['is_failed_job_retryable']['BOOL']
            # data_s3_paths_associated = [path['S'] for path in new_image['data_s3_paths_associated']['L']]

    except Exception as e:
        raise e
