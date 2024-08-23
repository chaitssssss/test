# Check the status of mandatory job groups
def check_mandatory_jobs_status(mandatory_job_groups, cut_off_time_str):
    all_successful = True
    responses = {}
    failed_jobs = []
    yet_to_trigger_jobs = []
    today_date_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    cut_off_time = datetime.strptime(cut_off_time_str.replace('ZUTC', ''), '%H:%M:%S').replace(tzinfo=ZoneInfo("UTC"))
    
    for group_name in mandatory_job_groups:
        job_id = f"{group_name}|{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']
        
        now = datetime.now(ZoneInfo("UTC"))
        
        if now <= cut_off_time:
            if len(response['Items']) == 0:
                yet_to_trigger_jobs.append(job_id)
                all_successful = False  # Set to False if any job is yet to trigger
            elif response['Items'][0]['job_status'] != 'SUCCEEDED':
                all_successful = False
                failed_jobs.append(job_id)
        else:
            if len(response['Items']) == 0:
                yet_to_trigger_jobs.append(job_id)  # Add to yet_to_trigger_jobs if no entries
                all_successful = False
            elif response['Items'][0]['job_status'] != 'SUCCEEDED':
                all_successful = False
                failed_jobs.append(job_id)

    return all_successful, responses, failed_jobs, yet_to_trigger_jobs


# Check the status of optional job groups
def check_optional_jobs_status(optional_job_groups):
    all_successful = True
    responses = {}
    failed_jobs = []
    yet_to_trigger_jobs = []
    today_date_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    for group_name, cut_off_time_str in optional_job_groups.items():
        job_id = f"{group_name}|{today_date_str}"
        response = query_dynamo_db_job_status('batch_job_status', job_id)
        responses[job_id] = response['Items']
        
        time_obj = datetime.strptime(cut_off_time_str.replace('ZUTC', ''), '%H:%M:%S').time()
        cut_off_time = datetime.combine(datetime.today(), time_obj).replace(tzinfo=ZoneInfo("UTC"))
        now = datetime.now(ZoneInfo("UTC"))
        
        if now <= cut_off_time:
            if len(response['Items']) == 0:
                yet_to_trigger_jobs.append(job_id)
                all_successful = False  # Set to False if any job is yet to trigger
            elif response['Items'][0]['job_status'] != 'SUCCEEDED':
                all_successful = False
                failed_jobs.append(job_id)
        else:
             if len(response['Items']) == 0:
                yet_to_trigger_jobs.append(job_id)  # Add to yet_to_trigger_jobs if no entries
                all_successful = False
            elif response['Items'][0]['job_status'] != 'SUCCEEDED':
                all_successful = False
                failed_jobs.append(job_id)
    return all_successful, responses, failed_jobs, yet_to_trigger_jobs
