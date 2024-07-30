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

        time_str = cut_off_time_str.replace('ZUTC', '')
        time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
        today_date = datetime.today().date()
        datetime_obj = datetime.combine(today_date, time_obj)
        utc_timezone = pytz.UTC
        datetime_with_tz = utc_timezone.localize(datetime_obj)
        now = datetime.utcnow().time()

        if now <= datetime_with_tz.time() and (len(response['Items']) == 0 or response['Items'][0]['job_status'] != 'SUCCESS'):
            all_successful = False
            failed_optional_jobs.append(job_id)
    
    return all_successful, responses, failed_optional_jobs
