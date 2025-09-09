import boto3
import json

def lambda_handler(event, context):
    print("EVENT RECEIVED:")
    print(json.dumps(event, indent=2))

    glue = boto3.client('glue')
    
    # Get EventBridge event (on-glue-weeklyetl-success)
    detail = event.get('detail', {})          
    job_name = detail.get('jobName')

    # Get state (SUCCEEDED, FAILED, TIMEOUT, etc.)
    job_state = detail.get('state')           

    # Only allow succesfully run weekly-etl
    if job_name != 'weekly_etl':
        return {'status': 'ignored', 'reason': 'job not allowed', 'job': job_name}
    if job_state and job_state != 'SUCCEEDED':
        return {'status': 'ignored', 'reason': f'job state is {job_state}', 'job': job_name}

    # Crawlers for weekly-etl, prints for CloudWatch logs monitoring
    weekly_crawlers = [
        'crawler_loan_applications',
        'crawler_transactions',
        'crawler_customers'
    ]

    results = []
    for crawler in weekly_crawlers:
        try:
            status = glue.get_crawler(Name=crawler)['Crawler']['State']
            if status == 'READY':
                glue.start_crawler(Name=crawler)
                print(f"Started crawler: {crawler}")
                results.append({'crawler': crawler, 'action': 'started'})
            else:
                print(f"Skipped {crawler}, state: {status}")
                results.append({'crawler': crawler, 'action': 'skipped', 'state': status})
        except Exception as e:
            print(f"Error with {crawler}: {e}")
            results.append({'crawler': crawler, 'action': 'error', 'error': str(e)})

    return {
        'status': 'completed',
        'job': job_name,
        'crawlers_processed': results
    }
