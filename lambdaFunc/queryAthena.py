import boto3
import time
import urllib.parse

athena = boto3.client('athena')
s3 = boto3.client('s3')

DATABASE = 'car_rental_analytics'
OUTPUT_BUCKET = 'car-rental-datastore'
TEMP_OUTPUT = f's3://{OUTPUT_BUCKET}/athena-results/'
FINAL_OUTPUT_PREFIX = 'athena-exports/'

queries = {
    'top_locations.csv': """
        SELECT pickup_location, city, state, total_revenue
        FROM vlm_vehicle_location_metrics
        ORDER BY total_revenue DESC
        LIMIT 10
    """,
    'top_vehicle_types.csv': """
        SELECT vehicle_type, total_rental_hours, vehicle_type_revenue
        FROM vlm_vehicle_type_metrics
        ORDER BY total_rental_hours DESC
        LIMIT 5
    """,
    'top_spenders.csv': """
        SELECT user_id, first_name, last_name, user_total_spent
        FROM user_user_metrics
        ORDER BY user_total_spent DESC
        LIMIT 10
    """,
    'daily_revenue_trends.csv': """
        SELECT rental_date, total_transactions, daily_revenue
        FROM daily_daily_metrics
        ORDER BY rental_date ASC
    """
}

def run_query(query):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': TEMP_OUTPUT}
    )
    query_execution_id = response['QueryExecutionId']

    # Wait for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state != 'SUCCEEDED':
        raise Exception(f'Athena query failed: {state}')
    
    return query_execution_id

def move_result_file(query_execution_id, destination_key):
    result_key = f'athena-results/{query_execution_id}.csv'
    target_key = f'{FINAL_OUTPUT_PREFIX}{destination_key}'

    # Copy and rename result file
    s3.copy_object(
        Bucket=OUTPUT_BUCKET,
        CopySource=f'{OUTPUT_BUCKET}/{result_key}',
        Key=target_key
    )

    # Optional: delete temp file
    s3.delete_object(Bucket=OUTPUT_BUCKET, Key=result_key)

    return target_key

def lambda_handler(event, context):
    results = {}

    for filename, sql in queries.items():
        print(f"Running query for: {filename}")
        try:
            query_id = run_query(sql)
            final_path = move_result_file(query_id, filename)
            results[filename] = f's3://{OUTPUT_BUCKET}/{final_path}'
            print(f"Query {filename} stored at {final_path}")
        except Exception as e:
            print(f"Query {filename} failed: {str(e)}")
            results[filename] = "FAILED"