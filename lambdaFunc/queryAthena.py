import boto3
import time

athena = boto3.client('athena')

def lambda_handler(event, context):
    database = 'car_rental_analytics'
    output = 's3://car-rental-datastore/athena-results/'
    query = """
    SELECT pickup_location, total_revenue
    FROM vlm_vehicle_location_metrics
    ORDER BY total_revenue DESC
    LIMIT 5
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
    
    if state != 'SUCCEEDED':
        raise Exception(f'Athena query failed: {state}')
    
    return {"QueryExecutionId": query_execution_id}

import boto3
import time

athena = boto3.client('athena')

def lambda_handler(event, context):
    database = 'car_rental_analytics'
    output = 's3://car-rental-datastore/athena-results/'
    query = """
    SELECT pickup_location, total_revenue
    FROM vlm_vehicle_location_metrics
    ORDER BY total_revenue DESC
    LIMIT 5
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output}
    )
    
    query_execution_id = response['QueryExecutionId']
    
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)
    
    if state != 'SUCCEEDED':
        raise Exception(f'Athena query failed: {state}')
    
    return {"QueryExecutionId": query_execution_id}

