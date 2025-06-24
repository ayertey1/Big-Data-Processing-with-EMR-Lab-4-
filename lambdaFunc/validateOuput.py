import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = "car-rental-datastore"
    prefix = "processed/vehicle_location_metrics/"
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response and len(response['Contents']) > 0:
        return {"should_process": False}
    else:
        return {"should_process": True}
