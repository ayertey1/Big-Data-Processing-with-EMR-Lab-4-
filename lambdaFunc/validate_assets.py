import boto3

s3 = boto3.client('s3')

REQUIRED_FILES = [
    "scripts/vehicle_location_metrics.py",
    "scripts/user_transaction_metrics.py",
    "raw/vehicles/",
    "raw/users/",
    "raw/locations/",
    "raw/rental_transactions/"
]

def s3_path_exists(bucket, prefix):
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in result

def lambda_handler(event, context):
    bucket = "car-rental-datastore"
    missing = []

    for path in REQUIRED_FILES:
        if not s3_path_exists(bucket, path):
            missing.append(path)

    if missing:
        return {
            "valid": False,
            "missing": missing
        }

    return {
        "valid": True
    }
