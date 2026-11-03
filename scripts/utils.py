import boto3
from botocore.exceptions import ClientError

# Default MinIO Connection Configuration
MINIO_ENDPOINT = "http://localhost:9000"
AWS_ACCESS_KEY = "admin"
AWS_SECRET_KEY = "password123"
DEFAULT_BUCKET = "warehouse"

def get_s3_client(endpoint=MINIO_ENDPOINT, access_key=AWS_ACCESS_KEY, secret_key=AWS_SECRET_KEY):
    """Creates and returns a configured S3 client pointing to MinIO or AWS."""
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name='us-east-1'
    )

def ensure_bucket_exists(s3_client, bucket_name=DEFAULT_BUCKET):
    """Ensures the specified bucket exists, creating it if necessary."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"[S3 Utility] Bucket '{bucket_name}' already exists.")
    except ClientError:
        print(f"[S3 Utility] Creating bucket '{bucket_name}'...")
        s3_client.create_bucket(Bucket=bucket_name)

def upload_bytes(s3_client, bucket_name, s3_key, data_bytes):
    """Uploads raw bytes data to a specified S3 key."""
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=data_bytes
    )
    print(f"[S3 Utility] Uploaded object -> s3://{bucket_name}/{s3_key}")

def upload_file(s3_client, bucket_name, s3_key, local_file_path):
    """Uploads a local file to a specified S3 key."""
    with open(local_file_path, "rb") as f:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=f.read()
        )
    print(f"[S3 Utility] Uploaded file {local_file_path} -> s3://{bucket_name}/{s3_key}")

def delete_object(s3_client, bucket_name, s3_key):
    """Deletes an object from the S3 bucket."""
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
        print(f"[S3 Utility] Deleted object -> s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        print(f"[S3 Utility] Failed to delete s3://{bucket_name}/{s3_key}: {e}")