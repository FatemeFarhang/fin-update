# utils.py
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os 

# Initialize S3 client
def get_s3_client():
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("LIARA_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("LIARA_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("LIARA_SECRET_KEY")
        )
        return s3_client
    except NoCredentialsError:
        print("Credentials not available.")
        return None

# List all buckets
def list_buckets(s3_client):
    try:
        response = s3_client.list_buckets()
        return [bucket['Name'] for bucket in response.get('Buckets', [])]
    except ClientError as e:
        print(f"Error listing buckets: {e}")
        return []
    
def list_files(s3_client, bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except Exception as e:
        print(f"Error listing files: {e}")
        return []
    
def download_file(s3_client, bucket_name, file_name):
    try:
        file_path = f"./{file_name}"
        s3_client.download_file(bucket_name, file_name, file_path)
        print(f"File '{file_name}' downloaded successfully.")
        return file_path
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None
    