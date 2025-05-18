import os
import fnmatch
import boto3
from typing import List, Optional

def should_exclude(file_path: str, exclude_patterns: List[str]) -> bool:
    for pattern in exclude_patterns:
        if fnmatch.fnmatch(file_path, pattern):
            return True
    return False

def sync_directory_to_s3(
    local_directory: str,
    bucket_name: str,
    s3_client = None,
    s3_prefix: str = "",
    exclude_patterns: Optional[List[str]] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region_name: Optional[str] = None
) -> None:
    exclude_patterns = exclude_patterns or []

    if s3_client is None:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )

    local_directory = os.path.abspath(local_directory)

    files_to_upload = []
    for root, _, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            if not should_exclude(relative_path, exclude_patterns):
                files_to_upload.append((local_path, relative_path))

    for local_path, relative_path in files_to_upload:
        s3_key = os.path.join(s3_prefix, relative_path).replace(os.sep, '/')
        try:
            with open(local_path, "rb") as data:
                s3_client.upload_fileobj(data, bucket_name, s3_key)
            print(f"File '{relative_path}' uploaded successfully.")
        except Exception as e:
            print(f"Error uploading file: {e}")

if __name__ == "__main__":
    s3_client = boto3.client(
        's3',
        aws_access_key_id="your-access-key-id",
        aws_secret_access_key="your-secret-access-key",
        region_name="your-region-name"
    )
    sync_directory_to_s3(
        local_directory="./your_folder",
        bucket_name="your-bucket-name",
        s3_client=s3_client,
        s3_prefix="optional/s3/prefix",
        exclude_patterns=["*.tmp", "node_modules/*", "__pycache__/*"]
    )
