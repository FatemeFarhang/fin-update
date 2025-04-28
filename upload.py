# upload_file.py
import logging

logger = logging.getLogger('main')

# Upload a file to a bucket
def upload_file(s3_client, bucket_name, file, file_name):
    try:
        s3_client.upload_file(file, bucket_name, file_name)
        logger.info(f"File '{file_name}' uploaded successfully.")
    except Exception as e:
        logger.info(f"Error uploading file: {e}")
