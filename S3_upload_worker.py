import os
import boto3
import asyncio
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv
load_dotenv()

UPLOAD_WORKER_OFFLINE_MODE: bool = os.environ.get("UPLOAD_WORKER_OFFLINE_MODE", "True").lower() == "true"
S3_BUCKET_NAME: str = os.environ.get("S3_BUCKET_NAME", "")

print(f"\n--- S3 UPLOAD WORKER INITIALIZED | OFFLINE_MODE: {UPLOAD_WORKER_OFFLINE_MODE} ---\n")

if not UPLOAD_WORKER_OFFLINE_MODE and not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is strictly required when OFFLINE_MODE is False.")

def verify_aws_credentials() -> None:
    if UPLOAD_WORKER_OFFLINE_MODE:
        return
    try:
        sts = boto3.client("sts")
        sts.get_caller_identity()
        print("AWS credentials verified successfully.")
    except (NoCredentialsError, ClientError) as e:
        raise RuntimeError(f"AWS credentials validation failed. Please check your configuration: {e}") from e

verify_aws_credentials()

s3_client = boto3.client("s3") if not UPLOAD_WORKER_OFFLINE_MODE else None
upload_queue: asyncio.Queue[str] = asyncio.Queue()

def upload_to_s3_and_delete(file_path: str, bucket: str, object_name: str) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Target file for S3 upload does not exist: {file_path}")
    
    if s3_client is None:
        raise RuntimeError("S3 client is not initialized due to OFFLINE_MODE.")

    s3_client.upload_file(file_path, bucket, object_name)
    os.remove(file_path)

async def s3_upload_worker() -> None:
    if UPLOAD_WORKER_OFFLINE_MODE:
        return

    while True:
        file_path: str = await upload_queue.get()
        try:
            object_name: str = os.path.basename(file_path)
            await asyncio.to_thread(upload_to_s3_and_delete, file_path, S3_BUCKET_NAME, object_name)
            print(f"Uploaded and removed: {object_name}")
        except Exception as e:
            print(f"Critical S3 Upload Failure for {file_path}: {e}")
            raise RuntimeError(f"Failed to upload {file_path} to S3") from e
        finally:
            upload_queue.task_done()