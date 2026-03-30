import os
import json
import boto3
import asyncio
import pyarrow.parquet as pq
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

load_dotenv()

UPLOAD_WORKER_OFFLINE_MODE: bool = os.environ.get("UPLOAD_WORKER_OFFLINE_MODE", "True").lower() == "true"
S3_BUCKET_NAME: str = os.environ.get("S3_BUCKET_NAME", "")
INDEX_FILE_KEY: str = "market_index.json"
TRADE_FILE_SUFFIX: str = "__trades.parquet"

print(f"\n--- S3 UPLOAD WORKER INITIALIZED | OFFLINE_MODE: {UPLOAD_WORKER_OFFLINE_MODE} ---\n")

if not UPLOAD_WORKER_OFFLINE_MODE and not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME environment variable is strictly required when OFFLINE_MODE is False.")

def verify_aws_credentials() -> None:
    if UPLOAD_WORKER_OFFLINE_MODE:
        return
    try:
        sts = boto3.client("sts")
        sts.get_caller_identity()
    except (NoCredentialsError, ClientError) as e:
        raise RuntimeError(f"AWS credentials validation failed: {e}") from e

verify_aws_credentials()

s3_client = boto3.client("s3") if not UPLOAD_WORKER_OFFLINE_MODE else None
upload_queue: asyncio.Queue[str] = asyncio.Queue()

def _extract_metadata(file_path: str) -> dict[str, str]:
    schema = pq.read_schema(file_path)
    raw_meta: dict[bytes, bytes] = schema.metadata or {}
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_meta.items()}

def _update_remote_index(bucket: str, new_meta: dict[str, str], object_name: str) -> None:
    if s3_client is None:
        raise RuntimeError("S3 client not initialized.")
        
    index_data: list[dict[str, str]] = []
    try:
        response = s3_client.get_object(Bucket=bucket, Key=INDEX_FILE_KEY)
        index_data = json.loads(response["Body"].read().decode("utf-8"))
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchKey":
            raise RuntimeError(f"Failed to fetch index: {e}") from e

    new_meta["object_key"] = object_name
    
    # Remove older version of this market if it exists, then append new
    asset_id: str = new_meta.get("asset_id", "")
    index_data = [item for item in index_data if item.get("asset_id") != asset_id]
    index_data.append(new_meta)

    s3_client.put_object(
        Bucket=bucket,
        Key=INDEX_FILE_KEY,
        Body=json.dumps(index_data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )

def upload_to_s3_and_delete(file_path: str, bucket: str, object_name: str) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Target file does not exist: {file_path}")
    if s3_client is None:
        raise RuntimeError("S3 client is not initialized.")

    new_meta: dict[str, str] = _extract_metadata(file_path)
    s3_client.upload_file(file_path, bucket, object_name)
    if not object_name.endswith(TRADE_FILE_SUFFIX):
        _update_remote_index(bucket, new_meta, object_name)
    os.remove(file_path)

async def s3_upload_worker() -> None:
    if UPLOAD_WORKER_OFFLINE_MODE:
        return

    while True:
        file_path: str = await upload_queue.get()
        try:
            object_name: str = os.path.basename(file_path)
            await asyncio.to_thread(upload_to_s3_and_delete, file_path, S3_BUCKET_NAME, object_name)
            print(f"Uploaded {object_name} and updated index.")
        except Exception as e:
            print(f"Critical S3 Upload Failure for {file_path}: {e}")
        finally:
            upload_queue.task_done()
