import json
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from io import BytesIO

import boto3
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

def _get_aws_credentials() -> tuple[str, str]:
    access_key: str | None = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key: str | None = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set in the environment.")
        
    return access_key, secret_key

def fetch_metadata(
    bucket: str, 
    key: str, 
    region: str, 
    access_key: str, 
    secret_key: str
) -> dict[str, str]:
    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    response: dict[str, object] = s3.get_object(Bucket=bucket, Key=key)
    payload_stream = response.get("Body")
    
    if payload_stream is None:
        raise RuntimeError(f"Response body is empty for key {key}")
        
    payload: bytes = payload_stream.read()
    
    schema = pq.read_schema(BytesIO(payload))
    raw_meta: dict[bytes, bytes] = schema.metadata or {}
    
    meta: dict[str, str] = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_meta.items()}
    meta["object_key"] = key
    
    return meta

def generate_index(bucket_name: str, region: str) -> None:
    access_key, secret_key = _get_aws_credentials()

    s3 = boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    
    for page in paginator.paginate(Bucket=bucket_name):
        contents: list[dict[str, object]] = page.get("Contents", [])
        for item in contents:
            key: str = str(item.get("Key", ""))
            if key.endswith(".parquet") and "part-" not in key:
                keys.append(key)
    
    index_data: list[dict[str, str]] = []
    
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures: dict[Future, str] = {
            executor.submit(
                fetch_metadata, 
                bucket_name, 
                k, 
                region, 
                access_key, 
                secret_key
            ): k for k in keys
        }
        
        for future in as_completed(futures):
            try:
                result: dict[str, str] = future.result()
                index_data.append(result)
            except Exception as e:
                print(f"Failed to process {futures[future]}: {e}")

    s3.put_object(
        Bucket=bucket_name,
        Key="market_index.json",
        Body=json.dumps(index_data, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )
    
    print(f"Successfully generated market_index.json with {len(index_data)} records.")

if __name__ == "__main__":
    generate_index(bucket_name="rebrag-polymarket-logs", region="us-east-2")