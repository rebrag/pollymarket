from __future__ import annotations

import hashlib
from functools import lru_cache
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
import json

from .base import ParquetObject


class S3ParquetDataSource:
    def __init__(self, bucket: str, prefix: str = "", region: str = "us-east-1", include_part_files: bool = False) -> None:
        if not bucket.strip():
            raise ValueError("bucket name is strictly required.")
            
        self.bucket: str = bucket
        self.prefix: str = prefix.strip("/")
        self.client = boto3.client("s3", region_name=region)
        self.include_part_files: bool = include_part_files

    def get_master_index(self) -> list[dict[str, str]]:
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key="market_index.json")
            payload: bytes = obj["Body"].read()
            return json.loads(payload.decode("utf-8"))
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                return []
            raise RuntimeError(f"Failed to fetch market_index.json: {e}") from e

    def trade_object_key_for(self, object_key: str) -> str:
        if not object_key.endswith(".parquet"):
            raise ValueError("Expected parquet object key")
        return f"{object_key[:-8]}__trades.parquet"

    @staticmethod
    def _stable_id(object_key: str) -> str:
        if not object_key.strip():
            raise ValueError("object_key is strictly required.")
        return hashlib.sha1(object_key.encode("utf-8")).hexdigest()[:16]

    def _full_prefix(self) -> str:
        return f"{self.prefix}/" if self.prefix else ""

    def _key_with_prefix(self, object_key: str) -> str:
        prefix: str = self._full_prefix()
        return f"{prefix}{object_key}" if prefix else object_key

    def _strip_prefix(self, key: str) -> str:
        prefix: str = self._full_prefix()
        return key[len(prefix):] if prefix and key.startswith(prefix) else key
    
    

    @lru_cache(maxsize=2048)
    def _read_object_bytes(self, object_key: str) -> bytes:
        if not object_key.strip():
            raise ValueError("object_key is strictly required.")
            
        key: str = self._key_with_prefix(object_key)
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            return obj["Body"].read()
        except ClientError as e:
            raise RuntimeError(f"Failed to fetch {key} from S3: {e}") from e

    def list_parquet_objects(self) -> list[ParquetObject]:
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            objects: list[ParquetObject] = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=self._full_prefix()):
                for item in page.get("Contents", []):
                    key: str = str(item.get("Key", ""))
                    if not key.endswith(".parquet"):
                        continue
                        
                    object_key: str = self._strip_prefix(key)
                    if not self.include_part_files and object_key.rsplit("/", 1)[-1].startswith("part-"):
                        continue
                    if object_key.endswith("__trades.parquet"):
                        continue
                    if "__trades/" in object_key:
                        continue
                        
                    display_name: str = object_key.rsplit("/", 1)[-1].rsplit(".parquet", 1)[0]
                    objects.append(
                        ParquetObject(
                            market_id=self._stable_id(object_key),
                            object_key=object_key,
                            display_name=display_name,
                        )
                    )

            objects.sort(key=lambda o: o.object_key)
            return objects
        except ClientError as e:
            raise RuntimeError(f"Failed to list objects in bucket {self.bucket}: {e}") from e

    def _table(self, object_key: str) -> pa.Table:
        payload: bytes = self._read_object_bytes(object_key)
        return pq.read_table(BytesIO(payload))

    @lru_cache(maxsize=2048)
    def read_parquet_metadata(self, object_key: str) -> dict[str, str]:
        payload: bytes = self._read_object_bytes(object_key)
        schema = pq.read_schema(BytesIO(payload))
        raw: dict[bytes, bytes] = schema.metadata or {}
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in raw.items()}

    def read_parquet_head(self, object_key: str, limit: int) -> pa.Table:
        return self._table(object_key).slice(0, max(0, limit))

    def read_parquet_slice(self, object_key: str, limit: int, offset: int) -> pa.Table:
        return self._table(object_key).slice(max(0, offset), max(0, limit))

    def read_parquet_table(self, object_key: str) -> pa.Table:
        return self._table(object_key)

    def read_row_count(self, object_key: str) -> int:
        payload: bytes = self._read_object_bytes(object_key)
        pf = pq.ParquetFile(BytesIO(payload))
        return pf.metadata.num_rows if pf.metadata is not None else 0

    def object_exists(self, object_key: str) -> bool:
        key: str = self._key_with_prefix(object_key)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise RuntimeError(f"Failed to check object existence for {key}: {e}") from e
