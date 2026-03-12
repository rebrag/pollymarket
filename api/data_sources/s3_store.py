from __future__ import annotations

import hashlib
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from .base import ParquetObject


class S3ParquetDataSource:
    def __init__(self, bucket: str, prefix: str = "", region: str = "us-east-1", include_part_files: bool = False) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client("s3", region_name=region)
        self.include_part_files = include_part_files

    @staticmethod
    def _stable_id(object_key: str) -> str:
        return hashlib.sha1(object_key.encode("utf-8")).hexdigest()[:16]

    def _full_prefix(self) -> str:
        return f"{self.prefix}/" if self.prefix else ""

    def _key_with_prefix(self, object_key: str) -> str:
        prefix = self._full_prefix()
        return f"{prefix}{object_key}" if prefix else object_key

    def _strip_prefix(self, key: str) -> str:
        prefix = self._full_prefix()
        return key[len(prefix) :] if prefix and key.startswith(prefix) else key

    def _read_object_bytes(self, object_key: str) -> bytes:
        key = self._key_with_prefix(object_key)
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj["Body"].read()

    def list_parquet_objects(self) -> list[ParquetObject]:
        paginator = self.client.get_paginator("list_objects_v2")
        objects: list[ParquetObject] = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._full_prefix()):
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.endswith(".parquet"):
                    continue
                object_key = self._strip_prefix(key)
                if not self.include_part_files and object_key.rsplit("/", 1)[-1].startswith("part-"):
                    continue
                display_name = object_key.rsplit("/", 1)[-1].rsplit(".parquet", 1)[0]
                objects.append(
                    ParquetObject(
                        market_id=self._stable_id(object_key),
                        object_key=object_key,
                        display_name=display_name,
                    )
                )

        objects.sort(key=lambda o: o.object_key)
        return objects

    def _table(self, object_key: str) -> pa.Table:
        payload = self._read_object_bytes(object_key)
        return pq.read_table(BytesIO(payload))

    def read_parquet_metadata(self, object_key: str) -> dict[str, str]:
        payload = self._read_object_bytes(object_key)
        schema = pq.read_schema(BytesIO(payload))
        raw = schema.metadata or {}
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in raw.items()}

    def read_parquet_head(self, object_key: str, limit: int) -> pa.Table:
        return self._table(object_key).slice(0, max(0, limit))

    def read_parquet_slice(self, object_key: str, limit: int, offset: int) -> pa.Table:
        return self._table(object_key).slice(max(0, offset), max(0, limit))

    def read_parquet_table(self, object_key: str) -> pa.Table:
        return self._table(object_key)

    def read_row_count(self, object_key: str) -> int:
        payload = self._read_object_bytes(object_key)
        pf = pq.ParquetFile(BytesIO(payload))
        return pf.metadata.num_rows if pf.metadata is not None else 0
