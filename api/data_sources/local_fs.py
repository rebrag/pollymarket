from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .base import ParquetObject


class LocalParquetDataSource:
    def __init__(self, root_dir: str, include_part_files: bool = False) -> None:
        self.root = Path(root_dir).resolve()
        self.include_part_files = include_part_files

    @staticmethod
    def _stable_id(object_key: str) -> str:
        return hashlib.sha1(object_key.encode("utf-8")).hexdigest()[:16]

    def list_parquet_objects(self) -> list[ParquetObject]:
        if not self.root.exists():
            return []

        objects: list[ParquetObject] = []
        for path in sorted(self.root.rglob("*.parquet")):
            if not self.include_part_files and path.name.startswith("part-"):
                continue
            if path.name.endswith("__trades.parquet"):
                continue
            object_key = path.relative_to(self.root).as_posix()
            if "__trades/" in object_key:
                continue
            objects.append(
                ParquetObject(
                    market_id=self._stable_id(object_key),
                    object_key=object_key,
                    display_name=path.stem,
                )
            )
        return objects

    def trade_object_key_for(self, object_key: str) -> str:
        if not object_key.endswith(".parquet"):
            raise ValueError("Expected parquet object key")
        return f"{object_key[:-8]}__trades.parquet"

    def _resolve(self, object_key: str) -> Path:
        target = (self.root / object_key).resolve()
        if self.root not in target.parents and target != self.root:
            raise ValueError("Invalid object_key path")
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(object_key)
        return target

    def read_parquet_metadata(self, object_key: str) -> dict[str, str]:
        schema = pq.read_schema(self._resolve(object_key))
        raw = schema.metadata or {}
        return {k.decode("utf-8"): v.decode("utf-8") for k, v in raw.items()}

    def read_parquet_head(self, object_key: str, limit: int) -> pa.Table:
        table = pq.read_table(self._resolve(object_key))
        return table.slice(0, max(0, limit))

    def read_parquet_slice(self, object_key: str, limit: int, offset: int) -> pa.Table:
        table = pq.read_table(self._resolve(object_key))
        return table.slice(max(0, offset), max(0, limit))

    def read_parquet_table(self, object_key: str) -> pa.Table:
        return pq.read_table(self._resolve(object_key))

    def read_row_count(self, object_key: str) -> int:
        pf = pq.ParquetFile(self._resolve(object_key))
        return pf.metadata.num_rows if pf.metadata is not None else 0

    def object_exists(self, object_key: str) -> bool:
        try:
            self._resolve(object_key)
            return True
        except FileNotFoundError:
            return False
