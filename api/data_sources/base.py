from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa


@dataclass(frozen=True)
class ParquetObject:
    market_id: str
    object_key: str
    display_name: str


class ParquetDataSource(Protocol):
    def list_parquet_objects(self) -> list[ParquetObject]:
        ...

    def trade_object_key_for(self, object_key: str) -> str:
        ...

    def read_parquet_metadata(self, object_key: str) -> dict[str, str]:
        ...

    def read_parquet_head(self, object_key: str, limit: int) -> pa.Table:
        ...

    def read_parquet_slice(self, object_key: str, limit: int, offset: int) -> pa.Table:
        ...

    def read_parquet_table(self, object_key: str) -> pa.Table:
        ...

    def read_row_count(self, object_key: str) -> int:
        ...

    def object_exists(self, object_key: str) -> bool:
        ...
