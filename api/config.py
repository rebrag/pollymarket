from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    data_source: str = os.getenv("DATA_SOURCE", "local").strip().lower()
    local_data_root: str = os.getenv("LOCAL_DATA_ROOT", "./market_data").strip()
    aws_region: str = os.getenv("AWS_REGION", "us-east-1").strip()
    s3_bucket: str = os.getenv("S3_BUCKET", "").strip()
    s3_prefix: str = os.getenv("S3_PREFIX", "").strip("/")
    cache_ttl_seconds: int = int(os.getenv("DATA_CACHE_TTL_SECONDS", "15"))
    include_part_files: bool = _env_bool("INCLUDE_PART_FILES", True)

    def validate(self) -> None:
        if self.data_source not in {"local", "s3"}:
            raise ValueError("DATA_SOURCE must be either 'local' or 's3'")
        if self.data_source == "s3" and not self.s3_bucket:
            raise ValueError("S3_BUCKET is required when DATA_SOURCE=s3")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.validate()
    return settings
