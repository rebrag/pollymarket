from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

from dotenv import load_dotenv
load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_str(default: str, *names: str) -> str:
    for name in names:
        raw = os.getenv(name)
        if raw is not None and raw.strip() != "":
            return raw.strip()
    return default


@dataclass(frozen=True)
class Settings:
    data_source: str = _env_str("local","DATA_SOURCE").lower()
    local_data_root: str = _env_str("./market_data", "LOCAL_DATA_ROOT")
    aws_region: str = _env_str("us-east-1", "AWS_REGION", "AWS_DEFAULT_REGION")
    s3_bucket: str = _env_str("","S3_BUCKET_NAME")
    s3_prefix: str = _env_str("", "S3_PREFIX").strip("/")
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
