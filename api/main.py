from __future__ import annotations

from functools import lru_cache

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from api.config import Settings, get_settings
from api.data_sources.local_fs import LocalParquetDataSource
from api.data_sources.s3_store import S3ParquetDataSource
from api.schemas import (
    EventSummary,
    MarketMetadataDto,
    MarketRow,
    MarketSeriesPoint,
    MarketStats,
    MarketSummary,
    PaginatedResponse,
)
from api.service import MarketCatalogService
from api.utils.downsample import sample_indices

app = FastAPI(title="Parquet Explorer API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200", "http://127.0.0.1:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@lru_cache(maxsize=1)
def _build_catalog(
    data_source: str,
    local_data_root: str,
    aws_region: str,
    s3_bucket: str,
    s3_prefix: str,
    cache_ttl_seconds: int,
    include_part_files: bool,
) -> MarketCatalogService:
    if data_source == "local":
        source = LocalParquetDataSource(local_data_root, include_part_files=include_part_files)
    else:
        source = S3ParquetDataSource(
            bucket=s3_bucket,
            prefix=s3_prefix,
            region=aws_region,
            include_part_files=include_part_files,
        )
    return MarketCatalogService(source, cache_ttl_seconds=cache_ttl_seconds)


def get_catalog(settings: Settings = Depends(get_settings)) -> MarketCatalogService:
    return _build_catalog(
        settings.data_source,
        settings.local_data_root,
        settings.aws_region,
        settings.s3_bucket,
        settings.s3_prefix,
        settings.cache_ttl_seconds,
        settings.include_part_files,
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/v1/events", response_model=list[EventSummary])
def list_events(catalog: MarketCatalogService = Depends(get_catalog)) -> list[EventSummary]:
    return catalog.list_events()


@app.get("/api/v1/markets", response_model=PaginatedResponse[MarketSummary])
def list_markets(
    event_slug: str | None = Query(default=None),
    q: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    catalog: MarketCatalogService = Depends(get_catalog),
) -> PaginatedResponse[MarketSummary]:
    items = catalog.list_markets(event_slug=event_slug, query=q)
    return PaginatedResponse[MarketSummary](
        items=items[offset : offset + limit],
        total=len(items),
        limit=limit,
        offset=offset,
    )


@app.get("/api/v1/markets/{market_id}/metadata", response_model=MarketMetadataDto)
def get_metadata(
    market_id: str,
    catalog: MarketCatalogService = Depends(get_catalog),
) -> MarketMetadataDto:
    try:
        return catalog.get_metadata(market_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="market_id not found") from exc


@app.get("/api/v1/markets/{market_id}/rows", response_model=PaginatedResponse[MarketRow])
def get_rows(
    market_id: str,
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    catalog: MarketCatalogService = Depends(get_catalog),
) -> PaginatedResponse[MarketRow]:
    try:
        total, rows = catalog.get_rows(market_id, limit=limit, offset=offset)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="market_id not found") from exc

    return PaginatedResponse[MarketRow](items=rows, total=total, limit=limit, offset=offset)


@app.get("/api/v1/markets/{market_id}/series", response_model=list[MarketSeriesPoint])
def get_series(
    market_id: str,
    max_points: int = Query(default=500, ge=10, le=10000),
    catalog: MarketCatalogService = Depends(get_catalog),
) -> list[MarketSeriesPoint]:
    try:
        table = catalog.get_table(market_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="market_id not found") from exc

    ts = [float(v) for v in table.column("timestamp").to_pylist()]
    bids = [float(v) for v in table.column("best_bid").to_pylist()]
    asks = [float(v) for v in table.column("best_ask").to_pylist()]

    idx = sample_indices(len(ts), max_points=max_points)
    return [
        MarketSeriesPoint(timestamp=ts[i], best_bid=bids[i], best_ask=asks[i])
        for i in idx
    ]


@app.get("/api/v1/markets/{market_id}/stats", response_model=MarketStats)
def get_stats(
    market_id: str,
    catalog: MarketCatalogService = Depends(get_catalog),
) -> MarketStats:
    try:
        return catalog.get_stats(market_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="market_id not found") from exc
