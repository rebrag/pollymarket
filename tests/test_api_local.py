from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from api.config import get_settings
from api.main import _build_catalog, app
from api.data_sources.local_fs import LocalParquetDataSource


def test_local_source_lists_parquet() -> None:
    source = LocalParquetDataSource("./market_data")
    objects = source.list_parquet_objects()
    assert len(objects) > 0
    assert objects[0].object_key.endswith(".parquet")
    assert len(objects[0].market_id) == 16


def test_local_source_part_file_visibility_flag(tmp_path: Path) -> None:
    table = pa.table({"timestamp": [1.0], "best_bid": [0.1], "best_ask": [0.2]})
    pq.write_table(table, tmp_path / "combined.parquet")
    pq.write_table(table, tmp_path / "part-000001.parquet")

    hidden_parts = LocalParquetDataSource(str(tmp_path), include_part_files=False)
    visible_parts = LocalParquetDataSource(str(tmp_path), include_part_files=True)

    hidden_names = sorted(obj.display_name for obj in hidden_parts.list_parquet_objects())
    visible_names = sorted(obj.display_name for obj in visible_parts.list_parquet_objects())

    assert hidden_names == ["combined"]
    assert visible_names == ["combined", "part-000001"]


def test_api_endpoints_local(monkeypatch) -> None:
    monkeypatch.setenv("DATA_SOURCE", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", "./market_data")

    get_settings.cache_clear()
    _build_catalog.cache_clear()

    client = TestClient(app)

    health = client.get("/health")
    assert health.status_code == 200

    events = client.get("/api/v1/events")
    assert events.status_code == 200
    event_items = events.json()
    assert len(event_items) > 0

    markets = client.get("/api/v1/markets", params={"limit": 5, "offset": 0})
    assert markets.status_code == 200
    payload = markets.json()
    assert payload["total"] > 0
    assert len(payload["items"]) > 0

    market_id = payload["items"][0]["market_id"]

    metadata = client.get(f"/api/v1/markets/{market_id}/metadata")
    assert metadata.status_code == 200
    assert "event_slug" in metadata.json()

    rows = client.get(f"/api/v1/markets/{market_id}/rows", params={"limit": 10, "offset": 0})
    assert rows.status_code == 200
    assert rows.json()["limit"] == 10

    series = client.get(f"/api/v1/markets/{market_id}/series", params={"max_points": 25})
    assert series.status_code == 200
    assert len(series.json()) <= 25

    stats = client.get(f"/api/v1/markets/{market_id}/stats")
    assert stats.status_code == 200
    assert stats.json()["row_count"] >= 0


def test_missing_market_returns_404(monkeypatch) -> None:
    monkeypatch.setenv("DATA_SOURCE", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", "./market_data")

    get_settings.cache_clear()
    _build_catalog.cache_clear()

    client = TestClient(app)
    res = client.get("/api/v1/markets/notarealid/metadata")
    assert res.status_code == 404
