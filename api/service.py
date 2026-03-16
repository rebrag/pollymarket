from __future__ import annotations

import time
from dataclasses import dataclass

import pyarrow as pa

from api.data_sources.base import ParquetDataSource, ParquetObject
from api.schemas import EventSummary, MarketMetadataDto, MarketRow, MarketStats, MarketSummary

@dataclass
class IndexedMarket:
    market_id: str
    object_key: str
    display_name: str
    metadata: MarketMetadataDto
    row_count: int

class MarketCatalogService:
    def __init__(self, source: ParquetDataSource, cache_ttl_seconds: int = 15) -> None:
        self.source: ParquetDataSource = source
        self.cache_ttl_seconds: int = cache_ttl_seconds
        self._last_refresh_s: float = 0.0
        self._by_id: dict[str, IndexedMarket] = {}

    def _parse_metadata(self, meta: dict[str, str]) -> MarketMetadataDto:
        return MarketMetadataDto(
            asset_id=meta.get("asset_id", ""),
            event_slug=meta.get("event_slug", ""),
            event_title=meta.get("event_title", ""),
            market_question=meta.get("market_question", ""),
            outcomes=meta.get("outcomes", "[]"),
            min_tick_size=float(meta.get("min_tick_size", "0") or 0),
            min_order_size=int(float(meta.get("min_order_size", "0") or 0)),
            is_neg_risk=str(meta.get("is_neg_risk", "False")).lower() == "true",
            game_start_time=float(meta.get("game_start_time", "0") or 0),
        )

    def _apply_metadata_fallback(self, metadata: MarketMetadataDto, obj: ParquetObject) -> MarketMetadataDto:
        parts: list[str] = obj.object_key.split("/")
        inferred_event_slug: str = parts[0] if parts else "unknown"

        if not metadata.event_slug:
            metadata.event_slug = inferred_event_slug
        if not metadata.event_title:
            metadata.event_title = metadata.event_slug
        if not metadata.market_question:
            metadata.market_question = obj.display_name

        return metadata

    def _index_object(self, obj: ParquetObject) -> IndexedMarket | None:
        try:
            raw_meta: dict[str, str] = self.source.read_parquet_metadata(obj.object_key)
            metadata: MarketMetadataDto = self._parse_metadata(raw_meta)
            metadata = self._apply_metadata_fallback(metadata, obj)
            row_count: int = int(self.source.read_row_count(obj.object_key))
            
            return IndexedMarket(
                market_id=obj.market_id,
                object_key=obj.object_key,
                display_name=obj.display_name,
                metadata=metadata,
                row_count=row_count,
            )
        except Exception:
            return None

    def refresh_index(self, force: bool = False) -> None:
        now: float = time.time()
        if not force and (now - self._last_refresh_s) < self.cache_ttl_seconds:
            return

        next_by_id: dict[str, IndexedMarket] = {}

        get_index_func = getattr(self.source, "get_master_index", None)
        if callable(get_index_func):
            raw_index: list[dict[str, str]] = get_index_func()
            if raw_index:
                for meta_dict in raw_index:
                    market_id: str = meta_dict.get("asset_id", "")
                    object_key: str = meta_dict.get("object_key", "")
                    if not market_id or not object_key:
                        continue
                        
                    display_name: str = object_key.rsplit("/", 1)[-1].rsplit(".parquet", 1)[0]
                    
                    try:
                        metadata: MarketMetadataDto = self._parse_metadata(meta_dict)
                        row_count_str: str = meta_dict.get("row_count", "0")
                        row_count: int = int(row_count_str) if row_count_str.isdigit() else 0
                        
                        next_by_id[market_id] = IndexedMarket(
                            market_id=market_id,
                            object_key=object_key,
                            display_name=display_name,
                            metadata=metadata,
                            row_count=row_count,
                        )
                    except Exception:
                        continue
                        
                self._by_id = next_by_id
                self._last_refresh_s = now
                return

        for obj in self.source.list_parquet_objects():
            indexed: IndexedMarket | None = self._index_object(obj)
            if indexed is not None:
                next_by_id[indexed.market_id] = indexed

        self._by_id = next_by_id
        self._last_refresh_s = now

    def _market_summary(self, item: IndexedMarket) -> MarketSummary:
        return MarketSummary(
            market_id=item.market_id,
            object_key=item.object_key,
            display_name=item.display_name,
            event_slug=item.metadata.event_slug,
            event_title=item.metadata.event_title,
            market_question=item.metadata.market_question,
            row_count=item.row_count,
        )

    def list_events(self) -> list[EventSummary]:
        self.refresh_index()
        grouped: dict[str, tuple[EventSummary, int]] = {}
        
        for item in self._by_id.values():
            key: str = item.metadata.event_slug or "unknown"
            if key not in grouped:
                grouped[key] = (
                    EventSummary(
                        event_slug=key,
                        event_title=item.metadata.event_title or key,
                        market_count=0,
                        game_start_time=item.metadata.game_start_time,
                    ),
                    item.row_count,
                )
            summary, max_rows = grouped[key]
            summary.market_count += 1
            if item.row_count > max_rows:
                summary.game_start_time = item.metadata.game_start_time
                grouped[key] = (summary, item.row_count)

        events: list[EventSummary] = [summary for summary, _ in grouped.values()]
        return sorted(
            events,
            key=lambda e: (
                e.game_start_time <= 0,
                e.game_start_time if e.game_start_time > 0 else float("inf"),
                e.event_slug,
                e.event_title,
            ),
        )

    def list_markets(self, event_slug: str | None, query: str | None) -> list[MarketSummary]:
        self.refresh_index()
        q: str = (query or "").strip().lower()

        markets: list[MarketSummary] = []
        for item in self._by_id.values():
            if event_slug and item.metadata.event_slug != event_slug:
                continue
            if q and q not in item.display_name.lower() and q not in item.metadata.market_question.lower():
                continue
            markets.append(self._market_summary(item))

        markets.sort(key=lambda m: (m.event_slug, m.display_name))
        return markets

    def get_market(self, market_id: str) -> IndexedMarket:
        self.refresh_index()
        market: IndexedMarket | None = self._by_id.get(market_id)
        if market is None:
            raise KeyError(f"Market ID not found: {market_id}")
        return market

    def get_metadata(self, market_id: str) -> MarketMetadataDto:
        return self.get_market(market_id).metadata

    def get_rows(self, market_id: str, limit: int, offset: int) -> tuple[int, list[MarketRow]]:
        market: IndexedMarket = self.get_market(market_id)
        table: pa.Table = self.source.read_parquet_slice(market.object_key, limit=limit, offset=offset)
        
        rows: list[MarketRow] = [
            MarketRow(timestamp=float(ts), best_bid=float(bb), best_ask=float(ba))
            for ts, bb, ba in zip(
                table.column("timestamp").to_pylist(),
                table.column("best_bid").to_pylist(),
                table.column("best_ask").to_pylist(),
            )
        ]
        return market.row_count, rows

    def get_table(self, market_id: str) -> pa.Table:
        market: IndexedMarket = self.get_market(market_id)
        return self.source.read_parquet_table(market.object_key)

    def get_stats(self, market_id: str) -> MarketStats:
        market: IndexedMarket = self.get_market(market_id)
        table: pa.Table = self.source.read_parquet_table(market.object_key)

        if table.num_rows == 0:
            return MarketStats(
                row_count=0,
                first_ts=None,
                last_ts=None,
                min_best_bid=None,
                max_best_bid=None,
                min_best_ask=None,
                max_best_ask=None,
            )

        ts: list[float] = [float(v) for v in table.column("timestamp").to_pylist()]
        bids: list[float] = [float(v) for v in table.column("best_bid").to_pylist()]
        asks: list[float] = [float(v) for v in table.column("best_ask").to_pylist()]

        return MarketStats(
            row_count=market.row_count,
            first_ts=ts[0],
            last_ts=ts[-1],
            min_best_bid=min(bids),
            max_best_bid=max(bids),
            min_best_ask=min(asks),
            max_best_ask=max(asks),
        )