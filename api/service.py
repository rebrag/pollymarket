from __future__ import annotations

import time
from dataclasses import dataclass
from math import ceil

import pyarrow as pa

from api.data_sources.base import ParquetDataSource, ParquetObject
from api.schemas import EventSummary, MarketMetadataDto, MarketRow, MarketStats, MarketSummary, TradeBucketDto, TradeRowDto, TradeStatsDto

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
            min_tick_size=float(meta.get("min_tick_size", "0") or 0.0),
            min_order_size=int(float(meta.get("min_order_size", "0") or 0)),
            is_neg_risk=str(meta.get("is_neg_risk", "False")).lower() == "true",
            game_start_time=float(meta.get("game_start_time", "0") or 0.0),
            volume=float(meta.get("volume", "0") or 0.0),
            volume_24hr=float(meta.get("volume_24hr", "0") or 0.0),
            liquidity=float(meta.get("liquidity", "0") or 0.0),
            image_url=meta.get("image_url", ""),
            resolution_source=meta.get("resolution_source", ""),
            end_date=meta.get("end_date", ""),
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
            volume=item.metadata.volume,
            image_url=item.metadata.image_url,
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

    def _trade_object_key(self, market_id: str) -> str:
        market: IndexedMarket = self.get_market(market_id)
        object_key: str = self.source.trade_object_key_for(market.object_key)
        if not self.source.object_exists(object_key):
            raise FileNotFoundError(f"Trade parquet not found for market: {market_id}")
        return object_key

    def get_trade_table(self, market_id: str) -> pa.Table:
        return self.source.read_parquet_table(self._trade_object_key(market_id))

    @staticmethod
    def _filter_trade_rows(
        table: pa.Table,
        start_ts: float | None,
        end_ts: float | None,
        min_size: float | None = None,
    ) -> list[TradeRowDto]:
        rows: list[TradeRowDto] = []
        for timestamp, asset_id, price, size, side, fee_rate_bps, transaction_hash, notional_usd in zip(
            table.column("timestamp").to_pylist(),
            table.column("asset_id").to_pylist(),
            table.column("price").to_pylist(),
            table.column("size").to_pylist(),
            table.column("side").to_pylist(),
            table.column("fee_rate_bps").to_pylist(),
            table.column("transaction_hash").to_pylist(),
            table.column("notional_usd").to_pylist(),
        ):
            ts_value: float = float(timestamp)
            size_value: float = float(size)
            if start_ts is not None and ts_value < start_ts:
                continue
            if end_ts is not None and ts_value > end_ts:
                continue
            if min_size is not None and size_value < min_size:
                continue
            rows.append(
                TradeRowDto(
                    timestamp=ts_value,
                    asset_id=str(asset_id),
                    price=float(price),
                    size=size_value,
                    side=str(side),
                    fee_rate_bps=float(fee_rate_bps),
                    transaction_hash=str(transaction_hash),
                    notional_usd=float(notional_usd),
                )
            )
        return rows

    def get_trades(
        self,
        market_id: str,
        limit: int,
        offset: int,
        start_ts: float | None,
        end_ts: float | None,
        min_size: float | None = None,
    ) -> tuple[int, list[TradeRowDto]]:
        table: pa.Table = self.get_trade_table(market_id)
        filtered: list[TradeRowDto] = self._filter_trade_rows(table, start_ts=start_ts, end_ts=end_ts, min_size=min_size)
        total: int = len(filtered)
        return total, filtered[offset : offset + limit]

    def get_trade_series(
        self,
        market_id: str,
        start_ts: float | None,
        end_ts: float | None,
        min_size: float | None,
        max_points: int,
    ) -> list[TradeRowDto]:
        table: pa.Table = self.get_trade_table(market_id)
        filtered: list[TradeRowDto] = self._filter_trade_rows(table, start_ts=start_ts, end_ts=end_ts, min_size=min_size)
        if len(filtered) <= max_points:
            return filtered

        stride: float = len(filtered) / max_points
        sampled: list[TradeRowDto] = []
        for idx in range(max_points):
            sampled.append(filtered[min(len(filtered) - 1, int(idx * stride))])
        return sampled

    def get_trade_stats(
        self,
        market_id: str,
        start_ts: float | None,
        end_ts: float | None,
    ) -> TradeStatsDto:
        table: pa.Table = self.get_trade_table(market_id)
        filtered: list[TradeRowDto] = self._filter_trade_rows(table, start_ts=start_ts, end_ts=end_ts)
        if not filtered:
            return TradeStatsDto(
                trade_count=0,
                buy_count=0,
                sell_count=0,
                max_trade_size=0.0,
                max_notional_usd=0.0,
                first_ts=None,
                last_ts=None,
            )

        buy_count: int = sum(1 for trade in filtered if trade.side == "BUY")
        sell_count: int = sum(1 for trade in filtered if trade.side == "SELL")
        max_trade_size: float = max(trade.size for trade in filtered)
        max_notional_usd: float = max(trade.notional_usd for trade in filtered)
        return TradeStatsDto(
            trade_count=len(filtered),
            buy_count=buy_count,
            sell_count=sell_count,
            max_trade_size=max_trade_size,
            max_notional_usd=max_notional_usd,
            first_ts=filtered[0].timestamp,
            last_ts=filtered[-1].timestamp,
        )

    @staticmethod
    def _bucket_size_seconds(start_ts: float | None, end_ts: float | None, max_points: int) -> int:
        if start_ts is None or end_ts is None or end_ts <= start_ts:
            return max(5, ceil(max_points / 50))
        span_s: float = end_ts - start_ts
        if span_s <= 30 * 60:
            return 5
        if span_s <= 6 * 60 * 60:
            return 15
        if span_s <= 24 * 60 * 60:
            return 60
        return 300

    @staticmethod
    def _percentile_float(values: list[float], percentile: float) -> float:
        if not values:
            return 0.0
        sorted_values: list[float] = sorted(values)
        index: int = min(len(sorted_values) - 1, max(0, ceil((percentile / 100.0) * len(sorted_values)) - 1))
        return sorted_values[index]

    @staticmethod
    def _percentile_int(values: list[int], percentile: float) -> int:
        if not values:
            return 0
        sorted_values: list[int] = sorted(values)
        index: int = min(len(sorted_values) - 1, max(0, ceil((percentile / 100.0) * len(sorted_values)) - 1))
        return sorted_values[index]

    def get_trade_markers(
        self,
        market_id: str,
        start_ts: float | None,
        end_ts: float | None,
        max_points: int,
    ) -> list[TradeBucketDto]:
        table: pa.Table = self.get_trade_table(market_id)
        trades: list[TradeRowDto] = self._filter_trade_rows(table, start_ts=start_ts, end_ts=end_ts)
        if not trades:
            return []

        effective_start_ts: float = trades[0].timestamp if start_ts is None else start_ts
        effective_end_ts: float = trades[-1].timestamp if end_ts is None else end_ts
        first_ts: float = effective_start_ts
        bucket_size_s: int = self._bucket_size_seconds(effective_start_ts, effective_end_ts, max_points=max_points)
        buckets: dict[int, dict[str, float | int]] = {}

        for trade in trades:
            bucket_index: int = int((trade.timestamp - first_ts) // bucket_size_s) if trade.timestamp >= first_ts else 0
            bucket = buckets.get(bucket_index)
            if bucket is None:
                bucket = {
                    "trade_count": 0,
                    "total_size": 0.0,
                    "total_notional_usd": 0.0,
                    "max_trade_size": 0.0,
                    "max_notional_usd": 0.0,
                    "buy_count": 0,
                    "sell_count": 0,
                    "price_weighted_sum": 0.0,
                }
                buckets[bucket_index] = bucket

            bucket["trade_count"] = int(bucket["trade_count"]) + 1
            bucket["total_size"] = float(bucket["total_size"]) + trade.size
            bucket["total_notional_usd"] = float(bucket["total_notional_usd"]) + trade.notional_usd
            bucket["max_trade_size"] = max(float(bucket["max_trade_size"]), trade.size)
            bucket["max_notional_usd"] = max(float(bucket["max_notional_usd"]), trade.notional_usd)
            bucket["price_weighted_sum"] = float(bucket["price_weighted_sum"]) + (trade.price * trade.size)
            if trade.side == "BUY":
                bucket["buy_count"] = int(bucket["buy_count"]) + 1
            else:
                bucket["sell_count"] = int(bucket["sell_count"]) + 1

        max_notionals: list[float] = [float(bucket["max_notional_usd"]) for bucket in buckets.values()]
        trade_counts: list[int] = [int(bucket["trade_count"]) for bucket in buckets.values()]
        large_trade_threshold: float = self._percentile_float(max_notionals, 95.0)
        high_frequency_threshold: int = self._percentile_int(trade_counts, 95.0)

        markers: list[TradeBucketDto] = []
        for bucket_index in sorted(buckets):
            bucket = buckets[bucket_index]
            total_size: float = float(bucket["total_size"])
            bucket_start_ts: float = first_ts + (bucket_index * bucket_size_s)
            bucket_end_ts: float = bucket_start_ts + bucket_size_s
            avg_price: float = float(bucket["price_weighted_sum"]) / total_size if total_size > 0 else 0.0
            max_notional_usd: float = float(bucket["max_notional_usd"])
            trade_count: int = int(bucket["trade_count"])
            markers.append(
                TradeBucketDto(
                    bucket_start_ts=bucket_start_ts,
                    bucket_end_ts=bucket_end_ts,
                    trade_count=trade_count,
                    total_size=total_size,
                    total_notional_usd=float(bucket["total_notional_usd"]),
                    max_trade_size=float(bucket["max_trade_size"]),
                    max_notional_usd=max_notional_usd,
                    buy_count=int(bucket["buy_count"]),
                    sell_count=int(bucket["sell_count"]),
                    avg_price=avg_price,
                    is_large_trade_bucket=max_notional_usd >= large_trade_threshold,
                    is_high_frequency_bucket=trade_count >= high_frequency_threshold,
                )
            )

        if len(markers) <= max_points:
            return markers

        stride: float = len(markers) / max_points
        sampled: list[TradeBucketDto] = []
        for idx in range(max_points):
            sampled.append(markers[min(len(markers) - 1, int(idx * stride))])
        return sampled

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
