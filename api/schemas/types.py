from __future__ import annotations

from typing import Generic, TypeVar

from pydantic import BaseModel, Field


T = TypeVar("T")


class EventSummary(BaseModel):
    event_slug: str
    event_title: str
    market_count: int
    game_start_time: float = 0.0


class MarketSummary(BaseModel):
    market_id: str
    object_key: str
    display_name: str
    event_slug: str
    event_title: str
    market_question: str
    row_count: int
    volume: float = 0.0
    image_url: str = ""


class MarketMetadataDto(BaseModel):
    asset_id: str
    event_slug: str
    event_title: str
    market_question: str
    outcomes: str
    min_tick_size: float
    min_order_size: int
    is_neg_risk: bool
    game_start_time: float
    volume: float = 0.0
    volume_24hr: float = 0.0
    liquidity: float = 0.0
    image_url: str = ""
    resolution_source: str = ""
    end_date: str = ""


class MarketRow(BaseModel):
    timestamp: float
    best_bid: float
    best_ask: float


class MarketSeriesPoint(BaseModel):
    timestamp: float
    best_bid: float
    best_ask: float


class MarketStats(BaseModel):
    row_count: int
    first_ts: float | None
    last_ts: float | None
    min_best_bid: float | None
    max_best_bid: float | None
    min_best_ask: float | None
    max_best_ask: float | None


class TradeRowDto(BaseModel):
    timestamp: float
    asset_id: str
    price: float
    size: float
    side: str
    fee_rate_bps: float
    transaction_hash: str
    notional_usd: float


class TradeBucketDto(BaseModel):
    bucket_start_ts: float
    bucket_end_ts: float
    trade_count: int
    total_size: float
    total_notional_usd: float
    max_trade_size: float
    max_notional_usd: float
    buy_count: int
    sell_count: int
    avg_price: float
    is_large_trade_bucket: bool
    is_high_frequency_bucket: bool


class TradeStatsDto(BaseModel):
    trade_count: int
    buy_count: int
    sell_count: int
    max_trade_size: float
    max_notional_usd: float
    first_ts: float | None
    last_ts: float | None


class PaginatedResponse(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
