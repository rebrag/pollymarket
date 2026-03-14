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


class MarketMetadataDto(BaseModel):
    asset_id: str = ""
    event_slug: str = ""
    event_title: str = ""
    market_question: str = ""
    outcomes: str = "[]"
    min_tick_size: float = 0.0
    min_order_size: int = 0
    is_neg_risk: bool = False
    game_start_time: float = 0.0


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


class PaginatedResponse(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
