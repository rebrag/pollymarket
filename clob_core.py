"""
clob_core.py — Shared CLOB helpers used by poly_parquet_generator and orderbook-gui.

Single source of truth for: WebSocket URL, orderbook construction, book hydration,
price-change application, best-price extraction, unsubscription detection, and
WS message serialisation.
"""

import datetime
import json

from models import Event, Market, Orderbook

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


# ── Market / asset helpers ────────────────────────────────────────────────────

def parse_asset_id(market: Market) -> str:
    return market["clobTokenIds"].strip('[]"').partition('",')[0]


def parse_token_ids(market: Market) -> list[str]:
    """Return all token IDs for this market (usually two for a binary market)."""
    try:
        return json.loads(market["clobTokenIds"])
    except Exception:
        return [parse_asset_id(market)]


def create_orderbook_skeleton(event: Event, market: Market, asset_id: str) -> Orderbook:
    try:
        gst = datetime.datetime.fromisoformat(market.get("gameStartTime", "")).timestamp()
    except Exception:
        gst = 0.0

    token_ids = parse_token_ids(market)
    try:
        outcomes_list = json.loads(market.get("outcomes", "[]"))
    except Exception:
        outcomes_list = []
    idx            = token_ids.index(asset_id) if asset_id in token_ids else 0
    paired_idx     = 1 - idx if len(token_ids) >= 2 else idx
    outcome        = outcomes_list[idx]        if idx        < len(outcomes_list) else ""
    paired_outcome = outcomes_list[paired_idx] if paired_idx < len(outcomes_list) else ""
    paired_id      = token_ids[paired_idx]     if paired_idx < len(token_ids)     else ""

    return {
        "asset_id":          asset_id,
        "outcome":           outcome,
        "paired_asset_id":   paired_id,
        "paired_outcome":    paired_outcome,
        "event_slug":        event.get("slug", ""),
        "event_title":       event.get("title", ""),
        "market_question":   market.get("question", ""),
        "outcomes":          market.get("outcomes", "[]"),
        "min_tick_size":     float(market.get("orderPriceMinTickSize", 0.001)),
        "min_order_size":    int(market.get("orderMinSize", 5)),
        "is_neg_risk":       bool(market.get("negRisk", False)),
        "lastTradePrice":    float(market.get("lastTradePrice", 0.0)),
        "spread":            float(market.get("spread", 0.0)),
        "is_active":         bool(market.get("active", False)),
        "game_start_time":   gst,
        "bids":              {},
        "asks":              {},
        "volume":            float(market.get("volume", 0.0)),
        "volume_24hr":       float(market.get("volume24hr", 0.0)),
        "liquidity":         float(market.get("liquidity", 0.0)),
        "image_url":         market.get("image", ""),
        "resolution_source": market.get("resolutionSource", ""),
        "end_date":          market.get("endDate", ""),
    }


def create_orderbooks(events: list[Event]) -> dict[str, Orderbook]:
    books: dict[str, Orderbook] = {}
    for event in events:
        for market in event.get("markets", []):
            aid = parse_asset_id(market)
            books[aid] = create_orderbook_skeleton(event, market, aid)
    return books


# ── Orderbook mutation ────────────────────────────────────────────────────────

def hydrate_orderbook(book: Orderbook, event: dict) -> None:
    """Replace bids/asks with a full snapshot from a book or list event."""
    book["bids"] = {lvl["price"]: float(lvl["size"]) for lvl in event.get("bids", [])}
    book["asks"] = {lvl["price"]: float(lvl["size"]) for lvl in event.get("asks", [])}
    ltp = event.get("last_trade_price", "0")
    if ltp and ltp != "0":
        book["lastTradePrice"] = float(ltp)
    ts = event.get("timestamp")
    if ts:
        book["last_update"] = datetime.datetime.fromtimestamp(
            float(ts) / 1000.0, tz=datetime.timezone.utc
        )


def apply_price_change(book: Orderbook, change: dict) -> None:
    """Apply a single incremental level update from a price_change event."""
    price = change["price"]
    size  = float(change["size"])
    side  = change.get("side", "").upper()
    if side == "BUY":
        if size == 0:
            book["bids"].pop(price, None)
        else:
            book["bids"][price] = size
    elif side == "SELL":
        if size == 0:
            book["asks"].pop(price, None)
        else:
            book["asks"][price] = size


def apply_tick_size_change(book: Orderbook, new_tick: float) -> None:
    """Update a book's min_tick_size in place (called on tick_size_change events
    and when a price_change contains a 3-decimal price on a 2-decimal market)."""
    book["min_tick_size"] = new_tick


def has_sub_cent_price(price_str: str) -> bool:
    """Return True if price_str has 3 or more significant decimal places (e.g. '0.131').
    Uses only index arithmetic — no string allocation — so it's safe in the hot WS path.
    Polymarket does not emit trailing zeros, so '0.131' and '0.001' both return True,
    while '0.13' and '0.10' return False.
    """
    dot = price_str.find(".")
    return dot >= 0 and (len(price_str) - dot - 1) >= 3


# ── Derived book state ────────────────────────────────────────────────────────

def best_prices_from_book(book: Orderbook) -> tuple[float, float]:
    bids = book.get("bids", {})
    asks = book.get("asks", {})
    best_bid = max((float(p) for p, sz in bids.items() if sz > 0), default=0.0)
    best_ask = min((float(p) for p, sz in asks.items() if sz > 0), default=1.0)
    return best_bid, best_ask


def should_unsubscribe(best_bid: float, best_ask: float) -> bool:
    """True when a market has resolved or gone edge-priced (no longer tradeable)."""
    return best_bid >= 0.999 or best_ask <= 0.001


# ── WebSocket message builders ────────────────────────────────────────────────

def ws_initial_subscribe(asset_ids: list[str]) -> str:
    return json.dumps({"type": "market", "assets_ids": asset_ids, "initial_dump": True})


def ws_subscribe_more(asset_ids: list[str]) -> str:
    return json.dumps({
        "assets_ids": asset_ids,
        "operation": "subscribe",
        "custom_feature_enabled": False,
    })


def ws_unsubscribe(asset_ids: list[str]) -> str:
    return json.dumps({"operation": "unsubscribe", "assets_ids": asset_ids})
