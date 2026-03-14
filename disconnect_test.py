#This file serves as a test to see the cadence of disconnects without the more 
#complex loops that refresh gamma events on a timer.

import asyncio
import json
import websockets
from websockets.client import ClientConnection
from typing import cast
import datetime
import time
from zoneinfo import ZoneInfo

from models import ( WSPayload, Event, Market, Orderbook,BookEvent, PriceChangeEvent,LastTradePriceEvent,AssetUpdate)
from fetch_and_filter_gamma_events import fetch_and_filter_gamma_events
from history_logger_updated import HistoryLogger, Snapshot, MarketMetadata

WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_PERFORMANCE_CHECKER_S = 30.0

def parse_asset_id(market: Market) -> str:
    raw_ids: str = market["clobTokenIds"]
    return raw_ids.strip('[]"').partition('",')[0]

def create_orderbooks(events: list[Event]) -> dict[str, Orderbook]:
    books: dict[str, Orderbook] = {}
    for event in events:
        for market in event["markets"]:
            asset_id: str = parse_asset_id(market)
            books[asset_id] = create_orderbook_skeleton(event, market, asset_id)
    return books

async def log_unhandled_event(event: object) -> None:
    def write_file() -> None:
        with open("unhandled_events.txt", "a", encoding="utf-8") as f:
            json.dump(event, f, indent=2)
            f.write("\n")
    await asyncio.to_thread(write_file)

def print_gamma_refresh() -> None:
    now: datetime = datetime.datetime.now().astimezone()
    timestamp: str = now.strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"\n[{timestamp}] Refreshing gamma events...")

def get_subscribe_msg(asset_ids: list[str]) -> str:
    payload: WSPayload = {"type": "market", "assets_ids": asset_ids, "initial_dump": True}
    print(json.dumps(payload))
    return json.dumps(payload)

def subscribe_more_assets(asset_ids: list[str]) -> str:
    payload: WSPayload = {"assets_ids": asset_ids, "operation": "subscribe", "custom_feature_enabled": False}
    print(json.dumps(payload))
    return json.dumps(payload)

def get_unsubscribe_msg(asset_ids: list[str]) -> str:
    payload: WSPayload = {"operation": "unsubscribe", "assets_ids": asset_ids}
    print(json.dumps(payload))
    return json.dumps(payload)

def hydrate_orderbook(target_book: Orderbook, event: BookEvent) -> None:
    target_book["bids"] = {lvl["price"]: float(lvl["size"]) for lvl in event.get("bids", [])}
    target_book["asks"] = {lvl["price"]: float(lvl["size"]) for lvl in event.get("asks", [])}

    last_price_str: str = event.get("last_trade_price", "0")
    if last_price_str and last_price_str != "0":
        target_book["last_price"] = float(last_price_str)

    ts_val = event.get("timestamp")
    if ts_val:
        event_ms: float = float(ts_val)
        target_book["last_update"] = datetime.datetime.fromtimestamp(event_ms / 1000.0, tz=datetime.timezone.utc)

def create_orderbook_skeleton(event: Event, market: Market, asset_id: str) -> Orderbook:
    return {
        "asset_id": asset_id,
        "event_slug": event["slug"],
        "event_title": event["title"],
        "market_question": market["question"],
        "outcomes": market["outcomes"],
        "min_tick_size": float(market["orderPriceMinTickSize"]),
        "min_order_size": int(market["orderMinSize"]),
        "is_neg_risk": bool(market["negRisk"]),
        "lastTradePrice": market["lastTradePrice"],
        "spread": float(market["spread"]),
        "is_active": bool(market["active"]),
        "game_start_time": convert_polymarket_gamestarttime(market["gameStartTime"]),
        "bids": {},
        "asks": {},
    }

def best_prices_from_book(book: Orderbook) -> tuple[float, float]:
    bids = cast(dict[str, float], book.get("bids", {}))
    asks = cast(dict[str, float], book.get("asks", {}))
    best_bid: float = max((float(p) for p, sz in bids.items() if sz > 0), default=0.0)
    best_ask: float = min((float(p) for p, sz in asks.items() if sz > 0), default=1.0)
    return best_bid, best_ask

def convert_polymarket_gamestarttime(time_str: str) -> float:
    if not time_str:
        raise ValueError("time_str must be provided")
    parsed_dt: datetime.datetime = datetime.datetime.fromisoformat(time_str)
    return parsed_dt.timestamp()


    
async def handle_ws(orderbooks: dict[str, Orderbook]) -> None:
    unhandled_events: dict[str, int] = {}
    last_snapshot_ts: dict[str, float] = {}

    PERFORMANCE_CHECK_COUNT = 0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=10, ping_timeout=10) as ws:
                await ws.send(get_subscribe_msg(list(orderbooks)))
                print(f"Connected and subscribed to {len(orderbooks)} assets. Listening...")

                msg_count = 0
                max_ms = 0.0
                total_ms = 0.0
                last_print = time.perf_counter()

                async for message in ws:
                    start = time.perf_counter()
                    ws_event: object = json.loads(message)

                    if isinstance(ws_event, list):
                        initial_books: list[BookEvent] = cast(list[BookEvent], ws_event)
                        for book_event in initial_books:
                            asset_id: str = book_event["asset_id"]
                            if asset_id not in orderbooks:
                                continue
                            hydrate_orderbook(orderbooks[asset_id], book_event)

                            ts_s: float = float(book_event["timestamp"]) / 1000.0
                            best_bid, best_ask = best_prices_from_book(orderbooks[asset_id])
                        continue

                    event_type: str = str(ws_event.get("event_type"))

                    if event_type == "book":
                        book_event: BookEvent = cast(BookEvent, ws_event)
                        asset_id = book_event["asset_id"]
                        if asset_id in orderbooks:
                            hydrate_orderbook(orderbooks[asset_id], book_event)
                            ts_s = float(book_event["timestamp"]) / 1000.0
                            best_bid, best_ask = best_prices_from_book(orderbooks[asset_id])

                    elif event_type == "price_change":
                        price_event: PriceChangeEvent = cast(PriceChangeEvent, ws_event)
                        ts_s = float(price_event["timestamp"]) / 1000.0

                        for upd in price_event["price_changes"]:
                            asset_id = upd["asset_id"]
                            if asset_id not in orderbooks:
                                continue

                            best_bid = float(upd["best_bid"])
                            best_ask = float(upd["best_ask"])

                    elif event_type == "last_trade_price":
                        _trade_event: LastTradePriceEvent = cast(LastTradePriceEvent, ws_event)
                        pass
                    elif event_type in {"tick_size_change", "new_market", "best_bid_ask"}:
                        pass
                    else:
                        unhandled_events[event_type] = unhandled_events.get(event_type, 0) + 1
                        asyncio.create_task(log_unhandled_event(ws_event))

                    elapsed = (time.perf_counter() - start) * 1000.0
                    msg_count += 1
                    total_ms += elapsed
                    max_ms = max(max_ms, elapsed)

                    now = time.perf_counter()
                    if now - last_print > WS_PERFORMANCE_CHECKER_S:
                        avg = total_ms / msg_count if msg_count else 0.0
                        PERFORMANCE_CHECK_COUNT += 1
                        print(
                            f"Processing avg: {avg:.3f} ms | max: {max_ms:.3f} ms | "
                            f"msgs: {msg_count} | orderbooks: {len(orderbooks)} | [COUNT: {PERFORMANCE_CHECK_COUNT}]"
                        )
                        msg_count = 0
                        total_ms = 0.0
                        max_ms = 0.0
                        last_print = now

                    await asyncio.sleep(0)

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Websocket disconnected. Reconnecting in 3s... error: {e}")
            await asyncio.sleep(3)

        finally:
            # print(f"\nInitiating shutdown sequence at: Exporting all active markets to Parquet...")
            # for asset_id, book_data in list(orderbooks.items()):
            #     meta: MarketMetadata = market_metadata_from_book(book_data)
            #     logger_service.export_and_cleanup(asset_id, meta)
            # print("Parquet exports complete.")
            try:
                with open("orderbook_dict.txt", "w", encoding="utf-8") as f:
                    json.dump(
                        orderbooks,
                        f,
                        ensure_ascii=False,
                        indent=2,
                        default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else str(x),
                    )
                print("Orderbook state saved to orderbook_dict.txt")
            except Exception as e:
                print(f"Failed to save orderbook state: {e}")

            print("\n--- Unhandled Event Summary ---")
            if not unhandled_events:
                print("No unhandled events received.")
            else:
                for ev_type, count in unhandled_events.items():
                    print(f"{ev_type}: {count}")

async def main() -> None:
    events: list[Event] = fetch_and_filter_gamma_events()
    orderbooks: dict[str, Orderbook] = create_orderbooks(events)
    await handle_ws(orderbooks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down cleanly...")