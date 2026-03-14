##TO DO 
#get fetch_and_filter_gamma_events to re-run and attempt to subscribe to new assets not in asset_ids
#Get display book (probably another file based on the objects this script creates)
#Get 

import asyncio
import json
import websockets
from websockets.client import ClientConnection
from typing import cast
import datetime
import time
from zoneinfo import ZoneInfo

from models import ( WSPayload,Event, Market, Orderbook,BookEvent, PriceChangeEvent,LastTradePriceEvent,AssetUpdate)
from fetch_and_filter_gamma_events import fetch_and_filter_gamma_events
from history_logger_updated import HistoryLogger, Snapshot, MarketMetadata


logger_service: HistoryLogger = HistoryLogger(export_dir="./market_data")

WS_PERFORMANCE_CHECKER_S = 5
EVENT_REFRESH_SECONDS = 600
SNAPSHOT_COALESCE_S = 0.5
WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
inactive_assets: set[str] = set()

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

def receive_ws_message(message: any):
    raw_str: str = cast(str,message)
    try: ws_event: object = json.loads(raw_str)
    except json.JSONDecodeError as e:
        print(f'\nCRASH DETECTED: [{e}] | RAW PAYLOAD: {repr(raw_str)}\n')

def initial_subscribe_for_assets(asset_ids: list[str]) -> str:
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


def convert_polymarket_gamestarttime(time_str: str) -> float:
    if not time_str:
        raise ValueError("time_str must be provided")
    parsed_dt: datetime.datetime = datetime.datetime.fromisoformat(time_str)
    return parsed_dt.timestamp()

def parse_asset_id(market: Market) -> str:
    raw_ids: str = market["clobTokenIds"]
    return raw_ids.strip('[]"').partition('",')[0]

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

def create_orderbooks(events: list[Event]) -> dict[str, Orderbook]:
    books: dict[str, Orderbook] = {}
    for event in events:
        for market in event["markets"]:
            asset_id: str = parse_asset_id(market)
            books[asset_id] = create_orderbook_skeleton(event, market, asset_id)
    return books

def market_metadata_from_book(book: Orderbook) -> MarketMetadata:
    return MarketMetadata(
        asset_id=str(book.get("asset_id", "")),
        event_slug=str(book.get("event_slug", "unknown_event")),
        event_title=str(book.get("event_title", "unknown_title")),
        market_question=str(book.get("market_question", "unknown_question")),
        outcomes=str(book.get("outcomes", "[]")),
        min_tick_size=float(book.get("min_tick_size", 0.01)),
        min_order_size=int(book.get("min_order_size", 5)),
        is_neg_risk=bool(book.get("is_neg_risk", False)),
        game_start_time=float(book.get("game_start_time", 1000.0)),
    )

def ensure_logger_registered(asset_id: str) -> None:
    if asset_id not in logger_service._history:
        logger_service.register_asset(asset_id)

def maybe_log_snapshot(
    asset_id: str,
    ts_s: float,
    best_bid: float,
    best_ask: float,
    last_snapshot_ts: dict[str, float],
) -> None:
    last_ts: float = last_snapshot_ts.get(asset_id, -1e18)
    if ts_s - last_ts < SNAPSHOT_COALESCE_S:
        return

    ensure_logger_registered(asset_id)
    logger_service.log_snapshot(asset_id, Snapshot(timestamp=ts_s, best_bid=best_bid, best_ask=best_ask))
    last_snapshot_ts[asset_id] = ts_s

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

def best_prices_from_book(book: Orderbook) -> tuple[float, float]:
    bids = cast(dict[str, float], book.get("bids", {}))
    asks = cast(dict[str, float], book.get("asks", {}))
    best_bid: float = max((float(p) for p, sz in bids.items() if sz > 0), default=0.0)
    best_ask: float = min((float(p) for p, sz in asks.items() if sz > 0), default=1.0)
    return best_bid, best_ask

def should_unsubscribe(best_bid: float, best_ask: float) -> bool:
    return best_bid >= 0.999 or best_ask <= 0.001

async def finalize_asset(ws: ClientConnection, asset_id: str, orderbooks: dict[str, Orderbook]) -> None:
    if asset_id in inactive_assets:
        return
    book: Orderbook | None = orderbooks.get(asset_id)
    if book is None:
        inactive_assets.add(asset_id)
        return
    await ws.send(get_unsubscribe_msg([asset_id]))
    inactive_assets.add(asset_id)
    meta: MarketMetadata = market_metadata_from_book(book)
    asyncio.create_task(asyncio.to_thread(logger_service.export_and_cleanup, asset_id, meta))
    orderbooks.pop(asset_id, None)
    print(f"Unsubscribed from {meta.market_question} | orderbooks now: {len(orderbooks)}")

async def refresh_events_loop(ws: ClientConnection, orderbooks: dict[str, Orderbook]) -> None:
    while True:
        await asyncio.sleep(EVENT_REFRESH_SECONDS)
        print_gamma_refresh()
        new_events: list[Event] = await asyncio.to_thread(fetch_and_filter_gamma_events)
        new_asset_ids: list[str] = []
        for event in new_events:
            for market in event["markets"]:
                asset_id: str = parse_asset_id(market)
                if asset_id in inactive_assets or asset_id in orderbooks:
                    continue
                orderbooks[asset_id] = create_orderbook_skeleton(event, market, asset_id)
                new_asset_ids.append(asset_id)
        if new_asset_ids:
            print(f"Subscribing to {len(new_asset_ids)} new assets")
            await ws.send(subscribe_more_assets(new_asset_ids))

def process_book_message(ws_event: BookEvent, orderbooks: list[Orderbook], last_snapshot_ts):
    book_event: BookEvent = cast(BookEvent, ws_event)
    asset_id = book_event["asset_id"]
    if asset_id in orderbooks:
        hydrate_orderbook(orderbooks[asset_id], book_event)
        ts_s = float(book_event["timestamp"]) / 1000.0
        best_bid, best_ask = best_prices_from_book(orderbooks[asset_id])
        maybe_log_snapshot(asset_id, ts_s, best_bid, best_ask, last_snapshot_ts)

async def start_ws(orderbooks: dict[str, Orderbook]) -> None:
    unhandled_events: dict[str, int] = {}
    last_snapshot_ts: dict[str, float] = {}
    try:
        while True:
            refresh_task: asyncio.Task[None] | None = None
            try:
                async with websockets.connect(WS_URL, ping_interval=10, ping_timeout=10) as ws:
                    await ws.send(initial_subscribe_for_assets(list(orderbooks)))
                    print(f"Connected and subscribed to {len(orderbooks)} assets. Listening...")
                    refresh_task = asyncio.create_task(refresh_events_loop(ws, orderbooks))

                    msg_count = 0
                    max_ms = 0.0
                    total_ms = 0.0
                    last_print = time.perf_counter()

                    async for message in ws:
                        receive_ws_message(message)
                        start = time.perf_counter()
                        ws_event: object = json.loads(message)
                        if isinstance(ws_event, list):
                            initial_books: list[BookEvent] = cast(list[BookEvent], ws_event)
                            for book_event in initial_books:
                                asset_id: str = book_event["asset_id"]
                                hydrate_orderbook(orderbooks[asset_id], book_event)

                                ts_s: float = float(book_event["timestamp"]) / 1000.0
                                best_bid, best_ask = best_prices_from_book(orderbooks[asset_id])
                                maybe_log_snapshot(asset_id, ts_s, best_bid, best_ask, last_snapshot_ts)
                            continue

                        event_type: str = str(ws_event.get("event_type"))
                        if event_type == "book":
                            process_book_message(ws_event, orderbooks, last_snapshot_ts)
                        elif event_type == "price_change":
                            price_event: PriceChangeEvent = cast(PriceChangeEvent, ws_event)
                            ts_s = float(price_event["timestamp"]) / 1000.0
                            for upd in price_event["price_changes"]:
                                asset_id = upd["asset_id"]
                                if asset_id not in orderbooks:
                                    continue
                                best_bid = float(upd["best_bid"])
                                best_ask = float(upd["best_ask"])
                                maybe_log_snapshot(asset_id, ts_s, best_bid, best_ask, last_snapshot_ts)
                                if should_unsubscribe(best_bid, best_ask):
                                    await finalize_asset(ws, asset_id, orderbooks)

                        elif event_type == "last_trade_price":
                            _trade_event: LastTradePriceEvent = cast(LastTradePriceEvent, ws_event)
                            pass
                        elif event_type in {"tick_size_change", "new_market", "best_bid_ask"}: pass
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
                            print( f"Processing avg: {avg:.3f} ms | max: {max_ms:.3f} ms | msgs: {msg_count} | orderbooks: {len(orderbooks)}")
                            msg_count = 0
                            total_ms = 0.0
                            max_ms = 0.0
                            last_print = now

                        await asyncio.sleep(0)

            except websockets.exceptions.ConnectionClosed as e:
                print(f"Websocket disconnected. Reconnecting in 3s... error: {e}")
                await asyncio.sleep(3)
            finally:
                if refresh_task is not None:
                    refresh_task.cancel()
                    try:
                        await refresh_task
                    except asyncio.CancelledError:
                        pass
                    except websockets.exceptions.ConnectionClosed:
                        pass
    finally:
        print(f"\nInitiating shutdown sequence at: Exporting all active markets to Parquet...")
        for asset_id, book_data in list(orderbooks.items()):
            meta: MarketMetadata = market_metadata_from_book(book_data)
            await asyncio.to_thread(logger_service.export_and_cleanup, asset_id, meta)
        print("Parquet exports complete.")
        try:
            with open("orderbook_dict.txt", "w", encoding="utf-8") as f:
                json.dump(orderbooks,f,ensure_ascii=False,indent=2,default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else str(x))
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
    print(f"Successfully generated {len(orderbooks)} orderbook skeletons.")
    await start_ws(orderbooks)

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        print("\nCtrl+C detected. Shutting down cleanly...")
