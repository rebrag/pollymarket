##TO DO 
#get fetch_and_filter_gamma_events to re-run and attempt to subscribe to new assets not in asset_ids
#Get display book (probably another file based on the objects this script creates)
#Get 

import asyncio
import json
import websockets
from websockets.client import ClientConnection
from typing import TypedDict, cast
import datetime
from models import WSPayload, Event, Market, Orderbook, BookEvent, PriceChangeEvent, LastTradePriceEvent, TickSizeChangeEvent, AssetUpdate
from fetch_and_filter_gamma_events import fetch_and_filter_gamma_events
import time

from deprecated.history_logger import HistoryLogger, Snapshot, MarketMetadata

logger_service: HistoryLogger = HistoryLogger(export_dir="./market_data")

PARQUET_INTERVAL_S = 1.0
WS_PERFORMANCE_CHECKER_S = 5
EVENT_REFRESH_SECONDS = 600
logging_enabled = True
WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

inactive_assets: set[str] = set()

async def log_unhandled_event(event: object) -> None:
    def write_file() -> None:
        with open("unhandled_events.txt", "a", encoding="utf-8") as f:
            json.dump(event, f, indent=2)
            f.write("\n")
    await asyncio.to_thread(write_file)

def get_subscribe_msg(asset_ids: list[str]) -> str:
    payload: WSPayload = { "type": "market","assets_ids": asset_ids,"initial_dump": True}
    print(json.dumps(payload))
    return json.dumps(payload)

def subscribe_more_assets(asset_ids: list[str]) -> str:
    payload: WSPayload = {"assets_ids": asset_ids, "operation": "subscribe", "custom_feature_enabled": False}
    print(json.dumps(payload))
    return json.dumps(payload)

def get_unsubscribe_msg(asset_ids: list[str]) -> str:
    payload: WSPayload = {"operation": "unsubscribe","assets_ids": asset_ids}
    print(json.dumps(payload))
    return json.dumps(payload)

def convert_polymarket_gamestarttime(time_str: str) -> float:
    if not time_str:
        raise ValueError("time_str must be provided")
    parsed_dt: datetime.datetime = datetime.datetime.fromisoformat(time_str)
    return parsed_dt.timestamp()

def create_orderbooks(events: list[Event]) -> dict[str, Orderbook]:
    books: dict[str, Orderbook] = {}
    for event in events:
        for market in event['markets']:
            raw_ids: str = market['clobTokenIds'] #this string has both tokens with some delimiters
            asset_id: str = raw_ids.strip('[]"').partition('",')[0] #this gets the first of the two tokens
            books[asset_id] = {
                "asset_id": asset_id,
                "event_slug": event['slug'],
                "event_title": event['title'],
                "market_question": market['question'],
                "outcomes": market['outcomes'],
                "min_tick_size": float(market['orderPriceMinTickSize']),
                "min_order_size": int(market['orderMinSize']),
                "is_neg_risk": bool(market['negRisk']),
                "lastTradePrice": market['lastTradePrice'],
                "spread": float(market['spread']),
                "is_active": bool(market['active']),
                "game_start_time": convert_polymarket_gamestarttime(market['gameStartTime']),
                "bids": {},
                "asks": {},
            }
    return books

async def periodic_snapshot_loop(orderbooks: dict[str, dict], interval_s: float) -> None:
    while True:
        await asyncio.sleep(interval_s)
        current_time: float = time.time()
        
        for asset_id, book in orderbooks.items():
            try:
                if asset_id not in logger_service._history:
                    logger_service.register_asset(asset_id)
            except ValueError:
                continue
            
            bids: dict[float, float] = book.get("bids", {})
            asks: dict[float, float] = book.get("asks", {})
            
            best_bid: float = max((float(k) for k in bids.keys()), default=0.0)
            best_ask: float = min((float(k) for k in asks.keys()), default=1.0)
            
            snapshot: Snapshot = Snapshot(
                timestamp=current_time,
                best_bid=best_bid,
                best_ask=best_ask
            )
            logger_service.log_snapshot(asset_id, snapshot)

def hydrate_orderbook(target_book: Orderbook, event: BookEvent) -> None:
    target_book["bids"] = {
        level["price"]: float(level["size"]) 
        for level in event.get("bids", [])
    }
    target_book["asks"] = {
        level["price"]: float(level["size"]) 
        for level in event.get("asks", [])
    }
    last_price_str: str = event.get("last_trade_price", "0")
    if last_price_str and last_price_str != "0":
        target_book["last_price"] = float(last_price_str)
    ts_val = event.get("timestamp")
    if ts_val:
        event_ms: float = float(ts_val)
        target_book["last_update"] = datetime.datetime.fromtimestamp(
            event_ms / 1000.0,
            tz=datetime.timezone.utc
        )
        now_ms: float = datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000.0
        latency: float = now_ms - event_ms
        # if logging_enabled == True:
        #     print(f"[{target_book['asset_id'][:8]}...] Synced Book | Latency: {latency:.2f}ms")

async def refresh_events_loop(ws: ClientConnection, orderbooks: dict[str, Orderbook], asset_ids: list[str]) -> None:
    while True:
        await asyncio.sleep(EVENT_REFRESH_SECONDS)
        print("\nRefreshing gamma events...")
        new_events: list[Event] = await asyncio.to_thread(fetch_and_filter_gamma_events)
        new_asset_ids: list[str] = []
        for event in new_events:
            for market in event["markets"]:
                raw_ids: str = market["clobTokenIds"]
                asset_id_one: str = raw_ids.strip('[]"').partition('",')[0]

                # 1. Ensure orderbook exists
                if asset_id_one not in orderbooks:
                    orderbooks[asset_id_one] = {
                        "asset_id": asset_id_one,
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
                        "game_start_time": convert_polymarket_gamestarttime(market['gameStartTime']),
                        "bids": {},
                        "asks": {},
                    }

                # 2. Subscribe if not already subscribed --AND NOT IN INACTIVE ASSETS
                if asset_id_one not in asset_ids:
                    if asset_id_one not in inactive_assets:
                        new_asset_ids.append(asset_id_one)
                    else:
                        print(f'{asset_id_one} not in asset_ids but is in inactive_assets')

        # 3. Send subscription request
        if new_asset_ids:
            print(f"Subscribing to {len(new_asset_ids)} new assets")
            await ws.send(subscribe_more_assets(new_asset_ids))
            asset_ids.extend(new_asset_ids)

async def handle_ws(orderbooks: dict[str, Orderbook]) -> None:
    asset_ids: list[str] = list(orderbooks.keys())
    if not asset_ids:
        raise ValueError("Cannot start websocket connection with an empty orderbooks dictionary.")
    unhandled_events: dict[str, int] = {}
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=10, ping_timeout=10) as ws:
                await ws.send(get_subscribe_msg(asset_ids))
                print(f"Connected and subscribed to {len(asset_ids)} assets. Listening...")
                asyncio.create_task(refresh_events_loop(ws,orderbooks,asset_ids))

                asyncio.create_task(periodic_snapshot_loop(orderbooks, PARQUET_INTERVAL_S))

                msg_count = 0
                max_ms = 0.0
                total_ms = 0.0
                last_print = time.perf_counter()
                unhandled_event_types = set()

                async for message in ws:
                    start = time.perf_counter()
                    try:
                        ws_event: object = json.loads(message)
                    except json.JSONDecodeError as e:
                        print("\n" + "!" * 30)
                        print(f"CRASH DETECTED: {e}")
                        print(f"RAW PAYLOAD: {repr(message)}")
                        print("!" * 30 + "\n")
                        continue
                    
                    if isinstance(ws_event, list):
                        initial_book_list: list[BookEvent] = cast(list[BookEvent], ws_event)
                        
                        for book_event in initial_book_list:
                            asset_id: str = book_event["asset_id"]
                            if asset_id in orderbooks:
                                hydrate_orderbook(orderbooks[asset_id], book_event)
                        continue
                    event_type: str = str(ws_event["event_type"])
                    
                    if event_type == "book":
                        book_event: BookEvent = cast(BookEvent, ws_event)
                        asset_id: str = book_event["asset_id"]
                        if asset_id in orderbooks:
                            hydrate_orderbook(orderbooks[asset_id], book_event)

                    elif event_type == "price_change":
                        price_event: PriceChangeEvent = cast(PriceChangeEvent, ws_event)
                        price_changes: list[AssetUpdate] = price_event["price_changes"]
                        # if price_changes[0]['best_bid'] == '0.99' or price_changes[0]['best_ask'] == '0.01':
                        #     print(json.dumps(price_event, indent=2))
                        if price_changes[0]['best_bid'] == '0.999' or price_changes[0]['best_ask'] == '0.001':
                            # print(price_changes)
                            if price_changes[0]["asset_id"] in asset_ids:
                                await ws.send(get_unsubscribe_msg([price_changes[0]["asset_id"]]))
                                asset_ids.remove(price_changes[0]["asset_id"])
                                inactive_assets.add(price_changes[0]["asset_id"])

                                print(f'Unsubscribed from {orderbooks[price_changes[0]["asset_id"]]['market_question']} and now len(asset_ids) = {len(asset_ids)}')
                                print(f'{price_changes[0]["asset_id"]} added to inactive_asets')

                                if price_changes[0]["asset_id"] in orderbooks:
                                    book_data: dict = orderbooks[price_changes[0]["asset_id"]]
                                    event_slug: str = str(book_data.get("event_slug", "unknown_event"))
                                    market_question: str = str(book_data.get("market_question", "unknown_question"))
                                    meta: MarketMetadata = MarketMetadata(
                                            asset_id=str(book_data.get("asset_id", price_changes[0]["asset_id"])),
                                            event_slug=str(book_data.get("event_slug", "unknown_event")),
                                            event_title=str(book_data.get("event_title", "unknown_title")),
                                            market_question=str(book_data.get("market_question", "unknown_question")),
                                            outcomes=str(book_data.get("outcomes", "[]")),
                                            min_tick_size=float(book_data.get("min_tick_size", 0.01)),
                                            min_order_size=int(book_data.get("min_order_size", 5)),
                                            is_neg_risk=bool(book_data.get("is_neg_risk", False)),
                                            game_start_time=float(book_data.get("game_start_time"))
                                        )
                                    asyncio.create_task(asyncio.to_thread(logger_service.export_and_cleanup, price_changes[0]["asset_id"], meta))
                                    orderbooks.pop(price_changes[0]["asset_id"], None)

                            if price_changes[1]["asset_id"] in asset_ids:
                                await ws.send(get_unsubscribe_msg([price_changes[1]["asset_id"]]))
                                asset_ids.remove(price_changes[1]["asset_id"])
                                inactive_assets.add(price_changes[1]["asset_id"])
                                print(f'Unsubscribed from {orderbooks[price_changes[1]["asset_id"]]['market_question']} and now len(asset_ids) = {len(asset_ids)}')
                                print(f'{price_changes[1]["asset_id"]} added to inactive_asets')

                                if price_changes[1]["asset_id"] in orderbooks:
                                    book_data: dict = orderbooks[price_changes[1]["asset_id"]]
                                    event_slug: str = str(book_data.get("event_slug", "unknown_event"))
                                    market_question: str = str(book_data.get("market_question", "unknown_question"))

                                    meta: MarketMetadata = MarketMetadata(
                                            asset_id=str(book_data.get("asset_id", price_changes[1]["asset_id"])),
                                            event_slug=str(book_data.get("event_slug", "unknown_event")),
                                            event_title=str(book_data.get("event_title", "unknown_title")),
                                            market_question=str(book_data.get("market_question", "unknown_question")),
                                            outcomes=str(book_data.get("outcomes", "[]")),
                                            min_tick_size=float(book_data.get("min_tick_size", 0.01)),
                                            min_order_size=int(book_data.get("min_order_size", 5)),
                                            is_neg_risk=bool(book_data.get("is_neg_risk", False)),
                                            game_start_time=float(book_data.get("game_start_time", 1000.0))
                                        )
                                    asyncio.create_task(asyncio.to_thread(logger_service.export_and_cleanup, price_changes[1]["asset_id"], meta))
                                    orderbooks.pop(price_changes[1]["asset_id"], None)

                        pass

                    elif event_type == "last_trade_price":
                        trade_event: LastTradePriceEvent = cast(LastTradePriceEvent, ws_event)
                        pass
                    elif event_type == 'tick_size_change':
                        pass
                    elif event_type == 'new_market': #not intending to do anything with this event type yet
                        pass
                    elif event_type == 'best_bid_ask': #not intending to do anything with this event type yet
                        pass
                    else:
                        unhandled_events[event_type] = unhandled_events.get(event_type, 0) + 1
                        asyncio.create_task(log_unhandled_event(ws_event))

                    elapsed = (time.perf_counter() - start) * 1000
                    msg_count += 1
                    total_ms += elapsed
                    max_ms = max(max_ms, elapsed)

                    now = time.perf_counter()

                    if now - last_print > WS_PERFORMANCE_CHECKER_S:
                        avg = total_ms / msg_count if msg_count else 0
                        print(f"Processing avg: {avg:.3f} ms | max: {max_ms:.3f} ms | msgs: {msg_count} | orderbooks: {len(orderbooks)}")

                        msg_count = 0
                        total_ms = 0.0
                        max_ms = 0.0
                        last_print = now  
                        
                    await asyncio.sleep(0)  

        except websockets.exceptions.ConnectionClosedError as e:
            print(f'Websocket disconnected. Reconnecting in 3s... error: {e}')
            await asyncio.sleep(3)

        finally:
            print("\nInitiating shutdown sequence: Exporting all active markets to Parquet...")
            for asset_id, book_data in orderbooks.items():
                meta: MarketMetadata = MarketMetadata(
                    asset_id=str(book_data.get("asset_id", asset_id)),
                    event_slug=str(book_data.get("event_slug", "unknown_event")),
                    event_title=str(book_data.get("event_title", "unknown_title")),
                    market_question=str(book_data.get("market_question", "unknown_question")),
                    outcomes=str(book_data.get("outcomes", "[]")),
                    min_tick_size=float(book_data.get("min_tick_size", 0.01)),
                    min_order_size=int(book_data.get("min_order_size", 5)),
                    is_neg_risk=bool(book_data.get("is_neg_risk", False)),
                    game_start_time=float(book_data.get("game_start_time", 1000.0))
                )
                logger_service.export_and_cleanup(asset_id, meta)
            
            print(f"Parquet exports complete.")

            try:
                with open("orderbook_dict.txt", "w", encoding="utf-8") as f:
                    json.dump(
                        orderbooks, 
                        f, 
                        ensure_ascii=False, 
                        indent=2, 
                        default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else str(x)
                    )
                print(f"Orderbook state saved to orderbook_dict.txt")
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
    if not orderbooks:
        raise RuntimeError("No orderbook skeletons generated. Halting.")
    print(f"Successfully generated {len(orderbooks)} orderbook skeletons.")
    await handle_ws(orderbooks)
    

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Shutting down cleanly...")