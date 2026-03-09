import asyncio,json,websockets,datetime
from typing import cast
from models import WSPayload,Event,Orderbook,BookEvent,PriceChangeEvent,LastTradePriceEvent,TickSizeChangeEvent,AssetUpdate
from fetch_and_filter_gamma_events import fetch_and_filter_gamma_events
import time

logging_enabled=True
WS_URL:str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
EVENT_REFRESH_SECONDS:int = 60



def get_subscribe_msg(asset_ids:list[str])->str:
    payload:WSPayload={"type":"market","assets_ids":asset_ids,"initial_dump":True}
    return json.dumps(payload)

def get_unsubscribe_msg(asset_ids:list[str])->str:
    payload:WSPayload={"operation":"unsubscribe","assets_ids":asset_ids}
    return json.dumps(payload)

def create_orderbooks(events:list[Event])->dict[str,Orderbook]:
    books:dict[str,Orderbook]={}
    for event in events:
        for market in event["markets"]:
            raw_ids:str=market["clobTokenIds"]
            asset_id:str=raw_ids.strip('[]"').partition('",')[0]
            books[asset_id]={
                "asset_id":asset_id,
                "event_slug":event["slug"],
                "event_title":event["title"],
                "market_question":market["question"],
                "outcomes":market["outcomes"],
                "min_tick_size":float(market["orderPriceMinTickSize"]),
                "min_order_size":int(market["orderMinSize"]),
                "is_neg_risk":bool(market["negRisk"]),
                "bids":{},
                "asks":{},
                "lastTradePrice":market["lastTradePrice"],
                "spread":float(market["spread"]),
                "is_active":bool(market["active"])
            }
    return books

def hydrate_orderbook(target_book: Orderbook, event: BookEvent)->None:
    target_book["bids"]={lvl["price"]:float(lvl["size"]) for lvl in event.get("bids",[])}
    target_book["asks"]={lvl["price"]:float(lvl["size"]) for lvl in event.get("asks",[])}
    last_price_str:str=event.get("last_trade_price","0")
    if last_price_str and last_price_str!="0":target_book["last_price"]=float(last_price_str)
    ts_val=event.get("timestamp")
    if ts_val:
        event_ms: float = float(ts_val)
        target_book["last_update"] = datetime.datetime.fromtimestamp(event_ms/1000.0,tz=datetime.timezone.utc)
        now_ms:float=datetime.datetime.now(datetime.timezone.utc).timestamp()*1000.0
        latency:float=now_ms-event_ms
        # if logging_enabled:
        #     print(f"[{target_book['asset_id'][:8]}...] Synced Book | Latency: {latency:.2f}ms")

async def refresh_events_loop(ws, events: list[Event], event_ids: set[str], orderbooks: dict[str,Orderbook], asset_ids: set[str])->None:
    while True:
        await asyncio.sleep(EVENT_REFRESH_SECONDS)
        print("\nRefreshing gamma events...")
        new_events:list[Event]=fetch_and_filter_gamma_events()
        new_asset_ids:set[str]=set()
        for event in new_events:
            event_id:str=str(event.get("id"))
            if event_id in event_ids:continue
            event_ids.add(event_id);events.append(event)
            new_books=create_orderbooks([event])
            for asset_id,book in new_books.items():
                if asset_id not in orderbooks:
                    orderbooks[asset_id]=book
                    new_asset_ids.add(asset_id)
        if new_asset_ids:
            print(f"Subscribing to {len(new_asset_ids)} new assets")
            await ws.send(get_subscribe_msg(list(new_asset_ids)))
            asset_ids.update(new_asset_ids)

async def handle_ws(events:list[Event],event_ids:set[str],orderbooks:dict[str,Orderbook])->None:
    asset_ids:set[str]=set(orderbooks.keys())
    if not asset_ids:raise ValueError("Cannot start websocket connection with empty assets")
    unhandled_events: dict[str,int]={}
    try:
        async with websockets.connect(WS_URL) as ws:
            await ws.send(get_subscribe_msg(list(asset_ids)))
            print(f"Connected and subscribed to {len(asset_ids)} assets. Listening...")
            asyncio.create_task(refresh_events_loop(ws,events,event_ids,orderbooks,asset_ids))

            msg_count = 0
            max_ms = 0.0
            total_ms = 0.0
            last_print = time.perf_counter()

            async for message in ws:
                start = time.perf_counter()
                try:
                    ws_event: object = json.loads(message)
                except json.JSONDecodeError as e:
                    print("\n"+"!"*30);print(f"CRASH DETECTED: {e}")
                    print(f"RAW PAYLOAD: {repr(message)}")
                    print("!"*30+"\n")
                    continue
                if isinstance(ws_event,list):
                    initial_book_list: list[BookEvent] = cast(list[BookEvent],ws_event)
                    for book_event in initial_book_list:
                        asset_id:str = book_event["asset_id"]
                        if asset_id in orderbooks:
                            hydrate_orderbook(orderbooks[asset_id],book_event)
                    continue

                event_type: str = str(ws_event["event_type"])
                if event_type=="book":
                    book_event:BookEvent=cast(BookEvent,ws_event)
                    asset_id:str=book_event["asset_id"]
                    if asset_id in orderbooks:
                        hydrate_orderbook(orderbooks[asset_id],book_event)
                elif event_type=="price_change":
                    price_event: PriceChangeEvent = cast(PriceChangeEvent,ws_event)
                    price_changes: list[AssetUpdate] = price_event["price_changes"]
                    for change in price_changes:
                        asset_id: str = change["asset_id"]
                        best_bid: str = change["best_bid"]
                        best_ask: str = change["best_ask"]
                        if best_bid=="0.999" or best_ask=="0.001":
                            if asset_id in asset_ids:
                                await ws.send(get_unsubscribe_msg([asset_id]))
                                asset_ids.remove(asset_id)
                                print(f"Unsubscribed {asset_id[:8]}... | {len(asset_ids)} assets remaining")
                elif event_type=="last_trade_price":
                    trade_event:LastTradePriceEvent=cast(LastTradePriceEvent,ws_event);pass
                elif event_type=="tick_size_change":
                    print(json.dumps(ws_event,indent=2))
                else:
                    unhandled_events[event_type]=unhandled_events.get(event_type,0)+1

                elapsed = (time.perf_counter() - start) * 1000
                msg_count += 1
                total_ms += elapsed
                max_ms = max(max_ms, elapsed)

                now = time.perf_counter()

                if now - last_print > 5:
                    avg = total_ms / msg_count if msg_count else 0
                    print(f"Processing avg: {avg:.3f} ms | max: {max_ms:.3f} ms | msgs: {msg_count}")

                    msg_count = 0
                    total_ms = 0.0
                    max_ms = 0.0
                    last_print = now


    finally:
        try:
            with open("orderbook_dict.txt","w",encoding="utf-8") as f:
                json.dump(orderbooks,f,ensure_ascii=False,indent=2,
                    default=lambda x:x.isoformat() if isinstance(x,datetime.datetime) else str(x))
            print("\nOrderbook state saved to orderbook_dict.txt")
        except Exception as e:print(f"\nFailed to save orderbook state: {e}")
        print("\n--- Unhandled Event Summary ---")
        if not unhandled_events:print("No unhandled events received.")
        else:
            for ev_type,count in unhandled_events.items():
                print(f"{ev_type}: {count}")

async def main()->None:
    events: list[Event] = fetch_and_filter_gamma_events()
    event_ids: set[str] = {str(e.get("id")) for e in events if e.get("id") is not None}
    orderbooks: dict[str,Orderbook] = create_orderbooks(events)
    if not orderbooks:
        raise RuntimeError("No orderbook skeletons generated. Halting.")
    print(f"Successfully generated {len(orderbooks)} orderbook skeletons.")
    await handle_ws(events,event_ids,orderbooks)

if __name__=="__main__":
    asyncio.run(main())