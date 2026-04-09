"""
orderbook-gui/main.py

High-frequency live orderbook viewer for Polymarket.
Connects to the same WebSocket feed as poly_parquet_generator.py.
Uses DearPyGui for GPU-accelerated rendering above monitor refresh rate.
"""

import asyncio
import ctypes
import json
import queue
import sys
import threading
import time
import webbrowser
from collections import deque
from pathlib import Path

import websockets
import dearpygui.dearpygui as dpg

# Allow importing shared modules from the parent project directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import Orderbook
from fetch_and_filter_gamma_events import fetch_and_filter_gamma_events
from clob_core import (
    WS_URL,
    parse_asset_id, create_orderbook_skeleton, create_orderbooks,
    hydrate_orderbook, apply_price_change, apply_tick_size_change,
    has_sub_cent_price, best_prices_from_book, should_unsubscribe,
    ws_initial_subscribe, ws_subscribe_more, ws_unsubscribe,
)
import order_client

USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
FILL_SOUND_PATH = Path(__file__).resolve().parent / "trade_filled.mp3"
FILL_SOUND_ALIAS = "trade_fill_sound"
_MCI_VOLUME_MAX = 1000

# ── Config ────────────────────────────────────────────────────────────────────

EVENT_REFRESH_SECS   = 600
DEPTH_LEVELS         = 20     # Price levels shown per side
LIST_REBUILD_SECS    = 1.0    # How often to check if market list changed
FILL_DEDUP_SECS      = 12.0   # Suppress repeated fill notifications for the same order/fill

# ── Shared state (all writes/reads guarded by `_lock`) ───────────────────────

_lock           = threading.Lock()
_orderbooks:    dict[str, Orderbook]      = {}
_last_trades:   dict[str, dict]           = {}   # asset_id → {price, size, side}
_asset_activity: dict[str, deque[float]] = {}   # asset_id → deque of event timestamps
_ws_status    = "Connecting..."
_msgs_per_sec = 0.0
_ws_latency_ms = 0.0   # EMA of (now - event_timestamp)
_sort_mode:   list[str] = ["volume"]             # "volume" or "activity" (mutable container)
_primary_ids: set[str]  = set()                  # asset IDs from initial create_orderbooks (not paired twins)

# User order tracking (written by user WS thread, read by main/render thread)
_open_orders:    dict[str, dict] = {}   # order_id → {asset_id, side, price, size_open, expiration}
_seen_trade_ids: set[str]        = set()  # "trade_id:status" — dedup duplicate trade events
_processed_fill_trade_ids: set[str] = set()  # trade IDs already applied/notified on first fill event
_recent_fill_keys: dict[str, float] = {}   # fill fingerprint → monotonic timestamp
_user_ws_status: str             = "—"   # displayed in header for diagnostics
_user_ws_msg_count: int          = 0     # total messages received on user WS

# Fill notifications — user WS thread writes, render thread reads
_fill_queue:       queue.SimpleQueue = queue.SimpleQueue()  # (msg: str, color: tuple)
_fill_popup_until: list[float]       = [0.0]                # epoch time to auto-hide popup

# Authenticated positions — asset_id → {size, avg_price, outcome}
# Fetched from the Polymarket data API; refreshed on startup and after fills.
_positions:           dict[str, dict] = {}
_positions_sync_lock: threading.Lock  = threading.Lock()
_POSITIONS_POLL_SECS  = 60            # background periodic refresh interval

# ── WebSocket asyncio loop (runs in background thread) ───────────────────────

_orders_sync_lock = threading.Lock()
_sound_settings: dict[str, object] = {
    "muted": False,
    "volume": 100,
}


async def _ws_loop(initial_books: dict[str, Orderbook]) -> None:
    global _ws_status, _msgs_per_sec, _ws_latency_ms

    inactive: set[str] = set()

    with _lock:
        _orderbooks.update(initial_books)

    while True:
        refresh_task = None
        try:
            async with websockets.connect(WS_URL, ping_interval=10, ping_timeout=10) as ws:
                with _lock:
                    ids = list(_orderbooks.keys())

                await ws.send(ws_initial_subscribe(ids))

                with _lock:
                    _ws_status = "Connected"

                msg_count  = 0
                last_stat  = time.perf_counter()

                async def _refresh_loop() -> None:
                    while True:
                        await asyncio.sleep(EVENT_REFRESH_SECS)
                        new_events = await asyncio.to_thread(fetch_and_filter_gamma_events)
                        new_ids: list[str] = []
                        with _lock:
                            existing = set(_orderbooks.keys())
                        for ev in new_events:
                            for mkt in ev.get("markets", []):
                                aid = parse_asset_id(mkt)
                                if aid not in inactive and aid not in existing:
                                    skel = create_orderbook_skeleton(ev, mkt, aid)
                                    with _lock:
                                        _orderbooks[aid] = skel
                                    new_ids.append(aid)
                        if new_ids:
                            await ws.send(ws_subscribe_more(new_ids))
                            with _lock:
                                _ws_status = "Connected"

                refresh_task = asyncio.create_task(_refresh_loop())

                async for message in ws:
                    try:
                        ev = json.loads(message)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(ev, list):
                        # Initial bulk snapshot
                        with _lock:
                            for be in ev:
                                aid = be.get("asset_id", "")
                                if aid in _orderbooks:
                                    hydrate_orderbook(_orderbooks[aid], be)
                    else:
                        etype = ev.get("event_type", "")
                        if etype == "book":
                            with _lock:
                                aid = ev.get("asset_id", "")
                                if aid in _orderbooks:
                                    hydrate_orderbook(_orderbooks[aid], ev)
                                    _asset_activity.setdefault(aid, deque()).append(time.time())

                        elif etype == "price_change":
                            to_finalize: list[str] = []
                            with _lock:
                                for ch in ev.get("price_changes", []):
                                    aid = ch.get("asset_id", "")
                                    if aid in _orderbooks:
                                        apply_price_change(_orderbooks[aid], ch)
                                        _asset_activity.setdefault(aid, deque()).append(time.time())
                                        bb = float(ch.get("best_bid", "0") or "0")
                                        ba = float(ch.get("best_ask", "1") or "1")
                                        if should_unsubscribe(bb, ba):
                                            inactive.add(aid)
                                            to_finalize.append(aid)
                                        # Auto-detect tick upgrade: if this price has 3 decimal
                                        # places but the book still records 2-decimal tick size,
                                        # update min_tick_size to 0.001 for that token.
                                        price_str = ch.get("price", "")
                                        if (_orderbooks[aid]["min_tick_size"] >= 0.01 - 1e-9
                                                and price_str
                                                and has_sub_cent_price(price_str)):
                                            apply_tick_size_change(_orderbooks[aid], 0.001)
                            if to_finalize:
                                with _lock:
                                    for aid in to_finalize:
                                        _orderbooks.pop(aid, None)
                                        _last_trades.pop(aid, None)
                                await ws.send(ws_unsubscribe(to_finalize))

                        elif etype == "tick_size_change":
                            with _lock:
                                aid      = ev.get("asset_id", "")
                                new_tick = ev.get("new_tick_size", "")
                                if aid in _orderbooks and new_tick:
                                    apply_tick_size_change(_orderbooks[aid], float(new_tick))

                        elif etype == "last_trade_price":
                            with _lock:
                                aid = ev.get("asset_id", "")
                                if aid in _orderbooks:
                                    _last_trades[aid] = {
                                        "price": float(ev.get("price", 0)),
                                        "size":  float(ev.get("size",  0)),
                                        "side":  ev.get("side", ""),
                                    }
                                    _orderbooks[aid]["lastTradePrice"] = float(ev.get("price", 0))

                    # Latency: use the timestamp field present in most events
                    ts_str = None
                    if isinstance(ev, list):
                        ts_str = ev[0].get("timestamp") if ev else None
                    else:
                        ts_str = ev.get("timestamp")
                    if ts_str:
                        try:
                            latency = time.time() * 1000.0 - float(ts_str)
                            _ws_latency_ms = _ws_latency_ms * 0.9 + latency * 0.1
                        except (ValueError, TypeError):
                            pass

                    msg_count += 1
                    now = time.perf_counter()
                    dt  = now - last_stat
                    if dt >= 1.0:
                        _msgs_per_sec = msg_count / dt
                        msg_count = 0
                        last_stat = now

                    await asyncio.sleep(0)

        except websockets.exceptions.ConnectionClosed as e:
            with _lock:
                _ws_status = f"Reconnecting...  ({e})"
            await asyncio.sleep(3)

        finally:
            if refresh_task:
                refresh_task.cancel()
                try:
                    await refresh_task
                except (asyncio.CancelledError, Exception):
                    pass


def _run_ws_thread(initial_books: dict[str, Orderbook]) -> None:
    asyncio.run(_ws_loop(initial_books))


# ── User WebSocket (authenticated — tracks own open orders) ───────────────────

def _format_ttl(expiration: int, now: float) -> str:
    """Return a compact countdown string for a GTD order expiration timestamp."""
    remaining = int(expiration - now - 59)
    if remaining <= 0:
        return "exp"
    if remaining < 60:
        return f"{remaining}s"
    if remaining < 3600:
        m, s = divmod(remaining, 60)
        return f"{m}m{s:02d}s"
    h, rem = divmod(remaining, 3600)
    return f"{h}h{rem // 60:02d}m"


def _normalize_order_side(side: object) -> str:
    side_txt = str(side or "").strip().upper()
    if side_txt in {"BUY", "B"}:
        return "BUY"
    if side_txt in {"SELL", "S"}:
        return "SELL"
    return side_txt


def _normalize_price_key(price: object) -> float:
    return round(float(price or 0), 4)


def _derive_size_open(order: dict) -> float:
    raw_size_open = order.get("size_open")
    if raw_size_open not in (None, ""):
        return float(raw_size_open or 0)
    raw_size = order.get("size")
    if raw_size not in (None, ""):
        return float(raw_size or 0)
    original_size = float(order.get("original_size", 0) or 0)
    size_matched = float(order.get("size_matched", 0) or 0)
    return max(0.0, original_size - size_matched)


def _store_open_order(order: dict) -> None:
    oid = str(order.get("id", "")).strip()
    if not oid:
        return
    size_open = _derive_size_open(order)
    if size_open <= 0:
        _open_orders.pop(oid, None)
        return
    _open_orders[oid] = {
        "id": oid,
        "asset_id": str(order.get("asset_id", "")).strip(),
        "side": _normalize_order_side(order.get("side", "")),
        "price": _normalize_price_key(order.get("price", 0)),
        "size_open": size_open,
        "original_size": float(order.get("original_size", order.get("size", 0)) or 0),
        "expiration": int(order.get("expiration", 0) or 0),
    }


def _infer_user_event_type(ev: dict) -> str:
    etype = str(ev.get("event_type", "")).strip().lower()
    if etype:
        return etype
    if isinstance(ev.get("order"), dict):
        return "order"
    if isinstance(ev.get("trade"), dict):
        return "trade"
    if "maker_orders" in ev or "trade_owner" in ev or "status" in ev:
        return "trade"
    if "price" in ev and "asset_id" in ev and ("type" in ev or "original_size" in ev):
        return "order"
    return ""


def _log_user_ws_message(index: int, payload: object) -> None:
    try:
        rendered = json.dumps(payload, ensure_ascii=False)
    except TypeError:
        rendered = repr(payload)
    # print(f"[user WS] msg #{index}: {rendered}")


def _sync_open_orders(asset_id: str = "") -> None:
    if not _orders_sync_lock.acquire(blocking=False):
        return
    try:
        orders = order_client.get_open_orders(asset_id)
        fresh: dict[str, dict] = {}
        for order in orders:
            oid = str(order.get("id", "")).strip()
            if not oid:
                continue
            size_open = _derive_size_open(order)
            if size_open <= 0:
                continue
            fresh[oid] = {
                "id": oid,
                "asset_id": str(order.get("asset_id", "")).strip(),
                "side": _normalize_order_side(order.get("side", "")),
                "price": _normalize_price_key(order.get("price", 0)),
                "size_open": size_open,
                "original_size": float(order.get("original_size", order.get("size", 0)) or 0),
                "expiration": int(order.get("expiration", 0) or 0),
            }
        with _lock:
            if asset_id:
                stale_ids = [oid for oid, rec in _open_orders.items() if rec.get("asset_id") == asset_id]
                for oid in stale_ids:
                    _open_orders.pop(oid, None)
            else:
                _open_orders.clear()
            _open_orders.update(fresh)
        print(f"[orders sync] loaded {len(fresh)} open orders" + (f" for {asset_id[:16]}…" if asset_id else ""))
    except Exception as exc:
        print(f"[orders sync] {exc}")
    finally:
        _orders_sync_lock.release()


def _schedule_open_orders_sync(asset_id: str = "") -> None:
    threading.Thread(
        target=_sync_open_orders,
        args=(asset_id,),
        daemon=True,
        name="orders-sync-thread",
    ).start()


def _sync_positions() -> None:
    """Fetch authenticated positions from the Polymarket data API and update _positions."""
    global _positions
    if not _positions_sync_lock.acquire(blocking=False):
        return  # already running
    try:
        fresh = order_client.get_positions()
        with _lock:
            _positions.clear()
            _positions.update(fresh)
        print(f"[positions] synced {len(fresh)} position(s)")
    except Exception as exc:
        print(f"[positions] sync failed: {exc}")
    finally:
        _positions_sync_lock.release()


def _schedule_positions_sync() -> None:
    threading.Thread(target=_sync_positions, daemon=True, name="positions-sync").start()


def _process_order_update(order: dict, sub_type: str) -> None:
    """Update _open_orders from a single user-WS order event."""
    oid = order.get("id", "")
    if not oid:
        print(f"[user WS] _process_order_update: no order id in {order}")
        return
    size_open = _derive_size_open(order)
    asset_id  = str(order.get("asset_id", "")).strip()
    print(f"[user WS] order {oid[:12]}… sub_type={sub_type!r} side={order.get('side')} price={order.get('price')} size_open={size_open} asset_id={asset_id[:16]}…")
    with _lock:
        if sub_type in ("PLACEMENT", "OPEN", ""):
            if size_open > 0:
                _store_open_order(order)
                print(f"[user WS] → added to _open_orders (total {len(_open_orders)})")
        elif sub_type == "UPDATE":
            if oid in _open_orders:
                if size_open > 0:
                    _open_orders[oid]["size_open"] = size_open
                    _open_orders[oid]["side"] = _normalize_order_side(order.get("side", _open_orders[oid]["side"]))
                    _open_orders[oid]["price"] = _normalize_price_key(order.get("price", _open_orders[oid]["price"]))
                    print(f"[user WS] → updated size_open={size_open}")
                else:
                    _open_orders.pop(oid, None)  # fully filled
                    print(f"[user WS] → removed (fully filled)")
        elif sub_type in ("CANCELLATION", "CANCEL", "CLOSED"):
            _open_orders.pop(oid, None)
            print(f"[user WS] → removed ({sub_type})")


def _emit_fill_notification(trade: dict) -> None:
    """Push a fill popup entry, trigger the chime, and refresh positions."""
    price    = float(trade.get("price", 0) or 0)
    size     = float(trade.get("size",  0) or 0)
    # taker_side is the most reliable field; fall back to generic side
    side     = str(trade.get("taker_side") or trade.get("side") or "").upper()
    color    = (80, 220, 80) if side == "BUY" else (255, 100, 100)
    msg      = f"FILLED: {side}  {size:.2f} @ {price:.4f}"
    _fill_queue.put((msg, color))
    threading.Thread(target=_play_fill_sound, daemon=True, name="fill-sound").start()
    _schedule_positions_sync()   # positions change on every fill


def _log_fill_trigger_event(trade: dict) -> None:
    """Print the user-WS trade payload that triggered the fill notification."""
    try:
        rendered = json.dumps(trade, ensure_ascii=False, sort_keys=True)
    except TypeError:
        rendered = repr(trade)
    print(f"[fill sound] trigger event: {rendered}")


def _apply_fill_to_positions(trade: dict) -> None:
    """Apply the first fill event optimistically to local positions for immediate UI updates."""
    asset_id = str(trade.get("asset_id", "")).strip()
    if not asset_id:
        return

    side = str(trade.get("taker_side") or trade.get("side") or "").upper()
    if side not in {"BUY", "SELL"}:
        return

    size = float(trade.get("size", 0) or 0)
    price = float(trade.get("price", 0) or 0)
    if size <= 0:
        return

    with _lock:
        current = _positions.get(asset_id)
        prev_size = float(current.get("size", 0) or 0) if current else 0.0
        prev_avg = float(current.get("avg_price", 0) or 0) if current else 0.0
        outcome = str((current or {}).get("outcome") or trade.get("outcome") or "").strip()

        if side == "BUY":
            new_size = prev_size + size
            new_avg = ((prev_size * prev_avg) + (size * price)) / new_size if new_size > 0 else price
        else:
            new_size = max(0.0, prev_size - size)
            new_avg = prev_avg

        if new_size >= 0.01:
            _positions[asset_id] = {
                "size": new_size,
                "avg_price": new_avg,
                "outcome": outcome,
            }
        else:
            _positions.pop(asset_id, None)


def _recent_fill_key(trade: dict) -> str:
    """Return a fingerprint for suppressing duplicate fill notifications."""
    order_ids: list[str] = []
    for field in ("order_id", "taker_order_id", "maker_order_id", "orderID"):
        value = str(trade.get(field, "")).strip()
        if value:
            order_ids.append(value)

    maker_orders = trade.get("maker_orders")
    if isinstance(maker_orders, list):
        for item in maker_orders:
            if not isinstance(item, dict):
                continue
            for field in ("order_id", "id", "maker_order_id"):
                value = str(item.get(field, "")).strip()
                if value:
                    order_ids.append(value)
                    break

    order_part = ",".join(sorted(set(order_ids)))
    asset_id = str(trade.get("asset_id", "")).strip()
    side = str(trade.get("taker_side") or trade.get("side") or "").upper()
    price = f"{float(trade.get('price', 0) or 0):.4f}"
    size = f"{float(trade.get('size', 0) or 0):.8f}"
    owner = str(trade.get("trade_owner", "")).strip().upper()
    trade_id = str(trade.get("id", "")).strip()

    if order_part:
        return f"orders={order_part}|asset={asset_id}|side={side}|price={price}|size={size}|owner={owner}"
    if trade_id:
        return f"trade={trade_id}|asset={asset_id}|side={side}|price={price}|size={size}|owner={owner}"
    return f"asset={asset_id}|side={side}|price={price}|size={size}|owner={owner}"


def _should_emit_fill_notification(trade: dict) -> bool:
    """Return True when this fill should produce a popup/sound."""
    key = _recent_fill_key(trade)
    now = time.monotonic()
    cutoff = now - FILL_DEDUP_SECS
    with _lock:
        stale_keys = [existing for existing, ts in _recent_fill_keys.items() if ts < cutoff]
        for existing in stale_keys:
            _recent_fill_keys.pop(existing, None)
        previous = _recent_fill_keys.get(key)
        if previous is not None and previous >= cutoff:
            return False
        _recent_fill_keys[key] = now
    return True


def _handle_user_event(ev: dict) -> None:
    """Dispatch a single event received from the user WebSocket."""
    etype = _infer_user_event_type(ev)

    if etype == "order":
        # Polymarket sends order data either nested under "order" or flat in the event
        order    = ev.get("order") or ev
        sub_type = (order.get("type") or ev.get("type") or "").upper()
        _process_order_update(order, sub_type)

    elif etype == "trade":
        trade    = ev.get("trade") or ev
        trade_id = trade.get("id", "")
        status   = trade.get("status", "").upper()
        key      = f"{trade_id}:{status}"
        with _lock:
            if key in _seen_trade_ids:
                return
            _seen_trade_ids.add(key)
        if status in ("MMATCHED", "MATCHED", "CONFIRMED"):
            if trade_id:
                with _lock:
                    first_fill_event = trade_id not in _processed_fill_trade_ids
                    if first_fill_event:
                        _processed_fill_trade_ids.add(trade_id)
            else:
                first_fill_event = _should_emit_fill_notification(trade)
            if first_fill_event:
                _apply_fill_to_positions(trade)
                _log_fill_trigger_event(trade)
                _emit_fill_notification(trade)
        # Size-open changes for the affected order arrive via a companion "order" UPDATE
        # event on the same WS message, so no extra handling needed here.
    else:
        print(f"[user WS] unhandled event: {ev}")


async def _user_ws_loop() -> None:
    """Authenticated user WebSocket: keeps _open_orders in sync with Polymarket."""
    global _user_ws_status, _user_ws_msg_count

    _user_ws_status = "Getting creds…"
    try:
        creds = await asyncio.to_thread(order_client.get_api_creds)
    except Exception as exc:
        _user_ws_status = f"Creds error: {exc}"
        print(f"[user WS] Cannot obtain API credentials ({exc}) — order overlay disabled.")
        return

    if not creds.get("apiKey"):
        _user_ws_status = "No API key"
        print("[user WS] No API key found — order overlay disabled.")
        return

    print(f"[user WS] Creds OK — apiKey={creds['apiKey'][:8]}…")
    _user_ws_status = "Connecting…"

    backoff = 3
    while True:
        try:
            async with websockets.connect(
                USER_WS_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                sub_msg = json.dumps({"auth": creds, "type": "user"})
                print(f"[user WS] Connected — sending sub: {sub_msg[:120]}")
                await ws.send(sub_msg)
                _user_ws_status = "Connected"
                backoff = 3  # reset after successful connect
                async for message in ws:
                    _user_ws_msg_count += 1
                    try:
                        ev = json.loads(message)
                    except json.JSONDecodeError:
                        print(f"[user WS] raw msg #{_user_ws_msg_count}: {message}")
                        continue
                    if _user_ws_msg_count <= 10:
                        _log_user_ws_message(_user_ws_msg_count, ev)
                    events = ev if isinstance(ev, list) else [ev]
                    for item in events:
                        _handle_user_event(item)
        except websockets.exceptions.ConnectionClosed as exc:
            _user_ws_status = f"Closed ({exc.code})"
            print(f"[user WS] Connection closed: {exc}")
        except Exception as exc:
            _user_ws_status = f"Error: {exc}"
            print(f"[user WS] {exc}")
        await asyncio.sleep(backoff)
        _user_ws_status = f"Reconnecting (backoff {backoff}s)…"
        backoff = min(backoff * 2, 60)


def _run_user_ws_thread() -> None:
    asyncio.run(_user_ws_loop())


def _play_fill_sound() -> None:
    """Play the bundled fill MP3 via Windows MCI (non-blocking — call in thread)."""
    try:
        if bool(_sound_settings["muted"]):
            return
        if not FILL_SOUND_PATH.is_file():
            return

        winmm = ctypes.windll.winmm
        winmm.mciSendStringW(f"close {FILL_SOUND_ALIAS}", None, 0, None)
        open_cmd = f'open "{FILL_SOUND_PATH}" type mpegvideo alias {FILL_SOUND_ALIAS}'
        if winmm.mciSendStringW(open_cmd, None, 0, None) != 0:
            return
        volume = int(round(_normalize_sound_volume(_sound_settings["volume"]) * _MCI_VOLUME_MAX / 100))
        winmm.mciSendStringW(f"setaudio {FILL_SOUND_ALIAS} volume to {volume}", None, 0, None)
        if winmm.mciSendStringW(f"play {FILL_SOUND_ALIAS}", None, 0, None) != 0:
            winmm.mciSendStringW(f"close {FILL_SOUND_ALIAS}", None, 0, None)
    except Exception:
        pass


def _position_summary(asset_id: str) -> tuple[str, tuple]:
    """Return (text, color) describing own open orders on asset_id and its paired counterpart."""
    with _lock:
        book       = _orderbooks.get(asset_id, {})
        paired_id  = book.get("paired_asset_id", "")
        outcome    = book.get("outcome", "YES") or "YES"
        p_outcome  = book.get("paired_outcome", "NO") or "NO"
        snap       = list(_open_orders.values())

    def _agg(aid: str) -> tuple[float, float]:
        buy_sz = sell_sz = 0.0
        for o in snap:
            if o["asset_id"] == aid:
                if o["side"] == "BUY":
                    buy_sz += o["size_open"]
                else:
                    sell_sz += o["size_open"]
        return buy_sz, sell_sz

    buy, sell         = _agg(asset_id)
    p_buy, p_sell     = _agg(paired_id) if paired_id else (0.0, 0.0)
    has_any = buy > 0 or sell > 0 or p_buy > 0 or p_sell > 0

    if not has_any:
        return "No open orders on this market", (90, 90, 90)

    parts: list[str] = []
    if buy > 0 or sell > 0:
        seg = f"{outcome}:"
        if buy  > 0: seg += f"  BUY {buy:.1f}"
        if sell > 0: seg += f"  SELL {sell:.1f}"
        parts.append(seg)
    if p_buy > 0 or p_sell > 0:
        seg = f"{p_outcome}:"
        if p_buy  > 0: seg += f"  BUY {p_buy:.1f}"
        if p_sell > 0: seg += f"  SELL {p_sell:.1f}"
        parts.append(seg)

    return "  |  ".join(parts), (80, 210, 220)


# ── GUI ────────────────────────────────────────────────────────────────────────
# All DPG calls happen on the main thread only.

D = DEPTH_LEVELS

# Pre-allocated tag arrays for ask/bid table cells (avoids string formatting each frame)
_ask_own   = [f"aon{i}"  for i in range(D)]  # user's resting size at this ask level
_ask_ttl   = [f"attl{i}" for i in range(D)]  # TTL remaining for that order
_ask_price = [f"ap{i}"   for i in range(D)]
_ask_size  = [f"as{i}"   for i in range(D)]
_ask_bar   = [f"ab{i}"   for i in range(D)]
_bid_own   = [f"bon{i}"  for i in range(D)]
_bid_ttl   = [f"bttl{i}" for i in range(D)]
_bid_price = [f"bp{i}"   for i in range(D)]
_bid_size  = [f"bs{i}"   for i in range(D)]
_bid_bar   = [f"bb{i}"   for i in range(D)]

# Combined table: rows are created dynamically in _populate_combined_table (no pre-allocated tags)

_selected:        list[str]        = [""]  # single-element mutable for simple mutation
_current_slug:    list[str]        = [""]  # event slug for the currently displayed market
_mkt_btn_tags:    dict[str, str]   = {}
_prev_market_ids: list[str]        = []
_last_list_check  = 0.0
_mkt_filter:      list[str]        = [""]  # current market search text (lower-cased on read)

# Order ticket — worker threads put (message, color) here; _update_ui drains it
_order_queue: queue.SimpleQueue = queue.SimpleQueue()

# Depth level for the live limit buttons: 0 = best bid/ask, 1 = second level, etc.
_buy_level:  list[int] = [0]
_sell_level: list[int] = [0]

_DEFAULT_REPEAT_BID_SETTINGS = {
    "price": 0.01,
    "size": 5.0,
    "ttl_secs": 1,
    "repeat_interval_secs": 3,
}
_repeat_bid_lock = threading.Lock()
_repeat_bid_enabled = False
_repeat_bid_asset_id = ""
_repeat_bid_size = 0.0
_repeat_bid_price = 0.0
_repeat_bid_stop_event: threading.Event | None = None

# ── Hotkey settings ────────────────────────────────────────────────────────────
# Human-readable labels for each assignable action (order matters for display)
_HOTKEY_LABELS: dict[str, str] = {
    "buy_limit":    "Buy Limit",
    "sell_limit":   "Sell Limit",
    "buy_lv-1":     "Buy at Bid Level -1",
    "buy_lv0":      "Buy at Bid Level 0",
    "buy_lv1":      "Buy at Bid Level 1",
    "buy_lv2":      "Buy at Bid Level 2",
    "sell_lv-1":    "Sell at Ask Level -1",
    "sell_lv0":     "Sell at Ask Level 0",
    "sell_lv1":     "Sell at Ask Level 1",
    "sell_lv2":     "Sell at Ask Level 2",
    "mkt_buy":      "Market Buy",
    "mkt_sell":     "Market Sell",
    "cancel_all":   "Cancel All Orders",
    "swap_paired":  "Swap to Paired Asset",
    "bid_lv_up":    "Bid Level: Deeper (+)",
    "bid_lv_dn":    "Bid Level: Shallower (−)",
    "bid_lv_rst":   "Bid Level: Reset to 0",
    "ask_lv_up":    "Ask Level: Deeper (+)",
    "ask_lv_dn":    "Ask Level: Shallower (−)",
    "ask_lv_rst":   "Ask Level: Reset to 0",
}
# action → DPG key code (None = unbound)
_hotkeys: dict[str, int | None] = {k: None for k in _HOTKEY_LABELS}
# Tracks which action is waiting for a key press; "" = not listening
_hotkey_listen_action: list[str] = [""]

_SETTINGS_PATH = Path(__file__).parent / "settings.json"
_COMPACT_LEVELS = (0, 1, 2)


def _normalize_compact_level(value: object) -> int:
    """Clamp persisted/UI compactness values to a supported level."""
    if isinstance(value, bool):
        return 1 if value else 0
    try:
        level = int(value)
    except (TypeError, ValueError):
        return 0
    return max(_COMPACT_LEVELS[0], min(_COMPACT_LEVELS[-1], level))


def _normalize_depth_bar_cap(value: object) -> float:
    try:
        cap = float(value)
    except (TypeError, ValueError):
        return 10000.0
    return max(1.0, cap)


def _normalize_sound_volume(value: object) -> int:
    try:
        volume = int(value)
    except (TypeError, ValueError):
        return 100
    return max(0, min(100, volume))


def _load_settings() -> None:
    """Load persisted settings from settings.json."""
    try:
        data = json.loads(_SETTINGS_PATH.read_text(encoding="utf-8"))
        for action, code in data.get("hotkeys", {}).items():
            if action in _hotkeys:
                _hotkeys[action] = int(code) if code is not None else None
        disp = data.get("display", {})
        if "compact_level" in disp:
            _display_settings["compact_level"] = _normalize_compact_level(disp["compact_level"])
        elif "compact_mode" in disp:
            _display_settings["compact_level"] = _normalize_compact_level(disp["compact_mode"])
        if "book_font_size" in disp:
            sz = int(disp["book_font_size"])
            if sz in _BOOK_FONT_SIZES:
                _display_settings["book_font_size"] = sz
        if "combined_table" in disp:
            _display_settings["combined_table"] = bool(disp["combined_table"])
        if "show_zero_prices" in disp:
            _display_settings["show_zero_prices"] = bool(disp["show_zero_prices"])
        if "depth_bar_max_size" in disp:
            _display_settings["depth_bar_max_size"] = _normalize_depth_bar_cap(disp["depth_bar_max_size"])
        sound = data.get("sound", {})
        if "muted" in sound:
            _sound_settings["muted"] = bool(sound["muted"])
        if "volume" in sound:
            _sound_settings["volume"] = _normalize_sound_volume(sound["volume"])
        repeater = data.get("repeat_bid", {})
        if "price" in repeater:
            _repeat_bid_settings["price"] = max(0.001, min(0.999, float(repeater["price"])))
        if "size" in repeater:
            _repeat_bid_settings["size"] = max(0.01, float(repeater["size"]))
        if "ttl_secs" in repeater:
            _repeat_bid_settings["ttl_secs"] = max(1, int(repeater["ttl_secs"]))
        if "repeat_interval_secs" in repeater:
            _repeat_bid_settings["repeat_interval_secs"] = max(1, int(repeater["repeat_interval_secs"]))
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        pass


def _save_settings() -> None:
    """Persist current settings to settings.json."""
    data = {
        "hotkeys": {k: v for k, v in _hotkeys.items()},
        "display": {k: v for k, v in _display_settings.items()},
        "sound": {k: v for k, v in _sound_settings.items()},
        "repeat_bid": {k: v for k, v in _repeat_bid_settings.items()},
    }
    _SETTINGS_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ── Display settings ──────────────────────────────────────────────────────────
_BOOK_FONT_SIZES = (10, 11, 12, 13, 14, 15, 16)   # selectable orderbook row sizes
_display_settings: dict = {
    "compact_level":   0,      # 0=off, 1=compact, 2=extra compact
    "book_font_size":  13,     # size of orderbook depth-row text
    "combined_table":  False,  # merge asks+bids into one continuous table
    "show_zero_prices": False, # show all prices 0.01-0.99 (or 0.001-0.999), filling gaps with "Size 0"
    "depth_bar_max_size": 10000.0,  # cap used when scaling depth bars
}
_repeat_bid_settings: dict = dict(_DEFAULT_REPEAT_BID_SETTINGS)

# Font handles — populated in main() after dpg.create_context()
# Keys: "ui_{level}", "book_{size}" for each size in _BOOK_FONT_SIZES
_fonts: dict[str, int] = {}

# Global + table themes by compactness level — populated in main()
_ui_theme_by_level: dict[int, int] = {}
_tbl_theme_by_level: dict[int, int] = {}
_depth_bar_theme_by_side: dict[str, int] = {}


def _bind_depth_bar_theme(bar_tag: int | str, side: str) -> None:
    theme = _depth_bar_theme_by_side.get(side)
    if theme:
        dpg.bind_item_theme(bar_tag, theme)


def _add_depth_bar(side: str, parent: int | str | None = None, tag: int | str | None = None) -> int | str:
    kwargs: dict[str, object] = {
        "default_value": 0.0,
        "overlay": "",
        "width": -1,
    }
    if parent is not None:
        kwargs["parent"] = parent
    if tag is not None:
        kwargs["tag"] = tag
    bar_tag = dpg.add_progress_bar(**kwargs)
    _bind_depth_bar_theme(bar_tag, side)
    return bar_tag


def _set_depth_bar(bar_tag: int | str, size: float, max_size: float) -> None:
    if max_size <= 0.0 or size <= 0.0:
        dpg.set_value(bar_tag, 0.0)
        return
    dpg.set_value(bar_tag, min(size / max_size, 1.0))


def _compute_depth_bar_scale(*sizes: float) -> float:
    cap = _normalize_depth_bar_cap(_display_settings["depth_bar_max_size"])
    actual_max = max((float(size) for size in sizes), default=0.0)
    if actual_max <= 0.0:
        return 1.0
    return min(actual_max, cap)


def _add_empty_depth_cell(parent: int | str | None = None) -> int | str:
    kwargs: dict[str, object] = {"default_value": ""}
    if parent is not None:
        kwargs["parent"] = parent
    return dpg.add_text(**kwargs)

def _apply_display_settings() -> None:
    """Rebind fonts and table padding to match the current _display_settings.
    Must be called from the main (render) thread after _build_ui() and font/theme setup."""
    compact_level = _normalize_compact_level(_display_settings["compact_level"])
    bsize = _display_settings["book_font_size"]
    combined = _display_settings["combined_table"]

    ui_theme = _ui_theme_by_level.get(compact_level)
    if ui_theme:
        dpg.bind_theme(ui_theme)

    ui_font = _fonts.get(f"ui_{compact_level}")
    if ui_font:
        dpg.bind_font(ui_font)

    # Orderbook row font — applied to split-table tags (combined table fonts bound dynamically)
    book_font = _fonts.get(f"book_{bsize}")
    if book_font:
        split_tags = (
            _ask_own + _ask_ttl + _ask_price + _ask_size
            + _bid_own + _bid_ttl + _bid_price + _bid_size
        )
        for tag in split_tags:
            dpg.bind_item_font(tag, book_font)

    tbl_theme = _tbl_theme_by_level.get(compact_level)
    if tbl_theme:
        for tbl_tag in ("tbl_asks", "tbl_bids", "tbl_combined"):
            if dpg.does_item_exist(tbl_tag):
                dpg.bind_item_theme(tbl_tag, tbl_theme)

    # Combined / split table visibility
    if dpg.does_item_exist("grp_split_tables"):
        dpg.configure_item("grp_split_tables",   show=not combined)
    if dpg.does_item_exist("grp_combined_table"):
        dpg.configure_item("grp_combined_table",  show=combined)

    # Compact: hide the section-label texts to reclaim vertical space
    for tag in ("lbl_asks", "lbl_bids"):
        if dpg.does_item_exist(tag):
            dpg.configure_item(tag, show=compact_level == 0)

    # Sync the Display tab widgets if the modal has been built
    if dpg.does_item_exist("disp_compact_level"):
        dpg.set_value("disp_compact_level", compact_level)
    if dpg.does_item_exist("disp_book_font_slider"):
        dpg.set_value("disp_book_font_slider", bsize)
    if dpg.does_item_exist("disp_combined_chk"):
        dpg.set_value("disp_combined_chk", combined)
    if dpg.does_item_exist("disp_zero_prices_chk"):
        dpg.set_value("disp_zero_prices_chk", _display_settings["show_zero_prices"])
    if dpg.does_item_exist("misc_depth_bar_max_size"):
        dpg.set_value("misc_depth_bar_max_size", _display_settings["depth_bar_max_size"])
    if dpg.does_item_exist("sound_muted_chk"):
        dpg.set_value("sound_muted_chk", bool(_sound_settings["muted"]))
    if dpg.does_item_exist("sound_volume_slider"):
        dpg.set_value("sound_volume_slider", _normalize_sound_volume(_sound_settings["volume"]))


# FA font handle — set in main() before _build_ui(); used by copy button and depth bars
_fa_font: int = 0

# Maps color tuple → DPG theme id for order_status input_text; populated in _build_ui
_status_theme: dict[tuple, int] = {}
_market_btn_default_theme: int = 0
_market_btn_held_theme: int = 0


def _copy_status_to_clipboard() -> None:
    """Copy the current order_status text to the Windows clipboard."""
    text = dpg.get_value("order_status")
    if not text or text == "—":
        return
    data = text.encode("utf-16-le") + b"\x00\x00"
    k32  = ctypes.windll.kernel32
    u32  = ctypes.windll.user32
    # GlobalLock returns a void pointer — must set restype or ctypes truncates it
    # to 32 bits on 64-bit Python, causing the access violation.
    k32.GlobalLock.restype = ctypes.c_void_p
    hMem = k32.GlobalAlloc(0x0002, len(data))   # GMEM_MOVEABLE
    pMem = k32.GlobalLock(hMem)
    ctypes.memmove(pMem, data, len(data))
    k32.GlobalUnlock(hMem)
    u32.OpenClipboard(0)
    u32.EmptyClipboard()
    u32.SetClipboardData(13, hMem)              # CF_UNICODETEXT = 13
    u32.CloseClipboard()


def _set_status(msg: str, color: tuple) -> None:
    """Update order_status input_text value and text color theme."""
    dpg.set_value("order_status", msg)
    theme = _status_theme.get(color)
    if theme:
        dpg.bind_item_theme("order_status", theme)


def _compute_order_expiration(ttl_seconds: int) -> int:
    return (int(time.time()) + 60 + ttl_seconds) if ttl_seconds > 0 else 0


def _sync_repeat_bid_settings_widgets() -> None:
    widget_defaults = {
        "repeat_bid_price": float(_repeat_bid_settings["price"]),
        "repeat_bid_size": float(_repeat_bid_settings["size"]),
        "repeat_bid_ttl": int(_repeat_bid_settings["ttl_secs"]),
        "repeat_bid_interval": int(_repeat_bid_settings["repeat_interval_secs"]),
    }
    for tag, value in widget_defaults.items():
        if dpg.does_item_exist(tag):
            dpg.set_value(tag, value)


def _open_repeat_bid_settings() -> None:
    _sync_repeat_bid_settings_widgets()
    if dpg.does_item_exist("repeat_bid_settings_modal"):
        dpg.configure_item("repeat_bid_settings_modal", show=True)


def _save_repeat_bid_settings() -> None:
    price = float(dpg.get_value("repeat_bid_price") or 0.0)
    size = float(dpg.get_value("repeat_bid_size") or 0.0)
    ttl_secs = int(dpg.get_value("repeat_bid_ttl") or 0)
    repeat_interval_secs = int(dpg.get_value("repeat_bid_interval") or 0)

    if not (0.001 <= price <= 0.999):
        _set_status("Repeater price must be between 0.001 and 0.999", (255, 180, 60))
        return
    if size <= 0:
        _set_status("Repeater size must be > 0", (255, 180, 60))
        return
    if ttl_secs <= 0:
        _set_status("Repeater TTL must be > 0", (255, 180, 60))
        return
    if repeat_interval_secs <= 0:
        _set_status("Repeater interval must be > 0", (255, 180, 60))
        return

    _repeat_bid_settings["price"] = price
    _repeat_bid_settings["size"] = size
    _repeat_bid_settings["ttl_secs"] = ttl_secs
    _repeat_bid_settings["repeat_interval_secs"] = repeat_interval_secs
    _save_settings()
    _refresh_repeat_bid_button()
    if dpg.does_item_exist("repeat_bid_settings_modal"):
        dpg.configure_item("repeat_bid_settings_modal", show=False)
    _set_status(
        f"Repeater saved: {price:.4f} x {size:.2f}  TTL {ttl_secs}s  every {repeat_interval_secs}s",
        (80, 220, 80),
    )


def _refresh_repeat_bid_button() -> None:
    if not dpg.does_item_exist("btn_repeat_bid"):
        return
    with _repeat_bid_lock:
        enabled = _repeat_bid_enabled
        asset_id = _repeat_bid_asset_id
        size = _repeat_bid_size
        price = _repeat_bid_price
    cfg_price = float(_repeat_bid_settings["price"])
    cfg_ttl = int(_repeat_bid_settings["ttl_secs"])
    cfg_interval = int(_repeat_bid_settings["repeat_interval_secs"])
    if enabled:
        suffix = f"  {size:.2f}" if size > 0 else ""
        label = f"STOP REPEAT BID @ {price:.2f}{suffix}"
        if asset_id:
            label += "  [ON]"
    else:
        label = f"REPEAT BID @ {cfg_price:.2f}  TTL {cfg_ttl}s / {cfg_interval}s"
    dpg.configure_item("btn_repeat_bid", label=label)


def _repeat_bid_loop(
    token_id: str,
    price: float,
    size: float,
    ttl_secs: int,
    repeat_interval_secs: int,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        expiration = _compute_order_expiration(ttl_secs)
        try:
            t0 = time.perf_counter()
            order_client.place_limit_order(
                token_id,
                "BUY",
                price,
                size,
                expiration=expiration,
            )
            ms = (time.perf_counter() - t0) * 1000
            _schedule_open_orders_sync(token_id)
            # _order_queue.put((
            #     f"Repeat bid OK: {price:.4f} x {size:.2f}  TTL {ttl_secs}s  ({ms:.0f}ms)",
            #     (80, 220, 80),
            # ))
        except Exception as exc:
            _order_queue.put((f"Repeat bid error: {exc}", (255, 80, 80)))
        if stop_event.wait(repeat_interval_secs):
            break

    global _repeat_bid_enabled, _repeat_bid_asset_id, _repeat_bid_size, _repeat_bid_price, _repeat_bid_stop_event
    with _repeat_bid_lock:
        if _repeat_bid_stop_event is stop_event:
            _repeat_bid_enabled = False
            _repeat_bid_asset_id = ""
            _repeat_bid_size = 0.0
            _repeat_bid_price = 0.0
            _repeat_bid_stop_event = None


def _stop_repeat_bid(announce: bool = True) -> None:
    global _repeat_bid_enabled, _repeat_bid_asset_id, _repeat_bid_size, _repeat_bid_price, _repeat_bid_stop_event
    with _repeat_bid_lock:
        enabled = _repeat_bid_enabled
        stop_event = _repeat_bid_stop_event
    if enabled:
        with _repeat_bid_lock:
            _repeat_bid_enabled = False
            _repeat_bid_asset_id = ""
            _repeat_bid_size = 0.0
            _repeat_bid_price = 0.0
            _repeat_bid_stop_event = None
        if stop_event is not None:
            stop_event.set()
        if announce:
            _set_status("Recurring bid stopped", (130, 130, 130))
        _refresh_repeat_bid_button()


def _start_repeat_bid_for_asset(aid: str, announce: bool = True) -> bool:
    global _repeat_bid_enabled, _repeat_bid_asset_id, _repeat_bid_size, _repeat_bid_price, _repeat_bid_stop_event
    if not aid:
        if announce:
            _set_status("No market selected", (130, 130, 130))
        return False

    price = float(_repeat_bid_settings["price"])
    size = float(_repeat_bid_settings["size"])
    ttl_secs = int(_repeat_bid_settings["ttl_secs"])
    repeat_interval_secs = int(_repeat_bid_settings["repeat_interval_secs"])
    if not (0.001 <= price <= 0.999):
        if announce:
            _set_status("Repeater price must be between 0.001 and 0.999", (255, 180, 60))
        return False
    if size <= 0:
        if announce:
            _set_status("Repeater size must be > 0", (255, 180, 60))
        return False
    if ttl_secs <= 0:
        if announce:
            _set_status("Repeater TTL must be > 0", (255, 180, 60))
        return False
    if repeat_interval_secs <= 0:
        if announce:
            _set_status("Repeater interval must be > 0", (255, 180, 60))
        return False

    stop_event = threading.Event()
    with _repeat_bid_lock:
        _repeat_bid_enabled = True
        _repeat_bid_asset_id = aid
        _repeat_bid_size = size
        _repeat_bid_price = price
        _repeat_bid_stop_event = stop_event

    if announce:
        _set_status(
            f"Recurring bid started @ {price:.4f} x {size:.2f}  (TTL {ttl_secs}s, every {repeat_interval_secs}s)",
            (220, 220, 60),
        )
    _refresh_repeat_bid_button()
    threading.Thread(
        target=_repeat_bid_loop,
        args=(aid, price, size, ttl_secs, repeat_interval_secs, stop_event),
        daemon=True,
        name="repeat-bid-thread",
    ).start()
    return True


def _toggle_repeat_bid() -> None:
    with _repeat_bid_lock:
        enabled = _repeat_bid_enabled
    if enabled:
        _stop_repeat_bid()
        return

    _start_repeat_bid_for_asset(_selected[0])


def _set_selected_market(aid: str) -> None:
    current = _selected[0]
    if not aid or aid == current:
        if aid:
            _apply_selection_highlight(aid)
        return

    with _repeat_bid_lock:
        restart_repeat_bid = _repeat_bid_enabled

    if restart_repeat_bid:
        _stop_repeat_bid(announce=False)

    _selected[0] = aid
    _apply_selection_highlight(aid)

    if restart_repeat_bid:
        _start_repeat_bid_for_asset(aid, announce=False)
        _set_status("Recurring bid moved to selected market", (220, 220, 60))


def _submit_limit(side: str, price: float | None = None) -> None:
    """Spawn a worker thread to place a GTC limit order."""
    aid = _selected[0]
    if not aid:
        _set_status("No market selected", (130, 130, 130))
        return
    if price is None:
        _set_status("No limit price selected", (255, 180, 60))
        return
    size  = dpg.get_value("order_size")
    ttl   = int(dpg.get_value("order_ttl"))
    if not (0.001 <= price <= 0.999):
        _set_status("Price must be between 0.001 and 0.999", (255, 180, 60))
        return
    if size <= 0:
        _set_status("Size must be > 0", (255, 180, 60))
        return
    expiration = _compute_order_expiration(ttl)
    ttl_label  = f"TTL {ttl}s" if ttl > 0 else "GTC"
    _set_status(f"Placing {side} limit @ {price:.4f} × {size:.2f} ({ttl_label})...", (220, 220, 60))

    def _worker(token_id=aid, s=side, p=price, sz=size, exp=expiration):
        try:
            t0 = time.perf_counter()
            order_client.place_limit_order(token_id, s, p, sz, expiration=exp)
            ms = (time.perf_counter() - t0) * 1000
            _schedule_open_orders_sync(token_id)
            _order_queue.put((f"OK {s}: {p:.4f} × {sz:.2f}  ({ms:.0f}ms)", (80, 220, 80)))
        except Exception as exc:
            _order_queue.put((f"Error: {exc}", (255, 80, 80)))

    threading.Thread(target=_worker, daemon=True).start()


def _submit_market(side: str) -> None:
    """Spawn a worker thread to place a FOK market order (size = USDC amount)."""
    aid = _selected[0]
    if not aid:
        _set_status("No market selected", (130, 130, 130))
        return
    amount = dpg.get_value("order_size")
    if amount <= 0:
        _set_status("Amount must be > 0", (255, 180, 60))
        return
    _set_status(f"Placing MKT {side} ${amount:.2f}...", (220, 220, 60))

    def _worker(token_id=aid, s=side, amt=amount):
        try:
            t0 = time.perf_counter()
            order_client.place_market_order(token_id, s, amt)
            ms = (time.perf_counter() - t0) * 1000
            _order_queue.put((f"OK MKT {s}: ${amt:.2f}  ({ms:.0f}ms)", (80, 220, 80)))
        except Exception as exc:
            _order_queue.put((f"Error: {exc}", (255, 80, 80)))

    threading.Thread(target=_worker, daemon=True).start()


def _submit_cancel_all() -> None:
    """Spawn a worker thread to cancel every open order on this account."""
    _set_status("Cancelling all orders...", (220, 220, 60))

    def _worker():
        try:
            resp = order_client.cancel_all()
            _schedule_open_orders_sync()
            # resp is typically {"canceled": [...]} or similar
            n = len(resp) if isinstance(resp, list) else resp.get("canceled", resp)
            summary = f"{len(n)} cancelled" if isinstance(n, list) else str(n)[:40]
            _order_queue.put((f"Cancelled: {summary}", (80, 220, 80)))
        except Exception as exc:
            _order_queue.put((f"Error: {exc}", (255, 80, 80)))

    threading.Thread(target=_worker, daemon=True).start()


def _key_name(key: int) -> str:
    """Convert a DPG key code to a short human-readable string."""
    for ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        if key == getattr(dpg, f"mvKey_{ch}", None):
            return ch
    for d in "0123456789":
        if key == getattr(dpg, f"mvKey_{d}", None):
            return d
    for n in range(1, 13):
        if key == getattr(dpg, f"mvKey_F{n}", None):
            return f"F{n}"
    _SPECIAL: dict[int | None, str] = {
        getattr(dpg, "mvKey_Space",     None): "Space",
        getattr(dpg, "mvKey_Return",    None): "Enter",
        getattr(dpg, "mvKey_Tab",       None): "Tab",
        getattr(dpg, "mvKey_Backspace", None): "Bksp",
        getattr(dpg, "mvKey_Delete",    None): "Del",
        getattr(dpg, "mvKey_Up",        None): "↑",
        getattr(dpg, "mvKey_Down",      None): "↓",
        getattr(dpg, "mvKey_Left",      None): "←",
        getattr(dpg, "mvKey_Right",     None): "→",
        getattr(dpg, "mvKey_Prior",     None): "PgUp",
        getattr(dpg, "mvKey_Next",      None): "PgDn",
        getattr(dpg, "mvKey_Home",      None): "Home",
        getattr(dpg, "mvKey_End",       None): "End",
    }
    return _SPECIAL.get(key, f"#{key}")


def _cancel_hotkey_assign() -> None:
    """Abort any in-progress hotkey capture, restoring the Assign button label."""
    action = _hotkey_listen_action[0]
    if action:
        if dpg.does_item_exist(f"hk_btn_{action}"):
            dpg.configure_item(f"hk_btn_{action}", label="Assign")
        _hotkey_listen_action[0] = ""


def _hotkey_dispatch(action: str) -> None:
    """Fire the callback for a hotkey action."""
    _dispatch: dict[str, object] = {
        "buy_limit":   _submit_buy_level,
        "sell_limit":  _submit_sell_level,
        "buy_lv-1":    lambda: _submit_buy_level_index(-1),
        "buy_lv0":     lambda: _submit_buy_level_index(0),
        "buy_lv1":     lambda: _submit_buy_level_index(1),
        "buy_lv2":     lambda: _submit_buy_level_index(2),
        "sell_lv-1":   lambda: _submit_sell_level_index(-1),
        "sell_lv0":    lambda: _submit_sell_level_index(0),
        "sell_lv1":    lambda: _submit_sell_level_index(1),
        "sell_lv2":    lambda: _submit_sell_level_index(2),
        "mkt_buy":     lambda: _submit_market("BUY"),
        "mkt_sell":    lambda: _submit_market("SELL"),
        "cancel_all":  _submit_cancel_all,
        "swap_paired": _switch_to_paired,
        "bid_lv_up":   _inc_buy_level,
        "bid_lv_dn":   _dec_buy_level,
        "bid_lv_rst":  _reset_buy_level,
        "ask_lv_up":   _inc_sell_level,
        "ask_lv_dn":   _dec_sell_level,
        "ask_lv_rst":  _reset_sell_level,
    }
    fn = _dispatch.get(action)
    if callable(fn):
        fn()


def _on_key_press(sender, app_data) -> None:
    """Global key handler: manages hotkey capture and dispatches bound hotkeys."""
    key    = app_data
    action = _hotkey_listen_action[0]

    if action:
        if key == dpg.mvKey_Escape:
            _cancel_hotkey_assign()
        else:
            _hotkeys[action] = key
            if dpg.does_item_exist(f"hk_key_{action}"):
                dpg.set_value(f"hk_key_{action}", _key_name(key))
            if dpg.does_item_exist(f"hk_btn_{action}"):
                dpg.configure_item(f"hk_btn_{action}", label="Assign")
            _hotkey_listen_action[0] = ""
            _save_settings()
        return

    if key == dpg.mvKey_Escape:
        if dpg.does_item_exist("settings_modal") and dpg.is_item_shown("settings_modal"):
            dpg.configure_item("settings_modal", show=False)
        else:
            dpg.stop_dearpygui()
        return

    for act, bound_key in _hotkeys.items():
        if bound_key is not None and bound_key == key:
            _hotkey_dispatch(act)
            break


def _make_hk_assign_cb(action: str):
    """Return a callback that starts listening for a key to bind to `action`."""
    def _cb():
        # Cancel any previous pending capture first
        if _hotkey_listen_action[0] and _hotkey_listen_action[0] != action:
            _cancel_hotkey_assign()
        _hotkey_listen_action[0] = action
        if dpg.does_item_exist(f"hk_btn_{action}"):
            dpg.configure_item(f"hk_btn_{action}", label="Press key…")
    return _cb


def _make_hk_clear_cb(action: str):
    """Return a callback that clears the hotkey binding for `action`."""
    def _cb():
        if _hotkey_listen_action[0] == action:
            _cancel_hotkey_assign()
        _hotkeys[action] = None
        if dpg.does_item_exist(f"hk_key_{action}"):
            dpg.set_value(f"hk_key_{action}", "—")
        _save_settings()
    return _cb


def _build_bid_levels(bids: list[tuple[float, float]], tick: float) -> list[float]:
    """Return an expanded list of bid prices with gap-fills and an improve-on-best level.

    Index layout:  result[level + 1] → price for that level.
      level -1 → best_bid + tick  (post inside/improve the book)
      level  0 → best_bid         (join the best resting bid)
      level  1 → lower_next + tick if gap > tick, else next resting bid
      level  2 → next resting bid (or next gap-fill), …

    For bids, a gap-fill sits at the *bottom* of the gap (lower_level + tick) so the
    trader pays the minimum price that still beats the level below — cheapest-possible
    queue improvement.
    """
    if not bids:
        return []
    t      = round(tick, 4)
    prices = [round(p, 4) for p, _ in bids]
    # level -1: improve on best (cap at 0.99)
    result: list[float] = [min(round(prices[0] + t, 4), 0.99)]
    result.append(prices[0])          # level 0: best bid
    for i in range(len(prices) - 1):
        gap = round(prices[i] - prices[i + 1], 4)
        if gap > t + 1e-9:
            result.append(round(prices[i + 1] + t, 4))   # gap-fill: one tick above next
        result.append(prices[i + 1])
    return result


def _build_ask_levels(asks: list[tuple[float, float]], tick: float) -> list[float]:
    """Return an expanded list of ask prices with gap-fills and an improve-on-best level.

    Index layout:  result[level + 1] → price for that level.
      level -1 → best_ask + tick  (post above best ask — better sell price)
      level  0 → best_ask         (join the best resting ask)
      level  1 → upper_next - tick if gap > tick, else next resting ask
      level  2 → next resting ask (or next gap-fill), …

    For asks, a gap-fill sits at the *top* of the gap (upper_level - tick) so the
    trader collects the maximum price that still beats the level above — best-possible
    queue improvement for the sell side.
    """
    if not asks:
        return []
    t      = round(tick, 4)
    prices = [round(p, 4) for p, _ in asks]
    # level -1: one tick above best ask (cap at 0.99)
    result: list[float] = [min(round(prices[0] + t, 4), 0.99)]
    result.append(prices[0])          # level 0: best ask
    for i in range(len(prices) - 1):
        gap = round(prices[i + 1] - prices[i], 4)
        if gap > t + 1e-9:
            result.append(round(prices[i + 1] - t, 4))   # gap-fill: one tick below next
        result.append(prices[i + 1])
    return result


def _inc_buy_level()   -> None: _buy_level[0] += 1
def _dec_buy_level()   -> None: _buy_level[0] = max(-1, _buy_level[0] - 1)
def _reset_buy_level() -> None: _buy_level[0] = 0
def _inc_sell_level()  -> None: _sell_level[0] += 1
def _dec_sell_level()  -> None: _sell_level[0] = max(-1, _sell_level[0] - 1)
def _reset_sell_level()-> None: _sell_level[0] = 0


def _submit_buy_level() -> None:
    """Submit a BUY limit at the current bid depth level (expanded with gap-fills)."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        book = _orderbooks.get(aid)
    if not book:
        return
    bids  = sorted([(float(p), s) for p, s in book.get("bids", {}).items() if s > 0],
                   key=lambda x: x[0], reverse=True)
    tick  = float(book.get("min_tick_size", 0.01) or 0.01)
    lvls  = _build_bid_levels(bids, tick)
    if not lvls:
        _set_status("No bid levels available", (255, 180, 60))
        return
    lv    = max(-1, min(_buy_level[0], len(lvls) - 2))
    _submit_limit("BUY", price=lvls[lv + 1])


def _submit_buy_level_index(level: int) -> None:
    """Submit a BUY limit at a fixed expanded bid level (0 = best, 1 = next gap-fill, …)."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        book = _orderbooks.get(aid)
    if not book:
        return
    bids  = sorted([(float(p), s) for p, s in book.get("bids", {}).items() if s > 0],
                   key=lambda x: x[0], reverse=True)
    tick  = float(book.get("min_tick_size", 0.01) or 0.01)
    lvls  = _build_bid_levels(bids, tick)
    idx   = level + 1  # level 0 → index 1 (level -1 is index 0, reserved for improve)
    if not lvls or idx >= len(lvls):
        _set_status(f"No bid level {level} available", (255, 180, 60))
        return
    _submit_limit("BUY", price=lvls[idx])


def _submit_sell_level() -> None:
    """Submit a SELL limit at the current ask depth level (expanded with gap-fills)."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        book = _orderbooks.get(aid)
    if not book:
        return
    asks  = sorted([(float(p), s) for p, s in book.get("asks", {}).items() if s > 0],
                   key=lambda x: x[0])
    tick  = float(book.get("min_tick_size", 0.01) or 0.01)
    lvls  = _build_ask_levels(asks, tick)
    if not lvls:
        _set_status("No ask levels available", (255, 180, 60))
        return
    lv    = max(-1, min(_sell_level[0], len(lvls) - 2))
    _submit_limit("SELL", price=lvls[lv + 1])


def _submit_sell_level_index(level: int) -> None:
    """Submit a SELL limit at a fixed expanded ask level (0 = best, 1 = next gap-fill, …)."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        book = _orderbooks.get(aid)
    if not book:
        return
    asks  = sorted([(float(p), s) for p, s in book.get("asks", {}).items() if s > 0],
                   key=lambda x: x[0])
    tick  = float(book.get("min_tick_size", 0.01) or 0.01)
    lvls  = _build_ask_levels(asks, tick)
    idx   = level + 1
    if not lvls or idx >= len(lvls):
        _set_status(f"No ask level {level} available", (255, 180, 60))
        return
    _submit_limit("SELL", price=lvls[idx])


def _set_sort(mode: str) -> None:
    _sort_mode[0] = mode
    dpg.configure_item("sort_vol_btn", label="[Volume]"  if mode == "volume"   else "Volume")
    dpg.configure_item("sort_act_btn", label="[Activity]" if mode == "activity" else "Activity")
    _prev_market_ids.clear()  # force list rebuild on next frame


def _on_mkt_filter_change(text: str) -> None:
    _mkt_filter[0] = text
    _prev_market_ids.clear()  # force list rebuild on next frame


def _switch_to_paired() -> None:
    """Swap the orderbook display to the paired token of the currently viewed asset."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        cur = _orderbooks.get(aid)
    if not cur:
        return
    paired_id = cur.get("paired_asset_id")
    if paired_id and paired_id in _orderbooks:
        _set_selected_market(paired_id)


def _build_ui() -> None:
    global _market_btn_default_theme, _market_btn_held_theme

    with dpg.theme() as _market_btn_default_theme:
        with dpg.theme_component(dpg.mvButton):
            dpg.add_theme_color(dpg.mvThemeCol_Text, (200, 200, 200))

    with dpg.theme() as _market_btn_held_theme:
        with dpg.theme_component(dpg.mvButton):
            dpg.add_theme_color(dpg.mvThemeCol_Text, (80, 210, 110))

    with dpg.window(tag="root", no_title_bar=True, no_move=True,
                    no_resize=True, no_scrollbar=True, no_close=True):

        # ── Header bar ────────────────────────────────────────────────────────
        # Two-column table: left cell = stats (stretches), right cell = settings btn (fixed)
        with dpg.table(header_row=False, borders_innerV=False,
                       borders_outerV=False, borders_outerH=False,
                       policy=dpg.mvTable_SizingFixedFit):
            dpg.add_table_column(width_stretch=True)
            dpg.add_table_column(width_fixed=True, init_width_or_weight=36)
            with dpg.table_row():
                with dpg.group(horizontal=True):
                    dpg.add_text("DPG PolyTerminal", color=(180, 200, 255))
                    dpg.add_spacer(width=20)
                    dpg.add_text("Connecting...", tag="hdr_status",  color=(140, 140, 140))
                    dpg.add_spacer(width=20)
                    dpg.add_text("—",           tag="hdr_rate",    color=(140, 140, 140))
                    dpg.add_spacer(width=20)
                    dpg.add_text("—",           tag="hdr_count",   color=(140, 140, 140))
                    dpg.add_spacer(width=20)
                    dpg.add_text("latency: —",  tag="hdr_latency", color=(140, 140, 140))
                    dpg.add_spacer(width=20)
                    dpg.add_text("userWS:", color=(140, 140, 140))
                    dpg.add_text("—", tag="hdr_user_ws", color=(140, 140, 140))
                dpg.add_button(
                    label="\uf013", tag="settings_btn", width=32, height=22,
                    callback=lambda: dpg.configure_item("settings_modal", show=True),
                )
        dpg.add_separator()

        # ── Three-column resizable layout ────────────────────────────────────
        # push_container_stack lets the child_windows below be table cells without
        # re-indenting their contents.
        _tbl = dpg.add_table(
            header_row=True, resizable=True,
            borders_innerV=True, borders_outerV=False,
            borders_outerH=False, borders_innerH=False,
            policy=dpg.mvTable_SizingFixedFit,
        )
        dpg.add_table_column(parent=_tbl, label="MARKETS",     width_fixed=True,  init_width_or_weight=310)
        dpg.add_table_column(parent=_tbl, label="ORDERBOOK",   width_stretch=True)
        dpg.add_table_column(parent=_tbl, label="ORDER TICKET", width_fixed=True,  init_width_or_weight=410)
        dpg.push_container_stack(dpg.add_table_row(parent=_tbl))

        if True:  # indent block — aligns visually; no runtime effect

            # Left: scrollable market list
            with dpg.child_window(tag="mkt_list", width=-1, height=-1, border=True):
                with dpg.group(horizontal=True):
                    dpg.add_text("Sort:", color=(140, 140, 140))
                    dpg.add_button(label="[Volume]", tag="sort_vol_btn",
                                   callback=lambda: _set_sort("volume"), small=True)
                    dpg.add_button(label="Activity", tag="sort_act_btn",
                                   callback=lambda: _set_sort("activity"), small=True)
                dpg.add_separator()
                with dpg.group(horizontal=True):
                    dpg.add_input_text(
                        tag="mkt_filter_input", hint="Filter markets…",
                        width=-28, callback=lambda _, d: _on_mkt_filter_change(d),
                    )
                    dpg.add_button(
                        label="×", width=22,
                        callback=lambda: (_on_mkt_filter_change(""),
                                          dpg.set_value("mkt_filter_input", "")),
                    )
                dpg.add_separator()
                dpg.add_text("Loading markets...", tag="mkt_placeholder")

            # Centre: orderbook depth
            with dpg.child_window(tag="book_depth", width=-1, height=-1, border=True):

                # ── Market identity ───────────────────────────────────────────
                with dpg.group(horizontal=True):
                    dpg.add_text("", tag="book_event", color=(210, 210, 100), wrap=0)
                    dpg.add_spacer(width=8)
                    dpg.add_button(
                        label="Open", tag="book_pm_btn", small=True,
                        callback=lambda: webbrowser.open(
                            f"https://polymarket.com/event/{_current_slug[0]}"
                        ) if _current_slug[0] else None,
                    )
                    dpg.add_button(
                        label="\uf35b", tag="book_pm_icon", small=True,
                        callback=lambda: webbrowser.open(
                            f"https://polymarket.com/event/{_current_slug[0]}"
                        ) if _current_slug[0] else None,
                    )
                dpg.add_text("", tag="book_question", color=(200, 200, 200), wrap=-1)

                # ── Meta / stats info table ───────────────────────────────────
                _LBL = (120, 120, 120)
                _VAL = (200, 200, 200)
                # Transparent button theme — makes paired ID/outcome look like clickable text
                with dpg.theme() as _link_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        (0,   0,   0,   0))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (60,  60, 120,  80))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  (80,  80, 160, 120))
                        dpg.add_theme_color(dpg.mvThemeCol_Text,          (160, 160, 200))
                with dpg.table(header_row=False,
                               borders_innerV=False, borders_outerV=False,
                               borders_outerH=False, borders_innerH=False,
                               policy=dpg.mvTable_SizingFixedFit):
                    dpg.add_table_column(width_fixed=True, init_width_or_weight=110)
                    dpg.add_table_column(width_stretch=True)
                    dpg.add_table_column(width_fixed=True, init_width_or_weight=110)
                    dpg.add_table_column(width_stretch=True)

                    with dpg.table_row():
                        dpg.add_text("Asset ID",       color=_LBL)
                        dpg.add_text("—", tag="meta_asset_id",        color=_VAL)
                        dpg.add_text("Outcome",        color=_LBL)
                        dpg.add_text("—", tag="meta_outcome",         color=_VAL)
                    with dpg.table_row():
                        dpg.add_text("Paired ID",      color=_LBL)
                        _pid_btn = dpg.add_button(label="—", tag="meta_paired_id",
                                                  small=True, callback=_switch_to_paired)
                        dpg.bind_item_theme(_pid_btn, _link_theme)
                        dpg.add_text("Paired outcome", color=_LBL)
                        _poc_btn = dpg.add_button(label="—", tag="meta_paired_outcome",
                                                  small=True, callback=_switch_to_paired)
                        dpg.bind_item_theme(_poc_btn, _link_theme)
                    with dpg.table_row():
                        dpg.add_text("Event slug",    color=_LBL)
                        dpg.add_text("—", tag="meta_event_slug",   color=_VAL)
                        dpg.add_text("Tick size",     color=_LBL)
                        dpg.add_text("—", tag="meta_tick_size",    color=_VAL)
                    with dpg.table_row():
                        dpg.add_text("Neg risk",      color=_LBL)
                        dpg.add_text("—", tag="meta_neg_risk",     color=_VAL)
                        dpg.add_text("Game start",    color=_LBL)
                        dpg.add_text("—", tag="meta_game_start",   color=_VAL)
                    with dpg.table_row():
                        dpg.add_text("Volume",        color=_LBL)
                        dpg.add_text("—", tag="meta_volume",       color=_VAL)
                        dpg.add_text("24h Volume",    color=_LBL)
                        dpg.add_text("—", tag="meta_vol24",        color=_VAL)
                    with dpg.table_row():
                        dpg.add_text("Liquidity",     color=_LBL)
                        dpg.add_text("—", tag="meta_liquidity",    color=_VAL)
                        dpg.add_text("Last trade",    color=_LBL)
                        dpg.add_text("—", tag="book_ltp",          color=(255, 220, 100))

                dpg.add_separator()

                # Active outcome banner + authenticated position (same row)
                with dpg.group(horizontal=True):
                    dpg.add_text("", tag="book_viewing_outcome", color=(255, 200, 80))
                    dpg.add_spacer(width=16)
                    # Position chip — hidden when no position is held
                    dpg.add_text("", tag="book_position", color=(60, 210, 100), show=False)

                with dpg.child_window(tag="book_depth_scroll", width=-1, height=-1, border=False):
                    _OWN_CLR = (80, 210, 220)   # cyan — resting order size
                    _TTL_CLR = (55, 160, 175)   # dimmer cyan — time remaining
    
                    def _book_table_columns():
                        dpg.add_table_column(label="Own",   width_fixed=True, init_width_or_weight=52)
                        dpg.add_table_column(label="TTL",   width_fixed=True, init_width_or_weight=44)
                        dpg.add_table_column(label="Price", width_fixed=True, init_width_or_weight=95)
                        dpg.add_table_column(label="Size",  width_fixed=True, init_width_or_weight=90)
                        dpg.add_table_column(label="Depth", width_stretch=True)
    
                    # ── Split layout (default) ────────────────────────────────────
                    with dpg.group(tag="grp_split_tables"):
                        dpg.add_text("ASKS  (sell side)", tag="lbl_asks", color=(255, 80, 80))
                        with dpg.table(tag="tbl_asks", header_row=True,
                                       borders_innerV=True, borders_outerH=True,
                                       borders_outerV=True, row_background=True,
                                       policy=dpg.mvTable_SizingFixedFit):
                            _book_table_columns()
                            for i in range(D):
                                with dpg.table_row():
                                    dpg.add_text("", tag=_ask_own[i],   color=_OWN_CLR)
                                    dpg.add_text("", tag=_ask_ttl[i],   color=_TTL_CLR)
                                    dpg.add_text("", tag=_ask_price[i], color=(255, 110, 110))
                                    dpg.add_text("", tag=_ask_size[i])
                                    _add_depth_bar("ask", tag=_ask_bar[i])
    
                        dpg.add_separator()
                        dpg.add_text("", tag="spread_line", color=(220, 220, 80))
                        dpg.add_separator()
    
                        dpg.add_text("BIDS  (buy side)", tag="lbl_bids", color=(80, 200, 80))
                        with dpg.table(tag="tbl_bids", header_row=True,
                                       borders_innerV=True, borders_outerH=True,
                                       borders_outerV=True, row_background=True,
                                       policy=dpg.mvTable_SizingFixedFit):
                            _book_table_columns()
                            for i in range(D):
                                with dpg.table_row():
                                    dpg.add_text("", tag=_bid_own[i],   color=_OWN_CLR)
                                    dpg.add_text("", tag=_bid_ttl[i],   color=_TTL_CLR)
                                    dpg.add_text("", tag=_bid_price[i], color=(80,  210, 80))
                                    dpg.add_text("", tag=_bid_size[i])
                                    _add_depth_bar("bid", tag=_bid_bar[i])
    
                    # ── Combined layout (asks above spread row above bids) ────────
                    # Rows are created dynamically in _update_book_display based on actual data
                    with dpg.group(tag="grp_combined_table", show=False):
                        dpg.add_text("", tag="spread_line_c", color=(220, 220, 80))
                        with dpg.table(tag="tbl_combined", header_row=True,
                                       borders_innerV=True, borders_outerH=True,
                                       borders_outerV=True, row_background=True,
                                       policy=dpg.mvTable_SizingFixedFit):
                            _book_table_columns()

            # Right: order ticket
            with dpg.child_window(tag="order_panel", width=-1, height=-1, border=True):

                dpg.add_text("ORDER TICKET", color=(210, 210, 100))
                dpg.add_separator()

                # Button themes — styled to feel like physical buttons:
                #   bright border = "raised edge highlight"; dark active = "pressed in"
                with dpg.theme() as _buy_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        ( 28, 115,  28))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, ( 50, 170,  50))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  (  8,  50,   8))
                        dpg.add_theme_color(dpg.mvThemeCol_Border,        ( 80, 200,  80))
                        dpg.add_theme_style(dpg.mvStyleVar_FrameBorderSize, 1)
                        dpg.add_theme_style(dpg.mvStyleVar_FrameRounding,   4)
                        dpg.add_theme_style(dpg.mvStyleVar_FramePadding,    6, 5)
                with dpg.theme() as _sell_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        (125,  25,  25))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (180,  45,  45))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  ( 60,  10,  10))
                        dpg.add_theme_color(dpg.mvThemeCol_Border,        (210,  75,  75))
                        dpg.add_theme_style(dpg.mvStyleVar_FrameBorderSize, 1)
                        dpg.add_theme_style(dpg.mvStyleVar_FrameRounding,   4)
                        dpg.add_theme_style(dpg.mvStyleVar_FramePadding,    6, 5)

                # Inputs
                with dpg.group(horizontal=True):
                    dpg.add_text("Size  ", color=(160, 160, 160))
                    dpg.add_input_float(
                        tag="order_size", width=-1, step=0, format="%.2f",
                        default_value=10.0, min_value=0.01, min_clamped=True,
                    )
                with dpg.group(horizontal=True):
                    dpg.add_text("TTL   ", color=(160, 160, 160))
                    dpg.add_input_int(
                        tag="order_ttl", width=-1, step=0,
                        default_value=0, min_value=0, min_clamped=True,
                    )
                dpg.add_text("(TTL in seconds; 0 = GTC)", color=(90, 90, 90))
                dpg.add_text("(Size in $ for market orders)", color=(90, 90, 90))
                dpg.add_separator()

                # Limit order buttons — label updates live with depth level
                dpg.add_text("LIMIT", color=(160, 160, 160))

                # Combined level controls (always one row)
                with dpg.group(horizontal=True):
                    dpg.add_text("Bid:", color=(120, 120, 120))
                    dpg.add_button(label="-", tag="buy_lv_dec", small=True,
                                   callback=_dec_buy_level)
                    dpg.add_text("0", tag="buy_lv_txt", color=(210, 210, 210))
                    dpg.add_button(label="+", tag="buy_lv_inc", small=True,
                                   callback=_inc_buy_level)
                    dpg.add_spacer(width=10)
                    dpg.add_text("Ask:", color=(120, 120, 120))
                    dpg.add_button(label="-", tag="sell_lv_dec", small=True,
                                   callback=_dec_sell_level)
                    dpg.add_text("0", tag="sell_lv_txt", color=(210, 210, 210))
                    dpg.add_button(label="+", tag="sell_lv_inc", small=True,
                                   callback=_inc_sell_level)

                # Narrow layout — BUY then SELL stacked vertically
                with dpg.group(tag="grp_limit_vert"):
                    _bl = dpg.add_button(label="BUY  @ —", tag="btn_buy_limit",
                                         width=-1, height=50, callback=_submit_buy_level)
                    dpg.bind_item_theme(_bl, _buy_theme)
                    _sl = dpg.add_button(label="SELL @ —", tag="btn_sell_limit",
                                         width=-1, height=50, callback=_submit_sell_level)
                    dpg.bind_item_theme(_sl, _sell_theme)

                # Wide layout — BUY | SELL side by side (shown when panel >= 400 px)
                with dpg.group(tag="grp_limit_horiz", show=False):
                    with dpg.table(header_row=False, borders_innerV=False,
                                   policy=dpg.mvTable_SizingStretchSame):
                        dpg.add_table_column()
                        dpg.add_table_column()
                        with dpg.table_row():
                            _bl_h = dpg.add_button(label="BUY  @ —", tag="btn_buy_limit_h",
                                                   width=-1, height=50,
                                                   callback=_submit_buy_level)
                            dpg.bind_item_theme(_bl_h, _buy_theme)
                            _sl_h = dpg.add_button(label="SELL @ —", tag="btn_sell_limit_h",
                                                   width=-1, height=50,
                                                   callback=_submit_sell_level)
                            dpg.bind_item_theme(_sl_h, _sell_theme)
                with dpg.group(horizontal=True):
                    _rb = dpg.add_button(
                        label=(
                            f"REPEAT BID @ {_repeat_bid_settings['price']:.2f}  "
                            f"TTL {_repeat_bid_settings['ttl_secs']}s / "
                            f"{_repeat_bid_settings['repeat_interval_secs']}s"
                        ),
                        tag="btn_repeat_bid",
                        width=-46,
                        height=40,
                        callback=_toggle_repeat_bid,
                    )
                    dpg.bind_item_theme(_rb, _buy_theme)
                    _rbs = dpg.add_button(
                        label="SET",
                        tag="btn_repeat_bid_settings",
                        width=40,
                        height=40,
                        callback=_open_repeat_bid_settings,
                    )
                dpg.add_separator()

                # Market order buttons
                dpg.add_text("MARKET", color=(160, 160, 160))

                # Narrow layout
                with dpg.group(tag="grp_mkt_vert"):
                    _bm = dpg.add_button(label="MKT BUY",  tag="btn_mkt_buy",
                                         width=-1, callback=lambda: _submit_market("BUY"))
                    dpg.bind_item_theme(_bm, _buy_theme)
                    _sm = dpg.add_button(label="MKT SELL", tag="btn_mkt_sell",
                                         width=-1, callback=lambda: _submit_market("SELL"))
                    dpg.bind_item_theme(_sm, _sell_theme)

                # Wide layout
                with dpg.group(tag="grp_mkt_horiz", show=False):
                    with dpg.table(header_row=False, borders_innerV=False,
                                   policy=dpg.mvTable_SizingStretchSame):
                        dpg.add_table_column()
                        dpg.add_table_column()
                        with dpg.table_row():
                            _bm_h = dpg.add_button(label="MKT BUY",  tag="btn_mkt_buy_h",
                                                   width=-1,
                                                   callback=lambda: _submit_market("BUY"))
                            dpg.bind_item_theme(_bm_h, _buy_theme)
                            _sm_h = dpg.add_button(label="MKT SELL", tag="btn_mkt_sell_h",
                                                   width=-1,
                                                   callback=lambda: _submit_market("SELL"))
                            dpg.bind_item_theme(_sm_h, _sell_theme)
                dpg.add_separator()

                # Cancel all — danger button (orange)
                with dpg.theme() as _cancel_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        (160,  80,   0))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (215, 115,   0))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  ( 90,  42,   0))
                        dpg.add_theme_color(dpg.mvThemeCol_Border,        (240, 150,  30))
                        dpg.add_theme_style(dpg.mvStyleVar_FrameBorderSize, 1)
                        dpg.add_theme_style(dpg.mvStyleVar_FrameRounding,   4)
                        dpg.add_theme_style(dpg.mvStyleVar_FramePadding,    6, 5)
                _ca = dpg.add_button(label="CANCEL ALL ORDERS", tag="btn_cancel_all",
                                     width=-1, height=50, callback=_submit_cancel_all)
                dpg.bind_item_theme(_ca, _cancel_theme)
                dpg.add_separator()

                # Status themes for input_text (color via theme, not configure_item)
                for _col in [(130,130,130),(80,220,80),(220,220,60),(255,180,60),(255,80,80)]:
                    with dpg.theme() as _t:
                        with dpg.theme_component(dpg.mvInputText):
                            dpg.add_theme_color(dpg.mvThemeCol_Text, _col)
                            dpg.add_theme_color(dpg.mvThemeCol_FrameBg, (30, 30, 30, 0))
                    _status_theme[_col] = _t

                # Selectable status line + copy button
                with dpg.group(horizontal=True):
                    _si = dpg.add_input_text(
                        tag="order_status", default_value="—",
                        readonly=True, width=-34,
                    )
                    dpg.bind_item_theme(_si, _status_theme[(130, 130, 130)])
                    _cb = dpg.add_button(label="\uf0c5", tag="copy_btn", width=28, small=True,
                                         callback=_copy_status_to_clipboard)
                    with dpg.tooltip(_cb):
                        dpg.add_text("Copy to clipboard")

        dpg.pop_container_stack()  # release table_row as active parent

        with dpg.window(
            tag="repeat_bid_settings_modal", label="Repeat Bid Settings",
            modal=True, show=False, no_move=False, no_resize=False,
            width=360, height=260, pos=[520, 180],
            on_close=lambda: dpg.configure_item("repeat_bid_settings_modal", show=False),
        ):
            _repeat_lbl_dim = (120, 120, 120)
            dpg.add_text("Recurring bid configuration", color=(210, 210, 100))
            dpg.add_spacer(height=8)
            with dpg.group(horizontal=True):
                dpg.add_text("Price", color=_repeat_lbl_dim)
                dpg.add_input_float(
                    tag="repeat_bid_price",
                    width=-1,
                    step=0,
                    format="%.4f",
                    default_value=float(_repeat_bid_settings["price"]),
                    min_value=0.001,
                    max_value=0.999,
                    min_clamped=True,
                    max_clamped=True,
                )
            with dpg.group(horizontal=True):
                dpg.add_text("Size ", color=_repeat_lbl_dim)
                dpg.add_input_float(
                    tag="repeat_bid_size",
                    width=-1,
                    step=0,
                    format="%.2f",
                    default_value=float(_repeat_bid_settings["size"]),
                    min_value=0.01,
                    min_clamped=True,
                )
            with dpg.group(horizontal=True):
                dpg.add_text("TTL  ", color=_repeat_lbl_dim)
                dpg.add_input_int(
                    tag="repeat_bid_ttl",
                    width=-1,
                    step=0,
                    default_value=int(_repeat_bid_settings["ttl_secs"]),
                    min_value=1,
                    min_clamped=True,
                )
            with dpg.group(horizontal=True):
                dpg.add_text("Every", color=_repeat_lbl_dim)
                dpg.add_input_int(
                    tag="repeat_bid_interval",
                    width=-1,
                    step=0,
                    default_value=int(_repeat_bid_settings["repeat_interval_secs"]),
                    min_value=1,
                    min_clamped=True,
                )
            dpg.add_text("TTL and repeat interval are in seconds.", color=_repeat_lbl_dim)
            dpg.add_spacer(height=12)
            with dpg.group(horizontal=True):
                dpg.add_button(label="Save", width=100, callback=_save_repeat_bid_settings)
                dpg.add_button(
                    label="Cancel",
                    width=100,
                    callback=lambda: dpg.configure_item("repeat_bid_settings_modal", show=False),
                )

        # ── Settings modal ────────────────────────────────────────────────────
        _LBL_DIM = (120, 120, 120)
        with dpg.window(
            tag="settings_modal", label="Settings",
            modal=True, show=False, no_move=False, no_resize=False,
            width=760, height=620, pos=[380, 100],
            on_close=lambda: dpg.configure_item("settings_modal", show=False),
        ):
            with dpg.tab_bar():

                # ── Display tab ───────────────────────────────────────────────
                with dpg.tab(label="Display"):
                    dpg.add_spacer(height=12)

                    def _on_compact_level(_, value):
                        _display_settings["compact_level"] = _normalize_compact_level(value)
                        _apply_display_settings()
                        _save_settings()

                    dpg.add_text("Compactness level", color=_LBL_DIM)
                    dpg.add_spacer(height=4)
                    dpg.add_slider_int(
                        label="0 = off, 1 = compact, 2 = extra compact",
                        tag="disp_compact_level",
                        default_value=_display_settings["compact_level"],
                        min_value=_COMPACT_LEVELS[0],
                        max_value=_COMPACT_LEVELS[-1],
                        width=260,
                        callback=_on_compact_level,
                    )

                    dpg.add_spacer(height=14)
                    dpg.add_separator()
                    dpg.add_spacer(height=10)

                    dpg.add_text("Orderbook row font size", color=_LBL_DIM)
                    dpg.add_spacer(height=4)

                    def _on_book_font_slider(_, value):
                        # Slider gives a float; snap to nearest valid size
                        snapped = min(_BOOK_FONT_SIZES, key=lambda s: abs(s - value))
                        _display_settings["book_font_size"] = snapped
                        dpg.set_value("disp_book_font_slider", snapped)
                        _apply_display_settings()
                        _save_settings()

                    dpg.add_slider_int(
                        tag="disp_book_font_slider",
                        default_value=_display_settings["book_font_size"],
                        min_value=_BOOK_FONT_SIZES[0],
                        max_value=_BOOK_FONT_SIZES[-1],
                        width=260,
                        callback=_on_book_font_slider,
                    )
                    dpg.add_text(
                        f"({_BOOK_FONT_SIZES[0]}–{_BOOK_FONT_SIZES[-1]} pt,  "
                        f"default 13 pt)",
                        color=_LBL_DIM,
                    )

                    dpg.add_spacer(height=14)
                    dpg.add_separator()
                    dpg.add_spacer(height=10)

                    def _on_combined_toggle(_, value):
                        _display_settings["combined_table"] = value
                        _apply_display_settings()
                        _save_settings()

                    dpg.add_checkbox(
                        label="Combined table  (asks + bids in one continuous table)",
                        tag="disp_combined_chk",
                        default_value=_display_settings["combined_table"],
                        callback=_on_combined_toggle,
                    )

                    dpg.add_spacer(height=14)
                    dpg.add_separator()
                    dpg.add_spacer(height=10)

                    def _on_zero_prices_toggle(_, value):
                        _display_settings["show_zero_prices"] = value
                        _save_settings()
                        _prev_market_ids.clear()  # force book display refresh

                    dpg.add_checkbox(
                        label="Show prices with size 0  (fills gaps, shows all 0.01–0.99)",
                        tag="disp_zero_prices_chk",
                        default_value=_display_settings["show_zero_prices"],
                        callback=_on_zero_prices_toggle,
                    )

                # ── Sound tab ─────────────────────────────────────────────────
                with dpg.tab(label="Misc"):
                    dpg.add_spacer(height=12)
                    dpg.add_text("Max Order Size for Depth Bars", color=_LBL_DIM)
                    dpg.add_spacer(height=4)

                    def _on_depth_bar_cap(_, value):
                        _display_settings["depth_bar_max_size"] = _normalize_depth_bar_cap(value)
                        dpg.set_value("misc_depth_bar_max_size", _display_settings["depth_bar_max_size"])
                        _save_settings()

                    dpg.add_input_float(
                        tag="misc_depth_bar_max_size",
                        default_value=_display_settings["depth_bar_max_size"],
                        min_value=1.0,
                        min_clamped=True,
                        step=0,
                        width=260,
                        format="%.0f",
                        callback=_on_depth_bar_cap,
                    )
                    dpg.add_text(
                        "Bars clamp at this size so outsized levels do not flatten smaller ones.",
                        color=_LBL_DIM,
                        wrap=520,
                    )

                with dpg.tab(label="Sound"):
                    dpg.add_spacer(height=12)

                    def _on_sound_muted(_, value):
                        _sound_settings["muted"] = bool(value)
                        _save_settings()

                    def _on_sound_volume(_, value):
                        _sound_settings["volume"] = _normalize_sound_volume(value)
                        dpg.set_value("sound_volume_slider", _sound_settings["volume"])
                        _save_settings()

                    dpg.add_checkbox(
                        label="Mute sounds",
                        tag="sound_muted_chk",
                        default_value=bool(_sound_settings["muted"]),
                        callback=_on_sound_muted,
                    )
                    dpg.add_spacer(height=14)
                    dpg.add_text("Fill sound volume", color=_LBL_DIM)
                    dpg.add_spacer(height=4)
                    dpg.add_slider_int(
                        tag="sound_volume_slider",
                        default_value=_normalize_sound_volume(_sound_settings["volume"]),
                        min_value=0,
                        max_value=100,
                        width=260,
                        callback=_on_sound_volume,
                    )

                # ── Hotkeys tab ───────────────────────────────────────────────
                with dpg.tab(label="Hotkeys"):
                    dpg.add_spacer(height=8)
                    dpg.add_text(
                        "Click Assign, then press any key. Esc cancels the assignment.",
                        color=_LBL_DIM,
                        wrap=-1,
                    )
                    dpg.add_spacer(height=6)
                    with dpg.table(
                        header_row=True,
                        borders_innerV=True, borders_outerH=True,
                        borders_outerV=True, row_background=True,
                        policy=dpg.mvTable_SizingStretchProp,
                    ):
                        dpg.add_table_column(label="Action", width_stretch=True,
                                             init_width_or_weight=2.6)
                        dpg.add_table_column(label="Key",    width_fixed=True,
                                             init_width_or_weight=95)
                        dpg.add_table_column(label="",       width_fixed=True,
                                             init_width_or_weight=170)

                        for _action, _lbl in _HOTKEY_LABELS.items():
                            with dpg.table_row():
                                dpg.add_text(_lbl, wrap=-1)
                                dpg.add_text("—", tag=f"hk_key_{_action}",
                                             color=(220, 210, 100))
                                with dpg.group(horizontal=True):
                                    dpg.add_button(
                                        label="Assign", tag=f"hk_btn_{_action}",
                                        small=True,
                                        callback=_make_hk_assign_cb(_action),
                                    )
                                    dpg.add_button(
                                        label="Clear", tag=f"hk_clr_{_action}",
                                        small=True,
                                        callback=_make_hk_clear_cb(_action),
                                    )


def _generate_all_prices(tick: float) -> list[float]:
    """Generate all possible prices from tick to 0.99 (or 0.999 for 3-decimal) in ascending order."""
    if tick >= 0.01 - 1e-9:  # 2-decimal market
        return [round(0.01 + i * 0.01, 4) for i in range(99)]
    else:  # 3-decimal market
        return [round(0.001 + i * 0.001, 4) for i in range(999)]


def _populate_full_price_grid(book_data: dict, own_asks: dict, own_bids: dict, now_ts: float) -> None:
    """Populate split tables with a price-aware grid, showing 0 for empty levels.
    Used when show_zero_prices is True. Dynamically creates rows.

    - tbl_asks: prices from 0.99 down to best_ask (ask colors), then spread zone
                (best_ask-tick down to best_bid+tick) with white price color and size 0.
    - tbl_bids: prices from best_bid down to 0.01 (bid colors), size 0 for empty levels.
    - Prices below best_ask are never shown in tbl_asks; prices above best_bid never in tbl_bids.
    """
    _OWN_CLR = (80, 210, 220)
    _TTL_CLR = (55, 160, 175)
    _ASK_DEFAULT = (255, 110, 110)
    _BID_DEFAULT = (80, 210, 80)
    _SPREAD_CLR  = (220, 220, 220)   # white-ish for spread-zone prices
    _OWN_HI = (80, 210, 220)
    _OWN_ROW_BG = (30, 90, 40, 110)

    raw_bids = book_data.get("bids", {})
    raw_asks = book_data.get("asks", {})
    tick = float(book_data.get("min_tick_size", 0.01) or 0.01)

    bid_map: dict[float, float] = {round(float(p), 4): float(s) for p, s in raw_bids.items() if float(s) > 0}
    ask_map: dict[float, float] = {round(float(p), 4): float(s) for p, s in raw_asks.items() if float(s) > 0}

    all_prices = _generate_all_prices(tick)  # ascending list
    best_ask = min(ask_map.keys()) if ask_map else round(all_prices[-1], 4)
    best_bid = max(bid_map.keys()) if bid_map else round(all_prices[0], 4)
    depth_scale_max = _compute_depth_bar_scale(*ask_map.values(), *bid_map.values())

    # ── Partition into zones ──────────────────────────────────────────────────
    ask_prices    = [p for p in all_prices if p >= best_ask]   # ask zone
    spread_prices = [p for p in all_prices if best_bid < p < best_ask]  # spread zone
    bid_prices    = [p for p in all_prices if p <= best_bid]   # bid zone

    # Delete all rows from both tables
    for tbl in ("tbl_asks", "tbl_bids"):
        for item in dpg.get_item_children(tbl, 1):
            dpg.delete_item(item)

    # ── Asks table: ask zone (highest first) then spread zone ────────────────
    row_i = 0
    for p in reversed(ask_prices):
        s = ask_map.get(p, 0.0)
        with dpg.table_row(parent="tbl_asks"):
            own = own_asks.get(p)
            if own:
                own_sz, own_exp = own
                dpg.add_text(f"{own_sz:.1f}", color=_OWN_CLR)
                dpg.add_text(_format_ttl(own_exp, now_ts) if own_exp > 0 else "", color=_TTL_CLR)
                price_color = _OWN_HI
            else:
                dpg.add_text("", color=_OWN_CLR)
                dpg.add_text("", color=_TTL_CLR)
                price_color = _ASK_DEFAULT
            dpg.add_text(f"{p:.4f}", color=price_color)
            dpg.add_text(f"{s:>8.1f}")
            bar_item = _add_depth_bar("ask")
            _set_depth_bar(bar_item, s, depth_scale_max)
            if own:
                try:
                    dpg.highlight_table_row("tbl_asks", row_i, _OWN_ROW_BG)
                except Exception:
                    pass
        row_i += 1

    for p in reversed(spread_prices):   # spread zone: highest first
        with dpg.table_row(parent="tbl_asks"):
            dpg.add_text("", color=_OWN_CLR)
            dpg.add_text("", color=_TTL_CLR)
            dpg.add_text(f"{p:.4f}", color=_SPREAD_CLR)
            dpg.add_text(f"{0.0:>8.1f}")
            _add_empty_depth_cell()
        row_i += 1

    # ── Bids table: bid zone (best bid first) ─────────────────────────────────
    for i, p in enumerate(reversed(bid_prices)):
        s = bid_map.get(p, 0.0)
        with dpg.table_row(parent="tbl_bids"):
            own = own_bids.get(p)
            if own:
                own_sz, own_exp = own
                dpg.add_text(f"{own_sz:.1f}", color=_OWN_CLR)
                dpg.add_text(_format_ttl(own_exp, now_ts) if own_exp > 0 else "", color=_TTL_CLR)
                price_color = _OWN_HI
            else:
                dpg.add_text("", color=_OWN_CLR)
                dpg.add_text("", color=_TTL_CLR)
                price_color = _BID_DEFAULT
            dpg.add_text(f"{p:.4f}", color=price_color)
            dpg.add_text(f"{s:>8.1f}")
            bar_item = _add_depth_bar("bid")
            _set_depth_bar(bar_item, s, depth_scale_max)
            if own:
                try:
                    dpg.highlight_table_row("tbl_bids", i, _OWN_ROW_BG)
                except Exception:
                    pass


def _populate_combined_table_with_all_prices(book_data: dict, own_asks: dict, own_bids: dict,
                                            now_ts: float) -> None:
    """Populate combined table with a price-aware grid, showing 0 for empty levels.
    Used when combined_table=True and show_zero_prices=True.

    Layout: ask zone to best ask, then spread zone, then bid zone.
    """
    _OWN_CLR     = (80, 210, 220)
    _TTL_CLR     = (55, 160, 175)
    _ASK_DEFAULT = (255, 110, 110)
    _BID_DEFAULT = (80, 210, 80)
    _SPREAD_CLR  = (220, 220, 220)
    _OWN_HI      = (80, 210, 220)
    _OWN_ROW_BG  = (30, 90, 40, 110)

    raw_bids = book_data.get("bids", {})
    raw_asks = book_data.get("asks", {})
    tick = float(book_data.get("min_tick_size", 0.01) or 0.01)

    bid_map: dict[float, float] = {round(float(p), 4): float(s) for p, s in raw_bids.items() if float(s) > 0}
    ask_map: dict[float, float] = {round(float(p), 4): float(s) for p, s in raw_asks.items() if float(s) > 0}

    all_prices = _generate_all_prices(tick)  # ascending
    best_ask = min(ask_map.keys()) if ask_map else round(all_prices[-1], 4)
    best_bid = max(bid_map.keys()) if bid_map else round(all_prices[0], 4)
    mid    = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid
    depth_scale_max = _compute_depth_bar_scale(*ask_map.values(), *bid_map.values())

    ask_prices    = [p for p in all_prices if p >= best_ask]
    spread_prices = [p for p in all_prices if best_bid < p < best_ask]
    bid_prices    = [p for p in all_prices if p <= best_bid]

    # Delete all existing rows (keep header)
    for item in dpg.get_item_children("tbl_combined", 1):
        dpg.delete_item(item)

    row_idx = 0

    # ── Ask zone (highest price first) ────────────────────────────────────────
    for p in reversed(ask_prices):
        s = ask_map.get(p, 0.0)
        with dpg.table_row(parent="tbl_combined"):
            own = own_asks.get(p)
            if own:
                own_sz, own_exp = own
                dpg.add_text(f"{own_sz:.1f}", color=_OWN_CLR)
                dpg.add_text(_format_ttl(own_exp, now_ts) if own_exp > 0 else "", color=_TTL_CLR)
                price_color = _OWN_HI
            else:
                dpg.add_text("", color=_OWN_CLR)
                dpg.add_text("", color=_TTL_CLR)
                price_color = _ASK_DEFAULT
            dpg.add_text(f"{p:.4f}", color=price_color)
            dpg.add_text(f"{s:>8.1f}")
            bar_item = _add_depth_bar("ask")
            _set_depth_bar(bar_item, s, depth_scale_max)
            if own:
                try: dpg.highlight_table_row("tbl_combined", row_idx, _OWN_ROW_BG)
                except Exception: pass
        row_idx += 1

    # ── Spread zone (size 0, white price, no bars) ────────────────────────────
    for p in reversed(spread_prices):
        with dpg.table_row(parent="tbl_combined"):
            dpg.add_text("", color=_OWN_CLR)
            dpg.add_text("", color=_TTL_CLR)
            dpg.add_text(f"{p:.4f}", color=_SPREAD_CLR)
            dpg.add_text(f"{0.0:>8.1f}")
            _add_empty_depth_cell()
        row_idx += 1


    # ── Bid zone (best bid first) ─────────────────────────────────────────────
    for p in reversed(bid_prices):
        s = bid_map.get(p, 0.0)
        with dpg.table_row(parent="tbl_combined"):
            own = own_bids.get(p)
            if own:
                own_sz, own_exp = own
                dpg.add_text(f"{own_sz:.1f}", color=_OWN_CLR)
                dpg.add_text(_format_ttl(own_exp, now_ts) if own_exp > 0 else "", color=_TTL_CLR)
                price_color = _OWN_HI
            else:
                dpg.add_text("", color=_OWN_CLR)
                dpg.add_text("", color=_TTL_CLR)
                price_color = _BID_DEFAULT
            dpg.add_text(f"{p:.4f}", color=price_color)
            dpg.add_text(f"{s:>8.1f}")
            bar_item = _add_depth_bar("bid")
            _set_depth_bar(bar_item, s, depth_scale_max)
            if own:
                try: dpg.highlight_table_row("tbl_combined", row_idx, _OWN_ROW_BG)
                except Exception: pass
        row_idx += 1

    dpg.set_value("spread_line_c",
                  f"  Mid: {mid:.4f}    Spread: {spread:.4f}  ({spread * 100:.2f}%)")


def _populate_combined_table(asks_disp: list, bids: list, own_asks: dict, own_bids: dict,
                            now_ts: float, depth_scale_max: float) -> None:
    """Dynamically create rows in tbl_combined with only the data we need (no empty rows).
    Rows are created with inline content (no pre-allocated tags); ownership highlighting
    uses row indices instead of tag-based coloring.

    Layout: ask rows (highest price first) → spread row → bid rows (best bid first).
    """
    _OWN_CLR = (80, 210, 220)
    _TTL_CLR = (55, 160, 175)
    _ASK_DEFAULT = (255, 110, 110)
    _BID_DEFAULT = (80, 210, 80)
    _OWN_HI = (80, 210, 220)

    # Delete all existing rows (keep header)
    for item in dpg.get_item_children("tbl_combined", 1):  # 1 skips the header row
        dpg.delete_item(item)

    # ── Ask rows (highest price first) ────────────────────────────────────────
    for i, (p, s) in enumerate(asks_disp):
        with dpg.table_row(parent="tbl_combined"):
            own = own_asks.get(_normalize_price_key(p))
            if own:
                own_sz, own_exp = own
                dpg.add_text(f"{own_sz:.1f}", color=_OWN_CLR)
                dpg.add_text(_format_ttl(own_exp, now_ts) if own_exp > 0 else "", color=_TTL_CLR)
                price_color = _OWN_HI
            else:
                dpg.add_text("", color=_OWN_CLR)
                dpg.add_text("", color=_TTL_CLR)
                price_color = _ASK_DEFAULT
            dpg.add_text(f"{p:.4f}", color=price_color)
            dpg.add_text(f"{s:>8.1f}")
            bar_item = _add_depth_bar("ask")
            _set_depth_bar(bar_item, s, depth_scale_max)
            try:
                if own:
                    dpg.highlight_table_row("tbl_combined", i, (30, 90, 40, 110))
            except Exception:
                pass

    # ── Spread separator row ──────────────────────────────────────────────────
    spread = 0.001 if not asks_disp or not bids else asks_disp[0][0] - bids[0][0]
    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks_disp[0][0] if asks_disp else 1.0
    mid = (best_bid + best_ask) / 2.0

    # ── Bid rows (best bid first) ─────────────────────────────────────────────
    for i, (p, s) in enumerate(bids):
        with dpg.table_row(parent="tbl_combined"):
            own = own_bids.get(_normalize_price_key(p))
            if own:
                own_sz, own_exp = own
                dpg.add_text(f"{own_sz:.1f}", color=_OWN_CLR)
                dpg.add_text(_format_ttl(own_exp, now_ts) if own_exp > 0 else "", color=_TTL_CLR)
                price_color = _OWN_HI
            else:
                dpg.add_text("", color=_OWN_CLR)
                dpg.add_text("", color=_TTL_CLR)
                price_color = _BID_DEFAULT
            dpg.add_text(f"{p:.4f}", color=price_color)
            dpg.add_text(f"{s:>8.1f}")
            bar_item = _add_depth_bar("bid")
            _set_depth_bar(bar_item, s, depth_scale_max)
            try:
                if own:
                    dpg.highlight_table_row("tbl_combined", len(asks_disp) + i, (30, 90, 40, 110))
            except Exception:
                pass

    dpg.set_value("spread_line_c",
                  f"  Mid: {mid:.4f}    Spread: {spread:.4f}  ({spread * 100:.2f}%)")


def _rebuild_market_list(ids: list[str]) -> None:
    """Delete existing market buttons and recreate for the current id list."""
    global _prev_market_ids, _mkt_btn_tags

    # Only show primary tokens — paired counterparts are accessible via the swap button
    if _primary_ids:
        ids = [a for a in ids if a in _primary_ids]

    # Apply search filter (case-insensitive substring match on market_question)
    query = _mkt_filter[0].lower().strip()
    if query:
        with _lock:
            ids = [a for a in ids
                   if query in _orderbooks.get(a, {}).get("market_question", "").lower()]

    # Sort ids by selected mode before creating buttons
    now = time.time()
    with _lock:
        snap = {aid: _orderbooks[aid] for aid in ids if aid in _orderbooks}
        positions = dict(_positions)
        if _sort_mode[0] == "activity":
            activity: dict[str, int] = {}
            for aid in ids:
                dq = _asset_activity.get(aid)
                if dq:
                    while dq and now - dq[0] > 10.0:
                        dq.popleft()
                    activity[aid] = len(dq)
                else:
                    activity[aid] = 0
            ids = sorted(ids, key=lambda a: activity.get(a, 0), reverse=True)
        else:
            ids = sorted(ids,
                         key=lambda a: float(snap.get(a, {}).get("volume_24hr", 0)),
                         reverse=True)

    # Remove old buttons
    for tag in _mkt_btn_tags.values():
        if dpg.does_item_exist(tag):
            dpg.delete_item(tag)
    if dpg.does_item_exist("mkt_placeholder"):
        dpg.delete_item("mkt_placeholder")
    _mkt_btn_tags.clear()
    _prev_market_ids = ids[:]

    for aid in ids:
        book  = snap.get(aid, {})
        q     = book.get("market_question", aid)
        label = (q[:44] + "...") if len(q) > 44 else q
        tag   = f"btn_{aid}"
        _mkt_btn_tags[aid] = tag

        def _make_cb(a: str = aid):
            def _cb():
                _set_selected_market(a)
            return _cb

        dpg.add_button(label=label, tag=tag, parent="mkt_list",
                       callback=_make_cb(), width=-1)
        _apply_market_button_style(aid, tag, book, positions)


def _market_has_position(aid: str, book: dict, positions: dict[str, dict]) -> bool:
    """Return True when any token in this binary market is currently held."""
    if aid in positions:
        return True
    paired_id = str(book.get("paired_asset_id", "")).strip()
    return bool(paired_id and paired_id in positions)


def _apply_market_button_style(aid: str, tag: str, book: dict, positions: dict[str, dict]) -> None:
    """Apply the market-list text color based on whether this market is held."""
    if not dpg.does_item_exist(tag):
        return
    theme = _market_btn_held_theme if _market_has_position(aid, book, positions) else _market_btn_default_theme
    if theme:
        dpg.bind_item_theme(tag, theme)


def _refresh_market_button_styles() -> None:
    """Refresh market-list button colors to reflect current holdings."""
    with _lock:
        snap = {aid: _orderbooks.get(aid, {}) for aid in _prev_market_ids}
        positions = dict(_positions)
    for aid, tag in _mkt_btn_tags.items():
        _apply_market_button_style(aid, tag, snap.get(aid, {}), positions)


def _apply_selection_highlight(selected_id: str) -> None:
    """Prefix selected button with >, clear prefix on others.
    When viewing a paired token, highlights its primary counterpart in the list."""
    highlight_id = selected_id
    if selected_id not in _primary_ids:
        with _lock:
            paired_of = _orderbooks.get(selected_id, {}).get("paired_asset_id", "")
        if paired_of in _primary_ids:
            highlight_id = paired_of

    with _lock:
        snap = {aid: _orderbooks.get(aid, {}) for aid in _prev_market_ids}

    for aid, tag in _mkt_btn_tags.items():
        if not dpg.does_item_exist(tag):
            continue
        q    = snap.get(aid, {}).get("market_question", aid)
        base = (q[:42] + "...") if len(q) > 42 else q
        dpg.configure_item(tag, label=("> " + base) if aid == highlight_id else ("  " + base))


def _update_book_display(selected: str) -> None:
    """Refresh all orderbook depth cells for the currently selected asset."""
    if not selected:
        return

    with _lock:
        book  = _orderbooks.get(selected)
        trade = dict(_last_trades.get(selected, {}))

    if not book:
        return

    raw_bids: dict[str, float] = book.get("bids", {})
    raw_asks: dict[str, float] = book.get("asks", {})

    bids = sorted([(float(p), s) for p, s in raw_bids.items() if s > 0],
                  key=lambda x: x[0], reverse=True)[:D]
    asks = sorted([(float(p), s) for p, s in raw_asks.items() if s > 0],
                  key=lambda x: x[0])[:D]

    # ── Live limit-button labels ──────────────────────────────────────────────
    # Build expanded level lists: level -1 = improve-on-best, 0 = best, 1 = gap-fill or next, …
    tick     = float(book.get("min_tick_size", 0.01) or 0.01)
    bid_lvls = _build_bid_levels(bids, tick)
    ask_lvls = _build_ask_levels(asks, tick)

    if bid_lvls:
        _buy_level[0] = max(-1, min(_buy_level[0], len(bid_lvls) - 2))
        buy_p = bid_lvls[_buy_level[0] + 1]
    else:
        _buy_level[0] = 0
        buy_p = None

    if ask_lvls:
        _sell_level[0] = max(-1, min(_sell_level[0], len(ask_lvls) - 2))
        sell_p = ask_lvls[_sell_level[0] + 1]
    else:
        _sell_level[0] = 0
        sell_p = None

    dpg.set_value("buy_lv_txt",  str(_buy_level[0]))
    dpg.set_value("sell_lv_txt", str(_sell_level[0]))

    def _hk(action: str) -> str:
        k = _hotkeys.get(action)
        return f"  ({_key_name(k)})" if k is not None else ""

    _buy_lbl  = f"BUY  @ {buy_p:.4f}{_hk('buy_limit')}"   if buy_p  is not None else f"BUY  @ —{_hk('buy_limit')}"
    _sell_lbl = f"SELL @ {sell_p:.4f}{_hk('sell_limit')}"  if sell_p is not None else f"SELL @ —{_hk('sell_limit')}"
    _mkt_buy_lbl  = f"MKT BUY{_hk('mkt_buy')}"
    _mkt_sell_lbl = f"MKT SELL{_hk('mkt_sell')}"
    _cancel_lbl   = f"CANCEL ALL ORDERS{_hk('cancel_all')}"

    for _tag in ("btn_buy_limit",   "btn_buy_limit_h"):
        if dpg.does_item_exist(_tag):
            dpg.configure_item(_tag, label=_buy_lbl)
    for _tag in ("btn_sell_limit",  "btn_sell_limit_h"):
        if dpg.does_item_exist(_tag):
            dpg.configure_item(_tag, label=_sell_lbl)
    for _tag in ("btn_mkt_buy",     "btn_mkt_buy_h"):
        if dpg.does_item_exist(_tag):
            dpg.configure_item(_tag, label=_mkt_buy_lbl)
    for _tag in ("btn_mkt_sell",    "btn_mkt_sell_h"):
        if dpg.does_item_exist(_tag):
            dpg.configure_item(_tag, label=_mkt_sell_lbl)
    if dpg.does_item_exist("btn_cancel_all"):
        dpg.configure_item("btn_cancel_all", label=_cancel_lbl)

    # ── Own-order price map for this asset ───────────────────────────────────
    now_ts = time.time()
    with _lock:
        own_snap = [o for o in _open_orders.values() if o.get("asset_id") == selected]

    # price → (total_size_open, earliest non-zero expiration or 0 for GTC)
    own_asks: dict[float, tuple[float, int]] = {}
    own_bids: dict[float, tuple[float, int]] = {}
    for o in own_snap:
        p, sz, exp = _normalize_price_key(o["price"]), o["size_open"], o["expiration"]
        if exp > 0 and exp < now_ts:
            continue  # expired but WS hasn't caught up yet
        bucket = own_asks if o["side"] == "SELL" else own_bids
        if p in bucket:
            prev_sz, prev_exp = bucket[p]
            merged_exp = min(e for e in (prev_exp, exp) if e > 0) if (prev_exp or exp) else 0
            bucket[p] = (prev_sz + sz, merged_exp)
        else:
            bucket[p] = (sz, exp)

    # Asks display: highest price at top, best ask just above the spread line
    asks_disp = list(reversed(asks))

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 1.0
    mid      = (best_bid + best_ask) / 2.0
    spread   = best_ask - best_bid

    depth_scale_max = _compute_depth_bar_scale(
        *(s for _, s in asks),
        *(s for _, s in bids),
    )

    _ASK_DEFAULT = (255, 110, 110)
    _BID_DEFAULT = (80,  210,  80)
    _OWN_HI      = (80,  210, 220)
    _OWN_ROW_BG  = (30,  90,  40, 110)
    combined     = _display_settings["combined_table"]
    show_zeros   = _display_settings["show_zero_prices"]

    if combined and show_zeros:
        # Combined table with full price grid (0.01-0.99, showing "Size 0" for gaps)
        _populate_combined_table_with_all_prices(book, own_asks, own_bids, now_ts)
    elif combined:
        # Combined table: dynamically populate with only actual rows
        _populate_combined_table(asks_disp, bids, own_asks, own_bids, now_ts, depth_scale_max)
    elif show_zeros:
        # Split tables with full price grid (0.01-0.99, showing "Size 0" for gaps)
        _populate_full_price_grid(book, own_asks, own_bids, now_ts)
    else:
        # Split tables: update pre-allocated tag-based rows (only actual prices)
        spread_txt = f"  Mid: {mid:.4f}    Spread: {spread:.4f}  ({spread * 100:.2f}%)"

        # ── Ask rows ──────────────────────────────────────────────────────────
        for i in range(D):
            if i < len(asks_disp):
                p, s = asks_disp[i]
                dpg.set_value(_ask_price[i], f"{p:.4f}")
                dpg.set_value(_ask_size[i],  f"{s:>8.1f}")
                _set_depth_bar(_ask_bar[i], s, depth_scale_max)
                own = own_asks.get(_normalize_price_key(p))
                if own:
                    own_sz, own_exp = own
                    dpg.set_value(_ask_own[i], f"{own_sz:.1f}")
                    dpg.set_value(_ask_ttl[i], _format_ttl(own_exp, now_ts) if own_exp > 0 else "")
                    dpg.configure_item(_ask_price[i], color=_OWN_HI)
                    try: dpg.highlight_table_row("tbl_asks", i, _OWN_ROW_BG)
                    except Exception: pass
                else:
                    dpg.set_value(_ask_own[i], "")
                    dpg.set_value(_ask_ttl[i], "")
                    dpg.configure_item(_ask_price[i], color=_ASK_DEFAULT)
                    try: dpg.unhighlight_table_row("tbl_asks", i)
                    except Exception: pass
            else:
                dpg.set_value(_ask_price[i], "")
                dpg.set_value(_ask_size[i],  "")
                _set_depth_bar(_ask_bar[i], 0.0, 1.0)
                dpg.set_value(_ask_own[i],   "")
                dpg.set_value(_ask_ttl[i],   "")
                dpg.configure_item(_ask_price[i], color=_ASK_DEFAULT)
                try: dpg.unhighlight_table_row("tbl_asks", i)
                except Exception: pass

        dpg.set_value("spread_line", spread_txt)

        # ── Bid rows ───────────────────────────────────────────────────────────
        for i in range(D):
            if i < len(bids):
                p, s = bids[i]
                dpg.set_value(_bid_price[i], f"{p:.4f}")
                dpg.set_value(_bid_size[i],  f"{s:>8.1f}")
                _set_depth_bar(_bid_bar[i], s, depth_scale_max)
                own = own_bids.get(_normalize_price_key(p))
                if own:
                    own_sz, own_exp = own
                    dpg.set_value(_bid_own[i], f"{own_sz:.1f}")
                    dpg.set_value(_bid_ttl[i], _format_ttl(own_exp, now_ts) if own_exp > 0 else "")
                    dpg.configure_item(_bid_price[i], color=_OWN_HI)
                    try: dpg.highlight_table_row("tbl_bids", i, _OWN_ROW_BG)
                    except Exception: pass
                else:
                    dpg.set_value(_bid_own[i], "")
                    dpg.set_value(_bid_ttl[i], "")
                    dpg.configure_item(_bid_price[i], color=_BID_DEFAULT)
                    try: dpg.unhighlight_table_row("tbl_bids", i)
                    except Exception: pass
            else:
                dpg.set_value(_bid_price[i], "")
                dpg.set_value(_bid_size[i],  "")
                _set_depth_bar(_bid_bar[i], 0.0, 1.0)
                dpg.set_value(_bid_own[i],   "")
                dpg.set_value(_bid_ttl[i],   "")
                dpg.configure_item(_bid_price[i], color=_BID_DEFAULT)
                try: dpg.unhighlight_table_row("tbl_bids", i)
                except Exception: pass

    # ── Header / meta ─────────────────────────────────────────────────────────
    _current_slug[0] = book.get("event_slug", "")
    dpg.set_value("book_event",    book.get("event_title", ""))
    dpg.set_value("book_question", book.get("market_question", ""))

    outcome = book.get("outcome", "")
    v_color = (255, 200, 80) if selected in _primary_ids else (160, 160, 220)
    dpg.configure_item("book_viewing_outcome", color=v_color)
    dpg.set_value("book_viewing_outcome", f"Viewing: {outcome}" if outcome else "")

    # ── Authenticated position chip ───────────────────────────────────────────
    with _lock:
        pos = _positions.get(selected)
    if pos and pos.get("size", 0) >= 0.01:
        sz  = pos["size"]
        avg = pos["avg_price"]
        # Display in cents (like the React widget: "84.8¢") when price < $1
        avg_str = f"{avg * 100:.1f}¢" if avg < 1.0 else f"${avg:.4f}"
        dpg.set_value("book_position", f"HOLD {sz:.2f} sh @ {avg_str}")
        dpg.configure_item("book_position", show=True)
    else:
        dpg.configure_item("book_position", show=False)

    def _trunc(s: str) -> str:
        return (s[:12] + "..." + s[-6:]) if len(s) > 20 else s

    dpg.set_value("meta_asset_id",  _trunc(book.get("asset_id", "")))
    dpg.set_value("meta_outcome",   book.get("outcome", "—") or "—")
    dpg.configure_item("meta_paired_id",      label=_trunc(book.get("paired_asset_id", "")))
    dpg.configure_item("meta_paired_outcome", label=book.get("paired_outcome", "—") or "—")
    dpg.set_value("meta_event_slug",     book.get("event_slug", "—"))
    dpg.set_value("meta_tick_size",      str(book.get("min_tick_size", "—")))
    dpg.set_value("meta_neg_risk",  "Yes" if book.get("is_neg_risk") else "No")

    gst = book.get("game_start_time")
    if gst:
        import datetime as _dt
        try:
            gst_disp = _dt.datetime.fromtimestamp(float(gst), tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except Exception:
            gst_disp = str(gst)
    else:
        gst_disp = "—"
    dpg.set_value("meta_game_start", gst_disp)

    vol = book.get("volume",     0.0)
    v24 = book.get("volume_24hr", 0.0)
    liq = book.get("liquidity",  0.0)
    dpg.set_value("meta_volume",    f"${vol:,.0f}")
    dpg.set_value("meta_vol24",     f"${v24:,.0f}")
    dpg.set_value("meta_liquidity", f"${liq:,.0f}")

    if trade:
        side  = trade.get("side", "").upper()
        color = (80, 220, 80) if side == "BUY" else (255, 90, 90)
        dpg.configure_item("book_ltp", color=color)
        dpg.set_value("book_ltp", f"{trade['price']:.4f}  ×  {trade['size']:.1f}  [{side}]")
    else:
        dpg.configure_item("book_ltp", color=(255, 220, 100))
        dpg.set_value("book_ltp", f"{book.get('lastTradePrice', 0.0):.4f}")


_last_positions_check = 0.0


def _update_ui() -> None:
    """Called every rendered frame — reads shared state, pushes to DPG widgets."""
    global _last_list_check, _last_positions_check

    # Header (cheap, every frame)
    dpg.set_value("hdr_status",  _ws_status)
    dpg.set_value("hdr_rate",    f"{_msgs_per_sec:.1f} msg/s")
    with _lock:
        n = len(_orderbooks)
    dpg.set_value("hdr_count",   f"{n} markets")
    lat = _ws_latency_ms
    lat_color = (80, 220, 80) if lat < 200 else (255, 200, 60) if lat < 400 else (255, 80, 80)
    dpg.configure_item("hdr_latency", color=lat_color)
    dpg.set_value("hdr_latency", f"ws latency: {lat:.0f} ms")
    uws = _user_ws_status
    uws_color = (80, 220, 80) if uws == "Connected" else (255, 200, 60) if "onnect" in uws else (255, 100, 100)
    dpg.configure_item("hdr_user_ws", color=uws_color)
    dpg.set_value("hdr_user_ws", uws)

    # Market list (throttled — only rebuild when set changes)
    now = time.perf_counter()
    if now - _last_positions_check >= _POSITIONS_POLL_SECS:
        _schedule_positions_sync()
        _last_positions_check = now
    if now - _last_list_check >= LIST_REBUILD_SECS:
        with _lock:
            ids = list(_orderbooks.keys())
        if ids != _prev_market_ids:
            _rebuild_market_list(ids)
            if not _selected[0] and ids:
                _selected[0] = ids[0]
        _last_list_check = now
    _refresh_market_button_styles()
    _apply_selection_highlight(_selected[0])

    # Orderbook depth (every frame — this is the high-frequency part)
    _update_book_display(_selected[0])

    # Responsive order ticket: switch between stacked and side-by-side button layouts
    try:
        _wide = dpg.get_item_rect_size("order_panel")[0] >= 400
        dpg.configure_item("grp_limit_vert",  show=not _wide)
        dpg.configure_item("grp_limit_horiz", show=_wide)
        dpg.configure_item("grp_mkt_vert",    show=not _wide)
        dpg.configure_item("grp_mkt_horiz",   show=_wide)
    except Exception:
        pass
    _refresh_repeat_bid_button()

    # Order status — drain at most one result per frame
    try:
        msg, color = _order_queue.get_nowait()
        _set_status(msg, color)
    except queue.Empty:
        pass


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    _load_settings()
    print("Fetching Polymarket events...")
    initial_events = fetch_and_filter_gamma_events()
    initial_books  = create_orderbooks(initial_events)

    global _primary_ids
    _primary_ids = set(initial_books.keys())

    # Subscribe to the paired token of every binary market (GUI-only — does not affect parquet pipeline)
    paired_additions: dict[str, Orderbook] = {}
    for book in list(initial_books.values()):
        paired_id = book.get("paired_asset_id")
        if paired_id and paired_id not in initial_books:
            paired = dict(book)
            paired["asset_id"]        = paired_id
            paired["outcome"]         = book["paired_outcome"]
            paired["paired_asset_id"] = book["asset_id"]
            paired["paired_outcome"]  = book["outcome"]
            paired["bids"]            = {}
            paired["asks"]            = {}
            paired["lastTradePrice"]  = 0.0
            paired_additions[paired_id] = paired
    initial_books.update(paired_additions)

    print(f"Loaded {len(_primary_ids)} primary + {len(paired_additions)} paired orderbook skeletons. Starting GUI...")

    # WebSocket runs in a daemon thread with its own asyncio event loop
    ws_thread = threading.Thread(
        target=_run_ws_thread,
        args=(initial_books,),
        daemon=True,
        name="ws-thread",
    )
    ws_thread.start()

    # User WebSocket — authenticated feed for own open-order tracking
    threading.Thread(
        target=_run_user_ws_thread,
        daemon=True,
        name="user-ws-thread",
    ).start()

    # DearPyGui — uncapped render loop for maximum freshness
    _schedule_open_orders_sync()
    _schedule_positions_sync()
    dpg.create_context()

    # Consolas for all text. Font Awesome Solid for icons — bundled with the app
    # so it works on any machine regardless of installed system fonts.
    _FA_SOLID = Path(__file__).parent / "fa-solid-900.ttf"
    # FA icon codepoints (Solid, FA 6 Free):
    #   \uf0c5  fa-copy          (copy button)
    #   \uf054  fa-chevron-right (market selection indicator)
    #   \uf0c8  fa-square        (depth bar fill)
    global _fa_font
    _CONSOLAS = "C:/Windows/Fonts/consola.ttf"
    with dpg.font_registry():
        _ui_font_handles: dict[int, int] = {}
        for _level, _size in ((0, 15), (1, 12), (2, 11)):
            with dpg.font(_CONSOLAS, _size) as _ui_font:
                dpg.add_font_range_hint(dpg.mvFontRangeHint_Default)
            _ui_font_handles[_level] = _ui_font
        # Orderbook row fonts — one per selectable size
        _book_font_handles: dict[int, int] = {}
        for _sz in _BOOK_FONT_SIZES:
            with dpg.font(_CONSOLAS, _sz) as _bfh:
                dpg.add_font_range_hint(dpg.mvFontRangeHint_Default)
            _book_font_handles[_sz] = _bfh
        # FA icon font (fixed size — icons don't need to scale with UI)
        with dpg.font(str(_FA_SOLID), 13) as _fa_font_local:
            dpg.add_font_range(0xf000, 0xf999)

    for _level, _font_handle in _ui_font_handles.items():
        _fonts[f"ui_{_level}"] = _font_handle
    for _sz, _fh in _book_font_handles.items():
        _fonts[f"book_{_sz}"] = _fh
    _fa_font = _fa_font_local

    # Apply the loaded (or default) UI font before building widgets
    dpg.bind_font(_fonts[f"ui_{_normalize_compact_level(_display_settings['compact_level'])}"])

    _build_ui()

    for _level, (_win_pad, _item_spacing, _frame_pad) in {
        0: ((8, 8), (8, 4), (4, 3)),
        1: ((6, 6), (6, 3), (3, 2)),
        2: ((4, 4), (4, 2), (2, 1)),
    }.items():
        with dpg.theme() as _ui_theme:
            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_style(dpg.mvStyleVar_WindowPadding, *_win_pad, category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_ItemSpacing, *_item_spacing, category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_FramePadding, *_frame_pad, category=dpg.mvThemeCat_Core)
        _ui_theme_by_level[_level] = _ui_theme

    for _level, (_pad_x, _pad_y) in {
        0: (4, 1),
        1: (2, 0),
        2: (1, 0),
    }.items():
        with dpg.theme() as _tbl_theme:
            with dpg.theme_component(dpg.mvTable):
                dpg.add_theme_style(dpg.mvStyleVar_CellPadding, _pad_x, _pad_y, category=dpg.mvThemeCat_Core)
        _tbl_theme_by_level[_level] = _tbl_theme

    for _side, _fill in {
        "ask": (178, 58, 58, 255),
        "bid": (46, 148, 78, 255),
    }.items():
        with dpg.theme() as _depth_theme:
            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_PlotHistogram, _fill)
                dpg.add_theme_color(dpg.mvThemeCol_FrameBg, (0, 0, 0, 0))
                dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 0, category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_FrameBorderSize, 0, category=dpg.mvThemeCat_Core)
        _depth_bar_theme_by_side[_side] = _depth_theme

    for _bar_tag in _ask_bar:
        _bind_depth_bar_theme(_bar_tag, "ask")
    for _bar_tag in _bid_bar:
        _bind_depth_bar_theme(_bar_tag, "bid")

    # Populate hotkey display widgets from loaded settings
    for _action, _code in _hotkeys.items():
        if _code is not None and dpg.does_item_exist(f"hk_key_{_action}"):
            dpg.set_value(f"hk_key_{_action}", _key_name(_code))

    # Bind FA font to every widget that uses FA codepoints (\uf013 cog, etc.)
    for _tag in ["copy_btn", "book_pm_icon", "settings_btn"]:
        dpg.bind_item_font(_tag, _fa_font_local)

    # Apply display settings — sets book-row font and table padding
    _apply_display_settings()
    dpg.create_viewport(
        title="DPG PolyTerminal",
        width=1440, height=900,
        min_width=900, min_height=600,
    )
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.set_primary_window("root", True)

    # Global key handler — manages hotkey capture, dispatch, Esc behaviour
    with dpg.handler_registry():
        dpg.add_key_press_handler(callback=_on_key_press)

    # Render loop — no frame cap, runs as fast as GPU allows
    while dpg.is_dearpygui_running():
        _update_ui()
        dpg.render_dearpygui_frame()

    dpg.destroy_context()


if __name__ == "__main__":
    main()
