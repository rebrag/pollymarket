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
    hydrate_orderbook, apply_price_change, best_prices_from_book, should_unsubscribe,
    ws_initial_subscribe, ws_subscribe_more, ws_unsubscribe,
)
import order_client

# ── Config ────────────────────────────────────────────────────────────────────

EVENT_REFRESH_SECS   = 600
DEPTH_LEVELS         = 20     # Price levels shown per side
BAR_MAX_CHARS        = 30     # Max width of depth bar (unicode blocks)
LIST_REBUILD_SECS    = 1.0    # How often to check if market list changed

# ── Shared state (all writes/reads guarded by `_lock`) ───────────────────────

_lock           = threading.Lock()
_orderbooks:    dict[str, Orderbook]      = {}
_last_trades:   dict[str, dict]           = {}   # asset_id → {price, size, side}
_asset_activity: dict[str, deque[float]] = {}   # asset_id → deque of event timestamps
_ws_status    = "Connecting..."
_msgs_per_sec = 0.0
_ws_latency_ms = 0.0   # EMA of (now - event_timestamp)
_sort_mode:   list[str] = ["volume"]             # "volume" or "activity" (mutable container)

# ── WebSocket asyncio loop (runs in background thread) ───────────────────────

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
                            if to_finalize:
                                with _lock:
                                    for aid in to_finalize:
                                        _orderbooks.pop(aid, None)
                                        _last_trades.pop(aid, None)
                                await ws.send(ws_unsubscribe(to_finalize))

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


# ── GUI ────────────────────────────────────────────────────────────────────────
# All DPG calls happen on the main thread only.

D = DEPTH_LEVELS

# Pre-allocated tag arrays for ask/bid table cells (avoids string formatting each frame)
_ask_price = [f"ap{i}" for i in range(D)]
_ask_size  = [f"as{i}" for i in range(D)]
_ask_bar   = [f"ab{i}" for i in range(D)]
_bid_price = [f"bp{i}" for i in range(D)]
_bid_size  = [f"bs{i}" for i in range(D)]
_bid_bar   = [f"bb{i}" for i in range(D)]

_selected:        list[str]        = [""]  # single-element mutable for simple mutation
_current_slug:    list[str]        = [""]  # event slug for the currently displayed market
_mkt_btn_tags:    dict[str, str]   = {}
_prev_market_ids: list[str]        = []
_last_list_check  = 0.0

# Order ticket — worker threads put (message, color) here; _update_ui drains it
_order_queue: queue.SimpleQueue = queue.SimpleQueue()

# FA font handle — set in main() before _build_ui(); used by copy button and depth bars
_fa_font: int = 0

# Maps color tuple → DPG theme id for order_status input_text; populated in _build_ui
_status_theme: dict[tuple, int] = {}


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


def _fill_bid_price() -> None:
    """Pre-fill the price input with the current best bid."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        book = _orderbooks.get(aid)
    if book:
        best_bid, _ = best_prices_from_book(book)
        if best_bid > 0.0:
            dpg.set_value("order_price", best_bid)


def _fill_ask_price() -> None:
    """Pre-fill the price input with the current best ask."""
    aid = _selected[0]
    if not aid:
        return
    with _lock:
        book = _orderbooks.get(aid)
    if book:
        _, best_ask = best_prices_from_book(book)
        if best_ask < 1.0:
            dpg.set_value("order_price", best_ask)


def _submit_limit(side: str) -> None:
    """Spawn a worker thread to place a GTC limit order."""
    aid = _selected[0]
    if not aid:
        _set_status("No market selected", (130, 130, 130))
        return
    price = dpg.get_value("order_price")
    size  = dpg.get_value("order_size")
    ttl   = int(dpg.get_value("order_ttl"))
    if not (0.001 <= price <= 0.999):
        _set_status("Price must be between 0.001 and 0.999", (255, 180, 60))
        return
    if size <= 0:
        _set_status("Size must be > 0", (255, 180, 60))
        return
    expiration = (int(time.time()) + 60 + ttl) if ttl > 0 else 0
    ttl_label  = f"TTL {ttl}s" if ttl > 0 else "GTC"
    _set_status(f"Placing {side} limit @ {price:.4f} × {size:.2f} ({ttl_label})...", (220, 220, 60))

    def _worker(token_id=aid, s=side, p=price, sz=size, exp=expiration):
        try:
            t0 = time.perf_counter()
            resp = order_client.place_limit_order(token_id, s, p, sz, expiration=exp)
            ms = (time.perf_counter() - t0) * 1000
            order_id = resp.get("orderID") or resp.get("id") or str(resp)
            _order_queue.put((f"OK {s}: {str(order_id)[:20]} ({ms:.0f}ms)", (80, 220, 80)))
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
            resp = order_client.place_market_order(token_id, s, amt)
            ms = (time.perf_counter() - t0) * 1000
            order_id = resp.get("orderID") or resp.get("id") or str(resp)
            _order_queue.put((f"OK MKT {s}: {str(order_id)[:20]} ({ms:.0f}ms)", (80, 220, 80)))
        except Exception as exc:
            _order_queue.put((f"Error: {exc}", (255, 80, 80)))

    threading.Thread(target=_worker, daemon=True).start()


def _submit_cancel_all() -> None:
    """Spawn a worker thread to cancel every open order on this account."""
    _set_status("Cancelling all orders...", (220, 220, 60))

    def _worker():
        try:
            resp = order_client.cancel_all()
            # resp is typically {"canceled": [...]} or similar
            n = len(resp) if isinstance(resp, list) else resp.get("canceled", resp)
            summary = f"{len(n)} cancelled" if isinstance(n, list) else str(n)[:40]
            _order_queue.put((f"Cancelled: {summary}", (80, 220, 80)))
        except Exception as exc:
            _order_queue.put((f"Error: {exc}", (255, 80, 80)))

    threading.Thread(target=_worker, daemon=True).start()


def _set_sort(mode: str) -> None:
    _sort_mode[0] = mode
    dpg.configure_item("sort_vol_btn", label="[Volume]"  if mode == "volume"   else "Volume")
    dpg.configure_item("sort_act_btn", label="[Activity]" if mode == "activity" else "Activity")
    _prev_market_ids.clear()  # force list rebuild on next frame


def _build_ui() -> None:
    with dpg.window(tag="root", no_title_bar=True, no_move=True,
                    no_resize=True, no_scrollbar=True, no_close=True):

        # ── Header bar ────────────────────────────────────────────────────────
        with dpg.group(horizontal=True):
            dpg.add_text("POLYMARKET LIVE ORDERBOOK", color=(180, 200, 255))
            dpg.add_spacer(width=20)
            dpg.add_text("Connecting...", tag="hdr_status",  color=(140, 140, 140))
            dpg.add_spacer(width=20)
            dpg.add_text("—",           tag="hdr_rate",    color=(140, 140, 140))
            dpg.add_spacer(width=20)
            dpg.add_text("—",           tag="hdr_count",   color=(140, 140, 140))
            dpg.add_spacer(width=20)
            dpg.add_text("latency: —",  tag="hdr_latency", color=(140, 140, 140))
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
        dpg.add_table_column(parent=_tbl, label="ORDER TICKET", width_fixed=True,  init_width_or_weight=230)
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
                        dpg.add_text("—", tag="meta_paired_id",       color=(160, 160, 200))
                        dpg.add_text("Paired outcome", color=_LBL)
                        dpg.add_text("—", tag="meta_paired_outcome",  color=(160, 160, 200))
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

                # Asks table (sell side) ──────────────────────────────────────
                dpg.add_text("ASKS  (sell side)", color=(255, 80, 80))
                with dpg.table(tag="tbl_asks", header_row=True,
                               borders_innerV=True, borders_outerH=True,
                               borders_outerV=True, row_background=True,
                               policy=dpg.mvTable_SizingFixedFit):
                    dpg.add_table_column(label="Price", width_fixed=True, init_width_or_weight=95)
                    dpg.add_table_column(label="Size",  width_fixed=True, init_width_or_weight=90)
                    dpg.add_table_column(label="Depth", width_stretch=True)
                    for i in range(D):
                        with dpg.table_row():
                            dpg.add_text("", tag=_ask_price[i], color=(255, 110, 110))
                            dpg.add_text("", tag=_ask_size[i])
                            dpg.add_text("", tag=_ask_bar[i],   color=(160, 55, 55))

                # Spread / mid line ───────────────────────────────────────────
                dpg.add_separator()
                dpg.add_text("", tag="spread_line", color=(220, 220, 80))
                dpg.add_separator()

                # Bids table (buy side) ───────────────────────────────────────
                dpg.add_text("BIDS  (buy side)", color=(80, 200, 80))
                with dpg.table(tag="tbl_bids", header_row=True,
                               borders_innerV=True, borders_outerH=True,
                               borders_outerV=True, row_background=True,
                               policy=dpg.mvTable_SizingFixedFit):
                    dpg.add_table_column(label="Price", width_fixed=True, init_width_or_weight=95)
                    dpg.add_table_column(label="Size",  width_fixed=True, init_width_or_weight=90)
                    dpg.add_table_column(label="Depth", width_stretch=True)
                    for i in range(D):
                        with dpg.table_row():
                            dpg.add_text("", tag=_bid_price[i], color=(80,  210, 80))
                            dpg.add_text("", tag=_bid_size[i])
                            dpg.add_text("", tag=_bid_bar[i],   color=(30,  120, 30))

            # Right: order ticket
            with dpg.child_window(tag="order_panel", width=-1, height=-1, border=True):

                dpg.add_text("ORDER TICKET", color=(210, 210, 100))
                dpg.add_separator()

                # Button themes — green for BUY, red for SELL
                with dpg.theme() as _buy_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        (25, 110, 25))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (40, 160, 40))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  (15,  80, 15))
                with dpg.theme() as _sell_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        (120, 25, 25))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (170, 40, 40))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  ( 85, 15, 15))

                # Inputs
                with dpg.group(horizontal=True):
                    dpg.add_text("Price ", color=(160, 160, 160))
                    dpg.add_input_float(
                        tag="order_price", width=-1, step=0, format="%.4f",
                        default_value=0.5, min_value=0.001, max_value=0.999,
                        min_clamped=True, max_clamped=True,
                    )
                with dpg.group(horizontal=True):
                    dpg.add_spacer(width=42)
                    dpg.add_button(label="< bid", small=True, callback=_fill_bid_price)
                    dpg.add_button(label="ask >", small=True, callback=_fill_ask_price)
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

                # Limit order buttons
                dpg.add_text("LIMIT", color=(160, 160, 160))
                _bl = dpg.add_button(label="BUY  LIMIT", width=-1,
                                     callback=lambda: _submit_limit("BUY"))
                dpg.bind_item_theme(_bl, _buy_theme)
                _sl = dpg.add_button(label="SELL LIMIT", width=-1,
                                     callback=lambda: _submit_limit("SELL"))
                dpg.bind_item_theme(_sl, _sell_theme)
                dpg.add_separator()

                # Market order buttons
                dpg.add_text("MARKET", color=(160, 160, 160))
                _bm = dpg.add_button(label="MKT BUY",  width=-1,
                                     callback=lambda: _submit_market("BUY"))
                dpg.bind_item_theme(_bm, _buy_theme)
                _sm = dpg.add_button(label="MKT SELL", width=-1,
                                     callback=lambda: _submit_market("SELL"))
                dpg.bind_item_theme(_sm, _sell_theme)
                dpg.add_separator()

                # Cancel all — danger button (orange)
                with dpg.theme() as _cancel_theme:
                    with dpg.theme_component(dpg.mvButton):
                        dpg.add_theme_color(dpg.mvThemeCol_Button,        (160,  80,  0))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (210, 110,  0))
                        dpg.add_theme_color(dpg.mvThemeCol_ButtonActive,  (120,  55,  0))
                _ca = dpg.add_button(label="CANCEL ALL ORDERS", width=-1,
                                     callback=_submit_cancel_all)
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


def _rebuild_market_list(ids: list[str]) -> None:
    """Delete existing market buttons and recreate for the current id list."""
    global _prev_market_ids, _mkt_btn_tags

    # Sort ids by selected mode before creating buttons
    now = time.time()
    with _lock:
        snap = {aid: _orderbooks[aid] for aid in ids if aid in _orderbooks}
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
                _selected[0] = a
                _apply_selection_highlight(a)
            return _cb

        dpg.add_button(label=label, tag=tag, parent="mkt_list",
                       callback=_make_cb(), width=-1)


def _apply_selection_highlight(selected_id: str) -> None:
    """Prefix selected button with ▶, clear prefix on others."""
    with _lock:
        snap = {aid: _orderbooks.get(aid, {}) for aid in _prev_market_ids}

    for aid, tag in _mkt_btn_tags.items():
        if not dpg.does_item_exist(tag):
            continue
        q    = snap.get(aid, {}).get("market_question", aid)
        base = (q[:42] + "...") if len(q) > 42 else q
        dpg.configure_item(tag, label=("> " + base) if aid == selected_id else ("  " + base))


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

    # Asks display: highest price at top, best ask just above the spread line
    asks_disp = list(reversed(asks))

    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 1.0
    mid      = (best_bid + best_ask) / 2.0
    spread   = best_ask - best_bid

    max_ask_sz = max((s for _, s in asks), default=1.0) or 1.0
    max_bid_sz = max((s for _, s in bids), default=1.0) or 1.0

    for i in range(D):
        if i < len(asks_disp):
            p, s = asks_disp[i]
            dpg.set_value(_ask_price[i], f"{p:.4f}")
            dpg.set_value(_ask_size[i],  f"{s:>8.1f}")
            dpg.set_value(_ask_bar[i],   "\uf0c8" * max(1, int(s / max_ask_sz * BAR_MAX_CHARS)))
        else:
            dpg.set_value(_ask_price[i], "")
            dpg.set_value(_ask_size[i],  "")
            dpg.set_value(_ask_bar[i],   "")

    dpg.set_value("spread_line",
                  f"  Mid: {mid:.4f}    Spread: {spread:.4f}  ({spread * 100:.2f}%)")

    for i in range(D):
        if i < len(bids):
            p, s = bids[i]
            dpg.set_value(_bid_price[i], f"{p:.4f}")
            dpg.set_value(_bid_size[i],  f"{s:>8.1f}")
            dpg.set_value(_bid_bar[i],   "\uf0c8" * max(1, int(s / max_bid_sz * BAR_MAX_CHARS)))
        else:
            dpg.set_value(_bid_price[i], "")
            dpg.set_value(_bid_size[i],  "")
            dpg.set_value(_bid_bar[i],   "")

    # ── Header / meta ─────────────────────────────────────────────────────────
    _current_slug[0] = book.get("event_slug", "")
    dpg.set_value("book_event",    book.get("event_title", ""))
    dpg.set_value("book_question", book.get("market_question", ""))

    def _trunc(s: str) -> str:
        return (s[:12] + "..." + s[-6:]) if len(s) > 20 else s

    dpg.set_value("meta_asset_id",       _trunc(book.get("asset_id", "")))
    dpg.set_value("meta_outcome",        book.get("outcome", "—") or "—")
    dpg.set_value("meta_paired_id",      _trunc(book.get("paired_asset_id", "")))
    dpg.set_value("meta_paired_outcome", book.get("paired_outcome", "—") or "—")
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


def _update_ui() -> None:
    """Called every rendered frame — reads shared state, pushes to DPG widgets."""
    global _last_list_check

    # Header (cheap, every frame)
    dpg.set_value("hdr_status",  _ws_status)
    dpg.set_value("hdr_rate",    f"{_msgs_per_sec:.1f} msg/s")
    with _lock:
        n = len(_orderbooks)
    dpg.set_value("hdr_count",   f"{n} markets")
    lat = _ws_latency_ms
    lat_color = (80, 220, 80) if lat < 200 else (255, 200, 60) if lat < 500 else (255, 80, 80)
    dpg.configure_item("hdr_latency", color=lat_color)
    dpg.set_value("hdr_latency", f"ws latency: {lat:.0f} ms")

    # Market list (throttled — only rebuild when set changes)
    now = time.perf_counter()
    if now - _last_list_check >= LIST_REBUILD_SECS:
        with _lock:
            ids = list(_orderbooks.keys())
        if ids != _prev_market_ids:
            _rebuild_market_list(ids)
            if not _selected[0] and ids:
                _selected[0] = ids[0]
            _apply_selection_highlight(_selected[0])
        _last_list_check = now

    # Orderbook depth (every frame — this is the high-frequency part)
    _update_book_display(_selected[0])

    # Order status — drain at most one result per frame
    try:
        msg, color = _order_queue.get_nowait()
        _set_status(msg, color)
    except queue.Empty:
        pass


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print("Fetching Polymarket events...")
    initial_events = fetch_and_filter_gamma_events()
    initial_books  = create_orderbooks(initial_events)
    print(f"Loaded {len(initial_books)} orderbook skeletons. Starting GUI...")

    # WebSocket runs in a daemon thread with its own asyncio event loop
    ws_thread = threading.Thread(
        target=_run_ws_thread,
        args=(initial_books,),
        daemon=True,
        name="ws-thread",
    )
    ws_thread.start()

    # DearPyGui — uncapped render loop for maximum freshness
    dpg.create_context()

    # Consolas for all text. Font Awesome Solid for icons — bundled with the app
    # so it works on any machine regardless of installed system fonts.
    _FA_SOLID = Path(__file__).parent / "fa-solid-900.ttf"
    # FA icon codepoints (Solid, FA 6 Free):
    #   \uf0c5  fa-copy          (copy button)
    #   \uf054  fa-chevron-right (market selection indicator)
    #   \uf0c8  fa-square        (depth bar fill)
    global _fa_font
    with dpg.font_registry():
        with dpg.font("C:/Windows/Fonts/consola.ttf", 15) as _default_font:
            dpg.add_font_range_hint(dpg.mvFontRangeHint_Default)
        with dpg.font(str(_FA_SOLID), 13) as _fa_font_local:
            dpg.add_font_range(0xf000, 0xf999)
    _fa_font = _fa_font_local
    dpg.bind_font(_default_font)

    _build_ui()
    # Bind FA font to every widget that uses FA codepoints
    for _tag in ["copy_btn", "book_pm_icon"] + _ask_bar + _bid_bar:
        dpg.bind_item_font(_tag, _fa_font_local)
    dpg.create_viewport(
        title="Polymarket Live Orderbook",
        width=1440, height=900,
        min_width=900, min_height=600,
    )
    dpg.setup_dearpygui()
    dpg.show_viewport()
    dpg.set_primary_window("root", True)

    # ESC to quit
    with dpg.handler_registry():
        dpg.add_key_press_handler(dpg.mvKey_Escape,
                                   callback=lambda: dpg.stop_dearpygui())

    # Render loop — no frame cap, runs as fast as GPU allows
    while dpg.is_dearpygui_running():
        _update_ui()
        dpg.render_dearpygui_frame()

    dpg.destroy_context()


if __name__ == "__main__":
    main()
