"""
orderbook-gui/main.py

High-frequency live orderbook viewer for Polymarket.
Connects to the same WebSocket feed as poly_parquet_generator.py.
Uses DearPyGui for GPU-accelerated rendering above monitor refresh rate.
"""

import asyncio
import json
import sys
import threading
import time
import webbrowser
from collections import deque
from pathlib import Path
from typing import cast

import websockets
import dearpygui.dearpygui as dpg

# Allow importing shared modules from the parent project directory
sys.path.insert(0, str(Path(__file__).parent.parent))

from models import Orderbook, Event, Market, BookEvent
from fetch_and_filter_gamma_events import fetch_and_filter_gamma_events

# ── Config ────────────────────────────────────────────────────────────────────

WS_URL               = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
EVENT_REFRESH_SECS   = 600
DEPTH_LEVELS         = 20     # Price levels shown per side
BAR_MAX_CHARS        = 30     # Max width of depth bar (unicode blocks)
LIST_REBUILD_SECS    = 1.0    # How often to check if market list changed

# ── Shared state (all writes/reads guarded by `_lock`) ───────────────────────

_lock           = threading.Lock()
_orderbooks:    dict[str, Orderbook]      = {}
_last_trades:   dict[str, dict]           = {}   # asset_id → {price, size, side}
_asset_activity: dict[str, deque[float]] = {}   # asset_id → deque of event timestamps
_ws_status    = "Connecting…"
_msgs_per_sec = 0.0
_ws_latency_ms = 0.0   # EMA of (now - event_timestamp)
_sort_mode:   list[str] = ["volume"]             # "volume" or "activity" (mutable container)

# ── WebSocket helpers ─────────────────────────────────────────────────────────

def _parse_asset_id(market: Market) -> str:
    return market["clobTokenIds"].strip('[]"').partition('",')[0]


def _make_skeleton(event: Event, market: Market, asset_id: str) -> Orderbook:
    import datetime

    def _gst(s: str) -> float:
        try:
            return datetime.datetime.fromisoformat(s).timestamp()
        except Exception:
            return 0.0

    return {
        "asset_id":          asset_id,
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
        "game_start_time":   _gst(market.get("gameStartTime", "")),
        "bids":              {},
        "asks":              {},
        "volume":            float(market.get("volume", 0.0)),
        "volume_24hr":       float(market.get("volume24hr", 0.0)),
        "liquidity":         float(market.get("liquidity", 0.0)),
        "image_url":         market.get("image", ""),
        "resolution_source": market.get("resolutionSource", ""),
        "end_date":          market.get("endDate", ""),
    }


def _build_orderbooks(events: list[Event]) -> dict[str, Orderbook]:
    result: dict[str, Orderbook] = {}
    for event in events:
        for market in event.get("markets", []):
            aid = _parse_asset_id(market)
            result[aid] = _make_skeleton(event, market, aid)
    return result


def _hydrate(book: Orderbook, event: dict) -> None:
    import datetime
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


def _apply_price_change(book: Orderbook, change: dict) -> None:
    """Apply an incremental level update from a price_change event."""
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

                await ws.send(json.dumps({
                    "type": "market", "assets_ids": ids, "initial_dump": True
                }))

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
                                aid = _parse_asset_id(mkt)
                                if aid not in inactive and aid not in existing:
                                    skel = _make_skeleton(ev, mkt, aid)
                                    with _lock:
                                        _orderbooks[aid] = skel
                                    new_ids.append(aid)
                        if new_ids:
                            await ws.send(json.dumps({
                                "assets_ids": new_ids,
                                "operation": "subscribe",
                                "custom_feature_enabled": False,
                            }))
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
                                    _hydrate(_orderbooks[aid], be)
                    else:
                        etype = ev.get("event_type", "")
                        if etype == "book":
                            with _lock:
                                aid = ev.get("asset_id", "")
                                if aid in _orderbooks:
                                    _hydrate(_orderbooks[aid], ev)
                                    _asset_activity.setdefault(aid, deque()).append(time.time())

                        elif etype == "price_change":
                            to_finalize: list[str] = []
                            with _lock:
                                for ch in ev.get("price_changes", []):
                                    aid = ch.get("asset_id", "")
                                    if aid in _orderbooks:
                                        _apply_price_change(_orderbooks[aid], ch)
                                        _asset_activity.setdefault(aid, deque()).append(time.time())
                                        bb = float(ch.get("best_bid", "0") or "0")
                                        ba = float(ch.get("best_ask", "1") or "1")
                                        if bb >= 0.999 or ba <= 0.001:
                                            inactive.add(aid)
                                            to_finalize.append(aid)
                            if to_finalize:
                                with _lock:
                                    for aid in to_finalize:
                                        _orderbooks.pop(aid, None)
                                        _last_trades.pop(aid, None)
                                await ws.send(json.dumps({
                                    "operation": "unsubscribe",
                                    "assets_ids": to_finalize,
                                }))

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
                _ws_status = f"Reconnecting…  ({e})"
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
            dpg.add_text("Connecting…", tag="hdr_status",  color=(140, 140, 140))
            dpg.add_spacer(width=20)
            dpg.add_text("—",           tag="hdr_rate",    color=(140, 140, 140))
            dpg.add_spacer(width=20)
            dpg.add_text("—",           tag="hdr_count",   color=(140, 140, 140))
            dpg.add_spacer(width=20)
            dpg.add_text("latency: —",  tag="hdr_latency", color=(140, 140, 140))
        dpg.add_separator()

        # ── Two-column layout ─────────────────────────────────────────────────
        with dpg.group(horizontal=True):

            # Left: scrollable market list
            with dpg.child_window(tag="mkt_list", width=310, height=-1, border=True):
                with dpg.group(horizontal=True):
                    dpg.add_text("Sort:", color=(140, 140, 140))
                    dpg.add_button(label="[Volume]", tag="sort_vol_btn",
                                   callback=lambda: _set_sort("volume"), small=True)
                    dpg.add_button(label="Activity", tag="sort_act_btn",
                                   callback=lambda: _set_sort("activity"), small=True)
                dpg.add_separator()
                dpg.add_text("Loading markets…", tag="mkt_placeholder")

            dpg.add_spacer(width=4)

            # Right: orderbook depth
            with dpg.child_window(tag="book_panel", width=-1, height=-1, border=True):

                with dpg.group(horizontal=True):
                    dpg.add_text("", tag="book_event", color=(210, 210, 100), wrap=0)
                    dpg.add_spacer(width=8)
                    dpg.add_button(
                        label="Open ↗", tag="book_pm_btn", small=True,
                        callback=lambda: webbrowser.open(
                            f"https://polymarket.com/event/{_current_slug[0]}"
                        ) if _current_slug[0] else None,
                    )
                dpg.add_text("", tag="book_question", color=(200, 200, 200), wrap=900)
                with dpg.group(horizontal=True):
                    dpg.add_text("Last trade:", color=(120, 120, 120))
                    dpg.add_text("—", tag="book_ltp", color=(255, 220, 100))
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

                dpg.add_separator()
                dpg.add_text("", tag="stats_line", color=(130, 130, 130))


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
        label = (q[:44] + "…") if len(q) > 44 else q
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
        base = (q[:42] + "…") if len(q) > 42 else q
        dpg.configure_item(tag, label=("▶ " + base) if aid == selected_id else ("   " + base))


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
            dpg.set_value(_ask_bar[i],   "█" * max(1, int(s / max_ask_sz * BAR_MAX_CHARS)))
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
            dpg.set_value(_bid_bar[i],   "█" * max(1, int(s / max_bid_sz * BAR_MAX_CHARS)))
        else:
            dpg.set_value(_bid_price[i], "")
            dpg.set_value(_bid_size[i],  "")
            dpg.set_value(_bid_bar[i],   "")

    # ── Header / meta ─────────────────────────────────────────────────────────
    _current_slug[0] = book.get("event_slug", "")
    dpg.set_value("book_event",    book.get("event_title", ""))
    dpg.set_value("book_question", book.get("market_question", ""))

    if trade:
        side  = trade.get("side", "").upper()
        color = (80, 220, 80) if side == "BUY" else (255, 90, 90)
        dpg.configure_item("book_ltp", color=color)
        dpg.set_value("book_ltp",
                      f"{trade['price']:.4f}  ×  {trade['size']:.1f}  [{side}]")
    else:
        dpg.configure_item("book_ltp", color=(255, 220, 100))
        dpg.set_value("book_ltp", f"{book.get('lastTradePrice', 0.0):.4f}")

    vol = book.get("volume",     0.0)
    v24 = book.get("volume_24hr", 0.0)
    liq = book.get("liquidity",  0.0)
    dpg.set_value("stats_line",
                  f"Vol: ${vol:,.0f}    24h Vol: ${v24:,.0f}    Liquidity: ${liq:,.0f}")


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
    dpg.set_value("hdr_latency", f"latency: {lat:.0f} ms")

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


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print("Fetching Polymarket events…")
    initial_events = fetch_and_filter_gamma_events()
    initial_books  = _build_orderbooks(initial_events)
    print(f"Loaded {len(initial_books)} orderbook skeletons. Starting GUI…")

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

    # Load a system font with Unicode block elements (█) and geometric shapes (▶)
    _FONT_PATH = "C:/Windows/Fonts/consola.ttf"
    with dpg.font_registry():
        with dpg.font(_FONT_PATH, 15) as _default_font:
            dpg.add_font_range_hint(dpg.mvFontRangeHint_Default)
            dpg.add_font_range(0x2580, 0x259F)  # Block Elements  (█ ▀ ▄ …)
            dpg.add_font_range(0x25A0, 0x25FF)  # Geometric Shapes (▶ ■ ● …)
    dpg.bind_font(_default_font)

    _build_ui()
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
