"""
lolesports_feed.py

Live LoL Esports data feed for DPG-PolyTerminal.
Polls the Riot/LoL Esports unofficial REST API for live pro match data.

Integration — add three lines to main.py:
    import lolesports_feed                          # top of file
    lolesports_feed.start_feed_thread()             # in main(), before dpg.create_context()
    lolesports_feed.update_ui()                     # inside _update_ui(), any position

Endpoints used:
  GET https://esports-api.lolesports.com/persisted/gw/getLive
      → detect live matches, get gameId and team names
  GET https://feed.lolesports.com/livestats/v1/window/{gameId}
      → team gold/kills/towers/objectives + per-player basic stats (~10s cadence)
  GET https://feed.lolesports.com/livestats/v1/details/{gameId}
      → per-player items, wards (~30s cadence)
"""

import threading
import time
import datetime
from pathlib import Path

import requests
import dearpygui.dearpygui as dpg

# ── API config ─────────────────────────────────────────────────────────────────
# This key is embedded publicly in the lolesports.com frontend — not a secret.
_API_KEY = "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z"
_HEADERS = {"x-api-key": _API_KEY}

_LIVE_URL   = "https://esports-api.lolesports.com/persisted/gw/getLive?hl=en-US"
_WINDOW_URL = "https://feed.lolesports.com/livestats/v1/window/{game_id}"
_DETAIL_URL = "https://feed.lolesports.com/livestats/v1/details/{game_id}"

LIVE_POLL_SECS   = 30    # how often to check for live matches
WINDOW_POLL_SECS = 10    # how often to poll /window (team gold, kills, towers)
DETAIL_POLL_SECS = 30    # how often to poll /details (per-player items)

_NUM_PLAYERS = 10        # 5 blue + 5 red

# ── Shared state (all writes guarded by _lock) ─────────────────────────────────

_lock = threading.Lock()

# Feed metadata
_status: str     = "Starting..."
_game_id: str    = ""
_match_name: str = ""   # "T1 vs Gen.G — LCK"

# Team aggregates — list of 2 dicts: [blue, red]
_teams: list[dict] = []

# Per-player stats — list of 10 dicts, blue side first
# Populated from /window (with names from gameMetadata) + /details (items)
_players: list[dict] = []

# Game clock (seconds) — estimated from frame count
_game_time_secs: int = 0

# ISO timestamp of the last received /window frame — used as startingTime for
# subsequent polls so we always advance forward through the game timeline.
_last_frame_iso: str = ""

# Per-game player metadata (summonerName, champion, role) — keyed by participantId
# Stable for the lifetime of a game; re-populated when game_id changes.
_player_meta: dict[int, dict] = {}   # participantId → {name, champion, role}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _iso_ago(seconds: int = 60) -> str:
    """ISO-8601 timestamp for `seconds` ago, floored to 10-second boundary (API requirement)."""
    t = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=seconds)
    floored = t.replace(second=(t.second // 10) * 10, microsecond=0)
    return floored.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _window_start_time(last_frame_iso: str) -> str:
    """
    Return the startingTime for the next /window or /details poll.
    - After the first successful response: use the last received frame's
      timestamp so the API returns the next batch of frames.
    - First poll (no frames yet): use 5 minutes ago. The API requires
      startingTime to fall inside the actual game timeline, so recent
      time is correct for any live game. 3 hours ago would land before
      game 2 of a series started and return 204.
    """
    if last_frame_iso:
        return last_frame_iso
    return _iso_ago(300)


def _fmt_gold(g: int) -> str:
    return f"{g / 1000:.1f}k" if g >= 1000 else str(g)


def _fmt_time(secs: int) -> str:
    m, s = divmod(max(0, secs), 60)
    return f"{m:02d}:{s:02d}"


def _dragon_icons(dragon_list: list) -> str:
    """Compact dragon soul / stack indicator."""
    counts: dict[str, int] = {}
    for d in dragon_list:
        key = str(d).lower()[:3]  # "fir", "wat", "eart", "air", "hex", "che"
        counts[key] = counts.get(key, 0) + 1
    return " ".join(f"{k}{v}" for k, v in counts.items()) if counts else "—"


# ── API fetch helpers ──────────────────────────────────────────────────────────

def _fetch_live_game() -> tuple[str, str, str]:
    """Return (game_id, match_name, event_start_iso) for the first inProgress LoL game."""
    try:
        r = requests.get(_LIVE_URL, headers=_HEADERS, timeout=10)
        r.raise_for_status()
        events = r.json().get("data", {}).get("schedule", {}).get("events", [])
        for ev in events:
            league     = ev.get("league", {}).get("name", "")
            start_iso  = ev.get("startTime", "")
            match      = ev.get("match", {})
            teams      = match.get("teams", [])
            for game in match.get("games", []):
                if game.get("state") == "inProgress":
                    gid = game.get("id", "")
                    t1  = teams[0].get("name", "?") if len(teams) > 0 else "?"
                    t2  = teams[1].get("name", "?") if len(teams) > 1 else "?"
                    return gid, f"{t1} vs {t2}  [{league}]", start_iso
    except Exception as exc:
        return "", f"Live check error: {exc}", ""
    return "", "No live match", ""


def _parse_player_meta(game_metadata: dict) -> dict[int, dict]:
    """Extract summonerName, champion, role from gameMetadata for all 10 players."""
    meta: dict[int, dict] = {}
    for side_key in ("blueTeamMetadata", "redTeamMetadata"):
        for p in game_metadata.get(side_key, {}).get("participantMetadata", []):
            pid = int(p.get("participantId", 0))
            meta[pid] = {
                "name":     p.get("summonerName", "?"),
                "champion": p.get("championId", "?"),
                "role":     p.get("role", ""),
            }
    return meta


def _parse_window(data: dict, current_meta: dict[int, dict]) -> None:
    """Parse /window response into _teams, _players, _game_time_secs."""
    global _teams, _players, _game_time_secs, _player_meta, _last_frame_iso

    frames = data.get("frames", [])
    if not frames:
        return
    frame = frames[-1]

    # Update player metadata once per game (or if empty)
    if not current_meta:
        current_meta = _parse_player_meta(data.get("gameMetadata", {}))
        with _lock:
            _player_meta = current_meta

    game_metadata = data.get("gameMetadata", {})

    # Team aggregates
    teams_parsed: list[dict] = []
    for side_key, side_label, meta_key in (
        ("blueTeam", "blue", "blueTeamMetadata"),
        ("redTeam",  "red",  "redTeamMetadata"),
    ):
        raw  = frame.get(side_key, {})
        tmeta = game_metadata.get(meta_key, {})
        teams_parsed.append({
            "side":    side_label,
            "name":    tmeta.get("esportsTeamId", side_label.upper()),
            "kills":   raw.get("totalKills", 0),
            "towers":  raw.get("towers", 0),
            "gold":    raw.get("totalGold", 0),
            "barons":  raw.get("barons", 0),
            "dragons": raw.get("dragons", []),
            "inhibs":  raw.get("inhibitors", 0),
        })

    # Per-player stats from participants in both team frames
    players_parsed: list[dict] = []
    for side_key in ("blueTeam", "redTeam"):
        for p in frame.get(side_key, {}).get("participants", []):
            pid   = int(p.get("participantId", 0))
            pmeta = current_meta.get(pid, {})
            players_parsed.append({
                "id":       pid,
                "name":     pmeta.get("name",     "?"),
                "champion": pmeta.get("champion",  "?"),
                "role":     pmeta.get("role",      ""),
                "level":    p.get("level",         0),
                "cs":       p.get("creepScore",    0),
                "gold":     p.get("totalGoldEarned", 0),
                "kills":    p.get("kills",         0),
                "deaths":   p.get("deaths",        0),
                "assists":  p.get("assists",        0),
                "items":    [],   # filled in from /details
            })

    # Estimate game clock from frame count (frames are ~10s apart)
    game_secs = len(frames) * 10

    # Advance the cursor to the last frame's timestamp for the next poll
    last_ts = frames[-1].get("rfc460Timestamp", "")

    with _lock:
        _teams           = teams_parsed
        _game_time_secs  = game_secs
        if last_ts:
            _last_frame_iso = last_ts
        # Merge items from existing players if details haven't updated yet
        existing_items = {p["id"]: p.get("items", []) for p in _players}
        for p in players_parsed:
            p["items"] = existing_items.get(p["id"], [])
        _players = players_parsed


def _parse_details(data: dict) -> None:
    """Merge /details item data into existing _players list."""
    frames = data.get("frames", [])
    if not frames:
        return
    frame = frames[-1]

    items_by_pid: dict[int, list] = {}
    for side_key in ("blueTeam", "redTeam"):
        for p in frame.get(side_key, {}).get("participants", []):
            pid = int(p.get("participantId", 0))
            # Items are a list of item IDs; display as count for now
            items_by_pid[pid] = p.get("items", [])

    with _lock:
        for p in _players:
            pid = p.get("id", 0)
            if pid in items_by_pid:
                p["items"] = items_by_pid[pid]


# ── Feed thread ────────────────────────────────────────────────────────────────

def _feed_loop() -> None:
    global _status, _game_id, _match_name, _last_frame_iso, _player_meta, _teams, _players

    last_live_check  = 0.0
    last_window_poll = 0.0
    last_detail_poll = 0.0

    while True:
        now = time.monotonic()

        # Check for a live game
        if now - last_live_check >= LIVE_POLL_SECS:
            gid, mname, _ = _fetch_live_game()
            with _lock:
                old_gid = _game_id
                if gid != old_gid:
                    _game_id         = gid
                    _match_name      = mname
                    _last_frame_iso  = ""   # reset cursor for new game
                    _player_meta     = {}
                    _teams           = []
                    _players         = []
                    last_window_poll = 0.0
                    last_detail_poll = 0.0
                _status = f"Live: {mname}" if gid else mname
            last_live_check = now

        with _lock:
            gid            = _game_id
            last_frame_iso = _last_frame_iso
            meta           = dict(_player_meta)

        if not gid:
            time.sleep(5)
            continue

        starting_time = _window_start_time(last_frame_iso)

        # Poll /window
        if now - last_window_poll >= WINDOW_POLL_SECS:
            try:
                url = _WINDOW_URL.format(game_id=gid)
                r   = requests.get(url, params={"startingTime": starting_time}, timeout=10)
                r.raise_for_status()
                _parse_window(r.json(), meta)
                with _lock:
                    _status = f"Live: {_match_name}  (OK)"
            except Exception as exc:
                with _lock:
                    _status = f"Window error: {exc}"
            last_window_poll = now

        # Poll /details less frequently
        if now - last_detail_poll >= DETAIL_POLL_SECS:
            try:
                url = _DETAIL_URL.format(game_id=gid)
                r   = requests.get(url, params={"startingTime": starting_time}, timeout=10)
                r.raise_for_status()
                _parse_details(r.json())
            except Exception:
                pass   # details are supplemental; silently skip on error
            last_detail_poll = now

        time.sleep(1)


def start_feed_thread() -> None:
    """Start the background polling thread. Call once from main() before dpg.create_context()."""
    threading.Thread(target=_feed_loop, daemon=True, name="lol-feed").start()


# ── DPG panel ──────────────────────────────────────────────────────────────────
# All tag strings are namespaced with "lol_" to avoid collisions with main.py.

_BLUE_CLR   = (100, 160, 255)
_RED_CLR    = (255, 100, 100)
_DIM_CLR    = (120, 120, 120)
_GOLD_CLR   = (255, 205, 60)
_GREEN_CLR  = (80, 220, 80)
_HEADER_CLR = (210, 210, 100)

# Pre-allocated player row tags — avoids string formatting each frame
_P_TAGS: list[dict[str, str]] = [
    {
        "name":    f"lol_p{i}_name",
        "champ":   f"lol_p{i}_champ",
        "level":   f"lol_p{i}_level",
        "cs":      f"lol_p{i}_cs",
        "gold":    f"lol_p{i}_gold",
        "kda":     f"lol_p{i}_kda",
        "items":   f"lol_p{i}_items",
    }
    for i in range(_NUM_PLAYERS)
]


def build_panel(x: int = 20, y: int = 60, width: int = 660, height: int = 420) -> None:
    """
    Build the LoL Esports floating window.
    Call from _build_ui() (or anywhere after dpg.create_context()).
    The window is shown/hidden via the "lol_window" tag.
    """
    with dpg.window(
        tag="lol_window",
        label="LoL Esports Live",
        pos=(x, y),
        width=width,
        height=height,
        no_close=False,
        show=True,
    ):
        # ── Status bar ────────────────────────────────────────────────────────
        with dpg.group(horizontal=True):
            dpg.add_text("STATUS", color=_DIM_CLR)
            dpg.add_text("Starting...", tag="lol_status_txt", color=_DIM_CLR)

        dpg.add_separator()

        # ── Match title + game clock ──────────────────────────────────────────
        with dpg.group(horizontal=True):
            dpg.add_text("", tag="lol_match_txt",  color=_HEADER_CLR)
            dpg.add_spacer(width=20)
            dpg.add_text("", tag="lol_timer_txt",  color=_DIM_CLR)

        dpg.add_spacer(height=4)

        # ── Team scorecard ────────────────────────────────────────────────────
        with dpg.table(
            tag="lol_team_tbl",
            header_row=True,
            borders_innerV=True,
            borders_outerH=True,
            borders_outerV=True,
            row_background=True,
            policy=dpg.mvTable_SizingStretchProp,
        ):
            dpg.add_table_column(label="Team",     init_width_or_weight=0.18)
            dpg.add_table_column(label="Gold",     init_width_or_weight=0.12)
            dpg.add_table_column(label="Kills",    init_width_or_weight=0.09)
            dpg.add_table_column(label="Towers",   init_width_or_weight=0.09)
            dpg.add_table_column(label="Barons",   init_width_or_weight=0.09)
            dpg.add_table_column(label="Dragons",  init_width_or_weight=0.20)
            dpg.add_table_column(label="Inhibs",   init_width_or_weight=0.09)

            for side, name_tag, gold_tag, kills_tag, towers_tag, barons_tag, drag_tag, inhib_tag, clr in (
                (
                    "blue",
                    "lol_blue_name", "lol_blue_gold", "lol_blue_kills",
                    "lol_blue_towers", "lol_blue_barons", "lol_blue_drags", "lol_blue_inhibs",
                    _BLUE_CLR,
                ),
                (
                    "red",
                    "lol_red_name", "lol_red_gold", "lol_red_kills",
                    "lol_red_towers", "lol_red_barons", "lol_red_drags", "lol_red_inhibs",
                    _RED_CLR,
                ),
            ):
                with dpg.table_row():
                    dpg.add_text("—", tag=name_tag,   color=clr)
                    dpg.add_text("—", tag=gold_tag,   color=_GOLD_CLR)
                    dpg.add_text("—", tag=kills_tag)
                    dpg.add_text("—", tag=towers_tag)
                    dpg.add_text("—", tag=barons_tag)
                    dpg.add_text("—", tag=drag_tag,   color=_GREEN_CLR)
                    dpg.add_text("—", tag=inhib_tag)

        dpg.add_spacer(height=6)

        # ── Player stats table ────────────────────────────────────────────────
        dpg.add_text("PLAYERS", color=_DIM_CLR)
        dpg.add_separator()

        with dpg.table(
            tag="lol_player_tbl",
            header_row=True,
            borders_innerV=True,
            borders_outerH=True,
            borders_outerV=True,
            row_background=True,
            policy=dpg.mvTable_SizingStretchProp,
        ):
            dpg.add_table_column(label="Name",    init_width_or_weight=0.18)
            dpg.add_table_column(label="Champ",   init_width_or_weight=0.14)
            dpg.add_table_column(label="Lvl",     init_width_or_weight=0.06)
            dpg.add_table_column(label="CS",      init_width_or_weight=0.07)
            dpg.add_table_column(label="Gold",    init_width_or_weight=0.10)
            dpg.add_table_column(label="KDA",     init_width_or_weight=0.14)
            dpg.add_table_column(label="Items",   init_width_or_weight=0.20)

            for i, tags in enumerate(_P_TAGS):
                side_clr = _BLUE_CLR if i < 5 else _RED_CLR
                with dpg.table_row():
                    dpg.add_text("—", tag=tags["name"],  color=side_clr)
                    dpg.add_text("—", tag=tags["champ"])
                    dpg.add_text("—", tag=tags["level"], color=_DIM_CLR)
                    dpg.add_text("—", tag=tags["cs"],    color=_DIM_CLR)
                    dpg.add_text("—", tag=tags["gold"],  color=_GOLD_CLR)
                    dpg.add_text("—", tag=tags["kda"])
                    dpg.add_text("—", tag=tags["items"], color=_DIM_CLR)


# ── Per-frame UI update ────────────────────────────────────────────────────────

def update_ui() -> None:
    """
    Push shared state to DPG widgets. Call every frame from _update_ui() in main.py.
    No-ops if the panel hasn't been built or the window is hidden.
    """
    if not dpg.does_item_exist("lol_window"):
        return
    if not dpg.is_item_shown("lol_window"):
        return

    with _lock:
        status    = _status
        match_name = _match_name
        teams     = list(_teams)
        players   = list(_players)
        game_secs = _game_time_secs

    # Status
    has_live = bool(match_name and "No live" not in status and "error" not in status.lower())
    status_color = _GREEN_CLR if has_live else _DIM_CLR
    dpg.configure_item("lol_status_txt", color=status_color)
    dpg.set_value("lol_status_txt", status)

    # Match title + clock
    dpg.set_value("lol_match_txt", match_name or "No live match")
    dpg.set_value("lol_timer_txt", _fmt_time(game_secs) if game_secs else "")

    # Team scorecard
    _team_tags = [
        ("lol_blue_name", "lol_blue_gold", "lol_blue_kills", "lol_blue_towers",
         "lol_blue_barons", "lol_blue_drags", "lol_blue_inhibs"),
        ("lol_red_name",  "lol_red_gold",  "lol_red_kills",  "lol_red_towers",
         "lol_red_barons", "lol_red_drags", "lol_red_inhibs"),
    ]
    for i, team in enumerate(teams[:2]):
        name_t, gold_t, kills_t, towers_t, barons_t, drag_t, inhib_t = _team_tags[i]
        dpg.set_value(name_t,   team.get("name",   "—"))
        dpg.set_value(gold_t,   _fmt_gold(team.get("gold",   0)))
        dpg.set_value(kills_t,  str(team.get("kills",  0)))
        dpg.set_value(towers_t, str(team.get("towers", 0)))
        dpg.set_value(barons_t, str(team.get("barons", 0)))
        dpg.set_value(drag_t,   _dragon_icons(team.get("dragons", [])))
        dpg.set_value(inhib_t,  str(team.get("inhibs", 0)))

    # If no team data yet, clear
    if not teams:
        for name_t, gold_t, kills_t, towers_t, barons_t, drag_t, inhib_t in _team_tags:
            for tag in (name_t, gold_t, kills_t, towers_t, barons_t, drag_t, inhib_t):
                dpg.set_value(tag, "—")

    # Player rows
    for i, tags in enumerate(_P_TAGS):
        if i < len(players):
            p = players[i]
            kda = f"{p['kills']}/{p['deaths']}/{p['assists']}"
            # Items: show count and first 3 IDs as a compact list
            item_ids = [str(it) for it in p.get("items", []) if it]
            items_str = " ".join(item_ids[:4]) if item_ids else "—"
            dpg.set_value(tags["name"],  p.get("name",    "?"))
            dpg.set_value(tags["champ"], p.get("champion","?"))
            dpg.set_value(tags["level"], str(p.get("level", 0)))
            dpg.set_value(tags["cs"],    str(p.get("cs",    0)))
            dpg.set_value(tags["gold"],  _fmt_gold(p.get("gold", 0)))
            dpg.set_value(tags["kda"],   kda)
            dpg.set_value(tags["items"], items_str)
        else:
            for tag in tags.values():
                dpg.set_value(tag, "—")


# ── CLI test harness ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json as _json

    print("LoL Esports Feed — CLI test mode")
    print("Fetching /getLive...\n")

    # ── Fetch and print raw /getLive response ──────────────────────────────────
    try:
        _r = requests.get(_LIVE_URL, headers=_HEADERS, timeout=10)
        _r.raise_for_status()
        _raw = _r.json()
    except Exception as _e:
        print(f"ERROR fetching live endpoint: {_e}")
        raise SystemExit(1)

    print("── /getLive raw response ─────────────────────────────────────────────")
    print(_json.dumps(_raw, indent=2))
    print()

    # ── Collect all inProgress games ───────────────────────────────────────────
    _live_options: list[dict] = []   # {game_id, match_name, game_number, event_start_iso}
    for _ev in _raw.get("data", {}).get("schedule", {}).get("events", []):
        _league     = _ev.get("league", {}).get("name", "?")
        _start_iso  = _ev.get("startTime", "")
        _teams_e    = _ev.get("match", {}).get("teams", [])
        _t1 = _teams_e[0].get("name", "?") if len(_teams_e) > 0 else "?"
        _t2 = _teams_e[1].get("name", "?") if len(_teams_e) > 1 else "?"
        for _g in _ev.get("match", {}).get("games", []):
            if _g.get("state") == "inProgress":
                _live_options.append({
                    "game_id":         _g.get("id", ""),
                    "match_name":      f"{_t1} vs {_t2}  [{_league}]",
                    "game_number":     _g.get("number", "?"),
                    "event_start_iso": _start_iso,
                })

    if not _live_options:
        print("No inProgress games found. Exiting.")
        raise SystemExit(0)

    # ── Match selector ─────────────────────────────────────────────────────────
    if len(_live_options) == 1:
        _chosen = _live_options[0]
        print(f"One live game found: {_chosen['match_name']}  (game {_chosen['game_number']})\n")
    else:
        print(f"{len(_live_options)} live games found:\n")
        for _i, _opt in enumerate(_live_options):
            print(f"  [{_i + 1}] {_opt['match_name']}  (game {_opt['game_number']})")
        print()
        while True:
            try:
                _sel = int(input(f"Select match [1–{len(_live_options)}]: ").strip())
                if 1 <= _sel <= len(_live_options):
                    break
            except (ValueError, EOFError):
                pass
            print("  Invalid selection, try again.")
        _chosen = _live_options[_sel - 1]
        print()

    gid            = _chosen["game_id"]
    mname          = _chosen["match_name"]
    _starting_time = _iso_ago(30)  # 30s ago → most recent frame (frames are every ~10s)

    _OUT_DIR     = Path(__file__).parent
    _window_path = _OUT_DIR / "debug_window.json"
    _details_path= _OUT_DIR / "debug_details.json"
    _cursor      = _iso_ago(30)   # seed: 30s ago → picks up the most recent frame

    print(f"Watching: {mname}  (game_id={gid})")
    print(f"Writing frames to {_window_path.name} / {_details_path.name}  (Ctrl+C to stop)\n")

    try:
        while True:
            # ── /window ───────────────────────────────────────────────────────
            try:
                wr = requests.get(
                    _WINDOW_URL.format(game_id=gid),
                    params={"startingTime": _cursor},
                    timeout=10,
                )
                if wr.status_code == 200 and wr.content:
                    w_data = wr.json()
                    _window_path.write_text(_json.dumps(w_data, indent=2), encoding="utf-8")
                    # Advance cursor to last frame so next poll picks up from there
                    frames = w_data.get("frames", [])
                    if frames:
                        _cursor = frames[-1].get("rfc460Timestamp", _cursor)
                    print(f"[window] {wr.status_code}  cursor → {_cursor}  ({len(frames)} frames)")
                else:
                    print(f"[window] {wr.status_code}  (no new frames at {_cursor})")
            except Exception as _we:
                print(f"[window] error: {_we}")

            # ── /details (same cursor) ────────────────────────────────────────
            try:
                dr = requests.get(
                    _DETAIL_URL.format(game_id=gid),
                    params={"startingTime": _cursor},
                    timeout=10,
                )
                if dr.status_code == 200 and dr.content:
                    _details_path.write_text(_json.dumps(dr.json(), indent=2), encoding="utf-8")
                    print(f"[details] {dr.status_code}  saved")
                else:
                    print(f"[details] {dr.status_code}  (no data)")
            except Exception as _de:
                print(f"[details] error: {_de}")

            print()
            time.sleep(10)   # frames arrive every ~10 seconds

    except KeyboardInterrupt:
        print("\nStopped.")
