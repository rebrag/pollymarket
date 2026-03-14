import json
import requests
from datetime import datetime, timedelta, timezone
from typing import cast
from models import Event, Market

# Constants
MIN_EVENT_VOL: int = 500
MIN_MARKET_VOL: int = 10000
H_BEFORE: int = 4
H_AFTER: int = 1
LOGGING_ENABLED = False

def format_local(utc_str: str) -> str:
    dt: datetime = datetime.fromisoformat(utc_str.replace("Z", "+00:00")).astimezone()
    return f"{dt.strftime('%b %d %I:%M%p')} {dt.tzname()}".replace('Eastern Standard Time', 'EST')

def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def fetch_and_filter_gamma_events() -> list[Event]:
    # Timing and API setup
    now: datetime = datetime.now(timezone.utc)
    win_start: datetime = now - timedelta(hours=H_BEFORE)
    win_end: datetime = now + timedelta(hours=H_AFTER)

    api_min: str = (now - timedelta(hours=5)).isoformat().replace("+00:00", "Z")
    api_max: str = (now + timedelta(hours=12)).isoformat().replace("+00:00", "Z")

    # Data Fetching
    resp: requests.Response = requests.get(f"https://gamma-api.polymarket.com/events?limit=500&active=true&closed=false&order=volume24hr&ascending=false&end_date_min={api_min}&end_date_max={api_max}&volume_min={MIN_MARKET_VOL}&tag_id=1")
    resp.raise_for_status()
    gamma_events: list[Event] = cast(list[Event], resp.json())

    # Processing and Filtering
    # filtered_events: list[Event] = []
    total_unfiltered_m: int = 0
    total_filtered_m: int = 0

    # Primary filter: Time window and Event volume
    filtered_events: list[Event] = [
        e for e in gamma_events
        if win_start <= parse_utc(e.get("startTime", "2026-01-01T00:00:00Z")) <= win_end
        and float(e.get("volume", 0)) > MIN_EVENT_VOL
    ]
    filtered_events.sort(key=lambda x: (x["startTime"], x["title"]))

    print(f"Current UTC: {now.strftime('%H:%M:%S')}")

    token_list = set()
    output_events = []

    for event in filtered_events:
        raw_markets: list[Market] = event.get('markets', [])
        
        # Market filter: Volume threshold
        valid_markets: list[Market] = [
            m for m in raw_markets 
            if float(m.get('volume', 0)) > MIN_EVENT_VOL
            and float(m.get('bestAsk', 0)) not in {0.001, 1.0}
            and float(m.get('spread', 0.99)) < 0.3
            and float(m.get('lastTradePrice', 0.00001)) > 0.00002
        ]
        valid_markets.sort(key=lambda y: float(y.get('volumeNum', 0)), reverse=True)
        event['markets'] = valid_markets

        total_unfiltered_m += len(raw_markets)
        total_filtered_m += len(valid_markets)
        output_events.append(event)

        # Output formatting
        time_lbl: str = format_local(event["startTime"])
        vol: int = round(float(event.get('volume', 0)))
        if LOGGING_ENABLED: print(f"{time_lbl}\tVol: {vol}\t{event['title']} (Markets: {len(valid_markets)}/{len(raw_markets)})")
        
        for m in valid_markets:
            m_vol: float = m.get('volumeNum', 0)
            if LOGGING_ENABLED: print(f"  └─ Market: {m['question']} \t volume: {round(m_vol,2)} \t bestAsk: {m['bestAsk']} \t spread: {m['spread']}")
            token_list.add(m["clobTokenIds"].strip('[]"').partition('",')[0])
            

    # Final Summary
    print("--- Final Totals ---")
    # print(f'len(token_list) = {len(token_list)}')
    print(f"Events (API/Window): {len(gamma_events)}/len(filtered_events):{len(filtered_events)} len(output_events): {len(output_events)}")
    print(f"Markets from Filtered (Unfiltered/Filtered): {total_unfiltered_m}/{total_filtered_m}")

    with open("filtered_gamma.txt", "w", encoding="utf-8") as f:
        json.dump(output_events, f, ensure_ascii=False, indent=2)

    return output_events


if __name__ == "__main__":
    fetch_and_filter_gamma_events()