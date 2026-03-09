import json
import requests
from datetime import datetime, timedelta, timezone
from typing import cast, List
from models import Event, Market

MIN_EVENT_VOL: int = 500
H_BEFORE: int = 4
H_AFTER: int = 1

def parse_utc(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

# Timing for the event window
now: datetime = datetime.now(timezone.utc)
win_start: datetime = now - timedelta(hours=H_BEFORE)
win_end: datetime = now + timedelta(hours=H_AFTER)

# API bounds
api_min: str = (now - timedelta(hours=5)).isoformat().replace("+00:00", "Z")
api_max: str = (now + timedelta(hours=12)).isoformat().replace("+00:00", "Z")

# Data Fetching
url: str = f"https://gamma-api.polymarket.com/events?limit=500&active=true&closed=false&order=volume24hr&ascending=false&end_date_min={api_min}&end_date_max={api_max}&volume_min=100&tag_id=1"
resp: requests.Response = requests.get(url)
resp.raise_for_status()
gamma_events: List[Event] = cast(List[Event], resp.json())

# Output collections
tracked_asset_ids: List[str] = []
filtered_events: List[Event] = []

# Filtering logic
eligible_events: List[Event] = [
    e for e in gamma_events
    if win_start <= parse_utc(e.get("startTime", "2026-01-01T00:00:00Z")) <= win_end
    and float(e.get("volume", 0)) > MIN_EVENT_VOL
]

for event in eligible_events:
    valid_event: bool = False
    for m in event.get('markets', []):
        # Filtering constraints
        vol: float = float(m.get('volume', 0))
        ask: float = float(m.get('bestAsk', 0))
        
        if vol > MIN_EVENT_VOL and ask not in {0.001, 1.0}:
            # Extract first token ID from the JSON-string format
            raw_ids: str = m.get("clobTokenIds", "[]")
            token_id: str = raw_ids.strip('[]"').partition('",')[0]
            
            if token_id:
                tracked_asset_ids.append(token_id)
                valid_event = True
    
    if valid_event:
        filtered_events.append(event)

# Clean up duplicates if a market appears in multiple events (rare but possible)
tracked_asset_ids = list(set(tracked_asset_ids))

print(f"Extraction Complete at {now.strftime('%H:%M:%S')} UTC")
print(f"Collected {len(tracked_asset_ids)} unique Token IDs for tracking.")
print(f"Example IDs: {tracked_asset_ids[:3]}")

with open("token_list.txt", "w", encoding="utf-8") as f:
    json.dump(tracked_asset_ids, f, ensure_ascii=False, indent=2)