import os
import pyarrow as pa
import pyarrow.parquet as pq
from dataclasses import dataclass

logging_enabled = True

@dataclass(slots=True)
class Snapshot:
    timestamp: float
    best_bid: float
    best_ask: float

@dataclass(slots=True)
class MarketMetadata:
    asset_id: str
    event_slug: str
    event_title: str
    market_question: str
    outcomes: str
    min_tick_size: float
    min_order_size: int
    is_neg_risk: bool
    game_start_time: float

class HistoryLogger:
    def __init__(self, export_dir: str) -> None:
        if not export_dir:
            raise ValueError("export_dir required")
        self.export_dir: str = export_dir
        self._history: dict[str, list[Snapshot]] = {}
        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)

    def register_asset(self, asset_id: str) -> None:
        if not asset_id:
            raise ValueError("asset_id required")
        if asset_id not in self._history:
            self._history[asset_id] = []

    def log_snapshot(self, asset_id: str, snapshot: Snapshot) -> None:
        if asset_id not in self._history:
            raise KeyError(f"Asset {asset_id} not registered")
        self._history[asset_id].append(snapshot)

    def export_and_cleanup(self, asset_id: str, metadata: MarketMetadata) -> None:
        if logging_enabled == True:
            print(f"Writing parquet for {metadata.market_question}")
        if asset_id not in self._history:
            return

        history: list[Snapshot] = self._history[asset_id]
        if not history:
            del self._history[asset_id]
            return

        timestamps: list[float] = [snap.timestamp for snap in history]
        bids: list[float] = [snap.best_bid for snap in history]
        asks: list[float] = [snap.best_ask for snap in history]

        custom_meta: dict[bytes, bytes] = {
            b"asset_id": metadata.asset_id.encode("utf-8"),
            b"event_slug": metadata.event_slug.encode("utf-8"),
            b"event_title": metadata.event_title.encode("utf-8"),
            b"market_question": metadata.market_question.encode("utf-8"),
            b"outcomes": metadata.outcomes.encode("utf-8"),
            b"min_tick_size": str(metadata.min_tick_size).encode("utf-8"),
            b"min_order_size": str(metadata.min_order_size).encode("utf-8"),
            b"is_neg_risk": str(metadata.is_neg_risk).encode("utf-8"),
        }

        schema: pa.Schema = pa.schema([
            ('timestamp', pa.float64()),
            ('best_bid', pa.float64()),
            ('best_ask', pa.float64())
        ], metadata=custom_meta)

        table: pa.Table = pa.Table.from_arrays(
            [
                pa.array(timestamps, type=pa.float64()),
                pa.array(bids, type=pa.float64()),
                pa.array(asks, type=pa.float64())
            ], 
            schema=schema
        )

        safe_slug: str = "".join(c if c.isalnum() or c in "-_" else "_" for c in metadata.event_slug)
        safe_question: str = "".join(c if c.isalnum() or c in " -_" else "_" for c in metadata.market_question)[:100]
        
        target_dir: str = os.path.join(self.export_dir, safe_slug)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        filepath: str = os.path.join(target_dir, f"{safe_question}.parquet")
        pq.write_table(table, filepath)

        del self._history[asset_id]