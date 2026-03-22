import os
import glob
import time
import threading
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq


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
    volume: float
    volume_24hr: float
    liquidity: float
    image_url: str
    resolution_source: str
    end_date: str


class HistoryLogger:
    """
    Stores in-memory snapshots per asset_id.

    Strategy:
      - During live ingestion: store snapshots in memory (log_snapshot).
      - On finalize (export_and_cleanup):
          1) flush in-memory snapshots to a new part file
          2) compact all part files into a single final parquet (one-file:one-market)
          3) delete part files + remove in-memory history
    """

    def __init__(self, export_dir: str) -> None:
        if not export_dir:
            raise ValueError("export_dir required")
        self.export_dir: str = export_dir
        self._history: dict[str, list[Snapshot]] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._part_seq: dict[str, int] = {}

        if not os.path.exists(self.export_dir):
            os.makedirs(self.export_dir)

    def _lock_for(self, asset_id: str) -> threading.Lock:
        lock = self._locks.get(asset_id)
        if lock is None:
            lock = threading.Lock()
            self._locks[asset_id] = lock
        return lock

    def register_asset(self, asset_id: str) -> None:
        if not asset_id:
            raise ValueError("asset_id required")
        with self._lock_for(asset_id):
            if asset_id not in self._history:
                self._history[asset_id] = []

    def log_snapshot(self, asset_id: str, snapshot: Snapshot) -> None:
        with self._lock_for(asset_id):
            if asset_id not in self._history:
                raise KeyError(f"Asset {asset_id} not registered")
            self._history[asset_id].append(snapshot)

    @staticmethod
    def _safe_slug(slug: str) -> str:
        return "".join(c if c.isalnum() or c in "-_" else "_" for c in slug)[:160] or "unknown_event"

    @staticmethod
    def _safe_question(question: str) -> str:
        # keep filenames manageable and cross-platform safe
        cleaned = "".join(c if c.isalnum() or c in " -_" else "_" for c in question)
        cleaned = cleaned.strip().replace("  ", " ")
        return cleaned[:120] or "unknown_question"

    def _event_dir(self, metadata: MarketMetadata) -> str:
        return os.path.join(self.export_dir, self._safe_slug(metadata.event_slug))

    def _parts_dir(self, metadata: MarketMetadata) -> str:
        # Directory per market where part files accumulate.
        return os.path.join(self._event_dir(metadata), self._safe_question(metadata.market_question))

    def get_parts_dir(self, metadata: MarketMetadata) -> str:
        return self._parts_dir(metadata)

    def _final_path(self, metadata: MarketMetadata) -> str:
        # One-file:one-market output
        return os.path.join(self._event_dir(metadata), f"{self._safe_question(metadata.market_question)}.parquet")

    def _ensure_dir(self, path: str) -> None:
        if not os.path.exists(path):
            os.makedirs(path)

    def _next_part_path(self, asset_id: str, metadata: MarketMetadata) -> str:
        parts_dir = self._parts_dir(metadata)
        self._ensure_dir(parts_dir)

        seq = self._part_seq.get(asset_id)
        if seq is None:
            # initialize from existing part files (for reconnect / restart scenarios)
            existing = glob.glob(os.path.join(parts_dir, "part-*.parquet"))
            if existing:
                # parse "part-000123.parquet"
                max_n = 0
                for p in existing:
                    base = os.path.basename(p)
                    try:
                        n = int(base.replace("part-", "").replace(".parquet", ""))
                        if n > max_n:
                            max_n = n
                    except ValueError:
                        continue
                seq = max_n + 1
            else:
                seq = 1

        self._part_seq[asset_id] = seq + 1
        return os.path.join(parts_dir, f"part-{seq:06d}.parquet")

    @staticmethod
    def _arrow_metadata(metadata: MarketMetadata) -> dict[bytes, bytes]:
        return {
            b"asset_id": metadata.asset_id.encode("utf-8"),
            b"event_slug": metadata.event_slug.encode("utf-8"),
            b"event_title": metadata.event_title.encode("utf-8"),
            b"market_question": metadata.market_question.encode("utf-8"),
            b"outcomes": metadata.outcomes.encode("utf-8"),
            b"min_tick_size": str(metadata.min_tick_size).encode("utf-8"),
            b"min_order_size": str(metadata.min_order_size).encode("utf-8"),
            b"is_neg_risk": str(metadata.is_neg_risk).encode("utf-8"),
            b"game_start_time": str(metadata.game_start_time).encode("utf-8"),
            b"volume": str(metadata.volume).encode("utf-8"),
            b"volume_24hr": str(metadata.volume_24hr).encode("utf-8"),
            b"liquidity": str(metadata.liquidity).encode("utf-8"),
            b"image_url": metadata.image_url.encode("utf-8"),
            b"resolution_source": metadata.resolution_source.encode("utf-8"),
            b"end_date": metadata.end_date.encode("utf-8"),
        }

    @staticmethod
    def _table_from_history(history: list[Snapshot], meta: dict[bytes, bytes]) -> pa.Table:
        timestamps: list[float] = [snap.timestamp for snap in history]
        bids: list[float] = [snap.best_bid for snap in history]
        asks: list[float] = [snap.best_ask for snap in history]

        schema: pa.Schema = pa.schema(
            [
                ("timestamp", pa.float64()),
                ("best_bid", pa.float64()),
                ("best_ask", pa.float64()),
            ],
            metadata=meta,
        )

        return pa.Table.from_arrays(
            [
                pa.array(timestamps, type=pa.float64()),
                pa.array(bids, type=pa.float64()),
                pa.array(asks, type=pa.float64()),
            ],
            schema=schema,
        )

    def flush_part_only(self, asset_id: str, metadata: MarketMetadata) -> None:
        part_path = self.export_part(asset_id, metadata)
        if part_path is not None:
            print(f"Writing parquet part for {metadata.market_question}")

    def export_part(self, asset_id: str, metadata: MarketMetadata) -> str | None:
        """
        Flush current in-memory snapshots for asset_id into a new part parquet.
        Returns the written file path or None if no snapshots.
        """
        with self._lock_for(asset_id):
            history = self._history.get(asset_id)
            if not history:
                return None

            meta = self._arrow_metadata(metadata)
            table = self._table_from_history(history, meta)
            part_path = self._next_part_path(asset_id, metadata)

            pq.write_table(table, part_path)

            # Clear in-memory snapshots but keep the key registered.
            self._history[asset_id] = []

            return part_path

    def compact_market(self, metadata: MarketMetadata, delete_parts: bool = True) -> str | None:
        """
        Combine parquet files for this market into one final parquet.
        If a final parquet already exists, it is included first so history is preserved.
        Returns final file path or None if there are no input files.
        """
        event_dir = self._event_dir(metadata)
        parts_dir = self._parts_dir(metadata)
        final_path = self._final_path(metadata)

        part_paths: list[str] = []
        if os.path.exists(parts_dir):
            part_paths = sorted(glob.glob(os.path.join(parts_dir, "part-*.parquet")))

        input_paths: list[str] = []
        if os.path.exists(final_path):
            input_paths.append(final_path)
        input_paths.extend(part_paths)

        if not input_paths:
            return None

        self._ensure_dir(event_dir)

        tmp_path = final_path + ".tmp"
        meta = self._arrow_metadata(metadata)

        writer: pq.ParquetWriter | None = None
        try:
            for p in input_paths:
                table = pq.read_table(p)
                if writer is None:
                    # Ensure metadata present on final schema
                    schema = table.schema.with_metadata(meta)
                    writer = pq.ParquetWriter(tmp_path, schema=schema)
                writer.write_table(table)

            if writer is not None:
                writer.close()

            os.replace(tmp_path, final_path)

        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

        if delete_parts:
            for p in part_paths:
                try:
                    os.remove(p)
                except Exception:
                    pass
            try:
                os.rmdir(parts_dir)
            except Exception:
                pass

        return final_path

    def export_and_cleanup(self, asset_id: str, metadata: MarketMetadata) -> None:
        """
        Finalize a market:
          - Flush a final part (if any in-memory history exists)
          - Compact all parts into one final parquet
          - Remove in-memory state for asset_id
        """
        with self._lock_for(asset_id):
            pass

        part_path: str | None = self.export_part(asset_id, metadata)
        if part_path is not None:
            print(f"Writing parquet part for {metadata.market_question}")

        final_path: str | None = self.compact_market(metadata, delete_parts=True)
        if final_path is not None:
            print(f"Writing parquet for {metadata.market_question}")

        with self._lock_for(asset_id):
            if asset_id in self._history:
                del self._history[asset_id]
            if asset_id in self._locks:
                del self._locks[asset_id]
            if asset_id in self._part_seq:
                del self._part_seq[asset_id]

        if final_path is None:
            raise ValueError(f"No parquet files generated or found for compaction: {asset_id}")

        return final_path
