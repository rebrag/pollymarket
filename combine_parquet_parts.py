from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

def combine_market_parts(market_dir: Path) -> None:
    part_files: list[Path] = sorted(market_dir.glob("part*.parquet"))
    if not part_files:
        return

    market_name: str = market_dir.name
    output_file: Path = market_dir / f"{market_name}.parquet"
    print(f"Combining {len(part_files)} parts for {market_name}")

    tables: list[pa.Table] = [pq.read_table(path) for path in part_files]
    merged = pa.concat_tables(tables, promote_options="none")
    source_meta = tables[0].schema.metadata if tables and tables[0].schema.metadata else None
    if source_meta:
        merged = merged.cast(merged.schema.with_metadata(source_meta))

    pq.write_table(merged, output_file)
    print(f"Created {output_file}")

def main() -> None:
    root: Path = Path("market_data")
    if not root.exists():
        raise RuntimeError("market_data directory not found")

    for event_dir in root.iterdir():
        if not event_dir.is_dir():
            continue
        for market_dir in event_dir.iterdir():
            if not market_dir.is_dir():
                continue
            combine_market_parts(market_dir)

if __name__ == "__main__":
    main()