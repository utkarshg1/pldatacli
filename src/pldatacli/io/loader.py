import polars as pl
from pathlib import Path


def load_lazyframe(path: Path) -> pl.LazyFrame:

    if path.suffix == ".csv":
        return pl.scan_csv(path, try_parse_dates=True)

    if path.suffix == ".parquet":
        return pl.scan_parquet(path)

    if path.suffix in (".ndjson", ".jsonl"):
        return pl.scan_ndjson(path)

    raise ValueError(f"Unsupported file format: '{path.suffix}'. Use .csv, .parquet, .ndjson, or .jsonl")
