import polars as pl
from pathlib import Path


def load_lazyframe(path: Path) -> pl.LazyFrame:

    if path.suffix == ".csv":
        return pl.scan_csv(path, try_parse_dates=True)

    if path.suffix == ".parquet":
        return pl.scan_parquet(path)

    raise ValueError("Unsupported File Format")
