import polars as pl
from pathlib import Path


def apply_export(df: pl.DataFrame, output: Path | None) -> None:
    if output is None:
        return

    suffix = output.suffix.lower()

    if suffix == ".csv":
        df.write_csv(output)
        print(f"Saved CSV → {output}")
    elif suffix == ".parquet":
        df.write_parquet(output)
        print(f"Saved Parquet → {output}")
    else:
        raise ValueError(f"Unsupported format '{suffix}'. Use .csv or .parquet")
