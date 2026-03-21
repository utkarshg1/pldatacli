import polars as pl
import typer
from pathlib import Path


def apply_export(df: pl.DataFrame, output: Path | None) -> None:
    if output is None:
        return

    suffix = output.suffix.lower()

    if suffix == ".csv":
        df.write_csv(output)
        typer.echo(f"Saved CSV → {output}")
    elif suffix == ".parquet":
        df.write_parquet(output)
        typer.echo(f"Saved Parquet → {output}")
    elif suffix in (".ndjson", ".jsonl"):
        df.write_ndjson(output)
        typer.echo(f"Saved NDJSON → {output}")
    else:
        raise ValueError(f"Unsupported format '{suffix}'. Use .csv, .parquet, .ndjson, or .jsonl")
