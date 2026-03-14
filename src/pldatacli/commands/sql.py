# pldatacli/commands/sql.py
import polars as pl
import typer
from pathlib import Path
from typing import List, Optional

from pldatacli.io.loader import load_lazyframe
from pldatacli.render.table import render_df
from pldatacli.commands.export import apply_export


def run_sql(
    files: List[Path],
    sql: str,
    head: Optional[int] = None,
    tail: Optional[int] = None,
    output: Optional[Path] = None,
) -> None:
    """
    Core logic: register files → execute SQL → collect → render/export
    """
    if not files:
        raise typer.BadParameter("At least one file is required")

    ctx = pl.SQLContext(eager_execution=False)

    # Register all input files
    for i, path in enumerate(files):
        lf = load_lazyframe(
            path
        )  # reuse your existing loader (handles csv/parquet/ndjson/...)
        table_name = (
            "data" if i == 0 else path.stem.lower().replace("-", "_").replace(".", "_")
        )
        ctx.register(table_name, lf)

    try:
        lazy_result = ctx.execute(sql)
        df = lazy_result.collect(streaming=True)
    except Exception as e:
        typer.echo(f"SQL execution failed: {e}", err=True)
        raise typer.Exit(1)

    # Apply optional row limit (same as in query command)
    if head is not None:
        df = df.head(head)
    if tail is not None:
        df = df.tail(tail)

    render_df(df)
    apply_export(df, output)
