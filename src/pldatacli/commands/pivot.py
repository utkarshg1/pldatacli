# src/pldatacli/commands/pivot.py
from pathlib import Path
from typing import List, Optional

import polars as pl
import typer

from pldatacli.io.loader import load_lazyframe
from pldatacli.render.table import render_df
from pldatacli.commands.filter import apply_filters
from pldatacli.commands.export import apply_export
from pldatacli.commands.rounding import apply_round
from pldatacli.commands.truncate import apply_truncate


def pivot_command(
    file: Path,
    filters: Optional[List[str]],
    index: List[str],
    on: str,
    values: str,
    aggregate: str = "sum",
    round_digits: Optional[int] = None,
    truncate: Optional[str] = None,
    output: Optional[Path] = None,
):
    """Create a pivot table from the input file."""

    valid_aggs = {
        "sum",
        "mean",
        "min",
        "max",
        "first",
        "last",
        "median",
        "count",
        "len",
    }

    if aggregate not in valid_aggs:
        typer.echo(
            f"Error: --aggregate must be one of {', '.join(sorted(valid_aggs))}",
            err=True,
        )
        raise typer.Exit(1)

    lf = load_lazyframe(file)

    lf = apply_filters(lf, filters)
    # Before the main pivot
    unique_on = (
        lf.select(pl.col(on).unique())
        .collect()  # or .collect() if small
        .to_series()
        .to_list()
    )

    lf = apply_truncate(lf, truncate)

    # Perform pivot
    lf = lf.pivot(
        on=on,
        on_columns=unique_on,
        index=index if index else None,
        values=values,
        aggregate_function=aggregate,
    )

    lf = lf.sort(by=index if index else None)

    if round_digits:
        lf = apply_round(lf, round_digits)

    df = lf.collect()

    render_df(df)
    apply_export(df, output)
