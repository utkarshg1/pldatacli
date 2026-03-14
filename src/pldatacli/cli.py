import typer
from pathlib import Path

from pldatacli.io.loader import load_lazyframe
from pldatacli.commands.filter import apply_filters
from pldatacli.commands.truncate import apply_truncate
from pldatacli.commands.agg import apply_agg
from pldatacli.commands.sort import apply_sort
from pldatacli.commands.limit import apply_limit
from pldatacli.commands.rounding import apply_round
from pldatacli.commands.schema import schema_command
from pldatacli.commands.export import apply_export
from pldatacli.render.table import render_df, render_schema

app = typer.Typer()


@app.command()
def query(
    file: Path,
    filter: list[str] = typer.Option(None),
    truncate: str = typer.Option(None),
    groupby: list[str] = typer.Option(None),
    agg: list[str] = typer.Option(None),
    sort: list[str] = typer.Option(None),
    head: int = typer.Option(None),
    tail: int = typer.Option(None),
    round_digits: int = typer.Option(None, "--round"),
    output: Path = typer.Option(None),
):
    lf = load_lazyframe(file)
    lf = apply_filters(lf, filter)
    lf = apply_truncate(lf, truncate)
    lf = apply_agg(lf, agg, groupby)
    lf = apply_sort(lf, sort)
    lf = apply_limit(lf, head, tail)
    lf = apply_round(lf, round_digits)

    df = lf.collect()
    render_df(df)
    apply_export(df, output)


@app.command()
def schema(file: Path):
    lf = load_lazyframe(file)
    sch, null_count, row_count = schema_command(lf)
    render_schema(sch, null_count, row_count)
