import typer
from pathlib import Path

from pldatacli.io.loader import load_lazyframe
from pldatacli.commands.filter import apply_filters
from pldatacli.commands.groupby import apply_groupby
from pldatacli.commands.agg import apply_agg
from pldatacli.commands.sort import apply_sort
from pldatacli.commands.limit import apply_limit
from pldatacli.commands.rounding import apply_round
from pldatacli.render.table import render_df

app = typer.Typer()


@app.command()
def query(
    file: Path,
    filter: list[str] = typer.Option(None),
    groupby: list[str] = typer.Option(None),
    agg: list[str] = typer.Option(None),
    sort: list[str] = typer.Option(None),
    head: int = typer.Option(None),
    tail: int = typer.Option(None),
    round_digits: int = typer.Option(None, "--round"),
):
    lf = load_lazyframe(file)
    lf = apply_filters(lf, filter)
    lf = apply_groupby(lf, groupby, agg)
    lf = apply_agg(lf, agg, groupby)
    lf = apply_sort(lf, sort)
    lf = apply_limit(lf, head, tail)
    lf = apply_round(lf, round_digits)

    df = lf.collect()
    render_df(df)
