import typer
from pathlib import Path
from typing import List, Optional

from pldatacli.io.loader import load_lazyframe
from pldatacli.commands.filter import apply_filters
from pldatacli.commands.truncate import apply_truncate
from pldatacli.commands.agg import apply_agg
from pldatacli.commands.sort import apply_sort
from pldatacli.commands.limit import apply_limit
from pldatacli.commands.rounding import apply_round
from pldatacli.commands.schema import schema_command
from pldatacli.commands.export import apply_export
from pldatacli.commands.run import run_from_yaml
from pldatacli.render.table import render_df, render_schema
from pldatacli.commands.sql import run_sql
import importlib.metadata

__version__ = importlib.metadata.version("pldatacli")


# ── 2. Version callback (prints version and exits early)
def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"pldatacli version {__version__}")
        raise typer.Exit()  # important: stops execution


app = typer.Typer(
    # Optional: show help when no command is given
    no_args_is_help=True,
    # Optional: add --help description for the whole app
    help="Quick EDA CLI powered by Polars — filter, aggregate, SQL, schema, YAML pipelines, etc.",
)


# ── 3. This is the global callback — add --version here
@app.callback()
def main(
    version: Optional[bool] = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,  # ← crucial: runs before any command
        help="Show the version and exit.",
    ),
):
    """
    pldatacli — Polars-powered terminal data explorer
    """
    # You can put global setup code here if needed (logging, etc.)
    # If --version was used, the callback already exited
    pass


@app.command()
def query(
    file: Path = typer.Argument(
        ..., help="Path to the input file (csv, parquet, ndjson, etc.)"
    ),
    filter: List[str] = typer.Option(
        None,
        "--filter",
        "-f",
        help="Filter expressions like 'Sales > 100' or 'Region:West'",
    ),
    truncate: Optional[str] = typer.Option(
        None,
        "--truncate",
        "-t",
        help="Truncate datetime columns, e.g. 'Order Date:month'",
    ),
    groupby: List[str] = typer.Option(
        None, "--groupby", "-g", help="Columns to group by, e.g. 'Region' 'Category'"
    ),
    agg: List[str] = typer.Option(
        None, "--agg", "-a", help="Aggregations like 'Sales:sum,mean' 'Profit:max'"
    ),
    sort: List[str] = typer.Option(
        None, "--sort", "-s", help="Sort expressions like 'Profit_sum:desc' 'Sales'"
    ),
    head: Optional[int] = typer.Option(
        None, "--head", help="Show only the first N rows"
    ),
    tail: Optional[int] = typer.Option(
        None, "--tail", help="Show only the last N rows"
    ),
    round_digits: Optional[int] = typer.Option(
        None, "--round", help="Round numeric columns to N decimal places"
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Save result to file (csv/parquet/... by extension)",
    ),
):
    """
    Quick exploratory analysis on a single file using filters, aggregations, sorting, etc.

    Examples:
        pldatacli query Superstore.csv --agg "Profit:sum,mean" --groupby Region --sort "Profit_sum:desc"
        pldatacli query data.csv --filter "Sales > 500" "Category:Technology" --head 10
    """
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
def schema(
    file: Path = typer.Argument(..., help="Path to the input file"),
):
    """
    Show schema, data types, null counts and total row count of the file.

    Example:
        pldatacli schema Superstore.csv
    """
    lf = load_lazyframe(file)
    sch, null_count, row_count = schema_command(lf)
    render_schema(sch, null_count, row_count)


@app.command()
def run(
    yaml_file: Path = typer.Argument(
        ..., help="Path to the YAML file containing the analysis pipeline"
    ),
):
    """
    Execute a multi-step analysis defined in a YAML file.

    Example:
        pldatacli run analysis.yaml
    """
    run_from_yaml(yaml_file)


@app.command()
def sql(
    files: List[Path] = typer.Argument(
        ...,
        help="Input file(s). First file is registered as table 'data'; others as their filename stem.",
    ),
    sql: Optional[str] = typer.Option(
        None,
        "--sql",
        "-q",
        help="SQL query string to execute. Use table name 'data' for the first file.",
    ),
    sql_file: Optional[Path] = typer.Option(
        None,
        "--sql-file",
        "-f",
        help="Read SQL query from this file instead of using --sql",
    ),
    head: Optional[int] = typer.Option(
        None,
        "--head",
        help="Show only the first N rows of the result",
    ),
    tail: Optional[int] = typer.Option(
        None,
        "--tail",
        help="Show only the last N rows of the result",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="Save result to this file (csv/parquet/... depending on extension)",
    ),
):
    """
    Run arbitrary Polars SQL queries against one or more files.

    The first file is always registered as the table 'data'.
    Additional files are registered using their filename stem (lowercased, sanitized).

    Examples:
        pldatacli sql Superstore.csv -q "SELECT Category, SUM(Profit) AS total_profit FROM data GROUP BY Category ORDER BY total_profit DESC"
        pldatacli sql orders.csv customers.csv -q "SELECT * FROM data o JOIN customers c ON o.customer_id = c.id LIMIT 100" --head 20
        pldatacli sql sales.parquet --sql-file complex_query.sql -o result.parquet
    """
    if sql_file:
        if sql:
            raise typer.BadParameter("Use either --sql or --sql-file, not both.")
        try:
            sql = sql_file.read_text(encoding="utf-8").strip()
        except Exception as e:
            typer.echo(f"Failed to read SQL file: {e}", err=True)
            raise typer.Exit(1)

    if not sql:
        raise typer.BadParameter("No SQL provided. Use --sql or --sql-file.")

    run_sql(
        files=files,
        sql=sql,
        head=head,
        tail=tail,
        output=output,
    )


if __name__ == "__main__":
    app()
