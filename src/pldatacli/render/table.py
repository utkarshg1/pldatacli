from rich.console import Console
from rich.table import Table

console = Console()


def render_df(df):

    table = Table()

    for col in df.columns:
        table.add_column(col)

    for row in df.iter_rows():
        table.add_row(*[str(x) for x in row])

    console.print(table)


def render_schema(schema: dict[str, str], null_counts: dict[str, int], row_count: int):
    """Render schema information in a Rich table."""
    table = Table(title="LazyFrame Schema")
    table.add_column("Column")
    table.add_column("Dtype")
    table.add_column("Nulls", justify="right")

    for col, dtype in schema.items():
        table.add_row(col, dtype, str(null_counts.get(col, 0)))

    console.print(table)
    console.print(f"Rows: {row_count}, Columns: {len(schema)}")
