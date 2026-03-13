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
