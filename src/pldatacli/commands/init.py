from pathlib import Path
from typing import Literal

import typer

QUERY_TEMPLATE = """\
file: data.csv
filter:
  - "Column=Value"
  - "Sales > 100"
truncate: "Order Date:month"   # periods: year, quarter, month, week, day
groupby:
  - "Order Date_month"
  - Category
agg:
  - "Profit:sum,mean"
  - "Sales:sum"
sort:
  - "Profit_sum:desc"
head: 10
round: 2
output: result.csv             # Can save to csv or parquet
"""

PIVOT_TEMPLATE = """\
file: data.csv
truncate: "Order Date:month"   # periods: year, quarter, month, week, day — optional
pivot:
  column: "Category"           # unique values of this column become new columns
  index:                       # row grouping — single string or list
    - "Order Date_month"
    - Region
  values: "Sales"              # column to aggregate
  aggregate: "sum"             # sum, mean, min, max, first, last, median, count, len
round: 2
output: pivot_result.parquet   # Can save to csv or parquet
"""

TEMPLATES: dict[Literal["query", "pivot"], tuple[str, str]] = {
    "query": (QUERY_TEMPLATE, "query.yaml"),
    "pivot": (PIVOT_TEMPLATE, "pivot.yaml"),
}


def init_command(template: Literal["query", "pivot"], output: str | None) -> None:
    content, default_name = TEMPLATES[template]
    out_path = Path(output) if output else Path(default_name)

    if out_path.exists():
        typer.echo(
            f"File already exists: {out_path}. Use --output to specify a different path.",
            err=True,
        )
        raise typer.Exit(1)

    out_path.write_text(content)
    typer.echo(f"Created {out_path}")
