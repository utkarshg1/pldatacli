# pldatacli

A simple command-line tool for quick CSV data analysis using Polars, with lazy execution for efficiency.

---

# Tech Stack

* **Polars** – fast DataFrame engine with lazy execution for efficient data processing
* **Typer** – modern CLI framework for building command-line interfaces
* **Rich** – beautiful terminal rendering for clean table output

---

# PyPi Repository

Check the Repository on PyPI - [https://pypi.org/project/pldatacli/](https://pypi.org/project/pldatacli/)

---

# Installation

* Option 1: With pipx (Requires pipx to be installed)
```bash
pip install pldatacli
```
* Option 2: with uv package manager (Requires uv to be installed)
```bash
uv tool install pldatacli
```
* Option 3: with homebrew tap (Requires homebrew installed)
```bash
brew tap utkarshg1/pldatacli
brew install pldatacli
```

---

# Commands

| Command  | Description                                              |
|----------|----------------------------------------------------------|
| `query`  | Filter, aggregate, sort, and explore a single file       |
| `schema` | Inspect columns, dtypes, and null counts                 |
| `run`    | Execute a multi-step pipeline defined in a YAML file     |
| `sql`    | Run arbitrary SQL queries against one or more files      |
| `pivot`  | Create pivot tables with flexible aggregations           |
| `init`   | Generate a boilerplate YAML pipeline file                |

---

# Usage

## `query` — Exploratory Analysis

```bash
pldatacli query FILE [OPTIONS]
```

Example file:

```bash
SampleSuperstore.csv
```

---

### Filter rows

Single filter:

```bash
pldatacli query SampleSuperstore.csv \
  --filter "State:Texas"
```

Multiple filters:

```bash
pldatacli query SampleSuperstore.csv \
  --filter "State:Texas" \
  --filter "Category:Furniture"
```

Numeric filter:

```bash
pldatacli query SampleSuperstore.csv \
  --filter "Sales > 500"
```

---

### Truncate date column

Truncate a date column into a period-based group. Format: `col:period`

Supported periods: `year`, `quarter`, `month`, `week`, `day`

This creates a new derived column named `<col>_<period>` that can be used in `--groupby`.

By month:

```bash
pldatacli query SampleSuperstore.csv \
  --truncate "Order Date:month"
```

By quarter:

```bash
pldatacli query SampleSuperstore.csv \
  --truncate "Order Date:quarter"
```

By year:

```bash
pldatacli query SampleSuperstore.csv \
  --truncate "Order Date:year"
```

---

### Group by columns

Single column:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region
```

Multiple columns:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --groupby Category
```

Group by truncated date column:

```bash
pldatacli query SampleSuperstore.csv \
  --truncate "Order Date:month" \
  --groupby "Order Date_month"
```

---

### Aggregations

Single aggregation:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:sum"
```

Multiple aggregations on one column:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:sum,mean"
```

Multiple columns with aggregations:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --groupby Category \
  --agg "Sales:sum,mean" \
  --agg "Profit:sum"
```

---

### Sorting

Single sort:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:sum" \
  --sort "Profit_sum:desc"
```

Multiple sorts:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:sum" \
  --sort "Region:asc" \
  --sort "Profit_sum:desc"
```

---

### Rounding results

Round float columns to 2 digits:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:mean" \
  --round 2
```

Custom rounding:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:mean" \
  --round 4
```

---

### Limiting rows

Head:

```bash
pldatacli query SampleSuperstore.csv \
  --head 5
```

Tail:

```bash
pldatacli query SampleSuperstore.csv \
  --tail 10
```

---

### Save output to file

Save results as CSV:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:sum" \
  --output result.csv
```

Save results as Parquet:

```bash
pldatacli query SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit:sum" \
  --output result.parquet
```

> ⚡ Tip: Results are always printed to the terminal **and** saved to file simultaneously.

---

### Full query example

```bash
pldatacli query SampleSuperstore.csv \
  --filter "Region=West" \
  --truncate "Order Date:month" \
  --groupby "Order Date_month" \
  --groupby Category \
  --agg "Profit:sum,mean" \
  --agg "Sales:sum" \
  --sort "Profit_sum:desc" \
  --head 10 \
  --round 2 \
  --output monthly_west.csv
```

---

## `schema` — Inspect File Structure

Get columns, dtypes, and null counts without processing the full dataset:

```bash
pldatacli schema SampleSuperstore.csv
```

Example output:

```text
LazyFrame Schema
┏━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┓
┃ Column       ┃ Dtype   ┃ Nulls ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━┩
│ Ship Mode    │ String  │     0 │
│ Segment      │ String  │     0 │
│ Country      │ String  │     0 │
│ City         │ String  │     0 │
│ State        │ String  │     0 │
│ Postal Code  │ Int64   │     0 │
│ Region       │ String  │     0 │
│ Category     │ String  │     0 │
│ Sub-Category │ String  │     0 │
│ Sales        │ Float64 │     0 │
│ Quantity     │ Int64   │     0 │
│ Discount     │ Float64 │     0 │
│ Profit       │ Float64 │     0 │
└──────────────┴─────────┴───────┘
Rows: 9994, Columns: 13
```

> ⚡ Tip: Use `schema` before running queries to quickly inspect columns, types, and missing values.

---

## `run` — YAML Pipelines

Execute a reusable, multi-step analysis defined in a YAML file:

```bash
pldatacli run query.yaml
```

**Option 1** — Aggregation pipeline (`query.yaml`):

```yaml
file: Superstore.csv
filter:
  - "Region=West"
truncate: "Order Date:month"
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
output: monthly_west.csv
```

**Option 2** — Pivot pipeline (`pivot.yaml`):

```yaml
file: Superstore.csv
truncate: "Order Date:month"
pivot:
  column: "Region"
  index: "Order Date_month"
  values: "Profit"
  aggregate: "sum"
round: 2
output: pivot_result.csv
```

> `index` accepts a single string or a list of strings for multi-index pivots.

> ⚡ Tip: Store your YAML query files in version control alongside your data pipelines for reproducible analysis.

---

## `sql` — Ad-hoc SQL Queries

Run arbitrary Polars SQL queries against one or more files.

```bash
pldatacli sql FILE [FILES...] [OPTIONS]
```

The **first file** is always registered as the table `data`. Additional files are registered using their filename stem (lowercased, sanitized).

---

### Single file query

```bash
pldatacli sql Superstore.csv \
  -q "SELECT Category, SUM(Profit) AS total_profit FROM data GROUP BY Category ORDER BY total_profit DESC"
```

---

### Multi-file JOIN

```bash
pldatacli sql orders.csv customers.csv \
  -q "SELECT * FROM data o JOIN customers c ON o.customer_id = c.id LIMIT 100"
```

---

### Load SQL from a file

```bash
pldatacli sql sales.parquet \
  --sql-file complex_query.sql
```

> ⚡ Tip: Use `--sql-file` to keep complex queries in `.sql` files for readability and reuse. You cannot use `--sql` and `--sql-file` together.

---

### Limit output rows

```bash
pldatacli sql Superstore.csv \
  -q "SELECT * FROM data" \
  --head 20
```

```bash
pldatacli sql Superstore.csv \
  -q "SELECT * FROM data" \
  --tail 10
```

---

### Save SQL results to file

```bash
pldatacli sql Superstore.csv \
  -q "SELECT Region, SUM(Sales) AS total_sales FROM data GROUP BY Region" \
  --output result.parquet
```

---

### Full SQL example

```bash
pldatacli sql orders.csv customers.csv \
  --sql-file analysis.sql \
  --head 50 \
  --output joined_results.csv
```

---

## `pivot` — Pivot Tables

Create spreadsheet-style pivot tables with flexible row groupings, column expansion, and aggregations.

```bash
pldatacli pivot FILE [OPTIONS]
```

`--on` defines the column whose unique values become new columns, `--index` sets the row grouping, and `--values` is the column to aggregate.

---

### Basic pivot

```bash
pldatacli pivot SampleSuperstore.csv \
  --index Region \
  --on Category \
  --values Sales \
  --aggregate sum
```

---

### Multi-index pivot

Group rows by multiple columns:

```bash
pldatacli pivot SampleSuperstore.csv \
  --index Region \
  --index "Ship Mode" \
  --on Category \
  --values Sales \
  --aggregate sum
```

---

### Supported aggregations

| Aggregation | Description              |
|-------------|--------------------------|
| `sum`       | Total (default)          |
| `mean`      | Average                  |
| `min`       | Minimum value            |
| `max`       | Maximum value            |
| `first`     | First occurrence         |
| `last`      | Last occurrence          |
| `median`    | Median value             |
| `count`     | Count of non-null values |
| `len`       | Row count                |

```bash
pldatacli pivot SampleSuperstore.csv \
  --index Region \
  --on "Ship Mode" \
  --values OrderID \
  --aggregate count
```

---

### Pivot with date truncation

Combine `--truncate` with pivot to analyse trends over time:

```bash
pldatacli pivot SampleSuperstore.csv \
  --truncate "Order Date:month" \
  --index "Order Date_month" \
  --on Category \
  --values Sales \
  --aggregate sum
```

---

### Rounding pivot results

```bash
pldatacli pivot SampleSuperstore.csv \
  --index Region \
  --on Category \
  --values Profit \
  --aggregate mean \
  --round 2
```

---

### Save pivot output to file

```bash
pldatacli pivot SampleSuperstore.csv \
  --index Region \
  --on Category \
  --values Sales \
  --aggregate sum \
  --output pivot.csv
```

Save as Parquet:

```bash
pldatacli pivot sales.parquet \
  --index Region \
  --on "Ship Mode" \
  --values OrderID \
  --aggregate count \
  --output pivot.parquet
```

---

### Full pivot example

```bash
pldatacli pivot SampleSuperstore.csv \
  --truncate "Order Date:quarter" \
  --index "Order Date_quarter" \
  --index Region \
  --on Category \
  --values Revenue \
  --aggregate mean \
  --round 1 \
  --output quarterly_pivot.csv
```

---

## `init` — Generate Boilerplate YAML

Generate a ready-to-edit YAML pipeline file with commented examples:

```bash
pldatacli init TEMPLATE [OPTIONS]
```

Accepts `query` or `pivot` as the template type. Will not overwrite an existing file — use `--output` to choose a different path.

---

### Generate a query pipeline

```bash
pldatacli init query
```

Creates `query.yaml`:

```yaml
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
output: result.csv
```

---

### Generate a pivot pipeline

```bash
pldatacli init pivot
```

Creates `pivot.yaml`:

```yaml
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
output: pivot_result.csv
```

---

### Custom output path

```bash
pldatacli init query --output my_analysis.yaml
pldatacli init pivot --output my_pivot.yaml
```

> ⚡ Tip: Run `pldatacli init query` or `pldatacli init pivot` at the start of a new analysis to get a fully commented template, then edit it and run with `pldatacli run`.

---

# Version

Check the installed version:

```bash
pldatacli --version
```

or

```bash
pldatacli -v
```
