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

* Option 1: Directly with pip
```bash
pip install pldatacli
```
* Option 2: with uv package manager (Requires uv to be installed)
```bash
uv tool install pldatacli
```

---

# Usage

### Basic query command

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

Multiple aggregations:

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
  --filter "Region:West" \
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

### Run query from YAML file

Save a reusable query as a YAML file and run it with a single command.

```bash
pldatacli run query.yaml
```

Example `query.yaml`:

```yaml
file: Superstore.csv
filter:
  - "Region:West"
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

> ⚡ Tip: Store your YAML query files in version control alongside your data pipelines for reproducible analysis.

---

### Schema inspection

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
