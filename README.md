# pldatacli

A simple command-line tool for quick CSV data analysis.

---

# Tech Stack

* **Polars** – fast DataFrame engine with lazy execution for efficient data processing
* **Typer** – modern CLI framework for building command-line interfaces
* **Rich** – beautiful terminal rendering for clean table output

---

# Usage

### Basic command

```bash
pldatacli FILE [OPTIONS]
```

Example file:

```bash
SampleSuperstore.csv
```

---

### Filter rows

```bash
pldatacli SampleSuperstore.csv \
  --filter "State=Texas"
```

Multiple filters:

```bash
pldatacli SampleSuperstore.csv \
  --filter "State=Texas" \
  --filter "Category=Furniture"
```

---

### Group by columns

Single column:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region
```

Multiple columns:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --groupby Category
```

---

### Aggregations

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit=sum"
```

Multiple aggregations:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit=sum,mean"
```

Multiple columns with aggregations:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --groupby Category \
  --agg "Sales=sum,mean" \
  --agg "Profit=sum"
```

---

### Sorting

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit=sum" \
  --sort "Profit_sum:desc"
```

Multiple sorts:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit=sum" \
  --sort "Region:asc" \
  --sort "Profit_sum:desc"
```

---

### Rounding results

Round float columns to **2 digits**:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit=mean" \
  --round 2
```

Custom rounding:

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --agg "Profit=mean" \
  --round 4
```

---

### Full example

```bash
pldatacli SampleSuperstore.csv \
  --groupby Region \
  --groupby Category \
  --agg "Profit=sum,mean" \
  --sort "Profit_sum:desc" \
  --round 2
```
