import polars as pl

PERIOD_MAP = {
    "year": "1y",
    "quarter": "1q",
    "month": "1mo",
    "week": "1w",
    "day": "1d",
}


def apply_truncate(lf: pl.LazyFrame, truncate: str | None) -> pl.LazyFrame:
    """
    truncate: "col:period"  e.g. "order_date:month"
    Adds a new column <col>_<period> with the truncated date,
    and appends it to groupby automatically via the column name.
    """
    if not truncate:
        return lf

    parts = truncate.split(":")
    if len(parts) != 2:
        raise ValueError(
            "--truncate must be in format col:period e.g. order_date:month"
        )

    col, period = parts[0].strip(), parts[1].strip().lower()

    if period not in PERIOD_MAP:
        raise ValueError(f"Unknown period '{period}'. Choose from: {list(PERIOD_MAP)}")

    every = PERIOD_MAP[period]
    new_col = f"{col}_{period}"

    lf = lf.with_columns(pl.col(col).dt.truncate(every).alias(new_col))

    return lf
