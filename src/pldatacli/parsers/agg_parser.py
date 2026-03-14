import polars as pl


AGG_MAP = {
    "sum": lambda c: pl.col(c).sum(),
    "mean": lambda c: pl.col(c).mean(),
    "max": lambda c: pl.col(c).max(),
    "min": lambda c: pl.col(c).min(),
    "std": lambda c: pl.col(c).std(),
    "count": lambda c: pl.col(c).count(),
    "distinct_count": lambda c: pl.col(c).n_unique(),
}


def parse_aggs(agg_strings):

    if not agg_strings:
        return []

    exprs = []

    for a in agg_strings:
        col, ops = a.split(":")

        for op in ops.split(","):
            exprs.append(AGG_MAP[op](col).alias(f"{col}_{op}"))

    return exprs
