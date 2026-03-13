import polars as pl


def apply_limit(lf: pl.LazyFrame, head=None, tail=None):
    if head:
        return lf.head(head)

    if tail:
        return lf.tail(tail)

    return lf
