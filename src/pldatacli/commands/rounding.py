import polars as pl


def apply_round(lf: pl.LazyFrame, round_digits: int | None):

    if round_digits is None:
        return lf

    return lf.with_columns(
        pl.col(pl.Float64).round(round_digits),
        pl.col(pl.Float32).round(round_digits),
    )
