import polars as pl
from pldatacli.parsers.agg_parser import parse_aggs


def apply_agg(lf: pl.LazyFrame, agg_strings, groupby_cols=None):
    if not agg_strings:
        return lf

    agg_exprs = parse_aggs(agg_strings)

    if groupby_cols:
        return lf.agg(agg_exprs)

    return lf.select(agg_exprs)
