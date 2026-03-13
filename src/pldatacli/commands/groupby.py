import polars as pl
from pldatacli.parsers.agg_parser import parse_aggs


def apply_groupby(lf: pl.LazyFrame, groupby_cols, agg_strings):

    if not groupby_cols:
        return lf

    agg_exprs = parse_aggs(agg_strings)

    return lf.group_by(groupby_cols).agg(agg_exprs)
