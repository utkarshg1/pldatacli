import polars as pl

from pldatacli.parsers.filter_parser import parse_filter


def build_expr(col, op, val):

    c = pl.col(col)

    if op == "=":
        return c == val
    if op == "!=":
        return c != val
    if op == ">":
        return c > val
    if op == "<":
        return c < val
    if op == ">=":
        return c >= val
    if op == "<=":
        return c <= val
    raise ValueError(f"Unknown operator: {repr(op)}")


def apply_filters(lf: pl.LazyFrame, filters):

    if not filters:
        return lf

    for f in filters:
        col, op, val = parse_filter(f)

        expr = build_expr(col, op, val)

        lf = lf.filter(expr)

    return lf
