import polars as pl


def apply_sort(lf: pl.LazyFrame, sort_args):

    if not sort_args:
        return lf

    columns = []
    descending = []

    for s in sort_args:
        if ":" in s:
            col, direction = s.split(":")
            desc = direction.lower() == "desc"
        else:
            col = s
            desc = False

        columns.append(col)
        descending.append(desc)

    return lf.sort(columns, descending=descending)
