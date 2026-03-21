import polars as pl


def schema_command(lf: pl.LazyFrame):
    # Get column names without warnings
    sch = lf.collect_schema()
    col_names = sch.names()
    schema_dict = {c: str(sch[c]) for c in col_names}

    # Compute null counts lazily
    null_exprs = [pl.col(c).is_null().sum().alias(c) for c in col_names]
    nulls_df = lf.select(null_exprs).collect()
    null_counts = {c: nulls_df[c].item() for c in col_names}

    # Compute row count lazily
    row_count = lf.select([pl.count().alias("row_count")]).collect()["row_count"].item()

    # 4. Render
    return schema_dict, null_counts, row_count
