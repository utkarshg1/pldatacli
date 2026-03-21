OPERATORS = [">=", "<=", "!=", ">", "<", "="]


def cast_value(v):

    try:
        return int(v)
    except (ValueError, TypeError):
        try:
            return float(v)
        except (ValueError, TypeError):
            return v


def parse_filter(expr):

    for op in OPERATORS:
        if op in expr:
            col, val = expr.split(op, maxsplit=1)

            return col.strip(), op, cast_value(val.strip())

    raise ValueError(f"Invalid filter expression: {repr(expr)}")
