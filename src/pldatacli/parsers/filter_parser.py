OPERATORS = [">=", "<=", "!=", ">", "<", "="]


def cast_value(v):

    try:
        return int(v)
    except:
        try:
            return float(v)
        except:
            return v


def parse_filter(expr):

    for op in OPERATORS:
        if op in expr:
            col, val = expr.split(op)

            return col.strip(), op, cast_value(val.strip())

    raise ValueError("Invalid filter expression")
