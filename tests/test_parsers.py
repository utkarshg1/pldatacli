"""Tests for filter and aggregation parsers."""

import pytest
from pldatacli.parsers.filter_parser import cast_value, parse_filter
from pldatacli.parsers.agg_parser import parse_aggs


# ── cast_value ─────────────────────────────────────────────────────────────────

def test_cast_value_int():
    assert cast_value("42") == 42
    assert isinstance(cast_value("42"), int)


def test_cast_value_float():
    assert cast_value("3.14") == pytest.approx(3.14)
    assert isinstance(cast_value("3.14"), float)


def test_cast_value_string():
    assert cast_value("West") == "West"
    assert isinstance(cast_value("West"), str)


def test_cast_value_negative_int():
    assert cast_value("-5") == -5


def test_cast_value_negative_float():
    assert cast_value("-1.5") == pytest.approx(-1.5)


# ── parse_filter ───────────────────────────────────────────────────────────────

def test_parse_filter_eq():
    assert parse_filter("Region = West") == ("Region", "=", "West")


def test_parse_filter_ne():
    assert parse_filter("Sales != 0") == ("Sales", "!=", 0)


def test_parse_filter_gt():
    col, op, val = parse_filter("Sales > 500")
    assert col == "Sales"
    assert op == ">"
    assert val == 500


def test_parse_filter_lt():
    col, op, val = parse_filter("Profit < 100")
    assert col == "Profit"
    assert op == "<"
    assert val == 100


def test_parse_filter_gte():
    col, op, val = parse_filter("Sales >= 1000")
    assert op == ">="
    assert val == 1000


def test_parse_filter_lte():
    col, op, val = parse_filter("Sales <= 50.5")
    assert op == "<="
    assert val == pytest.approx(50.5)


def test_parse_filter_strips_whitespace():
    col, op, val = parse_filter("  Sales  >  100  ")
    assert col == "Sales"
    assert val == 100


def test_parse_filter_invalid_raises():
    with pytest.raises(ValueError, match="Invalid filter expression"):
        parse_filter("NoOperatorHere")


def test_parse_filter_error_includes_expression():
    with pytest.raises(ValueError, match="NoOperatorHere"):
        parse_filter("NoOperatorHere")


def test_parse_filter_compound_value():
    # Value containing the operator char should not break on maxsplit=1
    col, op, val = parse_filter("URL = http://example.com")
    assert col == "URL"
    assert op == "="
    assert val == "http://example.com"


# ── parse_aggs ─────────────────────────────────────────────────────────────────

def test_parse_aggs_empty():
    assert parse_aggs([]) == []
    assert parse_aggs(None) == []


def test_parse_aggs_single_op():
    exprs = parse_aggs(["Sales:sum"])
    assert len(exprs) == 1


def test_parse_aggs_multiple_ops():
    exprs = parse_aggs(["Sales:sum,mean,max"])
    assert len(exprs) == 3


def test_parse_aggs_multiple_columns():
    exprs = parse_aggs(["Sales:sum", "Profit:mean"])
    assert len(exprs) == 2


def test_parse_aggs_all_supported_ops():
    ops = ["sum", "mean", "max", "min", "std", "count", "distinct_count"]
    exprs = parse_aggs([f"Col:{','.join(ops)}"])
    assert len(exprs) == len(ops)


def test_parse_aggs_unknown_op_raises():
    with pytest.raises(ValueError, match="Unknown aggregation"):
        parse_aggs(["Sales:badop"])


def test_parse_aggs_error_lists_supported():
    with pytest.raises(ValueError, match="Supported"):
        parse_aggs(["Sales:badop"])


def test_parse_aggs_column_with_colon():
    # maxsplit=1 ensures columns with ":" in name are handled
    exprs = parse_aggs(["Col:Name:sum"])
    assert len(exprs) == 1
