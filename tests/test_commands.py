"""Tests for command-level logic using in-memory Polars DataFrames."""

import pytest
import polars as pl
from pldatacli.commands.filter import apply_filters
from pldatacli.commands.sort import apply_sort
from pldatacli.commands.limit import apply_limit
from pldatacli.commands.rounding import apply_round
from pldatacli.commands.agg import apply_agg


@pytest.fixture
def sample_lf() -> pl.LazyFrame:
    return pl.DataFrame(
        {
            "Region": ["East", "West", "East", "West", "North"],
            "Sales": [100.0, 200.0, 150.0, 80.0, 300.0],
            "Profit": [10.0, 20.0, -5.0, 8.0, 50.0],
            "Count": [1, 2, 3, 4, 5],
        }
    ).lazy()


# ── apply_filters ──────────────────────────────────────────────────────────────

def test_filter_no_filters(sample_lf):
    result = apply_filters(sample_lf, []).collect()
    assert len(result) == 5


def test_filter_greater_than(sample_lf):
    result = apply_filters(sample_lf, ["Sales > 100"]).collect()
    assert all(result["Sales"] > 100)


def test_filter_equal(sample_lf):
    result = apply_filters(sample_lf, ["Region = East"]).collect()
    assert all(result["Region"] == "East")


def test_filter_not_equal(sample_lf):
    result = apply_filters(sample_lf, ["Region != East"]).collect()
    assert all(result["Region"] != "East")


def test_filter_less_than(sample_lf):
    result = apply_filters(sample_lf, ["Profit < 0"]).collect()
    assert all(result["Profit"] < 0)


def test_filter_gte(sample_lf):
    result = apply_filters(sample_lf, ["Sales >= 200"]).collect()
    assert all(result["Sales"] >= 200)


def test_filter_lte(sample_lf):
    result = apply_filters(sample_lf, ["Sales <= 100"]).collect()
    assert all(result["Sales"] <= 100)


def test_filter_chained(sample_lf):
    result = apply_filters(sample_lf, ["Sales > 100", "Profit > 0"]).collect()
    assert all(result["Sales"] > 100)
    assert all(result["Profit"] > 0)


def test_filter_unknown_op_raises(sample_lf):
    with pytest.raises(ValueError, match="Invalid filter expression"):
        # parse_filter raises ValueError for unknown/unrecognised expressions
        apply_filters(sample_lf, ["Sales ~ 100"]).collect()


# ── apply_sort ────────────────────────────────────────────────────────────────

def test_sort_ascending(sample_lf):
    result = apply_sort(sample_lf, ["Sales:asc"]).collect()
    sales = result["Sales"].to_list()
    assert sales == sorted(sales)


def test_sort_descending(sample_lf):
    result = apply_sort(sample_lf, ["Sales:desc"]).collect()
    sales = result["Sales"].to_list()
    assert sales == sorted(sales, reverse=True)


def test_sort_default_ascending(sample_lf):
    result = apply_sort(sample_lf, ["Sales"]).collect()
    sales = result["Sales"].to_list()
    assert sales == sorted(sales)


def test_sort_no_sort(sample_lf):
    result = apply_sort(sample_lf, []).collect()
    assert len(result) == 5


# ── apply_limit ───────────────────────────────────────────────────────────────

def test_limit_head(sample_lf):
    result = apply_limit(sample_lf, head=3, tail=None).collect()
    assert len(result) == 3


def test_limit_tail(sample_lf):
    result = apply_limit(sample_lf, head=None, tail=2).collect()
    assert len(result) == 2


def test_limit_head_takes_priority(sample_lf):
    result = apply_limit(sample_lf, head=2, tail=3).collect()
    assert len(result) == 2


def test_limit_none(sample_lf):
    result = apply_limit(sample_lf, head=None, tail=None).collect()
    assert len(result) == 5


# ── apply_round ───────────────────────────────────────────────────────────────

def test_round_float_columns(sample_lf):
    lf = pl.DataFrame({"x": [1.1111, 2.2222], "y": [3.3333, 4.4444]}).lazy()
    result = apply_round(lf, 2).collect()
    assert result["x"].to_list() == [1.11, 2.22]
    assert result["y"].to_list() == [3.33, 4.44]


def test_round_no_digits(sample_lf):
    result = apply_round(sample_lf, None).collect()
    assert len(result) == 5


def test_round_does_not_alter_int_columns(sample_lf):
    result = apply_round(sample_lf, 1).collect()
    # Count column should remain unchanged
    assert result["Count"].to_list() == [1, 2, 3, 4, 5]


# ── apply_agg ─────────────────────────────────────────────────────────────────

def test_agg_no_agg(sample_lf):
    result = apply_agg(sample_lf, [], []).collect()
    assert len(result) == 5


def test_agg_global_sum(sample_lf):
    result = apply_agg(sample_lf, ["Sales:sum"], []).collect()
    assert result["Sales_sum"][0] == pytest.approx(830.0)


def test_agg_groupby_sum(sample_lf):
    result = apply_agg(sample_lf, ["Sales:sum"], ["Region"]).collect()
    regions = set(result["Region"].to_list())
    assert regions == {"East", "West", "North"}


def test_agg_multiple_ops(sample_lf):
    result = apply_agg(sample_lf, ["Sales:sum,mean"], []).collect()
    assert "Sales_sum" in result.columns
    assert "Sales_mean" in result.columns
