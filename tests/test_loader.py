"""Tests for the file loader."""

import json
import pytest
import polars as pl
from pathlib import Path
from pldatacli.io.loader import load_lazyframe


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    p = tmp_path / "data.csv"
    p.write_text("a,b\n1,2\n3,4\n")
    return p


@pytest.fixture
def parquet_file(tmp_path: Path) -> Path:
    p = tmp_path / "data.parquet"
    pl.DataFrame({"a": [1, 3], "b": [2, 4]}).write_parquet(p)
    return p


@pytest.fixture
def ndjson_file(tmp_path: Path) -> Path:
    p = tmp_path / "data.ndjson"
    lines = [json.dumps({"a": 1, "b": 2}), json.dumps({"a": 3, "b": 4})]
    p.write_text("\n".join(lines) + "\n")
    return p


@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    p = tmp_path / "data.jsonl"
    lines = [json.dumps({"x": 10}), json.dumps({"x": 20})]
    p.write_text("\n".join(lines) + "\n")
    return p


def test_load_csv_returns_lazyframe(csv_file):
    lf = load_lazyframe(csv_file)
    assert isinstance(lf, pl.LazyFrame)
    assert len(lf.collect()) == 2


def test_load_parquet_returns_lazyframe(parquet_file):
    lf = load_lazyframe(parquet_file)
    assert isinstance(lf, pl.LazyFrame)
    assert len(lf.collect()) == 2


def test_load_ndjson_returns_lazyframe(ndjson_file):
    lf = load_lazyframe(ndjson_file)
    assert isinstance(lf, pl.LazyFrame)
    assert len(lf.collect()) == 2


def test_load_jsonl_returns_lazyframe(jsonl_file):
    lf = load_lazyframe(jsonl_file)
    assert isinstance(lf, pl.LazyFrame)
    assert len(lf.collect()) == 2


def test_load_unsupported_format_raises(tmp_path):
    p = tmp_path / "data.xlsx"
    p.write_text("dummy")
    with pytest.raises(ValueError, match="Unsupported file format"):
        load_lazyframe(p)


def test_load_error_includes_extension(tmp_path):
    p = tmp_path / "data.xyz"
    p.write_text("dummy")
    with pytest.raises(ValueError, match=".xyz"):
        load_lazyframe(p)
