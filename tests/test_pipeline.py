"""Integration tests using real files and the YAML pipeline runner."""

import json
import pytest
import polars as pl
from pathlib import Path
from pldatacli.commands.run import run_from_yaml


SUPERSTORE = Path(__file__).parent.parent / "data" / "Superstore.csv"


@pytest.fixture
def superstore_yaml(tmp_path: Path) -> Path:
    yaml_file = tmp_path / "pipeline.yaml"
    yaml_file.write_text(
        f"file: {SUPERSTORE}\n"
        "filter: 'Sales > 500'\n"
        "groupby: Region\n"
        "agg: 'Sales:sum'\n"
        "sort: 'Sales_sum:desc'\n"
    )
    return yaml_file


@pytest.fixture
def small_csv(tmp_path: Path) -> Path:
    p = tmp_path / "sales.csv"
    pl.DataFrame(
        {
            "Region": ["East", "West", "East", "West"],
            "Category": ["A", "A", "B", "B"],
            "Sales": [100.0, 200.0, 150.0, 80.0],
        }
    ).write_csv(p)
    return p


@pytest.fixture
def query_yaml(tmp_path: Path, small_csv: Path) -> Path:
    yaml_file = tmp_path / "query.yaml"
    yaml_file.write_text(
        f"file: {small_csv}\n"
        "filter: 'Sales > 100'\n"
        "groupby: Region\n"
        "agg: 'Sales:sum,mean'\n"
    )
    return yaml_file


@pytest.fixture
def pivot_yaml(tmp_path: Path, small_csv: Path) -> Path:
    yaml_file = tmp_path / "pivot.yaml"
    yaml_file.write_text(
        f"file: {small_csv}\n"
        "pivot:\n"
        "  column: Category\n"
        "  values: Sales\n"
        "  index: Region\n"
        "  aggregate: sum\n"
    )
    return yaml_file


def test_query_pipeline_runs(query_yaml, capsys):
    run_from_yaml(query_yaml)
    captured = capsys.readouterr()
    # Should output a table
    assert "Sales" in captured.out


def test_query_pipeline_with_superstore(superstore_yaml, capsys):
    pytest.importorskip("polars")
    if not SUPERSTORE.exists():
        pytest.skip("Superstore.csv not found")
    run_from_yaml(superstore_yaml)
    captured = capsys.readouterr()
    assert "Sales_sum" in captured.out


def test_pivot_pipeline_runs(pivot_yaml, capsys):
    run_from_yaml(pivot_yaml)
    captured = capsys.readouterr()
    assert captured.out  # some output produced


def test_ndjson_pipeline_round_trip(tmp_path: Path):
    """Write a CSV, export to NDJSON, reload and verify."""
    csv_path = tmp_path / "source.csv"
    pl.DataFrame({"x": [1, 2, 3], "y": [4.0, 5.0, 6.0]}).write_csv(csv_path)

    # Export via YAML pipeline
    out_ndjson = tmp_path / "out.ndjson"
    yaml_file = tmp_path / "pipeline.yaml"
    yaml_file.write_text(f"file: {csv_path}\noutput: {out_ndjson}\n")
    run_from_yaml(yaml_file)

    assert out_ndjson.exists()
    # Reload and verify
    from pldatacli.io.loader import load_lazyframe
    lf = load_lazyframe(out_ndjson)
    result = lf.collect()
    assert len(result) == 3
    assert "x" in result.columns


def test_yaml_missing_file_key_raises(tmp_path: Path):
    yaml_file = tmp_path / "bad.yaml"
    yaml_file.write_text("groupby: Region\n")
    with pytest.raises(KeyError):
        run_from_yaml(yaml_file)
