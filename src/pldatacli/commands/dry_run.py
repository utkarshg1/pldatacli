"""
pldatacli/commands/dry_run.py

Dry-run logic for the query, pivot, and run commands.
Each function prints the pipeline steps that *would* be executed
without loading or processing any data.
"""

import yaml
import typer
from pathlib import Path

from pldatacli.commands.run import _as_list


# ── Helpers ───────────────────────────────────────────────────────────────────


def _header(command: str) -> None:
    typer.echo(
        typer.style(f"=== DRY RUN: {command} ===", fg=typer.colors.YELLOW, bold=True)
    )


def _footer() -> None:
    typer.echo(typer.style("No data was loaded or processed.", fg=typer.colors.YELLOW))


def _fmt_round(round_digits: int | None) -> str:
    return f"{round_digits} decimal places" if round_digits is not None else "(none)"


def _show_query_steps(
    filters: list,
    truncate: str | None,
    groupby: list,
    agg: list,
    sort: list,
    head: int | None,
    tail: int | None,
    round_digits: int | None,
) -> None:
    """Print the standard query pipeline steps (filter → truncate → groupby/agg → sort → limit → round)."""
    step = 1

    if filters:
        for f in filters:
            typer.echo(f"  Step {step}: filter       → {f}")
            step += 1
    else:
        typer.echo(f"  Step {step}: filter       → (none)")
        step += 1

    typer.echo(f"  Step {step}: truncate     → {truncate if truncate else '(none)'}")
    step += 1

    if groupby or agg:
        typer.echo(
            f"  Step {step}: groupby      → {', '.join(groupby) if groupby else '(none)'}"
        )
        step += 1
        if agg:
            for a in agg:
                typer.echo(f"  Step {step}: agg          → {a}")
                step += 1
        else:
            typer.echo(f"  Step {step}: agg          → (none)")
            step += 1
    else:
        typer.echo(f"  Step {step}: groupby/agg  → (none)")
        step += 1

    if sort:
        for s in sort:
            typer.echo(f"  Step {step}: sort         → {s}")
            step += 1
    else:
        typer.echo(f"  Step {step}: sort         → (none)")
        step += 1

    if head is not None:
        typer.echo(f"  Step {step}: limit        → head {head}")
    elif tail is not None:
        typer.echo(f"  Step {step}: limit        → tail {tail}")
    else:
        typer.echo(f"  Step {step}: limit        → (none)")
    step += 1

    typer.echo(f"  Step {step}: round        → {_fmt_round(round_digits)}")


def _show_pivot_steps(
    filters: list,
    truncate: str | None,
    index: list,
    on_col: str,
    values: str,
    aggregate: str,
    round_digits: int | None,
) -> None:
    """Print the pivot pipeline steps (filter → truncate → pivot → round)."""
    step = 1

    if filters:
        for f in filters:
            typer.echo(f"  Step {step}: filter       → {f}")
            step += 1
    else:
        typer.echo(f"  Step {step}: filter       → (none)")
        step += 1

    typer.echo(f"  Step {step}: truncate     → {truncate if truncate else '(none)'}")
    step += 1

    typer.echo(f"  Step {step}: pivot")
    typer.echo(f"    {'index':<18}: {', '.join(index) if index else '(none)'}")
    typer.echo(f"    {'on (column)':<18}: {on_col}")
    typer.echo(f"    {'values':<18}: {values}")
    typer.echo(f"    {'aggregate':<18}: {aggregate}")
    step += 1

    typer.echo(f"  Step {step}: round        → {_fmt_round(round_digits)}")


# ── Public API ────────────────────────────────────────────────────────────────


def dry_run_query(
    file: Path,
    filter: list[str],
    truncate: str | None,
    groupby: list[str],
    agg: list[str],
    sort: list[str],
    head: int | None,
    tail: int | None,
    round_digits: int | None,
    output: Path | None,
    generate_yaml: Path | None,
) -> None:
    """Print the query pipeline steps without executing anything."""
    _header("query")
    typer.echo(f"  {'Input file':<20}: {file}")
    _show_query_steps(filter, truncate, groupby, agg, sort, head, tail, round_digits)
    typer.echo(f"  {'Output':<20}: {output if output else '(display only)'}")
    typer.echo(
        f"  {'Generate YAML':<20}: {generate_yaml if generate_yaml else '(none)'}"
    )
    _footer()


def dry_run_pivot(
    file: Path,
    filter: list[str],
    truncate: str | None,
    on: str,
    index: list[str],
    values: str,
    aggregate: str,
    round_digits: int | None,
    output: Path | None,
    generate_yaml: Path | None,
) -> None:
    """Print the pivot pipeline config without executing anything."""
    _header("pivot")
    typer.echo(f"  {'Input file':<20}: {file}")
    _show_pivot_steps(filter, truncate, index, on, values, aggregate, round_digits)
    typer.echo(f"  {'Output':<20}: {output if output else '(display only)'}")
    typer.echo(
        f"  {'Generate YAML':<20}: {generate_yaml if generate_yaml else '(none)'}"
    )
    _footer()


def dry_run_run(yaml_file: Path) -> None:
    """Parse the YAML pipeline and print what would be executed without running it.

    Mirrors the exact branching logic of run_from_yaml so the output
    faithfully represents what would actually happen at runtime.
    """
    _header("run")
    typer.echo(f"  {'YAML file':<20}: {yaml_file}")

    # ── Load & validate YAML ──────────────────────────────────────────────────
    try:
        with open(yaml_file, "r") as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        typer.echo(
            typer.style(
                f"Error: YAML file '{yaml_file}' not found.", fg=typer.colors.RED
            ),
            err=True,
        )
        raise typer.Exit(1)
    except yaml.YAMLError as e:
        typer.echo(
            typer.style(f"Error parsing YAML: {e}", fg=typer.colors.RED), err=True
        )
        raise typer.Exit(1)

    if not isinstance(cfg, dict):
        typer.echo(
            typer.style("Error: YAML root must be a mapping.", fg=typer.colors.RED),
            err=True,
        )
        raise typer.Exit(1)

    if "file" not in cfg:
        typer.echo(
            typer.style(
                "Error: YAML is missing required key 'file'.", fg=typer.colors.RED
            ),
            err=True,
        )
        raise typer.Exit(1)

    typer.echo(f"  {'Input file':<20}: {cfg['file']}")
    output = cfg.get("output", None)
    typer.echo(f"  {'Output':<20}: {output if output else '(display only)'}")

    pivot_cfg = cfg.get("pivot", None)

    # ── Pivot branch ──────────────────────────────────────────────────────────
    if pivot_cfg is not None:
        typer.echo(f"  {'Pipeline type':<20}: pivot")

        if not isinstance(pivot_cfg, dict):
            typer.echo(
                typer.style(
                    f"Error: 'pivot' must be a mapping (dict), got {type(pivot_cfg).__name__!r}. "
                    "Check indentation under 'pivot:'.",
                    fg=typer.colors.RED,
                ),
                err=True,
            )
            raise typer.Exit(1)

        column = pivot_cfg.get("column", None)
        values = pivot_cfg.get("values", None)
        index = _as_list(pivot_cfg.get("index"))
        aggregate = pivot_cfg.get("aggregate", "sum")

        if not column or not values:
            missing = " ".join(
                k for k, v in [("column", column), ("values", values)] if not v
            )
            typer.echo(
                typer.style(
                    f"Error: 'pivot' block is missing required key(s): {missing}",
                    fg=typer.colors.RED,
                ),
                err=True,
            )
            raise typer.Exit(1)

        _show_pivot_steps(
            filters=_as_list(cfg.get("filter")),
            truncate=cfg.get("truncate", None),
            index=index,
            on_col=column,
            values=values,
            aggregate=aggregate,
            round_digits=cfg.get("round", None),
        )

    # ── Standard query branch ─────────────────────────────────────────────────
    else:
        typer.echo(f"  {'Pipeline type':<20}: query")
        _show_query_steps(
            filters=_as_list(cfg.get("filter")),
            truncate=cfg.get("truncate", None),
            groupby=_as_list(cfg.get("groupby")),
            agg=_as_list(cfg.get("agg")),
            sort=_as_list(cfg.get("sort")),
            head=cfg.get("head", None),
            tail=cfg.get("tail", None),
            round_digits=cfg.get("round", None),
        )

    _footer()
