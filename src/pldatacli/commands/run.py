import yaml
from pathlib import Path
from pldatacli.io.loader import load_lazyframe
from pldatacli.commands.filter import apply_filters
from pldatacli.commands.truncate import apply_truncate
from pldatacli.commands.agg import apply_agg
from pldatacli.commands.sort import apply_sort
from pldatacli.commands.limit import apply_limit
from pldatacli.commands.rounding import apply_round
from pldatacli.commands.export import apply_export
from pldatacli.commands.pivot import pivot_command
from pldatacli.render.table import render_df


def _as_list(val) -> list:
    """Coerce a YAML scalar-or-list value to a list."""
    if val is None:
        return []
    if isinstance(val, str):
        return [val]
    return list(val)


def run_from_yaml(yaml_path: Path):
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)

    file = Path(cfg["file"])
    output = cfg.get("output", None)

    # ── Pivot branch ──────────────────────────────────────────────────────────
    pivot_cfg = cfg.get("pivot", None)
    if pivot_cfg is not None:
        if not isinstance(pivot_cfg, dict):
            raise ValueError(
                f"'pivot' in YAML must be a mapping (dict), got {type(pivot_cfg).__name__!r}. "
                "Check that all keys under 'pivot:' are indented by at least 2 spaces."
            )

        column = pivot_cfg.get("column", None)
        values = pivot_cfg.get("values", None)
        index = pivot_cfg.get("index", [])
        aggregate = pivot_cfg.get("aggregate", "sum")

        if not column or not values:
            raise ValueError(
                f"'pivot' block is missing required key(s): "
                f"{'column' if not column else ''} {'values' if not values else ''}".strip()
            )

        if isinstance(index, str):
            index = [index]

        pivot_command(
            file=file,
            filters=cfg.get("filter", None),
            truncate=cfg.get("truncate", None),
            index=index,
            on=column,
            values=values,
            aggregate=aggregate,
            round_digits=cfg.get("round", None),
            output=Path(output) if output else None,
        )
        return

    # ── Standard agg/groupby branch ───────────────────────────────────────────
    filters = _as_list(cfg.get("filter"))
    groupby = _as_list(cfg.get("groupby"))
    agg = _as_list(cfg.get("agg"))
    sort = _as_list(cfg.get("sort"))

    lf = load_lazyframe(file)
    lf = apply_filters(lf, filters)
    lf = apply_truncate(lf, cfg.get("truncate", None))
    lf = apply_agg(lf, agg, groupby)
    lf = apply_sort(lf, sort)
    lf = apply_limit(lf, cfg.get("head", None), cfg.get("tail", None))
    lf = apply_round(lf, cfg.get("round", None))
    df = lf.collect()
    render_df(df)
    apply_export(df, Path(output) if output else None)
