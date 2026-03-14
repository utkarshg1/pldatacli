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
from pldatacli.render.table import render_df


def run_from_yaml(yaml_path: Path):
    with open(yaml_path) as f:
        cfg = yaml.safe_load(f)

    file = Path(cfg["file"])
    lf = load_lazyframe(file)

    filters = cfg.get("filter", [])
    groupby = cfg.get("groupby", [])
    agg = cfg.get("agg", [])
    sort = cfg.get("sort", [])

    # Ensure all list fields are actually lists
    if isinstance(filters, str):
        filters = [filters]
    if isinstance(groupby, str):
        groupby = [groupby]
    if isinstance(agg, str):
        agg = [agg]
    if isinstance(sort, str):
        sort = [sort]

    lf = apply_filters(lf, filters)
    lf = apply_truncate(lf, cfg.get("truncate", None))
    lf = apply_agg(lf, agg, groupby)
    lf = apply_sort(lf, sort)
    lf = apply_limit(lf, cfg.get("head", None), cfg.get("tail", None))
    lf = apply_round(lf, cfg.get("round", None))

    df = lf.collect()
    render_df(df)

    output = cfg.get("output", None)
    apply_export(df, Path(output) if output else None)
