"""CLI entry point — `dbprofile run --config config.yaml`."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console

from dbprofile.output_dir import auto_name, resolve_output_dir, run_stem

console = Console()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        format="%(levelname)s %(name)s: %(message)s",
        level=level,
    )


# ── Export-flag resolution ──────────────────────────────────────────────────
#
# The three --export-* flags share one mental model:
#   * 'none'      → skip this output explicitly
#   * 'auto'      → write to the auto-named path in the resolved out_dir
#   * <path>      → write to a custom path (overrides auto-naming)
#   * None        → "no opinion": defaults to 'auto' when --project-dir is set,
#                   skip when not. This keeps legacy ./reports/ behavior opt-in
#                   and the new dq_eda/ workflow opt-out.

def _resolve_export_path(
    flag_value: str | None,
    project_dir: str | None,
    stem: str,
    ext: str,
    run_at: datetime,
    out_dir: Path,
) -> str | None:
    """Return a concrete output path (or None to skip this export)."""
    if flag_value == "none":
        return None
    if flag_value is None:
        if not project_dir:
            return None
        flag_value = "auto"
    if flag_value == "auto":
        return str(out_dir / auto_name(stem, ext, run_at=run_at))
    return flag_value


def _resolve_export_toggle(
    flag_value: str | None,
    project_dir: str | None,
) -> bool:
    """For boolean exports (notebooks). Returns True iff we should write."""
    if flag_value == "none":
        return False
    if flag_value is None:
        return bool(project_dir)
    return True   # 'auto' or any non-'none' value enables it


def _write_notebooks_from_results(
    *,
    cfg,
    results: list,
    out_dir: Path,
    run_at: datetime,
    force: bool = False,
) -> None:
    """Write one notebook per table covered by `results`.

    Reuses the column metadata SchemaAuditCheck stored in result.detail so we
    don't need a live connector. Tables that lack a schema_audit result are
    skipped with a one-line warning — that's a misconfiguration (the user
    disabled the default check) rather than a bug.
    """
    from dbprofile.notebook.generator import build_notebook
    from dbprofile.notebook.helper_copy import copy_helpers
    from dbprofile.notebook.notebook_writer import write_notebook

    # Helpers are already seeded by the run() command earlier. Calling again
    # is cheap (silent no-op when state matches) and ensures dq_eda/ is ready
    # if we ever wire this from elsewhere.
    copy_helpers(out_dir)

    column_map, schema_map = _columns_from_results(results)
    target_tables = sorted({r.table for r in results})
    if not target_tables:
        return

    connector_type = (cfg.connection.dialect or "duckdb").lower()
    console.print(f"\n[bold]Building {len(target_tables)} notebook(s)…[/bold]")

    for table in target_tables:
        cols = column_map.get(table)
        if not cols:
            console.print(
                f"  [yellow]skipped {table}[/yellow] "
                f"(no schema_audit result — is the check disabled?)"
            )
            continue
        table_results = [r for r in results if r.table == table]
        nb = build_notebook(
            table=table,
            schema_name=schema_map.get(table, "main"),
            columns=cols,
            check_results=table_results,
            config=cfg,
            connector_type=connector_type,
        )
        path, outcome = write_notebook(nb, out_dir, table, force=force, run_at=run_at)
        console.print(f"  [green]{outcome:>30}[/green] → {path.name}")


@click.group()
def main() -> None:
    """dbprofile — automated SQL database profiling."""


@main.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to YAML config file.")
@click.option("--project-dir", "-p", default=None, type=click.Path(),
              help="Project folder. Outputs go to <project-dir>/dq_eda/. "
                   "Falls back to ./reports/ when omitted.")
@click.option("--output", "-o", default=None,
              help="Override the report output path from config.")
@click.option("--sample-rate", default=None, type=float,
              help="Override sample_rate from config (0.0–1.0).")
@click.option("--sample-method", default=None,
              type=click.Choice(["bernoulli", "system"], case_sensitive=False),
              help="Override sampling method: bernoulli (row-level, default) "
                   "or system (block-level, faster).")
@click.option("--dry-run", is_flag=True, default=False,
              help="Print queries without executing. Shows estimated BQ cost.")
@click.option("--export-json", default=None,
              help="JSON export. With --project-dir: enabled by default; "
                   "pass 'none' to skip or a path to override. Without "
                   "--project-dir: pass 'auto' or a path to enable.")
@click.option("--export-excel", default=None,
              help="Excel export. With --project-dir: enabled by default; "
                   "pass 'none' to skip or a path to override. Without "
                   "--project-dir: pass 'auto' or a path to enable.")
@click.option("--export-notebook", default=None,
              help="Per-table EDA notebook generation. Requires --project-dir. "
                   "Enabled by default; pass 'none' to skip.")
@click.option("--force", is_flag=True,
              help="Overwrite analyst-modified notebooks (originals saved to "
                   ".backups/). Mirrors `dbprofile notebook --force`.")
@click.option("--verbose", "-v", is_flag=True, default=False,
              help="Enable debug logging.")
def run(
    config: str,
    project_dir: str | None,
    output: str | None,
    sample_rate: float | None,
    sample_method: str | None,
    dry_run: bool,
    export_json: str | None,
    export_excel: str | None,
    export_notebook: str | None,
    force: bool,
    verbose: bool,
) -> None:
    """Profile a database and produce an HTML report.

    When --project-dir is set, this command also writes the JSON snapshot,
    Excel workbook, and per-table EDA notebooks by default — pass
    --export-* none to skip any of them. When --project-dir is omitted,
    only the HTML report is written unless --export-* is explicitly passed
    (legacy ./reports/ fallback).
    """
    _setup_logging(verbose)

    # Late imports so startup is fast for --help
    from dbprofile.config import load_config
    from dbprofile.connectors.base import get_connector
    from dbprofile.orchestrator import run_profile
    from dbprofile.report.renderer import render_report

    cfg = load_config(config)

    # Apply CLI overrides
    if sample_rate is not None:
        cfg.checks.sample_rate = sample_rate
    if sample_method is not None:
        cfg.checks.sample_method = sample_method
    if output is not None:
        cfg.report.output = output

    connector = get_connector(cfg)

    run_at = datetime.utcnow()

    try:
        results = run_profile(cfg, connector, dry_run=dry_run)
    finally:
        connector.close()

    if dry_run:
        console.print("[dim]Dry run complete — no queries were executed.[/dim]")
        return

    if not results:
        console.print("[yellow]No results produced. Check your config.[/yellow]")
        return

    # Resolve output dir + per-run filename stem.
    # auto_name() owns the date stamp so all outputs share one consistent format.
    out_dir = resolve_output_dir(project_dir)
    stem = run_stem(cfg)

    # Seed notebook helpers into dq_eda/ when the analyst opts into the
    # project-dir layout. No-op for the legacy ./reports/ fallback.
    if project_dir:
        from dbprofile.notebook.helper_copy import copy_helpers
        copy_helpers(out_dir)

    html_out = (
        output if output is not None
        else str(out_dir / auto_name(stem, "html", run_at=run_at))
    )

    # Resolve export flags into either a concrete path or None (skip).
    # When --project-dir is set, defaults flip to 'auto' so a single
    # command writes every artifact. Without --project-dir, only flags
    # the user explicitly passed take effect (legacy ./reports/ behavior).
    export_json = _resolve_export_path(
        export_json, project_dir, stem, "json", run_at, out_dir,
    )
    export_excel = _resolve_export_path(
        export_excel, project_dir, stem, "xlsx", run_at, out_dir,
    )
    notebook_enabled = _resolve_export_toggle(export_notebook, project_dir)

    # Write HTML report — also returns the template context for other exporters
    out_path, report_context = render_report(results, cfg, html_out, run_at=run_at)
    console.print(f"\n[bold green]Report written:[/bold green] {out_path}")

    # Optional JSON export
    if export_json:
        json_path = Path(export_json)
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps([r.to_dict() for r in results], indent=2, default=str),
            encoding="utf-8",
        )
        console.print(f"[bold green]JSON export:[/bold green] {json_path}")

    # Optional Excel export
    if export_excel:
        from dbprofile.report.excel_export import write_excel
        xl_path = write_excel(export_excel, report_context)
        console.print(f"[bold green]Excel workbook:[/bold green] {xl_path}")

    # Optional per-table EDA notebook generation. We have everything in memory
    # already (results + cfg); no need to re-run the profile.
    if notebook_enabled:
        _write_notebooks_from_results(
            cfg=cfg, results=results, out_dir=out_dir, run_at=run_at, force=force,
        )

    # BigQuery cost summary
    from dbprofile.connectors.base import BigQueryConnector
    if isinstance(connector, BigQueryConnector):
        gb = connector.total_bytes / 1e9
        cost = connector.total_cost_usd
        console.print(
            f"\n[dim]BigQuery: {gb:.3f} GB scanned total "
            f"(~${cost:.4f} at $6.25/TB)[/dim]"
        )

    # Exit with non-zero if critical issues found
    critical_count = sum(1 for r in results if r.severity == "critical")
    if critical_count:
        sys.exit(1)


@main.command()
@click.option("--json", "json_path", required=True, type=click.Path(exists=True),
              help="Path to a JSON file produced by a previous run (--export-json).")
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to the same YAML config used for the original run.")
@click.option("--project-dir", "-p", default=None, type=click.Path(),
              help="Project folder. Output goes to <project-dir>/dq_eda/. "
                   "Falls back to ./reports/ when omitted.")
@click.option("--output", "-o", default=None,
              help="Output path for the Excel workbook. "
                   "Defaults to auto-named in the resolved output directory.")
def excel(json_path: str, config: str, project_dir: str | None, output: str | None) -> None:
    """Build an Excel workbook from a saved JSON results file — no database needed.

    First run:   dbprofile run --config cfg.yaml --export-json auto --project-dir <dir>
    Later runs:  dbprofile excel --json <dir>/dq_eda/<file>.json
                                --config cfg.yaml --project-dir <dir>
    """
    from datetime import datetime

    from dbprofile.config import load_config
    from dbprofile.report.excel_export import write_excel
    from dbprofile.report.renderer import _build_report_context, load_results_from_json

    cfg = load_config(config)
    results = load_results_from_json(json_path)

    # Use the run_at from the first result if available
    run_at = results[0].run_at if results else datetime.utcnow()
    context = _build_report_context(results, cfg, run_at)

    if output is None:
        out_dir = resolve_output_dir(project_dir)
        output = str(out_dir / auto_name(run_stem(cfg), "xlsx", run_at=run_at))

    xl_path = write_excel(output, context)
    console.print(f"\n[bold green]Excel workbook:[/bold green] {xl_path}")


@main.command()
@click.option("--json", "json_path", required=True, type=click.Path(exists=True),
              help="Path to a JSON file produced by a previous run (--export-json).")
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to the same YAML config used for the original run.")
@click.option("--project-dir", "-p", default=None, type=click.Path(),
              help="Project folder. Output goes to <project-dir>/dq_eda/. "
                   "Falls back to ./reports/ when omitted.")
@click.option("--output", "-o", default=None,
              help="Output path for the HTML report. "
                   "Defaults to auto-named in the resolved output directory.")
def html(json_path: str, config: str, project_dir: str | None, output: str | None) -> None:
    """Rebuild an HTML report from a saved JSON results file — no database needed.

    First run:   dbprofile run --config cfg.yaml --export-json auto --project-dir <dir>
    Later runs:  dbprofile html --json <dir>/dq_eda/<file>.json
                                --config cfg.yaml --project-dir <dir>
    """
    from dbprofile.config import load_config
    from dbprofile.report.renderer import load_results_from_json, render_report

    cfg = load_config(config)
    results = load_results_from_json(json_path)

    run_at = results[0].run_at if results else datetime.utcnow()

    if output is None:
        out_dir = resolve_output_dir(project_dir)
        output = str(out_dir / auto_name(run_stem(cfg), "html", run_at=run_at))

    out_path, _ = render_report(results, cfg, output, run_at=run_at)
    console.print(f"\n[bold green]HTML report:[/bold green] {out_path}")


@main.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to YAML config file.")
@click.option("--project-dir", "-p", default=None, type=click.Path(),
              help="Project folder. Notebooks land in <project-dir>/dq_eda/. "
                   "Falls back to ./reports/ when omitted.")
@click.option("--json", "json_path", default=None, type=click.Path(exists=True),
              help="Re-generate from an existing JSON export "
                   "(no DB connection needed).")
@click.option("--tables", default=None, multiple=True,
              help="Limit to specific tables (default: all in config scope). "
                   "Pass once per table.")
@click.option("--update-helpers", is_flag=True,
              help="Refresh helpers in dq_eda/ even if analyst-modified "
                   "(originals saved to .backups/).")
@click.option("--force", is_flag=True,
              help="Overwrite analyst-modified notebooks "
                   "(originals saved to .backups/).")
def notebook(
    config: str,
    project_dir: str | None,
    json_path: str | None,
    tables: tuple[str, ...],
    update_helpers: bool,
    force: bool,
) -> None:
    """Generate a Jupyter EDA notebook for each profiled table."""
    from dbprofile.config import load_config
    from dbprofile.notebook.generator import build_notebook
    from dbprofile.notebook.helper_copy import copy_helpers
    from dbprofile.notebook.notebook_writer import write_notebook
    from dbprofile.report.renderer import load_results_from_json

    cfg = load_config(config)
    out_dir = resolve_output_dir(project_dir)

    # Always seed/refresh helpers — analysts opening the notebook need them.
    copy_helpers(out_dir, force=update_helpers)

    # Acquire check_results + per-table column lists. Two paths:
    #   --json:  reconstitute columns from SchemaAuditCheck.detail.columns
    #   default: run a fresh profile + connector.get_columns() for each table
    if json_path:
        results = load_results_from_json(json_path)
        column_map, schema_map = _columns_from_results(results)
        run_at = results[0].run_at if results else datetime.utcnow()
    else:
        from dbprofile.connectors.base import get_connector
        from dbprofile.orchestrator import run_profile

        connector = get_connector(cfg)
        try:
            results = run_profile(cfg, connector)
            column_map, schema_map = _columns_from_results(results)
            # Fill in any tables that lacked schema_audit results from the connector.
            for table in {r.table for r in results}:
                if table not in column_map:
                    schema = next(
                        (r.schema for r in results if r.table == table), "main",
                    )
                    column_map[table] = connector.get_columns(table, schema)
                    schema_map[table] = schema
        finally:
            connector.close()
        run_at = results[0].run_at if results else datetime.utcnow()

    if not results:
        console.print("[yellow]No results to build notebooks from.[/yellow]")
        return

    # Apply --tables filter
    target_tables = sorted({r.table for r in results})
    if tables:
        wanted = set(tables)
        target_tables = [t for t in target_tables if t in wanted]
        missing = wanted - set(target_tables)
        if missing:
            console.print(f"[yellow]--tables not found in scope: {sorted(missing)}[/yellow]")

    if not target_tables:
        console.print("[yellow]No tables matched after --tables filter.[/yellow]")
        return

    connector_type = cfg.connection.dialect or "duckdb"
    console.print(f"[bold]Building {len(target_tables)} notebook(s)…[/bold]")

    for table in target_tables:
        cols = column_map.get(table, [])
        table_results = [r for r in results if r.table == table]
        schema_name = schema_map.get(table, "main")

        nb = build_notebook(
            table=table,
            schema_name=schema_name,
            columns=cols,
            check_results=table_results,
            config=cfg,
            connector_type=connector_type,
        )
        path, outcome = write_notebook(nb, out_dir, table, force=force, run_at=run_at)
        console.print(f"  [green]{outcome:>30}[/green] → {path.name}")


def _columns_from_results(
    results: list,
) -> tuple[dict[str, list[dict]], dict[str, str]]:
    """Extract per-table column lists + schemas from SchemaAuditCheck results.

    Returns (column_map, schema_map). column_map[table] is the list of
    column dicts {name, data_type, ...}. schema_map[table] is the schema
    name. Tables without a schema_audit result are simply absent from
    column_map (the caller can fall back to connector.get_columns()).
    """
    column_map: dict[str, list[dict]] = {}
    schema_map: dict[str, str] = {}
    for r in results:
        if r.check_name != "schema_audit":
            continue
        cols = (r.detail or {}).get("columns")
        if cols is not None:
            column_map[r.table] = cols
            schema_map[r.table] = r.schema
    return column_map, schema_map


@main.command()
@click.argument("baseline_json", type=click.Path(exists=True))
@click.argument("current_json", type=click.Path(exists=True))
@click.option("--output", "-o", default="diff_report.html")
def compare(baseline_json: str, current_json: str, output: str) -> None:
    """Compare two profiling runs and report regressions. (Coming soon)"""
    console.print(
        "[yellow]compare is not yet implemented. "
        "Track progress at github.com/your-org/dbprofile.[/yellow]"
    )
