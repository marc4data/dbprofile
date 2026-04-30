"""CLI entry point — `dbprofile run --config config.yaml`."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console

console = Console()


def _build_base_filename(cfg, run_at: datetime) -> str:
    """Build a filename stem that sorts logically by source then date.

    Format: dbtype_db_schema_YYYYMMDD
    Example: snowflake_analytics_dbt_malex_marts_20260418

    Alphabetical sorting groups runs by connector → database → schema,
    with date ordering within each group — ready for baseline comparison.
    """
    import re
    parts = []
    parts.append(cfg.connection.dialect or "db")
    db = cfg.scope.database or cfg.scope.dataset or cfg.scope.project or ""
    if db:
        parts.append(db)
    schemas = cfg.scope.schemas or []
    if schemas:
        parts.append("_".join(schemas))
    stem = "_".join(p.lower() for p in parts if p)
    stem = re.sub(r"[^a-z0-9_]+", "_", stem).strip("_")
    stamp = run_at.strftime("%Y%m%d")
    return f"{stem}_{stamp}"


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        format="%(levelname)s %(name)s: %(message)s",
        level=level,
    )


@click.group()
def main() -> None:
    """dbprofile — automated SQL database profiling."""


@main.command()
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to YAML config file.")
@click.option("--output", "-o", default=None,
              help="Override the report output path from config.")
@click.option("--sample-rate", default=None, type=float,
              help="Override sample_rate from config (0.0–1.0).")
@click.option("--sample-method", default=None,
              type=click.Choice(["bernoulli", "system"], case_sensitive=False),
              help="Override sampling method: bernoulli (row-level, default) or system (block-level, faster).")
@click.option("--dry-run", is_flag=True, default=False,
              help="Print queries without executing. Shows estimated BQ cost.")
@click.option("--export-json", default=None,
              help="Write raw results as JSON. Pass a path or 'auto' for auto-named file.")
@click.option("--export-excel", default=None,
              help="Write a profiling workbook (.xlsx). Pass a path or 'auto' for auto-named file.")
@click.option("--verbose", "-v", is_flag=True, default=False,
              help="Enable debug logging.")
def run(
    config: str,
    output: str | None,
    sample_rate: float | None,
    sample_method: str | None,
    dry_run: bool,
    export_json: str | None,
    export_excel: str | None,
    verbose: bool,
) -> None:
    """Profile a database and produce an HTML report."""
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

    # Build a descriptive base filename from data source + datetime
    # e.g. snowflake_analytics_dbt_malex_marts_20260418
    base_name = _build_base_filename(cfg, run_at)

    # Resolve output paths — CLI --output wins; otherwise auto-name from metadata
    html_out = f"./reports/{base_name}.html"
    if output is not None:
        html_out = output

    if export_json is not None:
        export_json = f"./reports/{base_name}.json" if export_json == "auto" else export_json
    if export_excel is not None:
        export_excel = f"./reports/{base_name}.xlsx" if export_excel == "auto" else export_excel

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
@click.option("--output", "-o", default="reports/profile.xlsx",
              help="Output path for the Excel workbook.")
def excel(json_path: str, config: str, output: str) -> None:
    """Build an Excel workbook from a saved JSON results file — no database needed.

    First run:   dbprofile run --config cfg.yaml --export-json reports/results.json
    Later runs:  dbprofile excel --json reports/results.json --config cfg.yaml --output reports/profile.xlsx
    """
    from dbprofile.config import load_config
    from dbprofile.report.renderer import load_results_from_json, _build_report_context
    from dbprofile.report.excel_export import write_excel
    from datetime import datetime

    cfg = load_config(config)
    results = load_results_from_json(json_path)

    # Use the run_at from the first result if available
    run_at = results[0].run_at if results else datetime.utcnow()
    context = _build_report_context(results, cfg, run_at)

    xl_path = write_excel(output, context)
    console.print(f"\n[bold green]Excel workbook:[/bold green] {xl_path}")


@main.command()
@click.option("--json", "json_path", required=True, type=click.Path(exists=True),
              help="Path to a JSON file produced by a previous run (--export-json).")
@click.option("--config", "-c", required=True, type=click.Path(exists=True),
              help="Path to the same YAML config used for the original run.")
@click.option("--output", "-o", default=None,
              help="Output path for the HTML report. Defaults to auto-named in reports/.")
def html(json_path: str, config: str, output: str | None) -> None:
    """Rebuild an HTML report from a saved JSON results file — no database needed.

    First run:   dbprofile run --config cfg.yaml --export-json auto
    Later runs:  dbprofile html --json reports/results.json --config cfg.yaml
    """
    from dbprofile.config import load_config
    from dbprofile.report.renderer import load_results_from_json, render_report

    cfg = load_config(config)
    results = load_results_from_json(json_path)

    run_at = results[0].run_at if results else datetime.utcnow()

    if output is None:
        base_name = _build_base_filename(cfg, run_at)
        output = f"./reports/{base_name}.html"

    out_path, _ = render_report(results, cfg, output, run_at=run_at)
    console.print(f"\n[bold green]HTML report:[/bold green] {out_path}")


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
