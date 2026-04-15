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
@click.option("--dry-run", is_flag=True, default=False,
              help="Print queries without executing. Shows estimated BQ cost.")
@click.option("--export-json", default=None,
              help="Also write raw results as JSON to this path.")
@click.option("--verbose", "-v", is_flag=True, default=False,
              help="Enable debug logging.")
def run(
    config: str,
    output: str | None,
    sample_rate: float | None,
    dry_run: bool,
    export_json: str | None,
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

    # Write HTML report
    out_path = render_report(results, cfg, cfg.report.output, run_at=run_at)
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
@click.argument("baseline_json", type=click.Path(exists=True))
@click.argument("current_json", type=click.Path(exists=True))
@click.option("--output", "-o", default="diff_report.html")
def compare(baseline_json: str, current_json: str, output: str) -> None:
    """Compare two profiling runs and report regressions. (Coming soon)"""
    console.print(
        "[yellow]compare is not yet implemented. "
        "Track progress at github.com/your-org/dbprofile.[/yellow]"
    )
