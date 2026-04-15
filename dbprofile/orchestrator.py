"""Orchestrator — discovers tables/columns, fans out checks, collects results."""

from __future__ import annotations

import logging
from typing import Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

from dbprofile.checks.base import CheckResult
from dbprofile.checks.format_validation import FormatValidationCheck
from dbprofile.checks.frequency_distribution import FrequencyDistributionCheck
from dbprofile.checks.null_density import NullDensityCheck
from dbprofile.checks.numeric_distribution import NumericDistributionCheck
from dbprofile.checks.row_count import RowCountCheck
from dbprofile.checks.schema_audit import SchemaAuditCheck
from dbprofile.checks.temporal_consistency import TemporalConsistencyCheck
from dbprofile.checks.uniqueness import UniquenessCheck
from dbprofile.config import ProfileConfig
from dbprofile.connectors.base import BaseConnector

logger = logging.getLogger(__name__)
console = Console()

# Registry: check name -> class
ALL_CHECKS = {
    "schema_audit": SchemaAuditCheck,
    "row_count": RowCountCheck,
    "null_density": NullDensityCheck,
    "uniqueness": UniquenessCheck,
    "numeric_distribution": NumericDistributionCheck,
    "frequency_distribution": FrequencyDistributionCheck,
    "temporal_consistency": TemporalConsistencyCheck,
    "format_validation": FormatValidationCheck,
}


def resolve_checks(config: ProfileConfig) -> list:
    """Return instantiated check objects based on config enabled/disabled lists."""
    enabled = config.checks.enabled
    disabled = set(config.checks.disabled)

    if enabled == ["all"] or "all" in enabled:
        names = [n for n in ALL_CHECKS if n not in disabled]
    else:
        names = [n for n in enabled if n not in disabled and n in ALL_CHECKS]

    return [ALL_CHECKS[n]() for n in names]


def resolve_columns(
    table: str,
    all_columns: list[dict[str, Any]],
    config: ProfileConfig,
) -> list[dict[str, Any]]:
    """Apply column_overrides from config to filter the column list."""
    override = config.scope.column_overrides.get(table)
    if not override:
        return all_columns

    if override.include:
        include_set = set(override.include)
        return [c for c in all_columns if c["name"] in include_set]

    if override.exclude:
        exclude_set = set(override.exclude)
        return [c for c in all_columns if c["name"] not in exclude_set]

    return all_columns


def run_profile(
    config: ProfileConfig,
    connector: BaseConnector,
    dry_run: bool = False,
) -> list[CheckResult]:
    """Main entry point — discover tables, run all checks, return results."""

    checks = resolve_checks(config)
    results: list[CheckResult] = []

    # Resolve schema list — explicit config wins, otherwise discover from connector
    if config.scope.dataset:
        # BigQuery: single dataset
        schemas = [config.scope.dataset]
    elif config.scope.schemas:
        schemas = config.scope.schemas
    else:
        console.print("[dim]Discovering schemas...[/dim]")
        schemas = connector.get_schemas()
        console.print(f"[dim]Found schemas: {', '.join(schemas)}[/dim]")

    exclude = set(config.scope.exclude_tables or [])

    # Build (schema, table) work list
    work: list[tuple[str, str]] = []
    if config.scope.tables:
        # Explicit table list — apply to all schemas (or just first if single)
        for schema in schemas:
            for table in config.scope.tables:
                work.append((schema, table))
    else:
        for schema in schemas:
            try:
                tables = connector.get_tables(schema)
                for table in tables:
                    if table not in exclude:
                        work.append((schema, table))
            except Exception as exc:
                console.print(f"[red]Could not list tables in {schema}: {exc}[/red]")

    if not work:
        console.print("[yellow]No tables found. Check your scope config.[/yellow]")
        return results

    console.print(f"[bold]Profiling {len(work)} table(s) across {len(schemas)} schema(s) with {len(checks)} check(s)[/bold]")
    console.print(f"  Sample rate: {config.checks.sample_rate * 100:.0f}%")
    if dry_run:
        console.print("[yellow]  DRY RUN — queries will be shown but not executed[/yellow]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for schema, table in work:
            label = f"{schema}.{table}" if len(schemas) > 1 else table
            task = progress.add_task(f"[cyan]{label}[/cyan]", total=None)

            # Fetch column metadata
            try:
                all_columns = connector.get_columns(table, schema)
            except Exception as exc:
                console.print(f"[red]  Could not fetch columns for {label}: {exc}[/red]")
                progress.remove_task(task)
                continue

            columns = resolve_columns(table, all_columns, config)

            if not columns:
                console.print(f"[yellow]  {label}: no columns after filtering[/yellow]")
                progress.remove_task(task)
                continue

            # Run each check
            for check in checks:
                progress.update(
                    task,
                    description=f"[cyan]{label}[/cyan] → [dim]{check.name}[/dim]",
                )
                if dry_run:
                    console.print(f"  [dim]Would run: {check.name} on {label}[/dim]")
                    continue

                try:
                    check_results = check.run(table, schema, columns, connector, config)
                    results.extend(check_results)
                except Exception as exc:
                    logger.error(f"Check {check.name} failed on {label}: {exc}", exc_info=True)
                    console.print(f"[red]  {check.name} on {label} failed: {exc}[/red]")

            progress.remove_task(task)

    # Summary
    if not dry_run:
        critical = sum(1 for r in results if r.severity == "critical")
        warnings = sum(1 for r in results if r.severity == "warn")
        console.print(
            f"\n[bold]Done.[/bold] "
            f"[red]{critical} critical[/red]  "
            f"[yellow]{warnings} warnings[/yellow]  "
            f"{len(results)} total results"
        )

    return results
