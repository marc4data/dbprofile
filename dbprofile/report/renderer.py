"""Jinja2 renderer — takes CheckResult list, produces a single self-contained HTML file."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from dbprofile.checks.base import CheckResult
from dbprofile.config import ProfileConfig


def _group_results(results: list[CheckResult]) -> dict[str, dict[str, list[CheckResult]]]:
    """Group results as: {table -> {check_name -> [CheckResult]}}."""
    grouped: dict[str, dict[str, list[CheckResult]]] = defaultdict(lambda: defaultdict(list))
    for r in results:
        grouped[r.table][r.check_name].append(r)
    return grouped


def _severity_counts(results: list[CheckResult]) -> dict[str, int]:
    counts: dict[str, int] = {"critical": 0, "warn": 0, "ok": 0, "info": 0}
    for r in results:
        counts[r.severity] = counts.get(r.severity, 0) + 1
    return counts


def _table_scorecard(
    table: str,
    checks: dict[str, list[CheckResult]],
    all_check_names: list[str],
) -> list[dict[str, Any]]:
    """Build a row-per-column, column-per-check severity grid for the heatmap."""
    # Collect all unique column names across all checks for this table
    columns: set[str] = set()
    for check_results in checks.values():
        for r in check_results:
            if r.column:
                columns.add(r.column)

    scorecard = []
    for col in sorted(columns):
        row: dict[str, Any] = {"column": col}
        for check_name in all_check_names:
            check_results = checks.get(check_name, [])
            col_results = [r for r in check_results if r.column == col]
            if not col_results:
                row[check_name] = "na"
            else:
                # Worst severity wins
                worst = "ok"
                for r in col_results:
                    if r.severity == "critical":
                        worst = "critical"
                        break
                    if r.severity == "warn" and worst != "critical":
                        worst = "warn"
                    if r.severity == "info" and worst == "ok":
                        worst = "info"
                row[check_name] = worst
        scorecard.append(row)
    return scorecard


def _issues_table(results: list[CheckResult]) -> list[dict[str, Any]]:
    """Return warn/critical results sorted critical-first for the executive summary."""
    issues = [r for r in results if r.severity in ("critical", "warn")]
    issues.sort(key=lambda r: (0 if r.severity == "critical" else 1, r.table, r.check_name))
    return [
        {
            "table": r.table,
            "check": r.check_name,
            "column": r.column or "—",
            "severity": r.severity,
            "metric": r.metric,
            "value": r.value,
            "anchor": f"{r.schema}.{r.table}.{r.check_name}.{r.column or 'table'}",
        }
        for r in issues
    ]


def render_report(
    results: list[CheckResult],
    config: ProfileConfig,
    output_path: str | Path,
    run_at: datetime | None = None,
) -> Path:
    """Render a self-contained HTML report and write it to output_path."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if run_at is None:
        run_at = datetime.utcnow()

    template_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=True)
    env.filters["tojson"] = lambda v: json.dumps(v, default=str)

    grouped = _group_results(results)
    all_check_names = list(
        {r.check_name for r in results}
    )
    all_check_names.sort()

    tables_data = {}
    for table, checks in grouped.items():
        scorecard = _table_scorecard(table, checks, all_check_names)
        sev = _severity_counts(
            [r for sublist in checks.values() for r in sublist]
        )
        tables_data[table] = {
            "checks": {
                name: [r.to_dict() for r in rs]
                for name, rs in checks.items()
            },
            "scorecard": scorecard,
            "severity_counts": sev,
        }

    overall = _severity_counts(results)
    issues = _issues_table(results)

    unique_tables = sorted(grouped.keys())
    unique_columns = len({r.column for r in results if r.column})

    context = {
        "run_at": run_at.strftime("%Y-%m-%d %H:%M UTC"),
        "project": config.scope.project or config.connection.project or "dbprofile",
        "dataset": config.scope.dataset or (
            config.scope.schemas[0] if config.scope.schemas else ""
        ),
        "sample_rate_pct": f"{config.checks.sample_rate * 100:.0f}%",
        "tables": unique_tables,
        "tables_data": tables_data,
        "all_check_names": all_check_names,
        "total_tables": len(unique_tables),
        "total_columns": unique_columns,
        "critical_count": overall.get("critical", 0),
        "warn_count": overall.get("warn", 0),
        "issues": issues,
        "include_charts": "charts" in config.report.include,
        "include_tables": "tables" in config.report.include,
    }

    template = env.get_template("template.html.j2")
    html = template.render(**context)
    output_path.write_text(html, encoding="utf-8")
    return output_path
