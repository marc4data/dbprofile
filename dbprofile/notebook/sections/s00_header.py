"""Header section — title, table metadata, and DQ summary callouts.

Replaces the inline title block the generator carried while sections
were being built out. The header now also surfaces the DQ findings the
profiler produced — one callout per (check_name, severity) bucket so
the analyst sees what to investigate before scrolling into the
sections.

Severity → callout tag mapping (mirrors dbprofile.notebook.cells):

  critical → [!CAUTION]   (red, demands attention)
  warn     → [!WARNING]   (yellow, worth a look)
  info     → [!NOTE]      (gray, informational)
  ok / no flags → [!NOTE] with a green "no issues found" message
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

import nbformat

from dbprofile.notebook.cells import callout_cell, md_cell, section_header

# Only severity buckets we surface as callouts (info results are too
# noisy for the header summary; they live in the body of each section).
_FLAGGED_SEVERITIES = ("critical", "warn")


def build_header_cells(
    *,
    table: str,
    schema_name: str,
    connector_type: str,
    check_results,
    run_at: datetime | None = None,
) -> list[nbformat.NotebookNode]:
    """Return the cells that open the notebook."""
    when = run_at or datetime.utcnow()
    flagged_by_check = _group_flagged_results(check_results, table)
    total_flagged = sum(
        len(rs) for buckets in flagged_by_check.values() for rs in buckets.values()
    )

    cells: list[nbformat.NotebookNode] = [
        section_header(1, f"{table} — EDA / Data Quality Review"),
        md_cell(_metadata_block(
            table=table, schema_name=schema_name, connector_type=connector_type,
            when=when, total_flagged=total_flagged,
        )),
    ]
    cells.extend(_callout_cells(flagged_by_check))
    return cells


# ── DQ summary helpers ───────────────────────────────────────────────────────


def _group_flagged_results(
    check_results,
    table: str,
) -> dict[str, dict[str, list]]:
    """Bucket results for `table` by (check_name → severity → [results])."""
    out: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for r in check_results:
        if getattr(r, "table", None) != table:
            continue
        if getattr(r, "severity", "") not in _FLAGGED_SEVERITIES:
            continue
        out[r.check_name][r.severity].append(r)
    return out


def _callout_cells(
    flagged_by_check: dict[str, dict[str, list]],
) -> list[nbformat.NotebookNode]:
    """One callout per (check_name, severity) bucket — empty → green note."""
    if not flagged_by_check:
        return [callout_cell(
            "info",
            "**No DQ issues flagged.** Every check produced ok or info "
            "results for this table.",
        )]

    cells: list[nbformat.NotebookNode] = []
    # Stable order: critical first, then warn, then by check name.
    for severity in _FLAGGED_SEVERITIES:
        for check_name in sorted(flagged_by_check.keys()):
            results = flagged_by_check[check_name].get(severity)
            if not results:
                continue
            cells.append(callout_cell(severity, _bullet_summary(check_name, results)))
    return cells


def _bullet_summary(check_name: str, results: list) -> str:
    """One callout body: '<check> (<n> <severity>): col1, col2, …'.

    Truncates to 6 columns to keep the callout scannable; the full list
    is always available in the corresponding section's deep-dive cell.
    """
    severity = results[0].severity
    cols = [r.column for r in results if getattr(r, "column", None)]
    title = check_name.replace("_", " ").title()

    body_lines = [f"**{title}** — {len(results)} {severity} finding(s)"]
    if cols:
        sample = cols[:6]
        more = "" if len(cols) <= 6 else f" (+ {len(cols) - 6} more)"
        body_lines.append("Columns: " + ", ".join(f"`{c}`" for c in sample) + more)
    return "\n".join(body_lines)


# ── Metadata block ───────────────────────────────────────────────────────────


def _metadata_block(
    *,
    table: str,
    schema_name: str,
    connector_type: str,
    when: datetime,
    total_flagged: int,
) -> str:
    flag_line = (
        "**No DQ issues flagged.**"
        if total_flagged == 0
        else f"**{total_flagged} DQ issue(s) flagged** — see callouts below."
    )
    return (
        f"**Schema:** `{schema_name}`  \n"
        f"**Table:** `{table}`  \n"
        f"**Connector:** `{connector_type}`  \n"
        f"**Generated:** {when.strftime('%Y-%m-%d %H:%M UTC')}  \n\n"
        f"**Purpose:** Exploratory analysis of `{table}`. "
        f"Investigate columns, distributions, and potential data quality issues.\n\n"
        f"{flag_line}"
    )
