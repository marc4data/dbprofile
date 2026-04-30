"""DQ Follow-up section — actionable inspection cells per flagged result.

Bridges the DQ summary callouts at the top of the notebook (s00) with
hands-on investigation. For every warn/critical result the profiler
produced, this section emits a sub-section with:

  - A `### {check title} — {column}` heading
  - A markdown line summarizing severity + the specific finding
  - A code cell that pulls the relevant rows / patterns / dupes out
    of `sample_df` so the analyst can immediately see what's wrong

Per-check cell builders:
  null_density          show rows where the column is NULL
                         (nulls cluster reveals the upstream cause)
  uniqueness            value_counts > 1, top 20
                         (which values are duplicated, how many times)
  format_validation     re-apply the regex from check.detail and
                         show rows that fail the match
  temporal_consistency  list of gap days from check.detail
  row_count             callout only — no investigation cell needed
                         when the table itself is empty/near-empty

Anything else (an unexpected check_name) gets a generic callout +
`sample_df['{col}'].head(20)` cell so it's still actionable, just less
tailored.
"""

from __future__ import annotations

import nbformat

from dbprofile.notebook.cells import callout_cell, code_cell, md_cell, section_header

_FLAGGED_SEVERITIES = ("critical", "warn")


def build_dq_followup_cells(
    *,
    table: str,
    check_results,
) -> list[nbformat.NotebookNode]:
    """Return the cells for the DQ Follow-up section.

    Returns an empty list when nothing is flagged for `table` — the
    section is silently skipped, matching how the Bivariate / Temporal
    sections behave when their preconditions don't hold.
    """
    flagged = _flagged_for_table(check_results, table)
    if not flagged:
        return []

    cells: list[nbformat.NotebookNode] = [
        section_header(2, "Data Quality Follow-up"),
        md_cell(
            "One sub-section per flagged column — critical findings "
            "first, then warnings. Each sub-section has a callout "
            "summarizing the finding and a code cell you can run "
            "against `sample_df` to inspect the problem rows."
        ),
    ]
    # Order: critical before warn, then by check_name, then by column.
    for r in _sort_findings(flagged):
        cells.extend(_per_finding_cells(r))
    return cells


# ── Sorting + filtering ──────────────────────────────────────────────────────


def _flagged_for_table(check_results, table: str) -> list:
    """Subset of results for `table` whose severity is critical or warn."""
    out = []
    for r in check_results:
        if getattr(r, "table", None) != table:
            continue
        if getattr(r, "severity", "") not in _FLAGGED_SEVERITIES:
            continue
        out.append(r)
    return out


def _sort_findings(results: list) -> list:
    """Order: critical first, then warn; within each, by check_name then column."""
    severity_rank = {"critical": 0, "warn": 1}
    return sorted(
        results,
        key=lambda r: (
            severity_rank.get(getattr(r, "severity", "warn"), 9),
            getattr(r, "check_name", ""),
            getattr(r, "column", "") or "",
        ),
    )


# ── Dispatch by check_name ───────────────────────────────────────────────────


def _per_finding_cells(r) -> list[nbformat.NotebookNode]:
    """Build the sub-section cells for one CheckResult."""
    check_name = getattr(r, "check_name", "")
    column = getattr(r, "column", None)
    severity = getattr(r, "severity", "warn")
    detail = getattr(r, "detail", {}) or {}

    title = check_name.replace("_", " ").title()
    head_line = f"{title} — `{column}`" if column else title

    cells: list[nbformat.NotebookNode] = [section_header(3, head_line)]

    # Dispatch by check name. Each builder returns the body cells
    # (callout + investigation cell) that follow the heading.
    builders = {
        "null_density":         _null_density_cells,
        "uniqueness":           _uniqueness_cells,
        "format_validation":    _format_validation_cells,
        "temporal_consistency": _temporal_consistency_cells,
        "row_count":            _row_count_cells,
    }
    builder = builders.get(check_name, _generic_cells)
    cells.extend(builder(column=column, severity=severity, detail=detail))
    return cells


# ── Per-check sub-section builders ───────────────────────────────────────────


def _null_density_cells(*, column, severity, detail) -> list[nbformat.NotebookNode]:
    null_pct = detail.get("null_pct")
    null_count = detail.get("null_count")
    summary = (
        f"`{column}` is NULL in **{null_pct:.2f}%** of rows ({null_count:,} rows) "
        "— inspect to see whether nulls cluster around a specific upstream condition."
        if isinstance(null_pct, (int, float))
        else f"`{column}` flagged for null density."
    )
    return [
        callout_cell(severity, summary),
        code_cell(
            f"# Rows where {column} is NULL — look for clustering by other columns.\n"
            f"sample_df[sample_df['{column}'].isna()].head(20)"
        ),
    ]


def _uniqueness_cells(*, column, severity, detail) -> list[nbformat.NotebookNode]:
    distinct_count = detail.get("distinct_count")
    distinct_pct = detail.get("distinct_pct")
    summary_bits = []
    if isinstance(distinct_count, int):
        summary_bits.append(f"{distinct_count:,} distinct values")
    if isinstance(distinct_pct, (int, float)):
        summary_bits.append(f"{distinct_pct:.2f}% distinct")
    summary = (
        f"`{column}` shows duplicates ({', '.join(summary_bits)}) — "
        "inspect which values repeat."
        if summary_bits
        else f"`{column}` flagged for uniqueness."
    )
    return [
        callout_cell(severity, summary),
        code_cell(
            f"# Top duplicate values in {column}.\n"
            f"_dupes = sample_df['{column}'].value_counts()\n"
            "_dupes = _dupes[_dupes > 1].head(20)\n"
            "display(_dupes)"
        ),
    ]


def _format_validation_cells(*, column, severity, detail) -> list[nbformat.NotebookNode]:
    pattern = detail.get("pattern")
    label = detail.get("format_label")
    violations = detail.get("violations")
    violation_pct = detail.get("violation_pct")

    summary_bits = []
    if label:
        summary_bits.append(f"`{label}` format")
    if isinstance(violations, int):
        summary_bits.append(f"{violations:,} bad rows")
    if isinstance(violation_pct, (int, float)):
        summary_bits.append(f"{violation_pct:.2f}%")
    summary = (
        f"`{column}` fails the {' / '.join(summary_bits)}."
        if summary_bits
        else f"`{column}` flagged for format validation."
    )

    if pattern:
        body = (
            f"# Re-apply the same regex the profiler used and show "
            f"rows that fail to match.\n"
            "import re\n"
            f"_pattern = r'{pattern}'\n"
            f"_bad = sample_df[~sample_df['{column}'].astype(str)\n"
            f"                  .str.match(_pattern, na=False)]\n"
            f"display(_bad[['{column}']].head(20))"
        )
    else:
        body = (
            f"# Inspect a sample of {column} values to spot the violations.\n"
            f"sample_df[['{column}']].head(20)"
        )
    return [
        callout_cell(severity, summary),
        code_cell(body),
    ]


def _temporal_consistency_cells(
    *, column, severity, detail,
) -> list[nbformat.NotebookNode]:
    gap_days = detail.get("gap_days") or []
    n_gaps = len(gap_days)
    if column:
        summary = (
            f"`{column}` has **{n_gaps} day(s)** with zero rows. "
            "Investigate whether these correspond to upstream load "
            "outages or are legitimate (holidays, weekends)."
        )
    else:
        summary = f"Temporal consistency flagged ({n_gaps} gap day(s))."

    if gap_days:
        # gap_days is a list of {"date": ..., "count": 0} dicts.
        body = (
            "# Gap days surfaced by TemporalConsistencyCheck.\n"
            f"_gap_days = {_gap_days_repr(gap_days)}\n"
            "display(pd.DataFrame(_gap_days))"
        )
    else:
        body = "# No specific gap days in the check detail.\nNone"
    return [
        callout_cell(severity, summary),
        code_cell(body),
    ]


def _row_count_cells(*, column, severity, detail) -> list[nbformat.NotebookNode]:
    is_empty = detail.get("is_empty")
    row_count = detail.get("row_count")
    if is_empty:
        summary = "**Table is empty** — nothing to investigate downstream until upstream load runs."
    elif isinstance(row_count, int):
        summary = f"Row count flagged: {row_count:,} rows."
    else:
        summary = "Row count flagged."
    # Row-count flags are table-level — no per-column investigation cell.
    return [callout_cell(severity, summary)]


def _generic_cells(*, column, severity, detail) -> list[nbformat.NotebookNode]:
    summary = (
        f"`{column}` flagged. Inspect the values below for whatever the "
        "check found unusual."
        if column else "Flagged finding — see check detail."
    )
    body = (
        f"sample_df[['{column}']].head(20)"
        if column else "# Refer to the check detail in the JSON export.\nNone"
    )
    return [
        callout_cell(severity, summary),
        code_cell(body),
    ]


# ── Helpers ──────────────────────────────────────────────────────────────────


def _gap_days_repr(gap_days: list) -> str:
    """Stringify gap_days as a Python literal the analyst can edit.

    Limit to the first 50 gaps so we don't bloat the cell with hundreds
    of date entries — the rest live in the JSON export.
    """
    rows = []
    for g in gap_days[:50]:
        date = g.get("date")
        count = g.get("count", 0)
        rows.append(f"    {{'date': {date!r}, 'count': {count!r}}},")
    if len(gap_days) > 50:
        rows.append(f"    # ... + {len(gap_days) - 50} more in the JSON export")
    body = "\n".join(rows) if rows else ""
    return "[\n" + body + "\n]"
