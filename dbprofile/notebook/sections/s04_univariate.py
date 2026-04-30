"""Univariate Analysis section — one chart panel per column kind.

Four sub-sections, each emitted only when at least one column matches:

  4a Flag & Ordinal Fields           BINARY + ORDINAL_CAT
       → plot_histograms(fields=[...], label_threshold=12)

  4b Categorical Columns             LOW_CAT  + HIGH_CAT
       → plot_string_profile(fields=[...])      (LOW_CAT, ≤15 distinct)
       → plot_string_profile_hc(fields=[...])   (HIGH_CAT, >15 distinct)

  4c Count & Aggregate Metrics       COUNT_METRIC
       → plot_field_aggregates(fields=[...])

  4d Distributions                   CONTINUOUS
       → plot_distribution(field=col, ...) one call per column,
         capped at MAX_CONTINUOUS_PANELS columns to keep the notebook
         scannable. When NumericDistributionCheck has p99 + non-negative
         min for a column, we pre-fill bin_min=0 and bin_max=p99 so the
         plot auto-zooms past tail outliers.
"""

from __future__ import annotations

from typing import Iterable

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header
from dbprofile.notebook.classify import ColumnKind

# Cap on individual plot_distribution calls. Phase-6 work will wire this
# to cfg.notebook.max_continuous_fields.
MAX_CONTINUOUS_PANELS = 12


def build_univariate_cells(
    *,
    columns: list[dict],
    classified: dict[str, ColumnKind],
    check_results: Iterable,
) -> list[nbformat.NotebookNode]:
    """Return the cells for the Univariate Analysis section."""
    by_kind = _columns_by_kind(columns, classified)
    numeric_dist = _numeric_dist_lookup(check_results)

    cells: list[nbformat.NotebookNode] = [section_header(2, "Univariate Analysis")]
    sub_emitted = False

    # 4a — Flags & ordinals (binary + ordinal_cat).
    flag_cols = by_kind[ColumnKind.BINARY] + by_kind[ColumnKind.ORDINAL_CAT]
    if flag_cols:
        cells.extend(_flag_panel_cells(flag_cols))
        sub_emitted = True

    # 4b — Categorical low / high cardinality.
    if by_kind[ColumnKind.LOW_CAT] or by_kind[ColumnKind.HIGH_CAT]:
        cells.extend(_categorical_panel_cells(
            low_cols=by_kind[ColumnKind.LOW_CAT],
            high_cols=by_kind[ColumnKind.HIGH_CAT],
        ))
        sub_emitted = True

    # 4c — Count / aggregate metrics.
    count_cols = by_kind[ColumnKind.COUNT_METRIC]
    if count_cols:
        cells.extend(_count_panel_cells(count_cols))
        sub_emitted = True

    # 4d — Continuous distributions.
    cont_cols = by_kind[ColumnKind.CONTINUOUS]
    if cont_cols:
        cells.extend(_continuous_panel_cells(cont_cols, numeric_dist))
        sub_emitted = True

    if not sub_emitted:
        cells.append(md_cell(
            "_No columns of a kind suitable for univariate plotting "
            "(binary, categorical, count, or continuous). Section skipped._"
        ))

    return cells


# ── Sub-section cell builders ────────────────────────────────────────────────


def _flag_panel_cells(flag_cols: list[str]) -> list[nbformat.NotebookNode]:
    return [
        section_header(3, "Flag & ordinal fields"),
        md_cell(
            "Histograms for binary flags and low-cardinality ordinals "
            "(month, day-of-week, hour). `label_threshold=12` shows numeric "
            "labels above each bar when the panel has 12 or fewer bars."
        ),
        code_cell(
            "plot_histograms(\n"
            "    df              = sample_df,\n"
            f"    fields          = {_fields_literal(flag_cols)},\n"
            "    label_threshold = 12,\n"
            ")"
        ),
    ]


def _categorical_panel_cells(
    *,
    low_cols: list[str],
    high_cols: list[str],
) -> list[nbformat.NotebookNode]:
    cells = [section_header(3, "Categorical columns")]
    if low_cols:
        cells.append(md_cell(
            f"Low-cardinality categoricals ({len(low_cols)}): "
            "frequency + cumulative percentage."
        ))
        cells.append(code_cell(
            "plot_string_profile(\n"
            "    df     = sample_df,\n"
            f"    fields = {_fields_literal(low_cols)},\n"
            ")"
        ))
    if high_cols:
        cells.append(md_cell(
            f"High-cardinality categoricals ({len(high_cols)}): "
            "top-N values per column with a null-rate strip."
        ))
        cells.append(code_cell(
            "plot_string_profile_hc(\n"
            "    df     = sample_df,\n"
            f"    fields = {_fields_literal(high_cols)},\n"
            "    top_n  = 20,\n"
            ")"
        ))
    return cells


def _count_panel_cells(count_cols: list[str]) -> list[nbformat.NotebookNode]:
    return [
        section_header(3, "Count & aggregate metrics"),
        md_cell(
            "Sum / mean / count totals for columns that look like "
            "aggregates (`*_count`, `*_total`, `*_amount`, `*_revenue`, …)."
        ),
        code_cell(
            "plot_field_aggregates(\n"
            "    df     = sample_df,\n"
            f"    fields = {_fields_literal(count_cols)},\n"
            ")"
        ),
    ]


def _continuous_panel_cells(
    cont_cols: list[str],
    numeric_dist: dict[str, dict],
) -> list[nbformat.NotebookNode]:
    """One plot_distribution call per continuous column, capped."""
    selected = cont_cols[:MAX_CONTINUOUS_PANELS]
    cells: list[nbformat.NotebookNode] = [section_header(3, "Distributions")]
    if len(cont_cols) > MAX_CONTINUOUS_PANELS:
        cells.append(md_cell(
            f"_Showing first {MAX_CONTINUOUS_PANELS} of {len(cont_cols)} "
            f"continuous columns. Add more `plot_distribution()` calls "
            f"as needed._"
        ))
    for col in selected:
        cells.append(section_header(4, f"`{col}` distribution"))
        cells.append(code_cell(_distribution_call(col, numeric_dist.get(col, {}))))
    return cells


def _distribution_call(col: str, dist_detail: dict) -> str:
    """Compose a plot_distribution(...) call.

    Pull bin_min/bin_max from NumericDistributionCheck stats when
    available — bin_min=0 only if the column is known non-negative,
    bin_max=p99 to keep tail outliers from compressing the histogram.
    """
    args = [
        "    df             = sample_df,",
        f"    field          = '{col}',",
        "    bin_cnt        = 20,",
    ]
    min_val = dist_detail.get("min")
    p99 = dist_detail.get("p99")
    if isinstance(min_val, (int, float)) and min_val >= 0:
        args.append("    bin_min        = 0,")
    if isinstance(p99, (int, float)) and p99 > 0:
        args.append(f"    bin_max        = {p99},")
    args.append("    cumulative_line = True,")
    return "plot_distribution(\n" + "\n".join(args) + "\n)"


# ── Helpers ──────────────────────────────────────────────────────────────────


def _columns_by_kind(
    columns: list[dict],
    classified: dict[str, ColumnKind],
) -> dict[ColumnKind, list[str]]:
    """Bucket column names by ColumnKind, preserving config column order."""
    buckets: dict[ColumnKind, list[str]] = {kind: [] for kind in ColumnKind}
    for col in columns:
        name = col.get("name")
        if not name:
            continue
        kind = classified.get(name, ColumnKind.UNKNOWN)
        buckets[kind].append(name)
    return buckets


def _numeric_dist_lookup(check_results: Iterable) -> dict[str, dict]:
    """Map column → NumericDistributionCheck.detail for that column."""
    out: dict[str, dict] = {}
    for r in check_results:
        if getattr(r, "check_name", "") != "numeric_distribution":
            continue
        col = getattr(r, "column", None)
        if not col:
            continue
        detail = getattr(r, "detail", {}) or {}
        out[col] = detail
    return out


def _fields_literal(cols: list[str]) -> str:
    """Render a Python list literal of column names, one per line.

    Output looks like:
        [
            'col_a',
            'col_b',
        ]
    """
    if not cols:
        return "[]"
    inner = "\n        ".join(f"'{c}'," for c in cols)
    return "[\n        " + inner + "\n    ]"
