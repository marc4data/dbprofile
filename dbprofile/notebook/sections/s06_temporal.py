"""Temporal Analysis section — daily volume line chart.

Gated on the same condition that triggers `daily_df` in s02: at least
one DATE-kinded column. When `daily_df` exists, this section emits a
simple matplotlib line chart of `row_cnt` over time.

Why a plain matplotlib line and not `plot_daily_trips()`?
  The eda_helpers `plot_daily_trips` function is hardcoded to expect
  columns named `borough` and `trip_cnt` (NYC TLC schema). Our generic
  `daily_df` from s02 has `[day, row_cnt]` instead. Rather than fake
  a borough column or constrain the analyst's data shape, this section
  emits a plain matplotlib chart and points at `plot_daily_trips()` in
  a comment so the analyst can swap if their data fits.

Temporal-consistency callouts (per the feature plan §6 6b — gap dates
from TemporalConsistencyCheck) are intentionally deferred to PR 7
(s07_dq_followup), where they sit alongside the other DQ deep-dives.
"""

from __future__ import annotations

import nbformat

from dbprofile.notebook.cells import code_cell, md_cell, section_header
from dbprofile.notebook.classify import ColumnKind


def build_temporal_cells(
    *,
    columns: list[dict],
    classified: dict[str, ColumnKind],
) -> list[nbformat.NotebookNode]:
    """Return the cells for the Temporal Analysis section.

    Returns an empty list when no DATE column was classified — s02
    won't have built `daily_df` either, so plotting has nothing to
    operate on.
    """
    date_col = _first_date_column(columns, classified)
    if not date_col:
        return []

    return [
        section_header(2, "Temporal Analysis"),
        md_cell(
            f"Daily row count from `{date_col}` (already aggregated into "
            f"`daily_df` by the Data Gathering section). Spot gaps, "
            f"weekend/weekday cycles, and load-spike anomalies."
        ),
        code_cell(_daily_volume_chart_source()),
    ]


# ── Internal helpers ─────────────────────────────────────────────────────────


def _first_date_column(
    columns: list[dict],
    classified: dict[str, ColumnKind],
) -> str | None:
    """First column the classifier flagged as DATE, in column order.

    Mirrors s02_data_gather._first_date_column so both sections agree on
    what 'the date column' is for this notebook.
    """
    for col in columns:
        name = col.get("name")
        if name and classified.get(name) == ColumnKind.DATE:
            return name
    return None


def _daily_volume_chart_source() -> str:
    """Plain matplotlib line chart over daily_df.

    Keeps the cell editable by the analyst — they can swap to
    plot_daily_trips() (or any other helper) if their data fits a
    different schema.
    """
    return (
        "# daily_df has columns [day, row_cnt] — a generic schema.\n"
        "# Swap in `plot_daily_trips(df=daily_df, date_col='day')` if your\n"
        "# data has the [date, borough, trip_cnt] schema that helper expects.\n"
        "fig, ax = plt.subplots(figsize=(12, 4))\n"
        "ax.plot(daily_df['day'], daily_df['row_cnt'], linewidth=1.2)\n"
        "ax.set_title('Daily row count', fontsize=14)\n"
        "ax.set_xlabel('Date')\n"
        "ax.set_ylabel('Rows')\n"
        "ax.grid(True, alpha=0.3)\n"
        "plt.tight_layout()\n"
        "plt.show()"
    )
