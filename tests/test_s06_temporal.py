"""Tests for dbprofile.notebook.sections.s06_temporal."""

from __future__ import annotations

import ast

from dbprofile.notebook.classify import ColumnKind
from dbprofile.notebook.sections.s06_temporal import build_temporal_cells


def _cols(*names_kinds):
    cols = [{"name": n, "data_type": "x"} for n, _ in names_kinds]
    classified = {n: k for n, k in names_kinds}
    return cols, classified


def _md_sources(cells):
    return [c["source"] for c in cells if c["cell_type"] == "markdown"]


def _code_sources(cells):
    return [c["source"] for c in cells if c["cell_type"] == "code"]


# ── Gating ───────────────────────────────────────────────────────────────────

class TestGating:
    def test_skipped_when_no_date_column(self):
        cols, classified = _cols(
            ("amount",  ColumnKind.CONTINUOUS),
            ("user_id", ColumnKind.STRING_ID),
        )
        assert build_temporal_cells(columns=cols, classified=classified) == []

    def test_emitted_when_date_column_present(self):
        cols, classified = _cols(
            ("ds",     ColumnKind.DATE),
            ("amount", ColumnKind.CONTINUOUS),
        )
        cells = build_temporal_cells(columns=cols, classified=classified)
        assert cells != []
        assert cells[0]["source"] == "## Temporal Analysis"

    def test_uses_first_date_column_in_intro(self):
        cols, classified = _cols(
            ("amount",     ColumnKind.CONTINUOUS),
            ("created_at", ColumnKind.DATE),
            ("updated_at", ColumnKind.DATE),
        )
        cells = build_temporal_cells(columns=cols, classified=classified)
        intro = next(s for s in _md_sources(cells) if "Daily row count" in s)
        assert "`created_at`" in intro
        assert "`updated_at`" not in intro


# ── Chart cell content ───────────────────────────────────────────────────────

class TestChartCell:
    def _cells(self):
        cols, classified = _cols(
            ("ds",     ColumnKind.DATE),
            ("amount", ColumnKind.CONTINUOUS),
        )
        return build_temporal_cells(columns=cols, classified=classified)

    def test_uses_daily_df(self):
        chart_src = self._cells()[2]["source"]
        assert "daily_df['day']" in chart_src
        assert "daily_df['row_cnt']" in chart_src

    def test_swap_to_plot_daily_trips_documented_in_comment(self):
        chart_src = self._cells()[2]["source"]
        # The cell should include a hint pointing analysts at the
        # alternative helper if their data fits.
        assert "plot_daily_trips" in chart_src


# ── Syntactic validity ──────────────────────────────────────────────────────

class TestSyntacticValidity:
    def test_all_code_parses(self):
        cols, classified = _cols(
            ("ds",     ColumnKind.DATE),
            ("amount", ColumnKind.CONTINUOUS),
        )
        cells = build_temporal_cells(columns=cols, classified=classified)
        for c in cells:
            if c["cell_type"] == "code":
                ast.parse(c["source"])
