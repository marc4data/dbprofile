"""Tests for dbprofile.notebook.sections.s05_bivariate."""

from __future__ import annotations

import ast

from dbprofile.notebook.classify import ColumnKind
from dbprofile.notebook.sections.s05_bivariate import (
    PAIR_CORR_CEIL,
    PAIR_CORR_FLOOR,
    TOP_PAIRS,
    build_bivariate_cells,
)


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
    def test_skipped_when_zero_continuous(self):
        cols, classified = _cols(
            ("user_id", ColumnKind.STRING_ID),
            ("ds",      ColumnKind.DATE),
        )
        assert build_bivariate_cells(columns=cols, classified=classified) == []

    def test_skipped_when_one_continuous(self):
        cols, classified = _cols(
            ("amount",  ColumnKind.CONTINUOUS),
            ("user_id", ColumnKind.STRING_ID),
        )
        # Need ≥2 numeric columns to compute pairs
        assert build_bivariate_cells(columns=cols, classified=classified) == []

    def test_emitted_when_two_or_more_continuous(self):
        cols, classified = _cols(
            ("amount",   ColumnKind.CONTINUOUS),
            ("trip_dist", ColumnKind.CONTINUOUS),
        )
        cells = build_bivariate_cells(columns=cols, classified=classified)
        assert cells != []
        assert cells[0]["source"] == "## Bivariate Analysis"


# ── 5a Correlation heatmap ───────────────────────────────────────────────────

class TestCorrelationHeatmap:
    def _cells(self):
        cols, classified = _cols(
            ("a", ColumnKind.CONTINUOUS),
            ("b", ColumnKind.CONTINUOUS),
            ("c", ColumnKind.CONTINUOUS),
        )
        return build_bivariate_cells(columns=cols, classified=classified)

    def test_heatmap_section_emitted(self):
        cells = self._cells()
        assert any(s.startswith("### Correlation matrix") for s in _md_sources(cells))

    def test_heatmap_uses_seaborn_and_corr(self):
        cells = self._cells()
        heatmap_src = next(
            c for c in _code_sources(cells) if "sns.heatmap" in c
        )
        assert "import seaborn as sns" in heatmap_src
        # corr() runs against numeric columns selected via select_dtypes
        assert "select_dtypes('number')" in heatmap_src
        assert ".corr()" in heatmap_src
        assert "annot=True" in heatmap_src
        assert "center=0" in heatmap_src
        assert "cmap='RdBu_r'" in heatmap_src


# ── 5b Top scatter pairs ─────────────────────────────────────────────────────

class TestScatterPairs:
    def _cells(self):
        cols, classified = _cols(
            ("a", ColumnKind.CONTINUOUS),
            ("b", ColumnKind.CONTINUOUS),
            ("c", ColumnKind.CONTINUOUS),
        )
        return build_bivariate_cells(columns=cols, classified=classified)

    def test_section_emitted(self):
        cells = self._cells()
        assert any(s.startswith("### Top scatter pairs") for s in _md_sources(cells))

    def test_filter_thresholds_baked_into_code(self):
        cells = self._cells()
        scatter_src = next(
            c for c in _code_sources(cells) if "_pairs.sort" in c
        )
        assert str(PAIR_CORR_FLOOR) in scatter_src
        assert str(PAIR_CORR_CEIL) in scatter_src
        assert f"_pairs[:{TOP_PAIRS}]" in scatter_src

    def test_emits_plot_scatter_loop(self):
        cells = self._cells()
        scatter_src = next(
            c for c in _code_sources(cells) if "plot_scatter(" in c
        )
        assert "for _x, _y, _ in _pairs" in scatter_src
        assert "x_field = _x" in scatter_src
        assert "y_field = _y" in scatter_src
        # Markdown intro mentions the runtime selection approach
        intros = _md_sources(cells)
        assert any("at runtime" in s for s in intros)


# ── Syntactic validity ──────────────────────────────────────────────────────

class TestSyntacticValidity:
    def test_all_emitted_code_parses(self):
        cols, classified = _cols(
            ("a", ColumnKind.CONTINUOUS),
            ("b", ColumnKind.CONTINUOUS),
        )
        cells = build_bivariate_cells(columns=cols, classified=classified)
        for c in cells:
            if c["cell_type"] == "code":
                ast.parse(c["source"])
