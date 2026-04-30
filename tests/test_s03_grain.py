"""Tests for dbprofile.notebook.sections.s03_grain."""

from __future__ import annotations

import ast

from dbprofile.notebook.classify import ColumnKind
from dbprofile.notebook.sections.s03_grain import build_grain_cells


def _cols_and_kinds():
    """A representative column mix for every test in the file."""
    cols = [
        {"name": "order_id",   "data_type": "VARCHAR"},
        {"name": "order_date", "data_type": "DATE"},
        {"name": "category",   "data_type": "VARCHAR"},
        {"name": "amount",     "data_type": "DOUBLE"},
    ]
    classified = {
        "order_id":   ColumnKind.STRING_ID,
        "order_date": ColumnKind.DATE,
        "category":   ColumnKind.LOW_CAT,
        "amount":     ColumnKind.CONTINUOUS,
    }
    return cols, classified


class TestCellShape:
    def test_emits_h2_header_first(self):
        cols, classified = _cols_and_kinds()
        cells = build_grain_cells(columns=cols, classified=classified)
        assert cells[0]["cell_type"] == "markdown"
        assert cells[0]["source"].startswith("## ")


class TestBoundaryConditions:
    def test_excludes_date_and_string_id_columns(self):
        cols, classified = _cols_and_kinds()
        cells = build_grain_cells(columns=cols, classified=classified)
        # Locate the boundary code cell (it references _boundary_cols).
        boundary_src = next(
            c["source"] for c in cells
            if c["cell_type"] == "code" and "_boundary_cols" in c["source"]
        )
        assert '"category"' in boundary_src
        assert '"amount"'   in boundary_src
        # Excluded kinds:
        assert '"order_date"' not in boundary_src
        assert '"order_id"'   not in boundary_src

    def test_skipped_when_only_date_and_id_columns(self):
        """If every column is excluded, no boundary cell is emitted."""
        cols = [
            {"name": "order_id",   "data_type": "VARCHAR"},
            {"name": "order_date", "data_type": "DATE"},
        ]
        classified = {"order_id": ColumnKind.STRING_ID, "order_date": ColumnKind.DATE}
        cells = build_grain_cells(columns=cols, classified=classified)
        assert not any(
            c["cell_type"] == "code" and "_boundary_cols" in c["source"]
            for c in cells
        )

    def test_uses_agg_with_correct_metrics(self):
        cols, classified = _cols_and_kinds()
        cells = build_grain_cells(columns=cols, classified=classified)
        boundary_src = next(
            c["source"] for c in cells
            if c["cell_type"] == "code" and "_boundary_cols" in c["source"]
        )
        assert ".agg(['nunique', 'min', 'max'])" in boundary_src


class TestCardinalitySummary:
    def test_emits_schema_and_describe_calls(self):
        cols, classified = _cols_and_kinds()
        cells = build_grain_cells(columns=cols, classified=classified)
        sources = [c["source"] for c in cells if c["cell_type"] == "code"]
        assert "schema(sample_df)" in sources
        assert "describe_by_type(sample_df)" in sources


class TestSyntacticValidity:
    def test_all_code_cells_parse(self):
        cols, classified = _cols_and_kinds()
        cells = build_grain_cells(columns=cols, classified=classified)
        for c in cells:
            if c["cell_type"] == "code":
                ast.parse(c["source"])
