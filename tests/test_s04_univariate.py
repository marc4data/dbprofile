"""Tests for dbprofile.notebook.sections.s04_univariate."""

from __future__ import annotations

import ast
from types import SimpleNamespace

from dbprofile.notebook.classify import ColumnKind
from dbprofile.notebook.sections.s04_univariate import (
    MAX_CONTINUOUS_PANELS,
    _distribution_call,
    build_univariate_cells,
)


def _cols(*names_kinds):
    """Build (columns_list, classified_dict) from (name, kind) pairs."""
    cols = [{"name": n, "data_type": "x"} for n, _ in names_kinds]
    classified = {n: k for n, k in names_kinds}
    return cols, classified


def _md_sources(cells):
    return [c["source"] for c in cells if c["cell_type"] == "markdown"]


def _code_sources(cells):
    return [c["source"] for c in cells if c["cell_type"] == "code"]


# ── Section structure ────────────────────────────────────────────────────────

class TestStructure:
    def test_emits_h2_header(self):
        cols, classified = _cols(("flag", ColumnKind.BINARY))
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        assert cells[0]["source"] == "## Univariate Analysis"

    def test_skipped_message_when_no_plottable_columns(self):
        # Only DATE + STRING_ID → nothing to chart univariately
        cols, classified = _cols(
            ("ds",      ColumnKind.DATE),
            ("user_id", ColumnKind.STRING_ID),
        )
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        assert any("skipped" in s.lower() for s in _md_sources(cells))


# ── 4a — Flags & ordinals ────────────────────────────────────────────────────

class TestFlagPanel:
    def test_emits_plot_histograms_for_binary_and_ordinal(self):
        cols, classified = _cols(
            ("is_active", ColumnKind.BINARY),
            ("month",     ColumnKind.ORDINAL_CAT),
        )
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        codes = _code_sources(cells)
        assert any("plot_histograms(" in c for c in codes)
        flag_call = next(c for c in codes if "plot_histograms(" in c)
        assert "'is_active'" in flag_call and "'month'" in flag_call
        assert "label_threshold = 12" in flag_call

    def test_skipped_when_no_flag_columns(self):
        cols, classified = _cols(("amount", ColumnKind.CONTINUOUS))
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        assert not any("plot_histograms(" in c for c in _code_sources(cells))


# ── 4b — Categorical low / high ──────────────────────────────────────────────

class TestCategoricalPanel:
    def test_low_uses_plot_string_profile(self):
        cols, classified = _cols(("category", ColumnKind.LOW_CAT))
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        codes = _code_sources(cells)
        assert any("plot_string_profile(" in c for c in codes)
        assert not any("plot_string_profile_hc(" in c for c in codes)

    def test_high_uses_plot_string_profile_hc(self):
        cols, classified = _cols(("zone_name", ColumnKind.HIGH_CAT))
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        codes = _code_sources(cells)
        assert any("plot_string_profile_hc(" in c for c in codes)

    def test_both_emits_two_calls(self):
        cols, classified = _cols(
            ("category",  ColumnKind.LOW_CAT),
            ("zone_name", ColumnKind.HIGH_CAT),
        )
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        codes = _code_sources(cells)
        assert any("plot_string_profile(" in c and "_hc(" not in c for c in codes)
        assert any("plot_string_profile_hc(" in c for c in codes)


# ── 4c — Count metrics ───────────────────────────────────────────────────────

class TestCountPanel:
    def test_emits_plot_field_aggregates(self):
        cols, classified = _cols(
            ("trip_count",    ColumnKind.COUNT_METRIC),
            ("total_revenue", ColumnKind.COUNT_METRIC),
        )
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        codes = _code_sources(cells)
        assert any("plot_field_aggregates(" in c for c in codes)
        agg_call = next(c for c in codes if "plot_field_aggregates(" in c)
        assert "'trip_count'" in agg_call
        assert "'total_revenue'" in agg_call


# ── 4d — Continuous distributions ────────────────────────────────────────────

class TestContinuousPanel:
    def test_one_call_per_continuous_column(self):
        cols, classified = _cols(
            ("amount",      ColumnKind.CONTINUOUS),
            ("trip_dist",   ColumnKind.CONTINUOUS),
            ("fare_amount", ColumnKind.CONTINUOUS),
        )
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        dist_calls = [c for c in _code_sources(cells) if "plot_distribution(" in c]
        assert len(dist_calls) == 3

    def test_caps_at_max_panels(self):
        many = [(f"c{i}", ColumnKind.CONTINUOUS) for i in range(MAX_CONTINUOUS_PANELS + 5)]
        cols, classified = _cols(*many)
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=[],
        )
        dist_calls = [c for c in _code_sources(cells) if "plot_distribution(" in c]
        assert len(dist_calls) == MAX_CONTINUOUS_PANELS
        # Truncation message present
        assert any("Showing first" in s for s in _md_sources(cells))

    def test_uses_p99_and_zero_min_when_check_says_non_negative(self):
        detail = {"min": 0, "p99": 95.5}
        call = _distribution_call("amount", detail)
        assert "field          = 'amount'" in call
        assert "bin_min        = 0," in call
        assert "bin_max        = 95.5," in call

    def test_skips_bin_min_when_min_is_negative(self):
        detail = {"min": -10, "p99": 50}
        call = _distribution_call("temp", detail)
        assert "bin_min" not in call
        assert "bin_max        = 50," in call

    def test_no_zoom_args_when_no_check_details(self):
        call = _distribution_call("amount", {})
        assert "bin_min" not in call
        assert "bin_max" not in call
        assert "field          = 'amount'" in call


# ── End-to-end syntactic validity ────────────────────────────────────────────

class TestSyntacticValidity:
    def test_all_emitted_code_cells_parse(self):
        cols, classified = _cols(
            ("is_active", ColumnKind.BINARY),
            ("month",     ColumnKind.ORDINAL_CAT),
            ("category",  ColumnKind.LOW_CAT),
            ("zone_name", ColumnKind.HIGH_CAT),
            ("trip_cnt",  ColumnKind.COUNT_METRIC),
            ("amount",    ColumnKind.CONTINUOUS),
        )
        # Mock NumericDistributionCheck result for 'amount'
        results = [SimpleNamespace(
            check_name="numeric_distribution",
            column="amount",
            detail={"min": 0, "p99": 100.0},
        )]
        cells = build_univariate_cells(
            columns=cols, classified=classified, check_results=results,
        )
        for c in cells:
            if c["cell_type"] == "code":
                ast.parse(c["source"])   # raises on bad syntax
