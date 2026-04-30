"""Tests for dbprofile.notebook.sections.s00_header."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from dbprofile.notebook.sections.s00_header import build_header_cells


def _result(table, check_name, severity, column=None, **detail):
    return SimpleNamespace(
        table=table,
        check_name=check_name,
        severity=severity,
        column=column,
        metric="x",
        value=0,
        detail=detail,
    )


# ── Cell shape ───────────────────────────────────────────────────────────────

class TestCellShape:
    def test_emits_h1_title_first(self):
        cells = build_header_cells(
            table="fct_orders", schema_name="main", connector_type="duckdb",
            check_results=[],
        )
        assert cells[0]["cell_type"] == "markdown"
        assert cells[0]["source"].startswith("# fct_orders")

    def test_metadata_block_includes_schema_table_connector(self):
        cells = build_header_cells(
            table="fct_orders", schema_name="main", connector_type="duckdb",
            check_results=[],
        )
        meta = cells[1]["source"]
        assert "**Schema:** `main`" in meta
        assert "**Table:** `fct_orders`" in meta
        assert "**Connector:** `duckdb`" in meta

    def test_run_at_appears_in_metadata(self):
        when = datetime(2026, 5, 1, 12, 30)
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=[], run_at=when,
        )
        assert "2026-05-01 12:30 UTC" in cells[1]["source"]


# ── DQ callouts ──────────────────────────────────────────────────────────────

class TestDQCallouts:
    def test_no_findings_yields_green_note(self):
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=[],
        )
        callout_sources = [c["source"] for c in cells if c["source"].startswith(">")]
        assert len(callout_sources) == 1
        # Green NOTE with friendly "no issues" message
        assert "[!NOTE]" in callout_sources[0]
        assert "No DQ issues flagged" in callout_sources[0]

    def test_metadata_summary_when_no_findings(self):
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=[],
        )
        assert "**No DQ issues flagged.**" in cells[1]["source"]

    def test_critical_uses_caution_tag(self):
        results = [
            _result("t", "null_density", "critical", column="email"),
        ]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callout_sources = [c["source"] for c in cells if c["source"].startswith(">")]
        assert any("[!CAUTION]" in s for s in callout_sources)
        assert any("`email`" in s for s in callout_sources)

    def test_warn_uses_warning_tag(self):
        results = [
            _result("t", "uniqueness", "warn", column="customer_id"),
        ]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callout_sources = [c["source"] for c in cells if c["source"].startswith(">")]
        assert any("[!WARNING]" in s for s in callout_sources)

    def test_info_results_are_not_surfaced_as_callouts(self):
        results = [_result("t", "row_count", "info", column=None)]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callout_sources = [c["source"] for c in cells if c["source"].startswith(">")]
        # Falls back to the green "no issues" note
        assert len(callout_sources) == 1
        assert "No DQ issues flagged" in callout_sources[0]

    def test_one_callout_per_check_name_per_severity(self):
        results = [
            _result("t", "null_density", "critical", column="email"),
            _result("t", "null_density", "critical", column="phone"),
            _result("t", "null_density", "warn", column="postal"),
            _result("t", "uniqueness", "critical", column="customer_id"),
        ]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callouts = [c["source"] for c in cells if c["source"].startswith(">")]
        # null_density-critical, null_density-warn, uniqueness-critical → 3
        assert len(callouts) == 3

    def test_critical_callouts_appear_before_warn(self):
        results = [
            _result("t", "null_density", "warn",     column="postal"),
            _result("t", "null_density", "critical", column="email"),
        ]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callouts = [c["source"] for c in cells if c["source"].startswith(">")]
        assert "[!CAUTION]" in callouts[0]
        assert "[!WARNING]" in callouts[1]

    def test_truncates_column_list_at_six(self):
        cols = [f"c{i}" for i in range(10)]
        results = [_result("t", "null_density", "critical", column=c) for c in cols]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callouts = [c["source"] for c in cells if c["source"].startswith(">")]
        assert "(+ 4 more)" in callouts[0]
        # First six columns appear, the last four don't
        for c in cols[:6]:
            assert f"`{c}`" in callouts[0]
        for c in cols[6:]:
            assert f"`{c}`" not in callouts[0]

    def test_other_table_results_ignored(self):
        results = [
            _result("OTHER",  "null_density", "critical", column="x"),
        ]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        callouts = [c["source"] for c in cells if c["source"].startswith(">")]
        # Only the green "no issues" note for table 't'
        assert len(callouts) == 1
        assert "No DQ issues flagged" in callouts[0]

    def test_total_count_in_metadata_summary(self):
        results = [
            _result("t", "null_density", "critical", column="email"),
            _result("t", "null_density", "critical", column="phone"),
            _result("t", "uniqueness",   "warn",     column="customer_id"),
        ]
        cells = build_header_cells(
            table="t", schema_name="s", connector_type="duckdb",
            check_results=results,
        )
        assert "3 DQ issue(s) flagged" in cells[1]["source"]
