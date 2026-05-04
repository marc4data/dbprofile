"""Tests for CHECK_DEFINITIONS surfacing in the HTML report.

Two surfaces:
  * Heatmap column headers (per-table heatmap)
  * Scoreboard column headers (top-of-report table)

Both should render with `title="<label> — <definition>"` so analysts see
what each check measures on hover.

Tests that the same dict is appended to notebook callouts live in
test_s00_header.py::TestCheckDefinitionsInCallouts.
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import pytest

from dbprofile.checks.base import CheckResult
from dbprofile.report.renderer import (
    CANONICAL_ORDER,
    CHECK_DEFINITIONS,
    CHECK_LABELS,
    _build_report_context,
)

# ── Module-level invariants ─────────────────────────────────────────────────


class TestModuleConstants:
    def test_every_check_has_a_definition(self):
        # Every check in the canonical order needs a definition; otherwise
        # the rendered tooltip would say '<label> — None' which is worse
        # than no tooltip at all.
        for cn in CANONICAL_ORDER:
            assert cn in CHECK_DEFINITIONS

    def test_definitions_are_meaningful_length(self):
        for cn, defn in CHECK_DEFINITIONS.items():
            assert len(defn) >= 30, f"{cn} definition too short to be useful"
            assert len(defn) <= 250, f"{cn} definition too long for a tooltip"


# ── Context dict ────────────────────────────────────────────────────────────


def _minimal_cfg():
    """ProfileConfig-shaped duck type for rendering."""
    return SimpleNamespace(
        connection=SimpleNamespace(
            dialect="duckdb", account=None, project=None,
            warehouse=None, role=None, database_path="/tmp/x.db",
        ),
        scope=SimpleNamespace(
            database="MAIN", dataset=None, project=None,
            schemas=["main"], tables=None, exclude_tables=[],
            column_overrides={},
        ),
        checks=SimpleNamespace(
            enabled=["all"], disabled=[],
            sample_rate=1.0, sample_method="bernoulli",
        ),
        report=SimpleNamespace(
            output="/tmp/out.html", include=["tables", "charts"],
            preview_rows=25,
            thresholds=SimpleNamespace(
                null_pct_warn=10.0, null_pct_critical=50.0,
                duplicate_pct_warn=0.001, duplicate_pct_critical=0.01,
                outlier_pct_warn=1.0, outlier_pct_critical=5.0,
                frequency_cardinality_limit=200, skew_day_pct=50.0,
            ),
        ),
    )


def _minimal_results():
    """One row_count + one schema_audit so the renderer has something to walk."""
    return [
        CheckResult(
            table="fct_orders", schema="main", column=None,
            check_name="row_count", metric="row_count", value=100,
            severity="ok", detail={"row_count": 100, "is_empty": False},
        ),
        CheckResult(
            table="fct_orders", schema="main", column=None,
            check_name="schema_audit", metric="column_count", value=3,
            severity="ok",
            detail={"columns": [
                {"name": "id", "data_type": "INT", "is_nullable": "NO"},
                {"name": "amount", "data_type": "DOUBLE", "is_nullable": "YES"},
                {"name": "category", "data_type": "VARCHAR", "is_nullable": "YES"},
            ]},
        ),
    ]


class TestContextDict:
    def test_check_definitions_in_context(self):
        ctx = _build_report_context(_minimal_results(), _minimal_cfg(),
                                    datetime(2026, 5, 3))
        assert "check_definitions" in ctx
        assert ctx["check_definitions"] is CHECK_DEFINITIONS


# ── End-to-end HTML render — definitions appear in tooltips ─────────────────


@pytest.fixture
def rendered_html(tmp_path):
    """Render the HTML report and return its source for grep-style assertions."""
    from dbprofile.report.renderer import render_report
    out = tmp_path / "report.html"
    render_report(_minimal_results(), _minimal_cfg(), str(out),
                  run_at=datetime(2026, 5, 3))
    return out.read_text(encoding="utf-8")


class TestRenderedTooltips:
    def test_definition_appears_in_scoreboard_tooltip(self, rendered_html):
        # The scoreboard column header should embed the definition in its title.
        # Check at least one well-known check.
        defn = CHECK_DEFINITIONS["null_density"]
        label = CHECK_LABELS["null_density"]
        assert f'title="{label} — {defn}"' in rendered_html

    def test_definition_appears_in_heatmap_tooltip(self, rendered_html):
        # Heatmap column headers use the same `title="<label> — <definition>"`
        # pattern. Both surfaces should agree.
        defn = CHECK_DEFINITIONS["uniqueness"]
        label = CHECK_LABELS["uniqueness"]
        assert f'title="{label} — {defn}"' in rendered_html

    def test_every_canonical_check_definition_renders(self, rendered_html):
        """Stronger smoke check: every check we surface in the canonical
        order shows up at least once in the rendered HTML's tooltips."""
        for cn in CANONICAL_ORDER:
            label = CHECK_LABELS[cn]
            defn = CHECK_DEFINITIONS[cn]
            tooltip = f"{label} — {defn}"
            assert tooltip in rendered_html, f"{cn} tooltip missing from HTML"
