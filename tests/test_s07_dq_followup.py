"""Tests for dbprofile.notebook.sections.s07_dq_followup."""

from __future__ import annotations

import ast
from types import SimpleNamespace

from dbprofile.notebook.sections.s07_dq_followup import build_dq_followup_cells


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


def _md_sources(cells):
    return [c["source"] for c in cells if c["cell_type"] == "markdown"]


def _code_sources(cells):
    return [c["source"] for c in cells if c["cell_type"] == "code"]


# ── Gating ───────────────────────────────────────────────────────────────────

class TestGating:
    def test_skipped_silently_when_no_findings(self):
        assert build_dq_followup_cells(table="t", check_results=[]) == []

    def test_skipped_when_only_info_results(self):
        results = [_result("t", "row_count", "info")]
        assert build_dq_followup_cells(table="t", check_results=results) == []

    def test_skipped_when_findings_for_other_table_only(self):
        results = [_result("OTHER", "null_density", "critical", column="x")]
        assert build_dq_followup_cells(table="t", check_results=results) == []

    def test_emitted_for_critical_finding(self):
        results = [_result("t", "null_density", "critical", column="email",
                           null_pct=42.0, null_count=420)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        assert cells != []
        assert cells[0]["source"] == "## Data Quality Follow-up"

    def test_emitted_for_warn_finding(self):
        results = [_result("t", "uniqueness", "warn", column="customer_id",
                           distinct_count=98, distinct_pct=98.0)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        assert cells != []


# ── Ordering ─────────────────────────────────────────────────────────────────

class TestOrdering:
    def test_critical_findings_appear_before_warns(self):
        results = [
            _result("t", "uniqueness",   "warn",     column="cust_id"),
            _result("t", "null_density", "critical", column="email"),
        ]
        cells = build_dq_followup_cells(table="t", check_results=results)
        h3_headings = [s for s in _md_sources(cells) if s.startswith("### ")]
        assert "Null Density" in h3_headings[0]
        assert "Uniqueness"   in h3_headings[1]

    def test_groups_by_check_name_then_column_within_severity(self):
        results = [
            _result("t", "uniqueness",   "critical", column="b"),
            _result("t", "null_density", "critical", column="z"),
            _result("t", "null_density", "critical", column="a"),
        ]
        cells = build_dq_followup_cells(table="t", check_results=results)
        h3_headings = [s for s in _md_sources(cells) if s.startswith("### ")]
        # null_density (a) → null_density (z) → uniqueness (b)
        assert "Null Density — `a`" in h3_headings[0]
        assert "Null Density — `z`" in h3_headings[1]
        assert "Uniqueness — `b`"   in h3_headings[2]


# ── null_density ─────────────────────────────────────────────────────────────

class TestNullDensity:
    def test_callout_includes_pct_and_count(self):
        results = [_result("t", "null_density", "critical", column="email",
                           null_pct=23.45, null_count=1234)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        callouts = [s for s in _md_sources(cells) if s.startswith(">")]
        assert "23.45%" in callouts[0]
        assert "1,234 rows" in callouts[0]

    def test_code_cell_filters_isna(self):
        results = [_result("t", "null_density", "critical", column="email",
                           null_pct=10.0, null_count=10)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "isna()" in c)
        assert "sample_df['email'].isna()" in code


# ── uniqueness ───────────────────────────────────────────────────────────────

class TestUniqueness:
    def test_code_cell_uses_value_counts(self):
        results = [_result("t", "uniqueness", "critical", column="customer_id",
                           distinct_count=42, distinct_pct=98.5)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "value_counts" in c)
        assert "sample_df['customer_id'].value_counts()" in code
        assert "_dupes[_dupes > 1]" in code

    def test_callout_includes_distinct_stats(self):
        results = [_result("t", "uniqueness", "warn", column="cust_id",
                           distinct_count=98, distinct_pct=98.0)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        callouts = [s for s in _md_sources(cells) if s.startswith(">")]
        assert "98 distinct" in callouts[0]
        assert "98.00%" in callouts[0]


# ── format_validation ────────────────────────────────────────────────────────

class TestFormatValidation:
    def test_with_pattern_emits_regex_match_cell(self):
        results = [_result("t", "format_validation", "critical", column="email",
                           pattern=r"^[^@]+@[^@]+\.[^@]+$",
                           format_label="email", violations=8, violation_pct=4.0)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "str.match" in c)
        assert "import re" in code
        assert ".str.match(_pattern" in code
        assert "_pattern = r'^[^@]+@[^@]+\\.[^@]+$'" in code

    def test_without_pattern_falls_back_to_head(self):
        results = [_result("t", "format_validation", "warn", column="phone")]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "head(20)" in c)
        assert "sample_df[['phone']].head(20)" in code


# ── temporal_consistency ─────────────────────────────────────────────────────

class TestTemporalConsistency:
    def test_lists_gap_days_in_cell(self):
        results = [_result("t", "temporal_consistency", "warn", column="ds",
                           gap_days=[
                               {"date": "2026-01-15", "count": 0},
                               {"date": "2026-01-16", "count": 0},
                           ])]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "_gap_days" in c)
        assert "'2026-01-15'" in code
        assert "'2026-01-16'" in code
        assert "pd.DataFrame(_gap_days)" in code

    def test_truncates_at_fifty_gaps(self):
        results = [_result(
            "t", "temporal_consistency", "warn", column="ds",
            gap_days=[{"date": f"2026-01-{i:02d}", "count": 0} for i in range(1, 60)],
        )]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "_gap_days" in c)
        assert "+ 9 more" in code

    def test_callout_mentions_gap_count(self):
        results = [_result("t", "temporal_consistency", "warn", column="ds",
                           gap_days=[{"date": "2026-01-15", "count": 0}])]
        cells = build_dq_followup_cells(table="t", check_results=results)
        callouts = [s for s in _md_sources(cells) if s.startswith(">")]
        # Number is bolded in the callout; check the surrounding context
        assert "1 day(s)" in callouts[0]
        assert "with zero rows" in callouts[0]

    def test_skipped_when_gap_days_is_empty(self):
        """Temporal consistency that fired but has no gap_days in the
        detail dict produces a useless empty placeholder cell. We filter
        those out at the actionability check rather than emit them."""
        results = [
            _result("t", "temporal_consistency", "warn", column="ds", gap_days=[]),
            _result("t", "temporal_consistency", "warn", column="other", gap_days=None),
        ]
        cells = build_dq_followup_cells(table="t", check_results=results)
        # Both findings are unactionable → section as a whole is empty
        assert cells == []

    def test_actionable_temporal_still_renders_when_others_empty(self):
        """If at least one temporal finding has gap_days, it renders; the
        empty ones still get filtered."""
        results = [
            _result("t", "temporal_consistency", "warn", column="empty_col", gap_days=[]),
            _result("t", "temporal_consistency", "warn", column="real_col",
                    gap_days=[{"date": "2026-01-15", "count": 0}]),
        ]
        cells = build_dq_followup_cells(table="t", check_results=results)
        h3_headings = [s for s in _md_sources(cells) if s.startswith("### ")]
        assert any("`real_col`" in h for h in h3_headings)
        assert not any("`empty_col`" in h for h in h3_headings)


# ── row_count ────────────────────────────────────────────────────────────────

class TestRowCount:
    def test_empty_table_callout_only_no_code_cell(self):
        results = [_result("t", "row_count", "critical", is_empty=True, row_count=0)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        # Heading + callout — no investigation cell
        assert sum(1 for c in cells if c["cell_type"] == "code") == 0
        callouts = [s for s in _md_sources(cells) if s.startswith(">")]
        assert "Table is empty" in callouts[0]

    def test_non_empty_row_count_uses_count_in_summary(self):
        results = [_result("t", "row_count", "warn", row_count=12)]
        cells = build_dq_followup_cells(table="t", check_results=results)
        callouts = [s for s in _md_sources(cells) if s.startswith(">")]
        assert "12 rows" in callouts[0]


# ── Generic fallback for unexpected check names ──────────────────────────────

class TestGenericFallback:
    def test_unknown_check_emits_head_cell(self):
        results = [_result("t", "experimental_check", "warn", column="x")]
        cells = build_dq_followup_cells(table="t", check_results=results)
        code = next(c for c in _code_sources(cells) if "head(20)" in c)
        assert "sample_df[['x']].head(20)" in code


# ── Syntactic validity ──────────────────────────────────────────────────────

class TestSyntacticValidity:
    def test_all_emitted_code_parses(self):
        # Mix of every check type — every emitted code cell must parse.
        results = [
            _result("t", "null_density",         "critical", column="email",
                    null_pct=10.0, null_count=10),
            _result("t", "uniqueness",           "warn",     column="cust_id",
                    distinct_count=98, distinct_pct=98.0),
            _result("t", "format_validation",    "warn",     column="email",
                    pattern=r"^.+$", format_label="x", violations=5,
                    violation_pct=2.5),
            _result("t", "temporal_consistency", "warn",     column="ds",
                    gap_days=[{"date": "2026-01-15", "count": 0}]),
            _result("t", "row_count",            "critical", is_empty=True),
        ]
        cells = build_dq_followup_cells(table="t", check_results=results)
        for c in cells:
            if c["cell_type"] == "code":
                ast.parse(c["source"])
