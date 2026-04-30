"""One test per check — all run against the in-memory DuckDB fixture."""

from __future__ import annotations

import pytest

from dbprofile.checks.format_validation import FormatValidationCheck
from dbprofile.checks.frequency_distribution import FrequencyDistributionCheck
from dbprofile.checks.null_density import NullDensityCheck
from dbprofile.checks.numeric_distribution import NumericDistributionCheck
from dbprofile.checks.row_count import RowCountCheck
from dbprofile.checks.schema_audit import SchemaAuditCheck
from dbprofile.checks.temporal_consistency import TemporalConsistencyCheck
from dbprofile.checks.uniqueness import UniquenessCheck
from dbprofile.config import (
    ChecksConfig,
    CheckThresholds,
    ConnectionConfig,
    ProfileConfig,
    ReportConfig,
    ScopeConfig,
)

# ── Minimal config for tests ──────────────────────────────────────────────────

def make_config(**overrides) -> ProfileConfig:
    thresholds = CheckThresholds(
        null_pct_warn=10.0,
        null_pct_critical=50.0,
        duplicate_pct_warn=0.001,
        duplicate_pct_critical=0.01,
        outlier_pct_warn=1.0,
        outlier_pct_critical=5.0,
        frequency_cardinality_limit=200,
    )
    return ProfileConfig(
        connection=ConnectionConfig(dialect="duckdb"),
        scope=ScopeConfig(schemas=["main"]),
        checks=ChecksConfig(sample_rate=1.0),
        report=ReportConfig(thresholds=thresholds),
        **overrides,
    )


def get_columns(connector, table: str) -> list[dict]:
    return connector.get_columns(table, "main")


# ── Check 1: Schema audit ─────────────────────────────────────────────────────

def test_schema_audit(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_nulls")
    results = SchemaAuditCheck().run("test_nulls", "main", cols, connector, cfg)

    assert len(results) == 1
    r = results[0]
    assert r.check_name == "schema_audit"
    # No all-null columns in the seed table → severity is "ok"
    # (changed from "info" when severity logic became binary: critical if any
    # all-null columns, ok otherwise).
    assert r.severity == "ok"
    assert r.value == len(cols)
    assert "columns" in r.detail
    assert len(r.detail["columns"]) == len(cols)


# ── Check 2: Row count ────────────────────────────────────────────────────────

def test_row_count(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_nulls")
    results = RowCountCheck().run("test_nulls", "main", cols, connector, cfg)

    count_results = [r for r in results if r.metric == "row_count"]
    assert len(count_results) == 1
    assert count_results[0].value == 1000  # seeded with 1000 rows

    # Should also produce daily_distribution result (has created_at date column)
    daily_results = [r for r in results if r.metric == "daily_distribution"]
    assert len(daily_results) == 1
    assert "series" in daily_results[0].detail


# ── Check 3: Null density ─────────────────────────────────────────────────────

def test_null_density(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_nulls")
    results = NullDensityCheck().run("test_nulls", "main", cols, connector, cfg)

    # email column: every 5th row is null = 20% → critical (threshold 50%) or warn (threshold 10%)
    email_result = next((r for r in results if r.column == "email"), None)
    assert email_result is not None
    assert email_result.metric == "null_pct"
    assert float(email_result.value) == pytest.approx(20.0, abs=1.0)
    assert email_result.severity in ("warn", "critical")

    # Severity is driven by thresholds: 20% > null_pct_warn(10%) but < null_pct_critical(50%)
    assert email_result.severity == "warn"

    # All results have populated detail dicts
    for r in results:
        if r.severity != "warn" or "error" not in r.detail:
            assert "null_count" in r.detail


# ── Check 4: Uniqueness ───────────────────────────────────────────────────────

def test_uniqueness(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_dupes")
    results = UniquenessCheck().run("test_dupes", "main", cols, connector, cfg)

    # id column has 10 distinct values in 100 rows → 90% duplicates.
    # Despite the name, distinct_pct is only 10% so it doesn't qualify as
    # an identifier (≥95% distinct). High-duplicate non-identifier columns
    # are classified as "info" — high duplicate_pct on an attribute is
    # expected, not a problem.
    id_result = next((r for r in results if r.column == "id"), None)
    assert id_result is not None
    assert id_result.metric == "duplicate_pct"
    assert float(id_result.value) > 50.0
    assert id_result.severity == "info"

    # detail must include distinct_count and top_duplicates
    assert "distinct_count" in id_result.detail
    assert "top_duplicates" in id_result.detail


# ── Check 5: Numeric distribution ────────────────────────────────────────────

def test_numeric_distribution(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_numeric")
    results = NumericDistributionCheck().run("test_numeric", "main", cols, connector, cfg)

    score_result = next((r for r in results if r.column == "score"), None)
    assert score_result is not None
    assert score_result.metric == "outlier_pct"

    detail = score_result.detail
    assert "p25" in detail
    assert "p50" in detail
    assert "p75" in detail
    assert "outlier_count" in detail
    # 2 seeded outliers out of 100 rows = 2%
    assert detail["outlier_count"] >= 1
    assert score_result.severity in ("warn", "critical")


# ── Check 6: Frequency distribution ──────────────────────────────────────────

def test_frequency_distribution(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_dupes")
    results = FrequencyDistributionCheck().run("test_dupes", "main", cols, connector, cfg)

    cat_result = next((r for r in results if r.column == "category"), None)
    assert cat_result is not None
    assert "series" in cat_result.detail
    assert len(cat_result.detail["series"]) > 0

    # Each series item has required keys
    for item in cat_result.detail["series"]:
        assert "value" in item
        assert "freq" in item
        assert "pct" in item


# ── Check 7: Temporal consistency ────────────────────────────────────────────

def test_temporal_consistency(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_gaps")
    results = TemporalConsistencyCheck().run("test_gaps", "main", cols, connector, cfg)

    assert len(results) >= 1
    r = results[0]
    assert r.check_name == "temporal_consistency"
    assert "series" in r.detail
    assert "gap_days" in r.detail

    # We seeded 4 gap days (indices 5, 6, 7, 15)
    gap_count = len(r.detail["gap_days"])
    assert gap_count >= 4
    assert r.severity == "critical"


# ── Check 8: Format validation ────────────────────────────────────────────────

def test_format_validation(connector):
    cfg = make_config()
    cols = get_columns(connector, "test_formats")
    results = FormatValidationCheck().run("test_formats", "main", cols, connector, cfg)

    email_result = next((r for r in results if r.column == "email"), None)
    assert email_result is not None
    assert email_result.metric == "violation_pct"

    # 20% of rows have bad emails → critical (threshold > 1%)
    assert float(email_result.value) == pytest.approx(20.0, abs=2.0)
    assert email_result.severity == "critical"
    assert "violations" in email_result.detail

    # status column: 3 distinct values — should not trigger enum warn
    status_result = next((r for r in results if r.column == "status"), None)
    if status_result:
        assert status_result.metric == "enum_cardinality"
        assert status_result.severity == "ok"
