"""Tests for dbprofile.notebook.classify — ColumnKind classifier.

One test per ColumnKind branch (9 total) plus a few priority-order checks
to verify the rules fire in the documented order.
"""

from __future__ import annotations

from types import SimpleNamespace

from dbprofile.notebook.classify import (
    ColumnFacts,
    ColumnKind,
    classify_columns,
    classify_one,
)


def _facts(name, dtype, n_unique=None):
    return ColumnFacts(name=name, data_type=dtype, n_unique=n_unique)


# ── One test per kind ────────────────────────────────────────────────────────


class TestSingleColumn:
    def test_date_from_dtype(self):
        assert classify_one(_facts("created_at", "TIMESTAMP")) == ColumnKind.DATE
        assert classify_one(_facts("ds",         "DATE"))      == ColumnKind.DATE

    def test_binary_from_bool_dtype(self):
        assert classify_one(_facts("is_active", "BOOLEAN")) == ColumnKind.BINARY

    def test_binary_from_int_with_two_uniques(self):
        assert classify_one(_facts("flag", "INT", n_unique=2)) == ColumnKind.BINARY

    def test_string_id_pattern(self):
        assert classify_one(_facts("user_id",  "VARCHAR")) == ColumnKind.STRING_ID
        assert classify_one(_facts("uuid",     "STRING"))  == ColumnKind.STRING_ID
        assert classify_one(_facts("hash_key", "TEXT"))    == ColumnKind.STRING_ID

    def test_low_cat_when_under_threshold(self):
        assert classify_one(_facts("status", "VARCHAR", n_unique=4)) == ColumnKind.LOW_CAT

    def test_high_cat_when_over_threshold(self):
        assert classify_one(_facts("zone_name", "VARCHAR", n_unique=200)) == ColumnKind.HIGH_CAT

    def test_count_metric_pattern(self):
        assert classify_one(_facts("trip_count",  "INT"))    == ColumnKind.COUNT_METRIC
        assert classify_one(_facts("revenue_cnt", "BIGINT")) == ColumnKind.COUNT_METRIC
        assert classify_one(_facts("total_amount", "FLOAT")) == ColumnKind.COUNT_METRIC

    def test_ordinal_cat_for_low_cardinality_named_ordinal(self):
        assert classify_one(_facts("month", "INT", n_unique=12)) == ColumnKind.ORDINAL_CAT
        assert classify_one(_facts("dow",   "INT", n_unique=7))  == ColumnKind.ORDINAL_CAT

    def test_continuous_for_other_numeric(self):
        assert classify_one(_facts("price", "DOUBLE", n_unique=1000)) == ColumnKind.CONTINUOUS

    def test_unknown_for_unrecognised_dtype(self):
        assert classify_one(_facts("blob_data", "BYTEA")) == ColumnKind.UNKNOWN


# ── Priority-order checks ────────────────────────────────────────────────────


class TestPriority:
    def test_date_beats_string_id_naming(self):
        # `created_id` would match the id pattern, but DATE wins on dtype.
        assert classify_one(_facts("created_id", "TIMESTAMP")) == ColumnKind.DATE

    def test_binary_beats_count_metric_naming(self):
        # `bool_total` matches the count pattern, but BINARY wins on dtype.
        assert classify_one(_facts("bool_total", "BOOLEAN")) == ColumnKind.BINARY

    def test_string_id_beats_low_cat(self):
        # An id-named string column with low cardinality stays STRING_ID.
        assert classify_one(_facts("acct_id", "VARCHAR", n_unique=3)) == ColumnKind.STRING_ID

    def test_count_metric_beats_continuous(self):
        # A high-cardinality numeric matching the count pattern stays COUNT_METRIC.
        assert classify_one(
            _facts("trip_count", "INT", n_unique=10_000)
        ) == ColumnKind.COUNT_METRIC


# ── classify_columns end-to-end ──────────────────────────────────────────────


class TestClassifyColumns:
    def test_pulls_cardinality_from_frequency_distribution(self):
        cols = [{"name": "status", "data_type": "VARCHAR"}]
        results = [
            SimpleNamespace(
                column="status", check_name="frequency_distribution",
                metric="distinct_count", value=4, detail={"distinct_count": 4},
            ),
        ]
        out = classify_columns(cols, results)
        assert out["status"] == ColumnKind.LOW_CAT

    def test_falls_back_to_uniqueness_when_no_freq_check(self):
        cols = [{"name": "country", "data_type": "VARCHAR"}]
        results = [
            SimpleNamespace(
                column="country", check_name="uniqueness",
                metric="duplicate_pct", value=98.0, detail={"distinct_count": 200},
            ),
        ]
        out = classify_columns(cols, results)
        assert out["country"] == ColumnKind.HIGH_CAT

    def test_no_cardinality_info_defaults_to_low_cat_for_strings(self):
        cols = [{"name": "name", "data_type": "VARCHAR"}]
        out = classify_columns(cols, [])
        assert out["name"] == ColumnKind.LOW_CAT

    def test_low_cardinality_threshold_override(self):
        cols = [{"name": "category", "data_type": "VARCHAR"}]
        results = [
            SimpleNamespace(
                column="category", check_name="frequency_distribution",
                metric="distinct_count", value=20, detail={"distinct_count": 20},
            ),
        ]
        # Default threshold (15) → HIGH_CAT
        assert classify_columns(cols, results)["category"] == ColumnKind.HIGH_CAT
        # Bumped threshold (25) → LOW_CAT
        assert classify_columns(cols, results, low_cardinality_threshold=25)["category"] == \
            ColumnKind.LOW_CAT

    def test_returns_kind_for_every_column(self):
        cols = [
            {"name": "ds",       "data_type": "DATE"},
            {"name": "user_id",  "data_type": "VARCHAR"},
            {"name": "status",   "data_type": "VARCHAR"},
            {"name": "amount",   "data_type": "DOUBLE"},
        ]
        out = classify_columns(cols, [])
        assert set(out.keys()) == {"ds", "user_id", "status", "amount"}
        assert out["ds"]      == ColumnKind.DATE
        assert out["user_id"] == ColumnKind.STRING_ID
        assert out["amount"]  == ColumnKind.CONTINUOUS
