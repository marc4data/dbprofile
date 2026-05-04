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

    def test_ordinal_cat_for_prefixed_ordinal_names(self):
        """The previous classifier required exact-match against an ordinal
        name set, so `pickup_month` and `dropoff_hour` fell through to
        CONTINUOUS. The regex match catches both."""
        assert classify_one(_facts("pickup_month", "INT", n_unique=12))  == ColumnKind.ORDINAL_CAT
        assert classify_one(_facts("dropoff_hour", "INT", n_unique=24))  == ColumnKind.ORDINAL_CAT
        assert classify_one(_facts("created_year", "INT", n_unique=5))   == ColumnKind.ORDINAL_CAT
        assert classify_one(_facts("event_day_of_week", "INT", n_unique=7)) \
            == ColumnKind.ORDINAL_CAT

    def test_ordinal_threshold_now_accommodates_hour_and_day_of_month(self):
        """ORDINAL_NUNIQUE_MAX bumped from 12 → 31 so hour (24) and
        day-of-month (31) both qualify."""
        assert classify_one(_facts("hour",         "INT", n_unique=24)) == ColumnKind.ORDINAL_CAT
        assert classify_one(_facts("day_of_month", "INT", n_unique=31)) == ColumnKind.ORDINAL_CAT

    def test_binary_by_name_pattern_when_cardinality_unknown(self):
        """*_ind / *_flag / is_* / has_* columns get BINARY even when no
        FrequencyDistributionCheck ran (so n_unique is None and the
        n_unique==2 rule can't fire)."""
        assert classify_one(_facts("airport_pickup_ind", "INT")) == ColumnKind.BINARY
        assert classify_one(_facts("weather_rain_day_ind", "INT")) == ColumnKind.BINARY
        assert classify_one(_facts("jfk_flat_rate_flag",  "INT")) == ColumnKind.BINARY
        assert classify_one(_facts("is_holiday",          "INT")) == ColumnKind.BINARY
        assert classify_one(_facts("has_subscription",    "INT")) == ColumnKind.BINARY

    def test_binary_name_pattern_does_not_match_unrelated_cols(self):
        """Names that contain ind/flag as substrings but not as our
        recognised patterns must NOT be reclassified."""
        # "industry" contains 'ind' but doesn't end in `_ind`
        assert classify_one(_facts("industry_code", "VARCHAR", n_unique=8)) \
            == ColumnKind.LOW_CAT
        # "rainfall" contains nothing matching, and it's continuous
        assert classify_one(_facts("rainfall_inches", "FLOAT", n_unique=500)) \
            == ColumnKind.CONTINUOUS

    def test_numeric_low_cardinality_lookup_id_is_low_cat(self):
        """vendor_id, payment_type, rate_code_id with low cardinality are
        categorical lookups, not continuous distributions."""
        assert classify_one(_facts("vendor_id",      "INT", n_unique=2))  == ColumnKind.BINARY
        # ↑ binary rule fires first when n_unique==2; the rest go to LOW_CAT
        assert classify_one(_facts("payment_type",   "INT", n_unique=5))  == ColumnKind.LOW_CAT
        assert classify_one(_facts("rate_code_id",   "INT", n_unique=6))  == ColumnKind.LOW_CAT
        assert classify_one(_facts("status_code",    "INT", n_unique=4))  == ColumnKind.LOW_CAT
        assert classify_one(_facts("currency_key",   "INT", n_unique=10)) == ColumnKind.LOW_CAT

    def test_numeric_id_falls_through_to_continuous_when_cardinality_unknown(self):
        """Without cardinality info we can't tell if it's a low-card lookup
        or a high-card primary key; CONTINUOUS is the safe default — the
        analyst overrides via cfg.notebook.columns when needed."""
        assert classify_one(_facts("vendor_id",     "INT")) == ColumnKind.CONTINUOUS
        assert classify_one(_facts("customer_id",   "BIGINT")) == ColumnKind.CONTINUOUS

    def test_numeric_id_falls_through_to_continuous_when_cardinality_high(self):
        """High-cardinality numeric IDs (real primary keys) stay
        CONTINUOUS — out of LOW_CAT range and no other rule matches."""
        assert classify_one(_facts("customer_id", "BIGINT", n_unique=100_000)) \
            == ColumnKind.CONTINUOUS

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
