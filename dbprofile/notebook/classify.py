"""Classify each column into the kind that drives notebook chart selection.

The notebook generator picks chart helpers based on a column's ColumnKind:

  DATE          → temporal section (plot_daily_trips)
  BINARY        → flag panel (plot_histograms with label_threshold)
  ORDINAL_CAT   → flag panel (numeric ordinals: month, dow, hour)
  LOW_CAT       → plot_string_profile          (≤ low_cardinality_threshold)
  HIGH_CAT      → plot_string_profile_hc       (>  low_cardinality_threshold)
  STRING_ID     → skip distribution; show in DQ section if flagged
  COUNT_METRIC  → plot_field_aggregates (sum / mean panel)
  CONTINUOUS    → plot_distribution + plot_boxplot
  UNKNOWN       → fall through to a peek

Inputs
------
columns        list[dict] from connector.get_columns(table)
                 each has {name, data_type, is_nullable}
check_results  list[CheckResult] from the same run; we mine cardinality
                 from FrequencyDistributionCheck (preferred) or
                 UniquenessCheck.detail.distinct_count (fallback)

Priority order — first match wins
---------------------------------
  1. data_type is date/datetime/timestamp  → DATE
  2. data_type is bool, OR int with n_unique == 2  → BINARY
  3. string AND name matches *_id / uuid pattern  → STRING_ID
  4. string AND n_unique ≤ low_cardinality_threshold  → LOW_CAT
  5. string AND n_unique > low_cardinality_threshold  → HIGH_CAT
  6. numeric AND name matches count/aggregate pattern  → COUNT_METRIC
  7. numeric AND n_unique ≤ ORDINAL_NUNIQUE_MAX AND name plausibly ordinal  → ORDINAL_CAT
  8. numeric  → CONTINUOUS
  9. fallback  → UNKNOWN
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from dbprofile.checks.base import BaseCheck

# ── Tuning constants ─────────────────────────────────────────────────────────

DEFAULT_LOW_CARDINALITY_THRESHOLD = 15
ORDINAL_NUNIQUE_MAX = 12

# Compiled regexes — substring matched against the *lowercased* column name.
_ID_NAME_RE     = re.compile(r"(^|_)(id|uuid|guid|key|hash)(_|$)")
_COUNT_NAME_RE  = re.compile(r"_(count|cnt|trips|revenue|total|amount|qty|sum|num)$")
_ORDINAL_NAMES  = {
    "month", "year", "day", "dow", "day_of_week", "weekday",
    "hour", "minute", "quarter", "week", "week_of_year",
}


# ── Public API ───────────────────────────────────────────────────────────────


class ColumnKind(str, Enum):
    DATE         = "date"
    BINARY       = "binary"
    ORDINAL_CAT  = "ordinal_cat"
    LOW_CAT      = "low_cat"
    HIGH_CAT     = "high_cat"
    STRING_ID    = "string_id"
    COUNT_METRIC = "count_metric"
    CONTINUOUS   = "continuous"
    UNKNOWN      = "unknown"


@dataclass(frozen=True)
class ColumnFacts:
    """Everything classify_one() needs to decide a kind for one column."""
    name: str
    data_type: str
    n_unique: int | None    # None if unknown (no FrequencyDistribution / Uniqueness result)


def classify_columns(
    columns: list[dict],
    check_results: Iterable,
    *,
    low_cardinality_threshold: int = DEFAULT_LOW_CARDINALITY_THRESHOLD,
    overrides: dict[str, str] | None = None,
) -> dict[str, ColumnKind]:
    """Map every column to a ColumnKind.

    columns        list of {name, data_type, is_nullable} from get_columns()
    check_results  iterable of CheckResult — used only to source cardinality
    overrides      optional {column_name: ColumnKind value} from
                   cfg.notebook.columns.<col>.kind. Wins over auto-classification.
                   Unknown column names are ignored (logged at INFO would be
                   ideal, kept silent here to avoid noise).
    """
    overrides = overrides or {}
    cardinality = _build_cardinality_map(check_results)
    out: dict[str, ColumnKind] = {}
    for col in columns:
        name = col["name"]
        if name in overrides:
            # Pydantic guarantees the value is a valid ColumnKind string.
            out[name] = ColumnKind(overrides[name])
            continue
        facts = ColumnFacts(
            name=name,
            data_type=col.get("data_type", ""),
            n_unique=cardinality.get(name),
        )
        out[name] = classify_one(
            facts, low_cardinality_threshold=low_cardinality_threshold,
        )
    return out


def classify_one(
    facts: ColumnFacts,
    *,
    low_cardinality_threshold: int = DEFAULT_LOW_CARDINALITY_THRESHOLD,
) -> ColumnKind:
    """Apply the priority-ordered rules to one column."""
    name_lc = facts.name.lower()
    dt = facts.data_type or ""

    # 1. Date / temporal
    if BaseCheck.is_temporal(dt):
        return ColumnKind.DATE

    is_numeric = BaseCheck.is_numeric(dt)
    is_string  = BaseCheck.is_string(dt)
    is_bool    = "bool" in dt.lower()

    # 2. Binary
    if is_bool or (is_numeric and facts.n_unique == 2):
        return ColumnKind.BINARY

    # 3. String identifier — check before LOW_CAT/HIGH_CAT so id columns
    # don't get charted as categoricals.
    if is_string and _ID_NAME_RE.search(name_lc):
        return ColumnKind.STRING_ID

    # 4 / 5. String categoricals — split on cardinality.
    if is_string:
        # No cardinality info → bias toward LOW_CAT (cheaper, safer to plot).
        n = facts.n_unique if facts.n_unique is not None else 0
        if n <= low_cardinality_threshold:
            return ColumnKind.LOW_CAT
        return ColumnKind.HIGH_CAT

    # 6. Count / aggregate metric — name-driven.
    if is_numeric and _COUNT_NAME_RE.search(name_lc):
        return ColumnKind.COUNT_METRIC

    # 7. Numeric ordinal (low cardinality + recognisable name).
    if (
        is_numeric
        and facts.n_unique is not None
        and facts.n_unique <= ORDINAL_NUNIQUE_MAX
        and name_lc in _ORDINAL_NAMES
    ):
        return ColumnKind.ORDINAL_CAT

    # 8. Continuous numeric — default for any other numeric.
    if is_numeric:
        return ColumnKind.CONTINUOUS

    # 9. Anything else.
    return ColumnKind.UNKNOWN


# ── Internal helpers ─────────────────────────────────────────────────────────


def _build_cardinality_map(check_results: Iterable) -> dict[str, int]:
    """Walk check_results once and return {column_name: distinct_count}.

    Preference order when multiple checks report a count for the same
    column:
      1. FrequencyDistributionCheck (metric == "distinct_count")
      2. UniquenessCheck (detail.distinct_count)
    """
    primary: dict[str, int] = {}
    fallback: dict[str, int] = {}

    for r in check_results:
        col = getattr(r, "column", None)
        if not col:
            continue
        check = getattr(r, "check_name", "")
        detail = getattr(r, "detail", {}) or {}

        if check == "frequency_distribution":
            # Stored either as the result's value (when high-cardinality) or
            # in detail (otherwise).
            val = detail.get("distinct_count")
            if val is None and getattr(r, "metric", "") == "distinct_count":
                val = r.value
            if val is not None:
                primary[col] = int(val)

        elif check == "uniqueness":
            val = detail.get("distinct_count")
            if val is not None:
                fallback[col] = int(val)

    return {**fallback, **primary}   # primary overrides fallback
