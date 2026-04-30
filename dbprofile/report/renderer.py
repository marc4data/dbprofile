"""Jinja2 renderer — takes CheckResult list, produces a single self-contained HTML file."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader

from dbprofile.checks.base import CheckResult
from dbprofile.config import ProfileConfig

# ---------------------------------------------------------------------------
# Canonical check order (spec section B)
# ---------------------------------------------------------------------------

CANONICAL_ORDER = [
    "schema_audit",
    "row_count",
    "null_density",
    "uniqueness",
    "numeric_distribution",
    "frequency_distribution",
    "temporal_consistency",
    "format_validation",
]

CHECK_LABELS = {
    "schema_audit":           "Schema Audit",
    "row_count":              "Row Count",
    "null_density":           "Null Density",
    "uniqueness":             "Uniqueness",
    "numeric_distribution":   "Numeric Distribution",
    "frequency_distribution": "Frequency Distribution",
    "temporal_consistency":   "Temporal Consistency",
    "format_validation":      "Format Validation",
}

CHECK_SHORT = {
    "schema_audit":           "SA",
    "row_count":              "RC",
    "null_density":           "ND",
    "uniqueness":             "UN",
    "numeric_distribution":   "NM",
    "frequency_distribution": "FR",
    "temporal_consistency":   "TC",
    "format_validation":      "FV",
}

NAV_LABELS = {
    "schema_audit":           "SCHEMA",
    "row_count":              "ROW CT",
    "null_density":           "NULL",
    "uniqueness":             "UNIQUE",
    "numeric_distribution":   "NUM DIST",
    "frequency_distribution": "FREQ DIST",
    "temporal_consistency":   "TEMP",
    "format_validation":      "FRMT VALID",
}

# Check weights for quality scoring (must sum to 1.0)
# Higher weight = greater influence on the 0-100 quality score.
CHECK_WEIGHTS: dict[str, float] = {
    "schema_audit":           0.15,
    "row_count":              0.15,
    "null_density":           0.25,
    "uniqueness":             0.20,
    "numeric_distribution":   0.10,
    "frequency_distribution": 0.05,
    "temporal_consistency":   0.05,
    "format_validation":      0.05,
}

# Points awarded per severity level (na = excluded from calculation)
_SEV_POINTS: dict[str, int] = {"ok": 100, "info": 100, "warn": 50, "critical": 0}


def _compute_quality_score(checks_dicts: dict[str, list[dict]]) -> dict:
    """Return a 0-100 quality score and diagnostics for one table.

    Algorithm:
      - Each check result is scored: ok/info=100, warn=50, critical=0; na excluded.
      - Each check's score = mean of its result scores.
      - Table score = weighted average of check scores (only checks that ran).
      - Coverage = checks that produced results / total canonical checks.
    """
    check_scores: dict[str, float] = {}
    for cn in CANONICAL_ORDER:
        results = checks_dicts.get(cn, [])
        if not results:
            continue
        points = [
            _SEV_POINTS[r["severity"]]
            for r in results
            if r.get("severity") in _SEV_POINTS
        ]
        if points:
            check_scores[cn] = sum(points) / len(points)

    if not check_scores:
        return {"score": 100, "by_check": {}, "coverage_pct": 0.0, "checks_run": 0}

    total_weight = sum(CHECK_WEIGHTS.get(cn, 0) for cn in check_scores)
    if total_weight == 0:
        score = 100
    else:
        weighted_sum = sum(
            check_scores[cn] * CHECK_WEIGHTS.get(cn, 0) for cn in check_scores
        )
        score = round(weighted_sum / total_weight)

    return {
        "score": max(0, min(100, score)),
        "by_check": {cn: round(s, 1) for cn, s in check_scores.items()},
        "coverage_pct": round(len(check_scores) / len(CANONICAL_ORDER) * 100, 1),
        "checks_run": len(check_scores),
    }


def _score_color(score: int) -> str:
    """Return a CSS hex color for a 0-100 quality score."""
    if score >= 90:
        return "#a6e3a1"   # green
    if score >= 75:
        return "#f9e2af"   # yellow
    if score >= 60:
        return "#fab387"   # orange
    return "#f38ba8"       # red


# ---------------------------------------------------------------------------
# Type shortening
# ---------------------------------------------------------------------------

def _short_type(data_type: str) -> str:
    dt = (data_type or "").upper()
    if any(k in dt for k in ("BIGINT", "SMALLINT", "TINYINT", "INT64")):
        return "INT"
    if "INT" in dt:
        return "INT"
    if any(k in dt for k in ("FLOAT", "DOUBLE", "REAL", "FLOAT64")):
        return "FLOAT"
    if any(k in dt for k in ("DECIMAL", "NUMERIC", "NUMBER")):
        return "DEC"
    if any(k in dt for k in ("VARCHAR", "STRING", "TEXT", "CHAR", "NVAR")):
        return "STR"
    if "BOOL" in dt:
        return "BOOL"
    if "TIMESTAMP" in dt or "DATETIME" in dt:
        return "TS"
    if "DATE" in dt:
        return "DATE"
    if "TIME" in dt:
        return "TIME"
    return dt[:5] if dt else "?"


# ---------------------------------------------------------------------------
# Inline SVG sparklines
# ---------------------------------------------------------------------------

def _make_numeric_sparkline(
    p25: float, p50: float, p75: float,
    p_min: float, p_max: float,
    w: int = 44, h: int = 14,
) -> str:
    """Box-plot style sparkline: whisker line + IQR box + median tick."""
    rng = p_max - p_min
    mid = h // 2
    if rng == 0:
        return (
            f'<svg width="{w}" height="{h}" style="vertical-align:middle">'
            f'<line x1="0" y1="{mid}" x2="{w}" y2="{mid}" '
            f'stroke="#4c5166" stroke-width="1"/></svg>'
        )

    def sx(v: float) -> float:
        return round(max(0.0, min(float(w), (v - p_min) / rng * w)), 1)

    x25, x50, x75 = sx(p25), sx(p50), sx(p75)
    box_w = max(x75 - x25, 2.0)

    return (
        f'<svg width="{w}" height="{h}" style="vertical-align:middle">'
        f'<line x1="0" y1="{mid}" x2="{w}" y2="{mid}" stroke="#4c5166" stroke-width="1"/>'
        f'<rect x="{x25}" y="3" width="{box_w}" height="{h - 6}" '
        f'fill="#89dceb" opacity="0.65" rx="1"/>'
        f'<line x1="{x50}" y1="1" x2="{x50}" y2="{h - 1}" '
        f'stroke="#89dceb" stroke-width="2"/>'
        f'</svg>'
    )


def _make_cardinality_sparkline(
    distinct_count: int, total: int,
    w: int = 44, h: int = 14,
) -> str:
    """Outlined box (STR/non-numeric columns) — gray border, white fill."""
    return (
        f'<svg width="{w}" height="{h}" style="vertical-align:middle">'
        f'<rect x="0.5" y="3.5" width="{w - 1}" height="{h - 8}" '
        f'fill="white" stroke="#9ca3af" stroke-width="1" rx="1"/>'
        f'</svg>'
    )


# ---------------------------------------------------------------------------
# Column classification helpers
# ---------------------------------------------------------------------------

_BINARY_STRINGS = {"0", "1", "true", "false", "y", "n", "yes", "no", "t", "f"}


def _is_binary(data_type: str, freq_series: list[dict]) -> bool:
    if "bool" in data_type.lower():
        return True
    if len(freq_series) == 2:
        vals = {str(item.get("value", "")).strip().lower() for item in freq_series}
        return vals.issubset(_BINARY_STRINGS)
    return False


def _is_identifier(distinct_count: int, total: int) -> bool:
    return total > 0 and (distinct_count / total) >= 0.95


# ---------------------------------------------------------------------------
# Per-column profiles (aggregate all check data into one dict per column)
# ---------------------------------------------------------------------------

def _build_column_profiles(
    checks: dict[str, list[dict]],
    schema_cols: list[dict],
    row_count: int,
) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}

    for col in schema_cols:
        name = col.get("name", "")
        dt = col.get("data_type", "")
        profiles[name] = {
            "ordinal": col.get("ordinal_position", 0),
            "data_type": dt,
            "type_short": _short_type(dt),
            "is_nullable": bool(col.get("is_nullable", True)),
            "null_pct": 0.0,
            "distinct_count": 0,
            "total": row_count,
            "is_identifier": False,
            "is_binary": False,
            "sparkline_svg": "",
            "check_severities": {},
        }

    # Null density
    for r in checks.get("null_density", []):
        col = r.get("column")
        if col and col in profiles:
            val = r.get("value")
            try:
                profiles[col]["null_pct"] = float(val) if val is not None else 0.0
            except (TypeError, ValueError):
                pass
            profiles[col]["check_severities"]["null_density"] = r.get("severity", "ok")

    # Uniqueness
    for r in checks.get("uniqueness", []):
        col = r.get("column")
        if not col or col not in profiles:
            continue
        detail = r.get("detail") or {}
        dc = int(detail.get("distinct_count") or 0)
        total = int(detail.get("total") or row_count or 1)
        profiles[col]["distinct_count"] = dc
        profiles[col]["total"] = total
        profiles[col]["is_identifier"] = _is_identifier(dc, total)
        profiles[col]["check_severities"]["uniqueness"] = r.get("severity", "ok")

    # Numeric distribution → numeric sparkline
    for r in checks.get("numeric_distribution", []):
        col = r.get("column")
        if not col or col not in profiles:
            continue
        d = r.get("detail", {}) if isinstance(r.get("detail"), dict) else {}
        profiles[col]["check_severities"]["numeric_distribution"] = r.get("severity", "ok")
        try:
            profiles[col]["sparkline_svg"] = _make_numeric_sparkline(
                float(d.get("p25") or 0), float(d.get("p50") or 0),
                float(d.get("p75") or 0), float(d.get("min") or 0),
                float(d.get("max") or 0),
            )
        except (TypeError, ValueError):
            pass

    # Frequency distribution → binary detection + cardinality sparkline
    for r in checks.get("frequency_distribution", []):
        col = r.get("column")
        if not col or col not in profiles:
            continue
        d = r.get("detail") or {}
        series = d.get("series") or []
        dt = profiles[col]["data_type"]
        profiles[col]["is_binary"] = _is_binary(dt, series)
        profiles[col]["check_severities"]["frequency_distribution"] = r.get("severity", "ok")
        if not profiles[col]["sparkline_svg"]:
            dc = int(d.get("distinct_count") or 0)
            total = profiles[col]["total"] or 1
            profiles[col]["sparkline_svg"] = _make_cardinality_sparkline(dc, total)

    # Remaining checks — capture severity only
    for cn in ("schema_audit", "temporal_consistency", "format_validation", "row_count"):
        for r in checks.get(cn, []):
            col = r.get("column")
            if col and col in profiles:
                sev = r.get("severity", "ok")
                existing = profiles[col]["check_severities"].get(cn, "ok")
                if sev == "critical" or (sev == "warn" and existing == "ok"):
                    profiles[col]["check_severities"][cn] = sev

    # Fill missing sparklines with cardinality bar
    for prof in profiles.values():
        if not prof["sparkline_svg"] and prof["total"] > 0:
            prof["sparkline_svg"] = _make_cardinality_sparkline(
                prof["distinct_count"], prof["total"]
            )

    return profiles


# ---------------------------------------------------------------------------
# EDA classification — groups columns by data-type strategy
# ---------------------------------------------------------------------------

_EDA_SUBTYPES = {
    ("A", "1"): "Low-cardinality",
    ("A", "2"): "High-cardinality",
    ("B", "1"): "Dates",
    ("B", "2"): "Datetimes",
    ("B", "3"): "Timestamps",
    ("C", "1"): "Indicators",
    ("C", "2"): "Integers",
    ("C", "3"): "Decimals",
    ("C", "4"): "Scientific Notation",
}


def _eda_classify(
    data_type: str, type_short: str, distinct_count: int, total: int
) -> tuple[str, str, str]:
    """Return (type_header, subtype_num, subtype_label) for one column.

    Type headers:  A = Strings, B = Dates/Datetimes, C = Numeric
    """
    dt = (data_type or "").upper()

    # ── B — Temporal ──
    if "TIMESTAMP" in dt or type_short == "TS":
        return ("B", "3", "Timestamps")
    if "DATETIME" in dt:
        return ("B", "2", "Datetimes")
    if type_short == "DATE" or dt == "DATE":
        return ("B", "1", "Dates")
    if type_short == "TIME":
        return ("B", "2", "Datetimes")

    # ── C — Numeric ──
    if type_short == "BOOL":
        return ("C", "1", "Indicators")
    if type_short == "INT":
        # Very low cardinality integers are flags/indicators
        if total > 0 and distinct_count <= 5:
            return ("C", "1", "Indicators")
        return ("C", "2", "Integers")
    if type_short == "DEC":
        return ("C", "3", "Decimals")
    if type_short == "FLOAT":
        return ("C", "4", "Scientific Notation")

    # ── A — Strings (and fallback) ──
    if total > 0 and distinct_count <= 200:
        return ("A", "1", "Low-cardinality")
    return ("A", "2", "High-cardinality")


# ---------------------------------------------------------------------------
# Heatmap scorecard (sorted by ordinal position)
# ---------------------------------------------------------------------------

def _build_scorecard(column_profiles: dict[str, dict]) -> list[dict]:
    # First pass: classify every column
    classified: list[tuple[str, dict, str, str, str]] = []
    for col_name, prof in sorted(
        column_profiles.items(), key=lambda x: (x[1].get("ordinal") or 0)
    ):
        grp, sub, label = _eda_classify(
            prof["data_type"], prof["type_short"],
            prof["distinct_count"], prof["total"],
        )
        classified.append((col_name, prof, grp, sub, label))

    # Second pass: assign sequence numbers within each (grp, sub)
    from collections import Counter
    subtype_seq: Counter = Counter()
    rows = []
    for col_name, prof, grp, sub, label in classified:
        subtype_seq[(grp, sub)] += 1
        seq = subtype_seq[(grp, sub)]
        eda_sort = f"{grp}{sub}{seq:03d}"

        row: dict[str, Any] = {
            "column": col_name,
            "ordinal": prof["ordinal"],
            "type_short": prof["type_short"],
            "data_type": prof["data_type"],
            "is_nullable": prof.get("is_nullable", True),
            "sparkline_svg": prof["sparkline_svg"],
            "null_pct": prof["null_pct"],
            "distinct_count": prof["distinct_count"],
            "total": prof["total"],
            "is_identifier": prof["is_identifier"],
            # EDA classification
            "eda_grp": grp,
            "eda_sub": sub,
            "eda_label": label,
            "eda_seq": f"{seq:03d}",
            "eda_sort": eda_sort,
        }
        for cn in CANONICAL_ORDER:
            row[cn] = prof["check_severities"].get(cn, "na")
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Severity helpers
# ---------------------------------------------------------------------------

def _worst(results: list[dict]) -> str:
    worst = "ok"
    for r in results:
        s = r.get("severity", "ok")
        if s == "critical":
            return "critical"
        if s == "warn":
            worst = "warn"
        elif s == "info" and worst == "ok":
            worst = "info"
    return worst if results else "na"


def _severity_counts(results: list[CheckResult]) -> dict[str, int]:
    counts: dict[str, int] = {"critical": 0, "warn": 0, "ok": 0, "info": 0}
    for r in results:
        counts[r.severity] = counts.get(r.severity, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Rule-based per-table summary (fallback when LLM is disabled)
# ---------------------------------------------------------------------------

def _rule_based_summary(checks: dict[str, list[dict]]) -> str:
    parts = []
    for cn in CANONICAL_ORDER:
        crits = [
            r for r in checks.get(cn, [])
            if r.get("severity") == "critical" and r.get("column")
        ]
        if crits:
            label = CHECK_LABELS.get(cn, cn)
            cols = [r["column"] for r in crits[:3]]
            suffix = "…" if len(crits) > 3 else ""
            parts.append(f"{len(crits)} critical in {label} ({', '.join(cols)}{suffix})")
    if not parts:
        total_warn = sum(
            1 for rs in checks.values() for r in rs if r.get("severity") == "warn"
        )
        if total_warn:
            return f"{total_warn} warning(s) — no critical issues."
        return "No significant data quality issues detected."
    return " · ".join(parts[:3])


# ---------------------------------------------------------------------------
# Top actions (rule-based, spec section F.1)
# ---------------------------------------------------------------------------

def _build_top_actions(all_results: list[CheckResult]) -> list[dict]:
    actions: list[dict] = []

    # 1. Temporal gaps — highest priority
    for r in all_results:
        if (r.check_name == "temporal_consistency"
                and r.metric == "gap_day_count"
                and isinstance(r.value, (int, float))
                and r.value > 0):
            min_d = r.detail.get("min_date", "")
            max_d = r.detail.get("max_date", "")
            actions.append({
                "text": (f"{r.table} · {r.column}: {int(r.value)} gap day(s) "
                         f"({min_d} → {max_d}). Investigate missing loads."),
                "anchor": f"{r.table}-temporal_consistency",
                "severity": "critical",
            })
            if len(actions) >= 2:
                break

    # 2. Critical null density — grouped by table
    null_crit: dict[str, list[str]] = defaultdict(list)
    for r in all_results:
        if r.check_name == "null_density" and r.severity == "critical" and r.column:
            null_crit[r.table].append(r.column)
    for table, cols in list(null_crit.items())[:2]:
        col_str = ", ".join(cols[:4]) + ("…" if len(cols) > 4 else "")
        actions.append({
            "text": f"{table}: {len(cols)} column(s) exceed null threshold — {col_str}",
            "anchor": f"{table}-null_density",
            "severity": "critical",
        })

    # 3. Format violations
    fmt_crit = [r for r in all_results if r.check_name == "format_validation" and r.severity == "critical"]
    if fmt_crit:
        tables = list(dict.fromkeys(r.table for r in fmt_crit))
        actions.append({
            "text": (f"{len(fmt_crit)} format violation(s) across "
                     f"{', '.join(tables[:2])} — review data against expected patterns."),
            "anchor": f"{fmt_crit[0].table}-format_validation",
            "severity": "critical",
        })

    # 4. Numeric outliers
    out_crit = [r for r in all_results if r.check_name == "numeric_distribution" and r.severity == "critical"]
    if out_crit:
        col_strs = [f"{r.table}.{r.column}" for r in out_crit[:3]]
        actions.append({
            "text": (f"Outlier values in {', '.join(col_strs)} — "
                     "review for data entry errors or valid extremes."),
            "anchor": f"{out_crit[0].table}-numeric_distribution",
            "severity": "critical",
        })

    # 5. Volume anomalies
    for r in all_results:
        if (r.check_name == "row_count"
                and r.metric == "daily_distribution"
                and r.severity in ("warn", "critical")):
            actions.append({
                "text": f"{r.table}: daily volume anomaly detected — possible duplicate loads or data gaps.",
                "anchor": f"{r.table}-row_count",
                "severity": r.severity,
            })
            break

    return actions[:5]


# ---------------------------------------------------------------------------
# Grouped issues for executive summary (spec section F.2)
# ---------------------------------------------------------------------------

def _build_grouped_issues(all_results: list[CheckResult]) -> list[tuple[str, list[dict]]]:
    """Issues grouped in canonical order, warn/critical only."""
    by_check: dict[str, list[dict]] = {cn: [] for cn in CANONICAL_ORDER}
    for r in all_results:
        if r.severity not in ("critical", "warn") or r.check_name not in by_check:
            continue
        by_check[r.check_name].append({
            "table": r.table,
            "column": r.column or "—",
            "severity": r.severity,
            "metric": r.metric,
            "value": r.value,
            "anchor": f"{r.table}-{r.check_name}",
        })
    return [(cn, issues) for cn, issues in by_check.items() if issues]


# ---------------------------------------------------------------------------
# Group raw results
# ---------------------------------------------------------------------------

def _group_results(results: list[CheckResult]) -> dict[str, dict[str, list[CheckResult]]]:
    grouped: dict[str, dict[str, list[CheckResult]]] = defaultdict(lambda: defaultdict(list))
    for r in results:
        grouped[r.table][r.check_name].append(r)
    return grouped


# ---------------------------------------------------------------------------
# Main render function
# ---------------------------------------------------------------------------

def _build_report_context(
    results: list[CheckResult],
    config: "ProfileConfig",
    run_at: datetime,
) -> dict:
    """Build the full template/export context from profiling results.

    Separated from HTML rendering so the Excel exporter and any future
    exporters can reuse the same processing without re-running queries.
    """
    grouped = _group_results(results)
    unique_tables = sorted(grouped.keys())
    unique_columns = len({r.column for r in results if r.column})

    tables_ctx: dict[str, dict] = {}
    for table, checks_by_name in grouped.items():
        # Convert CheckResult objects to plain dicts for the template
        checks_dicts: dict[str, list[dict]] = {
            cn: [r.to_dict() for r in rs]
            for cn, rs in checks_by_name.items()
        }

        # Schema columns from schema_audit detail
        schema_cols: list[dict] = []
        for r in checks_dicts.get("schema_audit", []):
            schema_cols = r.get("detail", {}).get("columns", [])
            break

        # Row count scalar
        row_count = 0
        for r in checks_dicts.get("row_count", []):
            if r.get("metric") == "row_count":
                row_count = int(r.get("value") or 0)

        col_profiles = _build_column_profiles(checks_dicts, schema_cols, row_count)
        scorecard = _build_scorecard(col_profiles)

        all_table = [r for rs in checks_by_name.values() for r in rs]
        sev_counts = _severity_counts(all_table)

        check_worst = {
            cn: (_worst(checks_dicts[cn]) if cn in checks_dicts else "na")
            for cn in CANONICAL_ORDER
        }

        quick_links = [
            (cn, CHECK_LABELS[cn], check_worst[cn])
            for cn in CANONICAL_ORDER
            if cn in checks_dicts
        ]

        quality = _compute_quality_score(checks_dicts)
        rows_sampled = round(row_count * config.checks.sample_rate)

        sample_rows_data = None
        for r in checks_dicts.get("sample_rows", []):
            d = r.get("detail") or {}
            if r.get("metric") == "sample_rows" and isinstance(d.get("rows"), list):
                sample_rows_data = d
                break

        check_scoreboard = []
        for cn in CANONICAL_ORDER:
            cn_results = checks_dicts.get(cn, [])
            check_scoreboard.append({
                "name": CHECK_LABELS[cn],
                "check": cn,
                "critical": sum(1 for r in cn_results if r.get("severity") == "critical"),
                "warn": sum(1 for r in cn_results if r.get("severity") == "warn"),
            })

        tables_ctx[table] = {
            "name": table,
            "row_count": row_count,
            "rows_sampled": rows_sampled,
            "col_count": len(schema_cols) or len(col_profiles),
            "severity_counts": sev_counts,
            "summary_text": _rule_based_summary(checks_dicts),
            "column_profiles": col_profiles,
            "scorecard": scorecard,
            "check_worst": check_worst,
            "quick_links": quick_links,
            "check_scoreboard": check_scoreboard,
            "quality_score": quality["score"],
            "quality_by_check": quality["by_check"],
            "coverage_pct": quality["coverage_pct"],
            "checks": {cn: checks_dicts.get(cn, []) for cn in CANONICAL_ORDER},
            "sample_rows": sample_rows_data,
        }

    overall = _severity_counts(results)

    total_rows_all = sum(tables_ctx[t]["row_count"] for t in unique_tables) or 1
    overall_quality_score = max(0, min(100, round(
        sum(tables_ctx[t]["quality_score"] * tables_ctx[t]["row_count"] / total_rows_all
            for t in unique_tables)
    )))
    total_rows_sampled = round(total_rows_all * config.checks.sample_rate)
    overall_coverage_pct = round(
        sum(tables_ctx[t]["coverage_pct"] for t in unique_tables) / max(len(unique_tables), 1),
        1,
    )

    conn = config.connection
    scope = config.scope
    account_display = conn.account or conn.project or "—"
    database_display = scope.dataset or scope.database or (scope.schemas[0] if scope.schemas else "—")
    schema_display = ", ".join(scope.schemas) if scope.schemas else (scope.dataset or "—")
    role_display = conn.role or "—"

    return {
        "run_at": run_at.strftime("%Y-%m-%d %H:%M UTC"),
        "project": scope.project or conn.project or "dbprofile",
        "dataset": scope.dataset or scope.database or (scope.schemas[0] if scope.schemas else ""),
        "sample_rate_pct": f"{config.checks.sample_rate * 100:.0f}%",
        "dialect_display": conn.dialect.capitalize(),
        "account_display": account_display,
        "database_display": database_display,
        "schema_display": schema_display,
        "role_display": role_display,
        "tables": unique_tables,
        "tables_ctx": tables_ctx,
        "canonical_order": CANONICAL_ORDER,
        "check_labels": CHECK_LABELS,
        "check_short": CHECK_SHORT,
        "nav_labels": NAV_LABELS,
        "total_tables": len(unique_tables),
        "total_columns": unique_columns,
        "total_rows": total_rows_all,
        "total_rows_sampled": total_rows_sampled,
        "overall_quality_score": overall_quality_score,
        "overall_coverage_pct": overall_coverage_pct,
        "score_color": _score_color,
        "critical_count": overall.get("critical", 0),
        "warn_count": overall.get("warn", 0),
        "top_actions": _build_top_actions(results),
        "include_charts": "charts" in config.report.include,
        "include_tables": "tables" in config.report.include,
        "preview_rows": config.report.preview_rows,
        # Config panels — scope, checks, report settings for the header
        "cfg_scope": {
            "database": scope.database or scope.dataset or "—",
            "schemas": ", ".join(scope.schemas) if scope.schemas else "all (auto-discover)",
            "tables": ", ".join(scope.tables) if scope.tables else "all",
            "exclude_tables": ", ".join(scope.exclude_tables) if scope.exclude_tables else "none",
        },
        "cfg_checks": {
            "enabled": ", ".join(config.checks.enabled),
            "disabled": ", ".join(config.checks.disabled) if config.checks.disabled else "none",
            "sample_rate": f"{config.checks.sample_rate * 100:.0f}%",
            "sample_method": config.checks.sample_method,
        },
        "cfg_thresholds": {
            "null_pct_warn": config.report.thresholds.null_pct_warn,
            "null_pct_critical": config.report.thresholds.null_pct_critical,
            "duplicate_pct_warn": config.report.thresholds.duplicate_pct_warn,
            "duplicate_pct_critical": config.report.thresholds.duplicate_pct_critical,
            "outlier_pct_warn": config.report.thresholds.outlier_pct_warn,
            "outlier_pct_critical": config.report.thresholds.outlier_pct_critical,
            "frequency_cardinality_limit": config.report.thresholds.frequency_cardinality_limit,
        },
    }


def load_results_from_json(json_path: str | Path) -> list[CheckResult]:
    """Reconstruct CheckResult objects from a --export-json file.

    Lets you regenerate the HTML report or Excel workbook from a saved
    JSON snapshot without re-running any database queries.
    """
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    results = []
    for d in data:
        run_at_raw = d.get("run_at")
        try:
            run_at_val = datetime.fromisoformat(run_at_raw) if run_at_raw else datetime.utcnow()
        except ValueError:
            run_at_val = datetime.utcnow()
        results.append(CheckResult(
            table=d["table"],
            schema=d["schema"],
            column=d.get("column"),
            check_name=d["check_name"],
            metric=d["metric"],
            value=d["value"],
            severity=d["severity"],
            detail=d.get("detail") or {},
            sql=d.get("sql") or "",
            run_at=run_at_val,
        ))
    return results


def render_report(
    results: list[CheckResult],
    config: "ProfileConfig",
    output_path: str | Path,
    run_at: datetime | None = None,
) -> tuple[Path, dict]:
    """Render the HTML report and return (path_written, report_context)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if run_at is None:
        run_at = datetime.utcnow()

    template_dir = Path(__file__).parent
    env = Environment(loader=FileSystemLoader(str(template_dir)), autoescape=False)
    env.filters["tojson"] = lambda v: json.dumps(v, default=str)

    context = _build_report_context(results, config, run_at)

    template = env.get_template("template.html.j2")
    html = template.render(**context)
    output_path.write_text(html, encoding="utf-8")
    return output_path, context
