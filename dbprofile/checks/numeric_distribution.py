"""Check 5 — Numeric distribution & outlier detection (numeric columns only)."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult


def _nice_bounds(low: float, high: float) -> tuple[float, float]:
    """Round fence boundaries outward to 'nice' numbers for clean axis labels."""
    r = high - low
    if r <= 0:
        return low, high
    magnitude = 10 ** math.floor(math.log10(r)) if r > 0 else 1
    step = magnitude / 2  # round to half-magnitude intervals
    return math.floor(low / step) * step, math.ceil(high / step) * step

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector

_PERCENTILES = [0.25, 0.50, 0.75, 0.95, 0.99]


class NumericDistributionCheck(BaseCheck):
    name = "numeric_distribution"

    def run(
        self,
        table: str,
        schema: str,
        columns: list[dict[str, Any]],
        connector: "BaseConnector",
        config: "ProfileConfig",
    ) -> list[CheckResult]:
        results = []
        table_ref = connector.qualified_table(table, schema, config.scope.project)
        sample = connector.sample_clause(config.checks.sample_rate)
        thresholds = config.report.thresholds

        numeric_cols = [c for c in columns if self.is_numeric(c["data_type"])]

        for col in numeric_cols:
            col_name = col["name"]

            # Basic stats query — no explicit CAST; column is already numeric
            # (filtered by is_numeric above). Most dialects handle integer→float
            # promotion automatically in AVG/STDDEV.
            stats_sql = f"""
SELECT
  AVG({col_name}) AS mean,
  MIN({col_name}) AS min_val,
  MAX({col_name}) AS max_val,
  STDDEV({col_name}) AS stddev,
  COUNT(*) AS total,
  SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) AS null_count
FROM {table_ref} {sample}
""".strip()

            # Percentile query (dialect-aware) — pass column name directly
            pct_sql = connector.percentile_sql(
                col_name,
                f"{table_ref} {sample}".strip(),
                _PERCENTILES,
            )

            try:
                stats_rows = connector.execute(stats_sql)
                pct_rows = connector.execute(pct_sql)

                if not stats_rows:
                    continue

                s = stats_rows[0]
                p = pct_rows[0] if pct_rows else {}

                mean = float(s.get("mean") or 0)
                min_val = float(s.get("min_val") or 0)
                max_val = float(s.get("max_val") or 0)
                stddev = float(s.get("stddev") or 0)
                total = int(s.get("total") or 0)
                non_null = total - int(s.get("null_count") or 0)

                p25 = float(p.get("p25") or 0)
                p50 = float(p.get("p50") or 0)
                p75 = float(p.get("p75") or 0)
                p95 = float(p.get("p95") or 0)
                p99 = float(p.get("p99") or 0)

                # IQR outlier detection: values outside 1.5 * IQR
                iqr = p75 - p25
                lower_fence = p25 - 1.5 * iqr
                upper_fence = p75 + 1.5 * iqr

                outlier_sql = f"""
SELECT COUNT(*) AS outlier_count
FROM {table_ref} {sample}
WHERE {col_name} IS NOT NULL
  AND ({col_name} < {lower_fence} OR {col_name} > {upper_fence})
""".strip()

                outlier_rows = connector.execute(outlier_sql)
                outlier_count = int((outlier_rows[0].get("outlier_count") or 0) if outlier_rows else 0)
                outlier_pct = round(100.0 * outlier_count / non_null, 4) if non_null else 0.0

                severity = self.severity_from_pct(
                    outlier_pct,
                    thresholds.outlier_pct_warn,
                    thresholds.outlier_pct_critical,
                )

                # ── Histogram within IQR fence (up to 20 bins) ──────────────
                histogram = None
                hist_range = upper_fence - lower_fence
                if hist_range > 0 and non_null > 0:
                    # Use IQR fence directly so bins cover the full ±1.5×IQR range
                    hist_low = lower_fence
                    hist_high = upper_fence
                    num_bins = 20
                    bin_width = hist_range / num_bins

                    hist_sql = f"""
SELECT
  LEAST(
    CAST(FLOOR(({col_name} - ({hist_low})) / ({bin_width})) AS INTEGER),
    {num_bins - 1}
  ) AS bin_num,
  COUNT(*) AS cnt
FROM {table_ref} {sample}
WHERE {col_name} IS NOT NULL
  AND {col_name} >= {hist_low}
  AND {col_name} <= {hist_high}
GROUP BY 1
ORDER BY 1
""".strip()
                    try:
                        hist_rows = connector.execute(hist_sql)
                        bin_counts = {
                            int(r["bin_num"]): int(r["cnt"])
                            for r in hist_rows
                            if r.get("bin_num") is not None and int(r["bin_num"]) >= 0
                        }
                        total_in_fence = sum(bin_counts.values())
                        bins = []
                        cumulative = 0.0
                        for i in range(num_bins):
                            count = bin_counts.get(i, 0)
                            pct = round(100.0 * count / total_in_fence, 2) if total_in_fence > 0 else 0.0
                            cumulative = round(cumulative + pct, 2)
                            bl = hist_low + i * bin_width
                            bh = hist_low + (i + 1) * bin_width
                            bins.append({
                                "bin": i,
                                "low": round(bl, 4),
                                "high": round(bh, 4),
                                "center": round((bl + bh) / 2, 4),
                                "count": count,
                                "pct": pct,
                                "cumulative_pct": cumulative,
                            })
                        histogram = {
                            "bins": bins,
                            "num_bins": num_bins,
                            "bin_width": round(bin_width, 6),
                            "hist_low": round(hist_low, 4),
                            "hist_high": round(hist_high, 4),
                            "total_in_fence": total_in_fence,
                        }
                    except Exception:
                        pass  # histogram is supplementary — fail silently

                detail: dict = {
                    "mean": round(mean, 4),
                    "min": round(min_val, 4),
                    "max": round(max_val, 4),
                    "stddev": round(stddev, 4),
                    "p25": round(p25, 4),
                    "p50": round(p50, 4),
                    "p75": round(p75, 4),
                    "p95": round(p95, 4),
                    "p99": round(p99, 4),
                    "iqr": round(iqr, 4),
                    "lower_fence": round(lower_fence, 4),
                    "upper_fence": round(upper_fence, 4),
                    "outlier_count": outlier_count,
                    "outlier_pct": outlier_pct,
                    "total": total,
                }
                if histogram is not None:
                    detail["histogram"] = histogram

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="outlier_pct",
                        value=outlier_pct,
                        severity=severity,
                        detail=detail,
                        sql=stats_sql + "\n---\n" + pct_sql,
                    )
                )
            except Exception as exc:
                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="outlier_pct",
                        value="error",
                        severity="warn",
                        detail={"error": str(exc)},
                        sql=stats_sql,
                    )
                )

        return results
