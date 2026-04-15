"""Check 5 — Numeric distribution & outlier detection (numeric columns only)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

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

            # Basic stats query
            stats_sql = f"""
SELECT
  AVG(CAST({col_name} AS DOUBLE)) AS mean,
  MIN(CAST({col_name} AS DOUBLE)) AS min_val,
  MAX(CAST({col_name} AS DOUBLE)) AS max_val,
  STDDEV(CAST({col_name} AS DOUBLE)) AS stddev,
  COUNT(*) AS total,
  SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) AS null_count
FROM {table_ref} {sample}
""".strip()

            # Percentile query (dialect-aware)
            pct_sql = connector.percentile_sql(
                f"CAST({col_name} AS DOUBLE)",
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
  AND (CAST({col_name} AS DOUBLE) < {lower_fence} OR CAST({col_name} AS DOUBLE) > {upper_fence})
""".strip()

                outlier_rows = connector.execute(outlier_sql)
                outlier_count = int((outlier_rows[0].get("outlier_count") or 0) if outlier_rows else 0)
                outlier_pct = round(100.0 * outlier_count / non_null, 4) if non_null else 0.0

                severity = self.severity_from_pct(
                    outlier_pct,
                    thresholds.outlier_pct_warn,
                    thresholds.outlier_pct_critical,
                )

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="outlier_pct",
                        value=outlier_pct,
                        severity=severity,
                        detail={
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
                        },
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
