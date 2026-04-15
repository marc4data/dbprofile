"""Check 7 — Temporal consistency & load watermark (date/timestamp columns only)."""

from __future__ import annotations

import statistics
from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


class TemporalConsistencyCheck(BaseCheck):
    name = "temporal_consistency"

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

        temporal_cols = [c for c in columns if self.is_temporal(c["data_type"])]

        for col in temporal_cols:
            col_name = col["name"]

            # Get date range first
            range_sql = f"""
SELECT
  MIN({col_name}) AS min_date,
  MAX({col_name}) AS max_date,
  COUNT(*) AS total
FROM {table_ref} {sample}
WHERE {col_name} IS NOT NULL
""".strip()

            try:
                range_rows = connector.execute(range_sql)
                if not range_rows or range_rows[0].get("min_date") is None:
                    continue

                rr = range_rows[0]
                min_date = str(rr["min_date"])[:10]  # truncate to YYYY-MM-DD
                max_date = str(rr["max_date"])[:10]
                total = int(rr["total"] or 0)

                # Daily counts with gap-filling via dialect-specific spine
                spine_sql = connector.generate_date_spine(
                    min_date, max_date, col_name, f"{table_ref} {sample}".strip()
                )

                try:
                    spine_rows = connector.execute(spine_sql)
                    series = [
                        {"date": str(r["d"])[:10], "count": int(r["n"] or 0)}
                        for r in spine_rows
                    ]
                except Exception:
                    # Fall back to simple daily counts without gap-filling
                    trunc = connector.date_trunc_day(col_name)
                    fallback_sql = f"""
SELECT {trunc} AS d, COUNT(*) AS n
FROM {table_ref} {sample}
WHERE {col_name} IS NOT NULL
GROUP BY 1 ORDER BY 1
""".strip()
                    fb_rows = connector.execute(fallback_sql)
                    series = [
                        {"date": str(r["d"])[:10], "count": int(r["n"] or 0)}
                        for r in fb_rows
                    ]

                # Gap detection: days with zero counts
                gap_days = [s for s in series if s["count"] == 0]

                # Anomaly detection: trailing-30-day average + 2 stddev threshold
                counts = [s["count"] for s in series]
                anomaly_days = []
                window = 30
                if len(counts) >= window + 1:
                    for i in range(window, len(series)):
                        window_counts = counts[i - window : i]
                        mean = statistics.mean(window_counts)
                        try:
                            stddev = statistics.stdev(window_counts)
                        except statistics.StatisticsError:
                            stddev = 0
                        threshold = mean + 2 * stddev
                        if counts[i] > threshold and threshold > 0:
                            anomaly_days.append({
                                "date": series[i]["date"],
                                "count": counts[i],
                                "expected_upper": round(threshold, 1),
                            })

                severity = "ok"
                if gap_days:
                    severity = "critical"
                elif anomaly_days:
                    severity = "warn"

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="gap_day_count",
                        value=len(gap_days),
                        severity=severity,
                        detail={
                            "min_date": min_date,
                            "max_date": max_date,
                            "total": total,
                            "series": series,
                            "gap_days": gap_days,
                            "anomaly_days": anomaly_days,
                            "date_column": col_name,
                        },
                        sql=range_sql,
                    )
                )
            except Exception as exc:
                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="gap_day_count",
                        value="error",
                        severity="warn",
                        detail={"error": str(exc)},
                        sql=range_sql,
                    )
                )

        return results
