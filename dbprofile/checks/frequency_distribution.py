"""Check 6 — Frequency distribution & cardinality (low-to-medium cardinality columns)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


class FrequencyDistributionCheck(BaseCheck):
    name = "frequency_distribution"

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
        cardinality_limit = config.report.thresholds.frequency_cardinality_limit

        for col in columns:
            col_name = col["name"]

            # First: get distinct count to decide whether to run full freq check
            distinct_sql = f"SELECT COUNT(DISTINCT {col_name}) AS n FROM {table_ref} {sample}".strip()

            try:
                d_rows = connector.execute(distinct_sql)
                distinct_count = int(d_rows[0]["n"]) if d_rows else 0

                if distinct_count > cardinality_limit:
                    # High cardinality — skip this check, note it as info
                    results.append(
                        CheckResult(
                            table=table,
                            schema=schema,
                            column=col_name,
                            check_name=self.name,
                            metric="distinct_count",
                            value=distinct_count,
                            severity="info",
                            detail={
                                "distinct_count": distinct_count,
                                "skipped": True,
                                "reason": f"distinct_count ({distinct_count}) > cardinality_limit ({cardinality_limit})",
                            },
                            sql=distinct_sql,
                        )
                    )
                    continue

                # Full frequency distribution
                freq_sql = f"""
SELECT
  CAST({col_name} AS VARCHAR) AS value,
  COUNT(*) AS freq,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM {table_ref} {sample}
GROUP BY {col_name}
ORDER BY freq DESC
LIMIT 30
""".strip()

                freq_rows = connector.execute(freq_sql)
                if not freq_rows:
                    continue

                series = [
                    {
                        "value": str(r.get("value", "")),
                        "freq": int(r.get("freq") or 0),
                        "pct": float(r.get("pct") or 0.0),
                    }
                    for r in freq_rows
                ]

                # Flag if the top value accounts for > 90% of rows
                top_pct = series[0]["pct"] if series else 0.0
                severity = "warn" if top_pct > 90.0 else "ok"

                # Cumulative pct for top-30
                cum = 0.0
                for item in series:
                    cum += item["pct"]
                    item["cumulative_pct"] = round(cum, 2)

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="top_value_pct",
                        value=round(top_pct, 4),
                        severity=severity,
                        detail={
                            "distinct_count": distinct_count,
                            "top_value_pct": round(top_pct, 4),
                            "series": series,
                        },
                        sql=freq_sql,
                    )
                )
            except Exception as exc:
                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="top_value_pct",
                        value="error",
                        severity="warn",
                        detail={"error": str(exc)},
                        sql=distinct_sql,
                    )
                )

        return results
