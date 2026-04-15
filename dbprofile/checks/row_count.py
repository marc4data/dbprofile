"""Check 2 — Row count & partition skew (table-level)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


class RowCountCheck(BaseCheck):
    name = "row_count"

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

        # --- total row count ---
        count_sql = f"SELECT COUNT(*) AS n FROM {table_ref} {sample}".strip()
        rows = connector.execute(count_sql)
        total = int(rows[0]["n"]) if rows else 0

        results.append(
            CheckResult(
                table=table,
                schema=schema,
                column=None,
                check_name=self.name,
                metric="row_count",
                value=total,
                severity="info",
                detail={"row_count": total},
                sql=count_sql,
            )
        )

        # --- daily time series (if a temporal column exists) ---
        temporal_cols = [c for c in columns if self.is_temporal(c["data_type"])]
        if temporal_cols and total > 0:
            date_col = temporal_cols[0]["name"]  # use the first temporal column found
            trunc = connector.date_trunc_day(date_col)
            ts_sql = (
                f"SELECT {trunc} AS d, COUNT(*) AS n "
                f"FROM {table_ref} {sample} "
                f"GROUP BY 1 ORDER BY 1"
            ).strip()

            try:
                ts_rows = connector.execute(ts_sql)
                series = [
                    {"date": str(r["d"]), "count": int(r["n"])}
                    for r in ts_rows
                    if r["d"] is not None
                ]

                # Flag days with zero rows as gaps
                gaps = [s for s in series if s["count"] == 0]

                # Flag skew: any single day > threshold % of total
                skew_threshold = config.report.thresholds.skew_day_pct
                skew_days = [
                    s for s in series
                    if total > 0 and (s["count"] / total * 100) > skew_threshold
                ]

                severity = "ok"
                if skew_days:
                    severity = "warn"

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=date_col,
                        check_name=self.name,
                        metric="daily_distribution",
                        value=len(series),
                        severity=severity,
                        detail={
                            "series": series,
                            "date_column": date_col,
                            "gap_days": gaps,
                            "skew_days": skew_days,
                        },
                        sql=ts_sql,
                    )
                )
            except Exception:
                # If daily grouping fails (e.g., type mismatch), skip it gracefully
                pass

        return results
