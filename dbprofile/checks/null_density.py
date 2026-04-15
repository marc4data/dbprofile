"""Check 3 — Null density & completeness (per column)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector

# Sentinel values that are "logically null" even if not SQL NULL
_STRING_SENTINELS = ("''", "'N/A'", "'n/a'", "'NULL'", "'none'", "'None'", "'NONE'")
_DATE_SENTINEL = "'9999-12-31'"


class NullDensityCheck(BaseCheck):
    name = "null_density"

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

        for col in columns:
            col_name = col["name"]
            dt = col["data_type"]

            # Build sentinel checks based on data type
            sentinel_cases = []
            if self.is_string(dt):
                for s in _STRING_SENTINELS:
                    sentinel_cases.append(f"SUM(CASE WHEN {col_name} = {s} THEN 1 ELSE 0 END)")
            if self.is_temporal(dt):
                sentinel_cases.append(
                    f"SUM(CASE WHEN CAST({col_name} AS VARCHAR) = {_DATE_SENTINEL} THEN 1 ELSE 0 END)"
                )

            sentinel_select = (
                ", " + ", ".join(sentinel_cases) + " AS sentinel_count"
                if sentinel_cases
                else ", 0 AS sentinel_count"
            )
            if len(sentinel_cases) > 1:
                # Combine multiple sentinel checks into one sum
                combined = " + ".join(f"({s})" for s in sentinel_cases)
                sentinel_select = f", ({combined}) AS sentinel_count"

            sql = (
                f"SELECT "
                f"  COUNT(*) AS total, "
                f"  SUM(CASE WHEN {col_name} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"  ROUND(100.0 * AVG(CASE WHEN {col_name} IS NULL THEN 1.0 ELSE 0.0 END), 4) AS null_pct"
                f"  {sentinel_select} "
                f"FROM {table_ref} {sample}"
            ).strip()

            try:
                rows = connector.execute(sql)
                if not rows:
                    continue
                row = rows[0]

                total = int(row.get("total") or 0)
                null_count = int(row.get("null_count") or 0)
                null_pct = float(row.get("null_pct") or 0.0)
                sentinel_count = int(row.get("sentinel_count") or 0)

                severity = self.severity_from_pct(
                    null_pct, thresholds.null_pct_warn, thresholds.null_pct_critical
                )

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="null_pct",
                        value=round(null_pct, 4),
                        severity=severity,
                        detail={
                            "total": total,
                            "null_count": null_count,
                            "null_pct": round(null_pct, 4),
                            "sentinel_count": sentinel_count,
                        },
                        sql=sql,
                    )
                )
            except Exception as exc:
                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="null_pct",
                        value="error",
                        severity="warn",
                        detail={"error": str(exc)},
                        sql=sql,
                    )
                )

        return results
