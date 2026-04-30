"""Check 4 — Uniqueness & duplicate detection (per column + table-level)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


class UniquenessCheck(BaseCheck):
    name = "uniqueness"

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

        # --- per-column distinct / duplicate check ---
        for col in columns:
            col_name = col["name"]

            sql = f"""
SELECT
  COUNT(*) AS total,
  COUNT(DISTINCT {col_name}) AS distinct_count,
  COUNT(*) - COUNT(DISTINCT {col_name}) AS duplicate_count,
  ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT {col_name})) / NULLIF(COUNT(*), 0), 4) AS duplicate_pct
FROM {table_ref} {sample}
""".strip()

            try:
                rows = connector.execute(sql)
                if not rows:
                    continue
                row = rows[0]

                total = int(row.get("total") or 0)
                distinct_count = int(row.get("distinct_count") or 0)
                duplicate_count = int(row.get("duplicate_count") or 0)
                duplicate_pct = float(row.get("duplicate_pct") or 0.0)
                distinct_pct = round(100.0 * distinct_count / total, 4) if total else 0.0

                severity = self.severity_from_pct(
                    duplicate_pct,
                    thresholds.duplicate_pct_warn,
                    thresholds.duplicate_pct_critical,
                )

                # Non-identifier columns (distinct% < 95%) have repeated values by design.
                # The template already groups these separately and labels them "expected".
                # Match that logic here: demote warn/critical → info so the heatmap agrees.
                # Identifier-like columns (distinct% ≥ 95%) still raise true warnings.
                is_identifier = total > 0 and (distinct_count / total) >= 0.95
                if not is_identifier and severity in ("warn", "critical"):
                    severity = "info"

                # Top duplicate values (most repeated)
                top_sql = f"""
SELECT {col_name} AS value, COUNT(*) AS n
FROM {table_ref} {sample}
GROUP BY {col_name}
HAVING COUNT(*) > 1
ORDER BY n DESC
LIMIT 20
""".strip()

                try:
                    top_rows = connector.execute(top_sql)
                    top_dupes = [
                        {"value": str(r["value"]), "count": int(r["n"])}
                        for r in top_rows
                    ]
                except Exception:
                    top_dupes = []

                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=col_name,
                        check_name=self.name,
                        metric="duplicate_pct",
                        value=round(duplicate_pct, 4),
                        severity=severity,
                        detail={
                            "total": total,
                            "distinct_count": distinct_count,
                            "distinct_pct": distinct_pct,
                            "duplicate_count": duplicate_count,
                            "duplicate_pct": round(duplicate_pct, 4),
                            "top_duplicates": top_dupes,
                            "is_identifier": is_identifier,
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
                        metric="duplicate_pct",
                        value="error",
                        severity="warn",
                        detail={"error": str(exc)},
                        sql=sql,
                    )
                )

        # --- table-level: full-row duplicate count ---
        col_names = ", ".join(c["name"] for c in columns)
        row_dup_sql = f"""
SELECT COUNT(*) AS total_rows,
       SUM(cnt) - SUM(1) AS duplicate_rows
FROM (
  SELECT {col_names}, COUNT(*) AS cnt
  FROM {table_ref} {sample}
  GROUP BY {col_names}
  HAVING COUNT(*) > 1
) t
""".strip()

        try:
            rows = connector.execute(row_dup_sql)
            if rows and rows[0].get("duplicate_rows") is not None:
                dup_rows = int(rows[0]["duplicate_rows"] or 0)
                results.append(
                    CheckResult(
                        table=table,
                        schema=schema,
                        column=None,
                        check_name=self.name,
                        metric="duplicate_row_count",
                        value=dup_rows,
                        severity="warn" if dup_rows > 0 else "ok",
                        detail={"duplicate_row_count": dup_rows},
                        sql=row_dup_sql,
                    )
                )
        except Exception:
            # Wide tables with non-groupable types may fail — skip gracefully
            pass

        return results
