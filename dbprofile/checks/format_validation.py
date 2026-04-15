"""Check 8 — Format & domain validation (varchar/text columns)."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


# Maps column name pattern -> (label, regex pattern)
# Patterns are applied when the column name contains the key substring.
_FORMAT_RULES: list[tuple[str, str, str]] = [
    ("email",    "email",    r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
    ("phone",    "phone",    r"^[\d\s\(\)\-\+\.]{7,15}$"),
    ("zip",      "zip_code", r"^\d{5}(-\d{4})?$"),
    ("postal",   "zip_code", r"^\d{5}(-\d{4})?$"),
    ("url",      "url",      r"^https?://"),
    ("website",  "url",      r"^https?://"),
    ("uuid",     "uuid",     r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"),
    ("guid",     "uuid",     r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"),
    ("country",  "country",  r"^[A-Z]{2}$"),
    ("currency", "currency", r"^[A-Z]{3}$"),
    ("iso_curr", "currency", r"^[A-Z]{3}$"),
]

# Columns whose names contain these substrings get a cardinality check instead
_ENUM_PATTERNS = ("status", "type", "category", "state", "flag", "indicator")
_ENUM_CARDINALITY_WARN = 50


class FormatValidationCheck(BaseCheck):
    name = "format_validation"

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

        string_cols = [c for c in columns if self.is_string(c["data_type"])]

        for col in string_cols:
            col_name = col["name"]
            col_lower = col_name.lower()

            # --- regex format checks ---
            matched_rule = None
            for key, label, pattern in _FORMAT_RULES:
                if key in col_lower:
                    matched_rule = (label, pattern)
                    break

            if matched_rule:
                label, pattern = matched_rule
                not_match_expr = connector.regex_not_match(col_name, pattern)
                sql = f"""
SELECT
  COUNT(*) AS total_non_null,
  SUM(CASE WHEN {not_match_expr} THEN 1 ELSE 0 END) AS violations
FROM {table_ref} {sample}
WHERE {col_name} IS NOT NULL
""".strip()

                try:
                    rows = connector.execute(sql)
                    if not rows:
                        continue
                    row = rows[0]
                    total = int(row.get("total_non_null") or 0)
                    violations = int(row.get("violations") or 0)
                    violation_pct = round(100.0 * violations / total, 4) if total else 0.0

                    severity = "ok"
                    if violation_pct > 1.0:
                        severity = "critical"
                    elif violation_pct > 0.1:
                        severity = "warn"

                    results.append(
                        CheckResult(
                            table=table,
                            schema=schema,
                            column=col_name,
                            check_name=self.name,
                            metric="violation_pct",
                            value=violation_pct,
                            severity=severity,
                            detail={
                                "format_label": label,
                                "pattern": pattern,
                                "total_non_null": total,
                                "violations": violations,
                                "violation_pct": violation_pct,
                            },
                            sql=sql,
                        )
                    )
                    continue  # don't also run enum check for the same column
                except Exception as exc:
                    results.append(
                        CheckResult(
                            table=table,
                            schema=schema,
                            column=col_name,
                            check_name=self.name,
                            metric="violation_pct",
                            value="error",
                            severity="warn",
                            detail={"error": str(exc), "format_label": label},
                            sql=sql,
                        )
                    )
                    continue

            # --- enum cardinality check ---
            if any(pat in col_lower for pat in _ENUM_PATTERNS):
                distinct_sql = f"SELECT COUNT(DISTINCT {col_name}) AS n FROM {table_ref} {sample}".strip()
                try:
                    d_rows = connector.execute(distinct_sql)
                    distinct_count = int(d_rows[0]["n"]) if d_rows else 0
                    severity = "warn" if distinct_count > _ENUM_CARDINALITY_WARN else "ok"
                    results.append(
                        CheckResult(
                            table=table,
                            schema=schema,
                            column=col_name,
                            check_name=self.name,
                            metric="enum_cardinality",
                            value=distinct_count,
                            severity=severity,
                            detail={
                                "distinct_count": distinct_count,
                                "warn_threshold": _ENUM_CARDINALITY_WARN,
                                "note": "High cardinality on enum-like column — possible free-text leak",
                            },
                            sql=distinct_sql,
                        )
                    )
                except Exception as exc:
                    results.append(
                        CheckResult(
                            table=table,
                            schema=schema,
                            column=col_name,
                            check_name=self.name,
                            metric="enum_cardinality",
                            value="error",
                            severity="warn",
                            detail={"error": str(exc)},
                            sql=distinct_sql,
                        )
                    )

        return results
