"""Check — Schema & metadata audit (table-level).

Static checks that run against the schema and a lightweight scan:
  - Column inventory (count, types, nullable flags)
  - All-null columns: columns that exist in the schema but contain zero non-null values
  - Missing contract columns: columns declared in config column_overrides but absent from the table
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


class SchemaAuditCheck(BaseCheck):
    name = "schema_audit"

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

        column_list = [
            {
                "name": col["name"],
                "data_type": col["data_type"],
                "is_nullable": col.get("is_nullable", True),
                "ordinal_position": col.get("ordinal_position", 0),
            }
            for col in columns
        ]

        # Missing contract columns
        override = config.scope.column_overrides.get(table)
        expected = override.include if override and override.include else []
        actual_names = {col["name"] for col in columns}
        missing = [c for c in expected if c not in actual_names]

        # Schema overview — always produced
        results.append(
            CheckResult(
                table=table,
                schema=schema,
                column=None,
                check_name=self.name,
                metric="column_count",
                value=len(columns),
                severity="critical" if missing else "ok",
                detail={
                    "columns": column_list,
                    "missing_from_contract": missing,
                },
                sql="(information_schema.columns — fetched by orchestrator)",
            )
        )

        # All-null column detection: one query, COUNT(col) per column.
        # COUNT(col) excludes NULLs; if result is 0 the column is entirely null.
        if columns:
            col_exprs = ", ".join(
                f"COUNT({col['name']}) AS col_{i}"
                for i, col in enumerate(columns)
            )
            scan_sql = f"SELECT COUNT(*) AS _total_, {col_exprs} FROM {table_ref} {sample}".strip()

            try:
                rows = connector.execute(scan_sql)
                if rows:
                    row = rows[0]
                    total = int(row.get("_total_") or 0)
                    if total > 0:
                        for i, col in enumerate(columns):
                            not_null_count = int(row.get(f"col_{i}") or 0)
                            if not_null_count == 0:
                                results.append(
                                    CheckResult(
                                        table=table,
                                        schema=schema,
                                        column=col["name"],
                                        check_name=self.name,
                                        metric="all_null_column",
                                        value=0,
                                        severity="warn",
                                        detail={
                                            "data_type": col["data_type"],
                                            "is_nullable": col.get("is_nullable", True),
                                            "rows_scanned": total,
                                            "non_null_count": 0,
                                        },
                                        sql=scan_sql,
                                    )
                                )
            except Exception:
                # Skip gracefully — some dialects may reject very wide SELECT lists
                pass

        return results
