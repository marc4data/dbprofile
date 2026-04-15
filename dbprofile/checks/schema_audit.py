"""Check 1 — Schema & metadata audit (table-level, informational)."""

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
        # columns already fetched by orchestrator — just report them
        column_list = [
            {
                "name": col["name"],
                "data_type": col["data_type"],
                "is_nullable": col.get("is_nullable", True),
                "ordinal_position": col.get("ordinal_position", 0),
            }
            for col in columns
        ]

        # Check for columns expected by config but missing from the table
        override = config.scope.column_overrides.get(table)
        expected = override.include if override and override.include else []
        actual_names = {col["name"] for col in columns}
        missing = [c for c in expected if c not in actual_names]

        return [
            CheckResult(
                table=table,
                schema=schema,
                column=None,
                check_name=self.name,
                metric="column_count",
                value=len(columns),
                severity="info",
                detail={
                    "columns": column_list,
                    "missing_from_contract": missing,
                },
                sql="(information_schema.columns — fetched by orchestrator)",
            )
        ]
