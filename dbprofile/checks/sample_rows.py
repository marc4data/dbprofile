"""Sample rows — fetch up to 100 rows from the table for inline preview."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dbprofile.checks.base import BaseCheck, CheckResult

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig
    from dbprofile.connectors.base import BaseConnector


class SampleRowsCheck(BaseCheck):
    name = "sample_rows"

    def run(
        self,
        table: str,
        schema: str,
        columns: list[dict[str, Any]],
        connector: "BaseConnector",
        config: "ProfileConfig",
    ) -> list[CheckResult]:
        table_ref = connector.qualified_table(table, schema, config.scope.project)
        sample = connector.sample_clause(config.checks.sample_rate)

        sql = f"SELECT * FROM {table_ref} {sample} LIMIT 100"

        try:
            rows = connector.execute(sql)
            if not rows:
                return []

            col_names = list(rows[0].keys())
            row_data = [
                [str(row.get(c)) if row.get(c) is not None else None for c in col_names]
                for row in rows
            ]

            return [CheckResult(
                table=table,
                schema=schema,
                column=None,
                check_name=self.name,
                metric="sample_rows",
                value=len(rows),
                severity="info",
                detail={
                    "columns": col_names,
                    "rows": row_data,
                },
                sql=sql,
            )]
        except Exception as exc:
            return [CheckResult(
                table=table,
                schema=schema,
                column=None,
                check_name=self.name,
                metric="sample_rows",
                value="error",
                severity="info",
                detail={"error": str(exc)},
                sql=sql,
            )]
