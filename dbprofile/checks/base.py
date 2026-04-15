"""Base types shared by all checks: CheckResult dataclass and BaseCheck ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dbprofile.config import CheckThresholds, ProfileConfig
    from dbprofile.connectors.base import BaseConnector


@dataclass
class CheckResult:
    """A single metric produced by one check against one column (or table)."""

    table: str
    schema: str                        # dataset/schema name
    column: str | None                 # None for table-level checks
    check_name: str                    # e.g. "null_density"
    metric: str                        # e.g. "null_pct"
    value: float | int | str           # the measured value
    severity: str                      # "ok" | "warn" | "critical" | "info"
    detail: dict[str, Any] = field(default_factory=dict)  # raw data for charts/tables
    sql: str = ""                      # the query that was run
    run_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "schema": self.schema,
            "column": self.column,
            "check_name": self.check_name,
            "metric": self.metric,
            "value": self.value,
            "severity": self.severity,
            "detail": self.detail,
            "sql": self.sql,
            "run_at": self.run_at.isoformat(),
        }


class BaseCheck(ABC):
    """Abstract base class every profiling check must implement."""

    name: str  # override in each subclass — used as the check identifier

    @abstractmethod
    def run(
        self,
        table: str,
        schema: str,
        columns: list[dict[str, Any]],
        connector: "BaseConnector",
        config: "ProfileConfig",
    ) -> list[CheckResult]:
        """Execute the check and return a list of CheckResult objects.

        Args:
            table:     Table name (unqualified).
            schema:    Schema/dataset name.
            columns:   List of column dicts with keys: name, data_type, is_nullable.
            connector: Live database connector (handles query execution + dialect SQL).
            config:    Full run config (thresholds, sample_rate, etc.).

        Returns:
            One or more CheckResult objects. Table-level checks return one result
            with column=None; column-level checks return one result per column.
        """

    # ------------------------------------------------------------------
    # Shared severity helpers
    # ------------------------------------------------------------------

    @staticmethod
    def severity_from_pct(
        value: float,
        warn: float,
        critical: float,
    ) -> str:
        """Map a percentage value to a severity level."""
        if value >= critical:
            return "critical"
        if value >= warn:
            return "warn"
        return "ok"

    @staticmethod
    def is_numeric(data_type: str) -> bool:
        numeric_keywords = (
            "int", "float", "double", "decimal", "numeric",
            "real", "number", "bigint", "smallint", "tinyint",
            "byteint", "money", "float64", "int64",
        )
        dt = data_type.lower()
        return any(kw in dt for kw in numeric_keywords)

    @staticmethod
    def is_string(data_type: str) -> bool:
        string_keywords = ("char", "text", "string", "varchar", "nvarchar", "clob")
        dt = data_type.lower()
        return any(kw in dt for kw in string_keywords)

    @staticmethod
    def is_temporal(data_type: str) -> bool:
        temporal_keywords = ("date", "time", "timestamp", "datetime")
        dt = data_type.lower()
        return any(kw in dt for kw in temporal_keywords)
