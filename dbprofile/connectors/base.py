"""Database connector abstraction.

Each dialect subclass handles:
  - Connection setup
  - information_schema queries (column discovery)
  - Dialect-specific SQL rendering (sampling, percentiles, regex, date_trunc, gap detection)
  - Query execution returning list[dict]

Factory function `get_connector(config)` returns the right subclass.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dbprofile.config import ProfileConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseConnector(ABC):
    """Common interface every dialect connector must implement."""

    dialect: str  # set in each subclass
    sample_method: str = "bernoulli"  # set by get_connector() from config.checks.sample_method

    @abstractmethod
    def execute(self, sql: str) -> list[dict[str, Any]]:
        """Run a SQL string and return rows as a list of dicts."""

    @abstractmethod
    def get_columns(self, table: str, schema: str) -> list[dict[str, Any]]:
        """Return column metadata for a table.

        Each dict must have at minimum:
          name, data_type, is_nullable (bool), ordinal_position (int)
        """

    @abstractmethod
    def get_tables(self, schema: str) -> list[str]:
        """Return all table names in the given schema/dataset."""

    # ------------------------------------------------------------------
    # Dialect SQL helpers — subclasses override as needed
    # ------------------------------------------------------------------

    def qualified_table(self, table: str, schema: str, project: str | None = None) -> str:
        """Return the fully-qualified table reference for use in SQL."""
        return f"{schema}.{table}"

    def sample_clause(self, sample_rate: float) -> str:
        """Return a TABLESAMPLE clause, or empty string for sample_rate >= 1.0.

        Method controlled by self.sample_method (set from config.checks.sample_method):
          bernoulli — row-level probability sampling (default)
          system    — block-level sampling (faster on large tables)
        """
        if sample_rate >= 1.0:
            return ""
        pct = sample_rate * 100
        if self.sample_method == "system":
            return f"TABLESAMPLE SYSTEM ({pct:.2f} PERCENT)"
        return f"TABLESAMPLE BERNOULLI ({pct:.2f} PERCENT)"

    def date_trunc_day(self, col: str) -> str:
        """Truncate a timestamp column to day precision."""
        return f"DATE_TRUNC('day', {col})"

    def percentile_sql(self, col: str, table_ref: str, percentiles: list[float]) -> str:
        """Return a SELECT that yields one column per percentile as p25, p50, etc."""
        select_parts = [
            f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col}) AS p{int(p * 100)}"
            for p in percentiles
        ]
        return f"SELECT {', '.join(select_parts)} FROM {table_ref}"

    def regex_match(self, col: str, pattern: str) -> str:
        """Return a SQL expression that is TRUE when col matches pattern."""
        # Default: Postgres/DuckDB syntax
        return f"{col} ~ '{pattern}'"

    def regex_not_match(self, col: str, pattern: str) -> str:
        """Return a SQL expression that is TRUE when col does NOT match pattern."""
        return f"NOT ({self.regex_match(col, pattern)})"

    def generate_date_spine(self, start: str, end: str, date_col: str, table_ref: str) -> str:
        """Return SQL producing (date, count) rows including zero-count gap days."""
        # Default: Postgres / DuckDB generate_series
        return f"""
WITH date_spine AS (
  SELECT d::date AS d
  FROM generate_series('{start}'::date, '{end}'::date, INTERVAL '1 day') AS gs(d)
),
daily AS (
  SELECT {self.date_trunc_day(date_col)}::date AS d, COUNT(*) AS n
  FROM {table_ref}
  GROUP BY 1
)
SELECT ds.d, COALESCE(daily.n, 0) AS n
FROM date_spine ds
LEFT JOIN daily ON ds.d = daily.d
ORDER BY 1
"""

    def get_schemas(self) -> list[str]:
        """Return all user-visible schema names. Override in connectors that support it."""
        return ["public"]

    def close(self) -> None:
        """Release any resources held by the connector."""


# ---------------------------------------------------------------------------
# DuckDB connector (used for tests and local profiling)
# ---------------------------------------------------------------------------

class DuckDBConnector(BaseConnector):
    dialect = "duckdb"

    def __init__(self, conn=None, database_path: str | None = None):
        """Accept an existing duckdb connection (for tests), a file path,
        or create an in-memory one."""
        import duckdb

        if conn is not None:
            self._conn = conn
        elif database_path:
            self._conn = duckdb.connect(database_path)
        else:
            self._conn = duckdb.connect(":memory:")

    def execute(self, sql: str) -> list[dict[str, Any]]:
        result = self._conn.execute(sql)
        cols = [desc[0] for desc in result.description]
        return [dict(zip(cols, row)) for row in result.fetchall()]

    def get_columns(self, table: str, schema: str) -> list[dict[str, Any]]:
        sql = f"""
SELECT column_name AS name,
       data_type,
       is_nullable = 'YES' AS is_nullable,
       ordinal_position
FROM information_schema.columns
WHERE table_name = '{table}'
  AND table_schema = '{schema}'
ORDER BY ordinal_position
"""
        return self.execute(sql)

    def get_tables(self, schema: str) -> list[str]:
        rows = self.execute(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'"
        )
        return [r["table_name"] for r in rows]

    def sample_clause(self, sample_rate: float) -> str:
        if sample_rate >= 1.0:
            return ""
        pct = sample_rate * 100
        method = "SYSTEM" if self.sample_method == "system" else "BERNOULLI"
        return f"USING SAMPLE {pct:.2f} PERCENT ({method})"

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# BigQuery connector (primary production dialect)
# ---------------------------------------------------------------------------

class BigQueryConnector(BaseConnector):
    dialect = "bigquery"

    def __init__(self, project: str, dataset: str, credentials_path: str | None = None,
                 source_project: str | None = None):
        from google.auth import default as google_auth_default
        from google.cloud import bigquery
        from google.oauth2 import service_account

        # billing project (your GCP project)
        self.project = project
        # data project (e.g. bigquery-public-data)
        self.source_project = source_project or project
        self.dataset = dataset

        creds_file = credentials_path
        if creds_file and Path(creds_file).exists():
            credentials = service_account.Credentials.from_service_account_file(
                creds_file,
                scopes=["https://www.googleapis.com/auth/bigquery"],
            )
            logger.info("BigQuery: using service account key file")
        else:
            credentials, _ = google_auth_default(
                scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            logger.info("BigQuery: using Application Default Credentials")

        self._client = bigquery.Client(project=project, credentials=credentials)
        self._total_bytes: int = 0

    # -- query execution --------------------------------------------------

    def execute(self, sql: str, dry_run: bool = False) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        job_config = bigquery.QueryJobConfig(dry_run=dry_run)
        job = self._client.query(sql, job_config=job_config)

        if dry_run:
            gb = (job.total_bytes_processed or 0) / 1e9
            cost = gb * 6.25 / 1000  # $6.25 per TB
            logger.info(f"  DRY RUN: {gb:.3f} GB estimated (${cost:.4f})")
            return []

        results = job.result()
        bytes_proc = job.total_bytes_processed or 0
        self._total_bytes += bytes_proc
        gb = bytes_proc / 1e9
        logger.debug(f"  {gb:.3f} GB scanned")

        return [dict(row) for row in results]

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    @property
    def total_cost_usd(self) -> float:
        return (self._total_bytes / 1e12) * 6.25

    # -- column / table discovery -----------------------------------------

    def get_columns(self, table: str, schema: str) -> list[dict[str, Any]]:
        sql = f"""
SELECT column_name AS name,
       data_type,
       is_nullable = 'YES' AS is_nullable,
       ordinal_position
FROM `{self.source_project}.{schema}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '{table}'
ORDER BY ordinal_position
"""
        return self.execute(sql)

    def get_tables(self, schema: str) -> list[str]:
        rows = self.execute(
            f"SELECT table_name FROM `{self.source_project}.{schema}`.INFORMATION_SCHEMA.TABLES "
            f"WHERE table_type = 'BASE TABLE'"
        )
        return [r["table_name"] for r in rows]

    # -- dialect overrides ------------------------------------------------

    def qualified_table(self, table: str, schema: str, project: str | None = None) -> str:
        proj = project or self.project
        return f"`{proj}.{schema}.{table}`"

    def sample_clause(self, sample_rate: float) -> str:
        if sample_rate >= 1.0:
            return ""
        pct = sample_rate * 100
        return f"TABLESAMPLE SYSTEM ({pct:.2f} PERCENT)"

    def date_trunc_day(self, col: str) -> str:
        return f"DATE_TRUNC({col}, DAY)"

    def percentile_sql(self, col: str, table_ref: str, percentiles: list[float]) -> str:
        # BigQuery percentile_cont requires window function form.
        # LIMIT must be on the outer query — BigQuery disallows LIMIT inside
        # a subquery that contains window functions.
        select_parts = [
            f"PERCENTILE_CONT({col}, {p}) OVER () AS p{int(p * 100)}"
            for p in percentiles
        ]
        inner = f"SELECT {', '.join(select_parts)} FROM {table_ref}"
        return f"SELECT * FROM ({inner}) LIMIT 1"

    def regex_match(self, col: str, pattern: str) -> str:
        escaped = pattern.replace("'", "\\'")
        return f"REGEXP_CONTAINS({col}, r'{escaped}')"

    def regex_not_match(self, col: str, pattern: str) -> str:
        return f"NOT {self.regex_match(col, pattern)}"

    def generate_date_spine(self, start: str, end: str, date_col: str, table_ref: str) -> str:
        return f"""
WITH date_spine AS (
  SELECT d
  FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)
  ) AS d
),
daily AS (
  SELECT DATE({date_col}) AS d, COUNT(*) AS n
  FROM {table_ref}
  GROUP BY 1
)
SELECT ds.d, COALESCE(daily.n, 0) AS n
FROM date_spine ds
LEFT JOIN daily ON ds.d = daily.d
ORDER BY 1
"""

    def close(self) -> None:
        self._client.close()


# ---------------------------------------------------------------------------
# Snowflake connector
# ---------------------------------------------------------------------------

class SnowflakeConnector(BaseConnector):
    dialect = "snowflake"

    def __init__(
        self,
        account: str,
        user: str,
        database: str,
        warehouse: str | None = None,
        role: str | None = None,
        password: str | None = None,
        private_key_path: str | None = None,
        private_key_passphrase: str | None = None,
    ):
        import snowflake.connector

        self._database = database
        self._warehouse = warehouse

        connect_kwargs: dict = {
            "account": account,
            "user": user,
            "database": database,
        }
        if warehouse:
            connect_kwargs["warehouse"] = warehouse
        if role:
            connect_kwargs["role"] = role

        if private_key_path:
            connect_kwargs["private_key"] = self._load_private_key(
                private_key_path, private_key_passphrase
            )
        elif password:
            connect_kwargs["password"] = password

        self._conn = snowflake.connector.connect(**connect_kwargs)
        logger.info(f"Snowflake: connected to {account}/{database}")

    @staticmethod
    def _load_private_key(path: str, passphrase: str | None) -> bytes:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        with open(path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=passphrase.encode() if passphrase else None,
                backend=default_backend(),
            )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def execute(self, sql: str) -> list[dict[str, Any]]:
        import snowflake.connector
        cursor = self._conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(sql)
        # Snowflake returns uppercase keys — normalize to lowercase for consistency
        return [{k.lower(): v for k, v in row.items()} for row in cursor.fetchall()]

    def get_schemas(self) -> list[str]:
        rows = self.execute(
            f"SELECT schema_name FROM {self._database}.INFORMATION_SCHEMA.SCHEMATA "
            f"WHERE schema_name != 'INFORMATION_SCHEMA' ORDER BY schema_name"
        )
        return [r["schema_name"] for r in rows]

    def get_columns(self, table: str, schema: str) -> list[dict[str, Any]]:
        sql = f"""
SELECT column_name AS name,
       data_type,
       (is_nullable = 'YES') AS is_nullable,
       ordinal_position
FROM {self._database}.INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = '{schema.upper()}'
  AND table_name   = '{table.upper()}'
ORDER BY ordinal_position
"""
        return self.execute(sql)

    def get_tables(self, schema: str) -> list[str]:
        rows = self.execute(
            f"SELECT table_name FROM {self._database}.INFORMATION_SCHEMA.TABLES "
            f"WHERE table_schema = '{schema.upper()}' AND table_type = 'BASE TABLE'"
        )
        return [r["table_name"] for r in rows]

    # -- dialect overrides ------------------------------------------------

    def qualified_table(self, table: str, schema: str, project: str | None = None) -> str:
        return f"{self._database}.{schema}.{table}"

    def sample_clause(self, sample_rate: float) -> str:
        if sample_rate >= 1.0:
            return ""
        pct = sample_rate * 100
        if self.sample_method == "system":
            return f"SAMPLE SYSTEM ({pct:.2f})"
        return f"SAMPLE BERNOULLI ({pct:.2f})"

    def date_trunc_day(self, col: str) -> str:
        return f"DATE_TRUNC('DAY', {col})"

    def percentile_sql(self, col: str, table_ref: str, percentiles: list[float]) -> str:
        # Snowflake uses standard ANSI PERCENTILE_CONT syntax
        select_parts = [
            f"PERCENTILE_CONT({p}) WITHIN GROUP (ORDER BY {col}) AS p{int(p * 100)}"
            for p in percentiles
        ]
        return f"SELECT {', '.join(select_parts)} FROM {table_ref}"

    def regex_match(self, col: str, pattern: str) -> str:
        escaped = pattern.replace("'", "\\'")
        return f"RLIKE({col}, '{escaped}')"

    def regex_not_match(self, col: str, pattern: str) -> str:
        return f"NOT {self.regex_match(col, pattern)}"

    def generate_date_spine(self, start: str, end: str, date_col: str, table_ref: str) -> str:
        # Snowflake has no generate_series — use GENERATOR with SEQ4()
        return f"""
WITH date_spine AS (
  SELECT DATEADD(DAY, SEQ4(), '{start}'::DATE) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => DATEDIFF('day', '{start}'::DATE, '{end}'::DATE) + 1))
),
daily AS (
  SELECT DATE_TRUNC('DAY', {date_col})::DATE AS d, COUNT(*) AS n
  FROM {table_ref}
  GROUP BY 1
)
SELECT ds.d, COALESCE(daily.n, 0) AS n
FROM date_spine ds
LEFT JOIN daily ON ds.d = daily.d
ORDER BY 1
"""

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_connector(config: "ProfileConfig") -> BaseConnector:
    """Return the appropriate connector for the configured dialect."""
    dialect = config.connection.dialect.lower()

    if dialect == "duckdb":
        connector: BaseConnector = DuckDBConnector(database_path=config.connection.database_path)

    elif dialect == "bigquery":
        connector = BigQueryConnector(
            project=config.connection.project,
            dataset=config.scope.dataset,
            credentials_path=config.connection.credentials_path,
            source_project=config.scope.project,
        )

    elif dialect == "snowflake":
        connector = SnowflakeConnector(
            account=config.connection.account,
            user=config.connection.user,
            database=config.scope.database,
            warehouse=config.connection.warehouse,
            role=config.connection.role,
            password=config.connection.password,
            private_key_path=config.connection.private_key_path,
            private_key_passphrase=config.connection.private_key_passphrase,
        )

    else:
        raise ValueError(
            f"Unsupported dialect: '{dialect}'. "
            "Supported dialects: bigquery, duckdb, snowflake"
        )

    # Plumb sampling method from config into connector
    connector.sample_method = config.checks.sample_method
    return connector
